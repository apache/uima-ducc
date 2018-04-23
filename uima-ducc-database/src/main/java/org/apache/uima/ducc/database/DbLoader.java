/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
*/
package org.apache.uima.ducc.database;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.uima.ducc.common.Pair;
import org.apache.uima.ducc.common.persistence.services.IStateServices;
import org.apache.uima.ducc.common.persistence.services.IStateServices.SvcMetaProps;
import org.apache.uima.ducc.common.persistence.services.IStateServices.SvcRegProps;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.common.DuccWorkMap;
import org.apache.uima.ducc.transport.event.common.IDuccWork;
import org.apache.uima.ducc.transport.event.common.IDuccWorkService;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SimpleStatement;

/**
 * Toy orientdb loader to load a historydb from ducc history
 */

public class DbLoader
{
    DuccLogger logger = DuccLogger.getLogger(DbLoader.class, "DBLOAD");
    String DUCC_HOME;
    String SVC_HISTORY_KEY = SvcRegProps.is_archived.columnName();
    String META_HISTORY_KEY = SvcMetaProps.is_archived.columnName();

    DbManager dbManager = null;
    boolean archive = true;        // for debug and test, bypass archive-rename
    HistoryManagerDb hmd = null;
    StateServicesDb  ssd = null;

    // String history_url = "remote:localhost/DuccHistory";
    // String state_url   = "plocal:/home/challngr/ducc_runtime_db/database/databases/DuccHistoryT";
    String state_url   = null;

    // String jobHistory = System.getProperty("user.home") + "/ducc_runtime_db/history/jobs";
    String jobHistory = "/history/jobs";

    // String reservationHistory = System.getProperty("user.home") + "/ducc_runtime/history/reservations";
    String reservationHistory = "/history/reservations";

    //String serviceHistory = System.getProperty("user.home") + "/ducc_runtime/history/services";
    String serviceHistory = "/history/services";

    //String serviceHistory = System.getProperty("user.home") + "/ducc_runtime/history/services";
    String serviceRegistryHistory = "/history/services-registry";

    //String serviceRegistry = System.getProperty("user.home") + "/ducc_runtime/state/services";
    String serviceRegistry = "/state/services";

    String checkpointFile = "/state/orchestrator.ckpt";

    int nthreads = 10;
    AtomicInteger counter = new AtomicInteger(0);

    //int joblimit         = 100;
    //int reservationlimit = 100;
    //int servicelimit     = 100;
    //int registrylimit    = 100;

    int joblimit = Integer.MAX_VALUE;
    int reservationlimit = Integer.MAX_VALUE;
    int servicelimit = Integer.MAX_VALUE;
    int registrylimit = Integer.MAX_VALUE;

    boolean dojobs         = true;
    boolean doreservations = true;
    boolean doservices     = true;
    boolean doregistry     = true;
    boolean docheckpoint   = true;
    
    // Jira 4804 For now don't save details in tables: jobs, reservations, & processes
    boolean saveDetails  = System.getenv("SAVE_DB_DETAILS") == null ? false : true;

    long jobBytes = 0;
    long resBytes = 0;
    long svcBytes = 0;
    long svcRegBytes= 0;

    AtomicInteger skippedServices = new AtomicInteger(0);

    public DbLoader(String from, String state_url, int nthreads)
        throws Exception
    {
    	//String methodName = "<ctr>";
        this.state_url = state_url;
        DUCC_HOME = System.getProperty("DUCC_HOME");        
        if ( DUCC_HOME == null ) {
            System.out.println("System property -DDUCC_HOME must be set.");
            System.exit(1);
        }
        
        if ( System.getProperty("DONT_ARCHIVE") != null ) archive = false;

        File f = new File(from);
        if ( ! f.isDirectory() ) {
            System.out.println("'from' must be a directory");
            System.exit(1);
        }

        jobHistory             = from + jobHistory;
        reservationHistory     = from + reservationHistory;
        serviceHistory         = from + serviceHistory;
        serviceRegistryHistory = from + serviceRegistryHistory;
        serviceRegistry        = from + serviceRegistry;
        checkpointFile         = from + checkpointFile;

        System.setProperty(DbManager.URL_PROPERTY, state_url);
        
        this.nthreads = nthreads;
    }

    void closeStream(InputStream in)
    {
        try { in.close(); } catch(Exception e) {}
    }

    public void loadJobs()
    	throws Exception
    {
        String methodName = "loadJobs";

        logger.info(methodName, null, " -------------------- Load jobs ----------------");
        System.out.println(" -------------------- Load jobs ----------------");
        File dir = new File(jobHistory);
        if ( !dir.isDirectory() ) {
            logger.info(methodName, null, "Cannot find job history; skipping load of jobs.");
            return;
        }

        File[] files = dir.listFiles();
        if ( files == null || files.length == 0 ) {
            logger.info(methodName, null, "No jobs to move to database.");
            return;
        }
        logger.info(methodName, null, "Reading", files.length, "jobs.");

        LinkedBlockingQueue<File> queue = new LinkedBlockingQueue<File>();

        int max_to_load = joblimit;
        // int max_to_load = 1000;  // or Integer.MAX_VALUE for 'all of them'
        int nth = Math.min(nthreads, max_to_load);
        nth = Math.min(nth, files.length);

        JobLoader[] loader = new JobLoader[nth];
        Thread[]    threads = new Thread[nth];        
        List<Long> ids = new ArrayList<Long>();

        for ( int i = 0; i < nth; i++ ) {
            loader[i] = new JobLoader(queue, ids);
            threads[i] = new Thread(loader[i]);
            threads[i].start();
        }


        int c = 0;
        for ( File f : files) {
            String s = f.toString();
            if ( s.endsWith(".dwj") ) {
                logger.trace(methodName, null, "Loading file", c++, ":", f);
                queue.offer(f);
                counter.getAndIncrement();

                if ( c >= max_to_load ) break;
            } else {
                logger.info(methodName, null, "Can't find history file", f);
            }                              
        }

        logger.info(methodName, null, "Waiting for the", nth, "threads to load the DB.");
        while ( (c = counter.get()) != 0 ) {
            try { 
                logger.trace(methodName, null, "Waiting for loads to finish, counter is", c, "(job).");
                //System.out.println("Waiting for job loads to finish, counter is " + c);
                Thread.sleep(1000); 
            } 
            catch ( Exception e ) {}
        }

        for ( int i = 0; i < nth; i++ ) {
            logger.trace(methodName, null, "Interrupt thread (job)", i);
            threads[i].interrupt();
        }
                    
        for ( int i = 0; i < nth; i++ ) {
            logger.trace(methodName, null, "Joining thread (job)", i);
            try { threads[i].join(); } catch ( InterruptedException e ) {}
        }

        if ( archive ) {
            File renameTo = new File(dir + ".archive");
            dir.renameTo(renameTo);
        }
    }

    public void loadReservations()
        throws Exception
    {
        String methodName = "loadReservations";

        logger.info(methodName, null, " -------------------- Load reservations ----------------");
        System.out.println(" -------------------- Load reservations ----------------");

        File dir = new File(reservationHistory);
        if ( ! dir.isDirectory() ) {
            logger.info(methodName, null, "No reservation directory found; skipping database load of reservations.");
            return;
        }

        File[] files = dir.listFiles();
        if ( files == null || files.length == 0 ) {
            logger.info(methodName, null, "No reservation history files to convert.");
            return;
        }
        logger.info(methodName, null, "Reading", files.length, "reservation instances.");
        
        LinkedBlockingQueue<File> queue = new LinkedBlockingQueue<File>();
        
        int max_to_load = reservationlimit;
        //int max_to_load = 1000;
        int nth = Math.min(nthreads, max_to_load);
        nth = Math.min(nth, files.length);
        ReservationLoader[] loader = new ReservationLoader[nth];
        Thread[] threads = new Thread[nth];
        ArrayList<Long> ids = new ArrayList<Long>();
        
        for ( int i = 0; i < nth; i++ ) {
            loader[i] = new ReservationLoader(queue, ids);
            threads[i] = new Thread(loader[i]);
            threads[i].start();
        }        

        int c   = 0;        
        for ( File f : files ) {
            String s = f.toString();
            if ( s.endsWith(".dwr") ) {
                logger.trace(methodName, null, "Loading file", c++, ":", f);
                
                queue.offer(f);
                counter.getAndIncrement();

                if ( c >= max_to_load ) break;
            } else {
                logger.info(methodName, null, "Can't find history file", f);
            }                              

        }

        logger.info(methodName, null, "Waiting for the", nth, "threads to load the DB.");
        while ( (c = counter.get()) != 0 ) {
            try { 
                logger.trace(methodName, null, "Waiting for reservation loads to finish, counter is", c);
                //System.out.println("Waiting for reservation loads to finish, counter is " + c);

                Thread.sleep(1000); 
            } 
            catch ( Exception e ) {}
        }

        for ( int i = 0; i < nth; i++ ) {
            logger.trace(methodName, null, "Interrupt thread (reservations).", i);
            threads[i].interrupt();
        }
                    
        for ( int i = 0; i < nth; i++ ) {
            logger.trace(methodName, null, "Joining thread (reservations).", i);
            try { threads[i].join(); } catch ( InterruptedException e ) {}
        }

        if ( archive ) {
            File renameTo = new File(dir + ".archive");
            dir.renameTo(renameTo);
        }
    }


    public void loadServices()
    	throws Exception
    {
        String methodName = "loadServices";

        logger.info(methodName, null, " -------------------- Load services ----------------");
        System.out.println(" -------------------- Load AP/Service Instances ----------------");

        File dir = new File(serviceHistory);
        if ( ! dir.isDirectory() ) {
            logger.info(methodName, null, "No service history directory found; skipping load of service history.");
            return;
        }

        File[] files = dir.listFiles();

        if ( files == null || files.length == 0 ) {
            logger.info(methodName, null, "No service history files to convert.");
            return;
        }
        logger.info(methodName, null, "Reading", files.length, "service instances.");
        
        LinkedBlockingQueue<File> queue = new LinkedBlockingQueue<File>();
        
        int max_to_load = servicelimit;
        // int max_to_load = 1000;
        int nth = Math.min(nthreads, max_to_load);
        nth = Math.min(nth, files.length);
        ServiceLoader[] loader = new ServiceLoader[nth];
        Thread[] threads = new Thread[nth];
        ArrayList<Long> ids = new ArrayList<Long>();

        for ( int i = 0; i < nth; i++ ) {
            loader[i] = new ServiceLoader(queue, ids);
            threads[i] = new Thread(loader[i]);
            threads[i].start();
        }

        int c   = 0;
        for ( File f : files ) {
            String s = f.toString();
            if ( s.endsWith(".dws") ) {
                logger.trace(methodName, null, "Loading file", c++, ":", f);

                queue.offer(f);
                counter.getAndIncrement();

                if ( c >= max_to_load ) break;
            } else {
                logger.info(methodName, null, "Can't find history file", f);
            }                              
        }

        logger.info(methodName, null, "Waiting for the", nth, "threads to load the DB.");
        while ( (c = counter.get()) != 0 ) {
            try { 
                logger.trace(methodName, null, "Waiting for loads to finish, counter is", c, "(service instances");
                //System.out.println("Waiting for AP/Service Instance  loads to finish, counter is " + c);
                Thread.sleep(1000); 
            } 
            catch ( Exception e ) {}
        }

        for ( int i = 0; i < nth; i++ ) {
            logger.trace(methodName, null, "Interrupt thread (services).", i);
            threads[i].interrupt();
        }
                    
        for ( int i = 0; i < nth; i++ ) {
            logger.trace(methodName, null, "Joining thread (services).", i);
            try { threads[i].join(); } catch ( InterruptedException e ) {}
        }

        if ( archive ) {
            File renameTo = new File(dir + ".archive");
            dir.renameTo(renameTo);
        }
    }

    public void loadServiceRegistry(String registry, boolean isHistory)
    {
        String methodName = "loadServiceRegistry";

        logger.info(methodName, null, " -------------------- Load registry; isHistory", isHistory, " ----------------");
        System.out.println(" -------------------- Load Service Registry " + (isHistory ? "(history)" : "(active registrations)") + "  ----------------");


        int c = 0;
        File dir = new File(registry);
        if ( ! dir.isDirectory() ) {
            logger.error(methodName, null, registry, "is not a directory and cannot be loaded.");
            return;
        }

        File[] files = dir.listFiles();
        
        if ( files == null || files.length == 0 ) {
            if ( isHistory ) {
                logger.info(methodName, null, "Nothing in service registry history to move to database");
            } else {
                logger.info(methodName, null, "Nothing in service registry to move to database");
            }
            return;
        }

        LinkedBlockingQueue<Pair<String, Boolean>> queue = new LinkedBlockingQueue<Pair<String, Boolean>>();

        int max_to_load = registrylimit;
        int nth = Math.min(nthreads, max_to_load);
        nth = Math.min(nth, files.length);

        ServiceRegistrationLoader[] loader = new ServiceRegistrationLoader[nth];
        Thread[] threads = new Thread[nth];
        ArrayList<Long> ids = new ArrayList<Long>();

        for ( int i = 0; i < nth; i++ ) {
            loader[i] = new ServiceRegistrationLoader(queue, ids);
            threads[i] = new Thread(loader[i]);
            threads[i].start();
        }

        logger.info(methodName, null, "Reading", files.length, "service files (2 per instance).");
        for ( File f : files ) {
            String s = f.toString();
            if ( s.endsWith(".svc") ) {
                int ndx = s.indexOf(".svc");
                String numeric = s.substring(0, ndx);
                queue.offer(new Pair<String, Boolean>(numeric, isHistory));
                counter.getAndIncrement();

                if ( ++c >= max_to_load ) break;
            }
            
        }

        logger.info(methodName, null, "Waiting for the", nth, "threads to load the DB.");
        while ( (c = counter.get()) != 0 ) {
            try { 
                logger.trace(methodName, null, "Waiting for service registry loads to finish, counter is", c);
                //System.out.println("Waiting for service registration loads to finish, counter is " + c);

                Thread.sleep(1000); 
            } 
            catch ( Exception e ) {}
        }

        for ( int i = 0; i < nth; i++ ) {
            logger.trace(methodName, null, "Interrupt thread (service registry).", i);
            threads[i].interrupt();
        }
                    
        for ( int i = 0; i < nth; i++ ) {
            logger.trace(methodName, null, "Joining thread (service registry).", i);
            try { threads[i].join(); } catch ( InterruptedException e ) {}
        }

        if ( archive ) {
            File renameTo = new File(dir + ".archive");
            dir.renameTo(renameTo);
        }
    }

    @SuppressWarnings("unchecked")
	void loadCheckpoint()
    	throws Exception
    {
        String methodName = "loadCheckpoint";

        File f = new File(checkpointFile);
        if ( ! f.exists() ) {
            logger.info(methodName, null, "No checkpoint file to convert.");
            return;
        }

        //
        // A note - the Checkpointable object might be in the "wrong" package and can't be 
        //          cast properly.  When putting it into database we have to pick out the
        //          fields anyway.  So here we use introspection to get the fields and
        //          create the database entries.
        //
        FileInputStream fis = null;
        ObjectInputStream in = null;
		try {
			fis = new FileInputStream(checkpointFile);
			in = new ObjectInputStream(fis);

			Object xobj = (Object) in.readObject();
            Class<?> cl = xobj.getClass();
            Field p2jfield = cl.getDeclaredField("processToJobMap");
            p2jfield.setAccessible(true);
            ConcurrentHashMap<DuccId, DuccId> p2jmap = (ConcurrentHashMap<DuccId, DuccId>) p2jfield.get(xobj);

            Field wmField = cl.getDeclaredField("workMap");
            wmField.setAccessible(true);
            DuccWorkMap workMap = (DuccWorkMap) wmField.get(xobj);

            hmd.checkpoint(workMap, p2jmap);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if ( fis != null ) {
				fis.close();
			}
			if ( in != null ) {
				in.close();
			}
		}

        if ( archive ) {
            File renameTo = new File(f + ".archive");
            f.renameTo(renameTo);        
        }
    }    

    void test()
    	throws Exception
    {
    	String methodName = "foo";
        DbHandle h = dbManager.open();
        SimpleStatement s = new SimpleStatement("SELECT * from " + HistoryManagerDb.JOB_HISTORY_TABLE + " limit 5000");
        //SimpleStatement s = new SimpleStatement("SELECT * from " + HistoryManagerDb.RES_TABLE + " limit 5000");
        //SimpleStatement s = new SimpleStatement("SELECT * from " + HistoryManagerDbB.SVC_TABLE + " limit 5000");
        logger.info(methodName, null, "Fetch size", s.getFetchSize());
        s.setFetchSize(100);
        long now = System.currentTimeMillis();

        try {
            int counter = 0;
            int nbytes = 0;
            ResultSet rs = h.execute(s);
            for ( Row r : rs ) {
                counter++;
                ByteBuffer b = r.getBytes("work");
                nbytes += b.array().length;
                logger.info(methodName, null, "found", r.getLong("ducc_id"), "of type", r.getString("type"));
            }
            
            logger.info(methodName, null, "Found", counter, "results. Total bytes", nbytes);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        logger.info(methodName, null, "Time to select:", System.currentTimeMillis() - now);
    }


    @SuppressWarnings("unused")
	void run()
    	throws Exception
    {
    	String methodName = "run";
        long now = System.currentTimeMillis();
        boolean run_test = false;

        String[] dbUrls = DbUtil.dbServersStringToArray(state_url);
        
        if ( run_test ) {
            try {
                dbManager = new DbManager(dbUrls, logger);
                dbManager.init();
                
                test();
                if ( true ) return;
            } finally {
                dbManager.shutdown();
            }
            return;

        }

        dbManager = new DbManager(dbUrls, logger);
        dbManager.init();

        if ( true ) {
            try {

                hmd = new HistoryManagerDb();
                hmd.init(logger, dbManager);

                // drop some of the history indices to speed up 
                System.out.println("Temporarily dropping some indexes");
                List<SimpleStatement> drops = HistoryManagerDb.dropIndices();
                DbHandle h = dbManager.open();
                if (saveDetails) // Jira 4804
                for ( SimpleStatement ss : drops ) {
                    System.out.println(ss.getQueryString());
                    h.execute(ss);
                }
                
                long nowt = System.currentTimeMillis();
                if ( docheckpoint ) loadCheckpoint();
                logger.info(methodName, null, "***** Time to load checkpoint A ****", System.currentTimeMillis() - nowt);

                // ---------- Load job history
                nowt = System.currentTimeMillis();
                if ( dojobs ) loadJobs();            
                logger.info(methodName, null, "**** Time to load jobs**** ", System.currentTimeMillis() - nowt, "Total bytes loaded:", jobBytes);

                // ---------- Load reservation history
                nowt = System.currentTimeMillis();
                if ( doreservations ) loadReservations();                         
                logger.info(methodName, null, "**** Time to load reservations ****", System.currentTimeMillis() - nowt, "Total bytes loaded:", resBytes);


                // ---------- Load service isntance and AP history
                nowt = System.currentTimeMillis();
                if ( doservices ) loadServices();
                logger.info(methodName, null, "**** Time to load services instances ****", System.currentTimeMillis() - nowt, "Total bytes loaded:", svcBytes);


                // ---------- Load service registry
                long totalSvcBytes = 0;
                if ( doregistry ) {
                    nowt = System.currentTimeMillis();
                    ssd = new StateServicesDb();
                    ssd.init(logger,dbManager);
                    loadServiceRegistry(serviceRegistry, false);
                    logger.info(methodName, null, "**** Time to load Service registry ****", System.currentTimeMillis() - nowt, "Total bytes loaded:", svcRegBytes);
                    totalSvcBytes = svcRegBytes;
                    
                    // ---------- Load service registry history
                    svcRegBytes = 0;
                    nowt = System.currentTimeMillis();
                    loadServiceRegistry(serviceRegistryHistory, true);                          
                    logger.info(methodName, null, "**** Time to load Service history ****", System.currentTimeMillis() - nowt, "Total bytes loaded:", svcRegBytes);
                    totalSvcBytes = svcRegBytes;

                    logger.info(methodName, null, "**** Skipped services:", skippedServices);
                    // don't shutdown the ssm.  we'll close the db in the 'finally' below
                }

                nowt = System.currentTimeMillis();
                logger.info(methodName, null, "**** Total load time ****", System.currentTimeMillis() - now, "Total bytes loaded:", (jobBytes + resBytes + svcBytes + totalSvcBytes));

                if ( docheckpoint ) loadCheckpoint();
                logger.info(methodName, null, "**** Time to reload checkpoint B ****", System.currentTimeMillis() - nowt);

                // recreate dropped indices
                System.out.println("Restoring indexes");
                List<SimpleStatement> indices = HistoryManagerDb.createIndices();
                h = dbManager.open();
                if (saveDetails) // Jira 4804
                for ( SimpleStatement ss : indices ) {
                    System.out.println(ss.getQueryString());
                    h.execute(ss);
                }

            } catch ( Exception e ) {
            	logger.error(methodName, null, e);

            } finally {
                if ( dbManager!= null ) dbManager.shutdown();
            }
        }

        // load the service registry
        // loadServiceRegistry();
    }

    public static void main(String[] args)
    {
        if ( args.length < 2 ) {
            System.out.println("Usage: DbLoader from to <num-threads>");
            System.out.println("");
            System.out.println("Where:");
            System.out.println("   from      is the DUCC_HOME you wish to convert,");
            System.out.println("   to        is the datbase URL,");
            System.out.println("   nthreads  is the number of loader threads to run.");
            System.out.println(" ");
            
            System.exit(1);
        }

        int nthreads = 10; 
        if (args.length > 2) {
          nthreads = Integer.valueOf(args[2]);
        }
        
    	DbLoader dbl = null;

        try {
            dbl = new DbLoader(args[0], args[1], nthreads);
            dbl.run();
        } catch ( Exception e  ) {
            e.printStackTrace();
        } 
    }

    static  PreparedStatement jobPrepare = null;
    class JobLoader
        implements Runnable
    {
        BlockingQueue<File> queue;
        List<Long> ids;
        JobLoader(BlockingQueue<File> queue, List<Long> ids)
                throws Exception
        {
            this.queue = queue;
            this.ids = ids;

            DbHandle h = dbManager.open();
            synchronized(JobLoader.class) {
                if ( jobPrepare == null ) {
                    jobPrepare = h.prepare("INSERT INTO " + HistoryManagerDb.JOB_HISTORY_TABLE + " (ducc_id, type, history, work) VALUES (?, ?, ?, ?)");
                }
            }
        }

        public void run()
        {
            String methodName = "JobLoader.run";
            while ( true ) {
            	File  f = null;
                long nbytes = 0;
                long duccid = 0;
                DuccId did = null;
                try {
                    f = queue.take();
                    FileInputStream fis = null;
                    // ObjectInputStream in = null;

                    try {
                        long now = System.currentTimeMillis();
                        
                        String s = f.getName();
                        int ndx = s.indexOf(".");
                        duccid = Long.parseLong(s.substring(0, ndx));
                        did = new DuccId(duccid);
                        nbytes = f.length();
                        if ( nbytes > 16*1024*1024) {
                            logger.warn(methodName, did, "Skipping outsized job", duccid, "length=", nbytes);
                            nbytes = 0;
                            continue;
                        }

                        byte[] buf = new byte[(int)nbytes];
                        fis = new FileInputStream(f);
                        fis.read(buf);

                        ByteBuffer bb = ByteBuffer.wrap(buf);
                        logger.trace(methodName, did, "Time to read job:", System.currentTimeMillis() - now+" MS", "bytes:", nbytes);
                        logger.trace(methodName, did, "Job", duccid, "Store CQL:", jobPrepare.getQueryString());
                        
                        long now1 = System.currentTimeMillis();
                        BoundStatement boundStatement = new BoundStatement(jobPrepare);
                        BoundStatement bound = boundStatement.bind(duccid, "job", true, bb);
                        DbHandle h = dbManager.open();

                        try {
                          h.execute(bound);
                        } catch (Exception e) {
                          logger.error(methodName,  did,  "Error:", e);
                        }
                        logger.trace(methodName, did, "Time to store job", duccid, "- Database update:", (System.currentTimeMillis() - now1) + " MS", "Total save time:", (System.currentTimeMillis() - now) + " MS");

                       if (saveDetails) { // Jira 4804
                        synchronized(ids) {
                            // any sync object is ok - but we want to effectively single thread the writing of the details as this tends
                            // to overrun the DB during this bulk load.
                            ByteArrayInputStream bais = new ByteArrayInputStream(buf);
                            ObjectInputStream ois = new ObjectInputStream(bais);
                            Object o = ois.readObject();
                            ois.close();            
                            bais.close();
                            
                            now = System.currentTimeMillis();
                            hmd.summarizeProcesses(h, (IDuccWork) o, "J");
                            hmd.summarizeJob(h, (IDuccWork) o, "J");
                            logger.trace(methodName, did, "Time to store process summaries for job", duccid, ":", (System.currentTimeMillis() - now));
                        }
                       }
                    } catch(Exception e) {
                        logger.error(methodName, did, e);
                    } finally {                        
                        closeStream(fis);
                        counter.getAndDecrement();
                    }

                } catch ( InterruptedException e ) {
                    return;
                } catch(Exception e) {
                    logger.error(methodName, null, e);
                } finally {

                    synchronized(ids) {
                        if ( nbytes > 0 ) {
                            ids.add(duccid);
                            jobBytes += nbytes;
                        }
                    }
                }
            }
        }
    }

    PreparedStatement servicePrepare = null;
    class ServiceLoader
        implements Runnable
    {

        BlockingQueue<File> queue;
        List<Long> ids;
        ServiceLoader(BlockingQueue<File> queue, List<Long> ids)
        	throws Exception
        {
            this.queue = queue;
            this.ids = ids;
            DbHandle h = dbManager.open();
            synchronized(ServiceLoader.class) {
                if ( servicePrepare == null ) {
                    servicePrepare = h.prepare("INSERT INTO " + HistoryManagerDb.SVC_HISTORY_TABLE + " (ducc_id, type, history, work) VALUES (?, ?, ?, ?);");            
                }
            }
        }

        public void run()
        {
            String methodName = "ServiceLoader.run";
            while ( true ) {
                File f = null;
                long nbytes = 0;
                long duccid = 0;
                DuccId did = null;
                try {
                    f = queue.take();
                    FileInputStream fis = null;
                    ObjectInputStream in = null;
                    
                    try {
                        long now = System.currentTimeMillis();
                        
                        String s = f.getName();
                        int ndx = s.indexOf(".");
                        duccid = Long.parseLong(s.substring(0, ndx));
                        did = new DuccId(duccid);
                        nbytes = f.length();
                        if ( nbytes > 16*1024*1024) {
                            logger.warn(methodName, did, "Skipping outsized service", duccid, "length=", nbytes);
                            nbytes = 0;
                            continue;
                        }

                        byte[] buf = new byte[(int)nbytes];
                        fis = new FileInputStream(f);
                        fis.read(buf);

                        ByteBuffer bb = ByteBuffer.wrap(buf);
                        logger.trace(methodName, did, "Time to read service", duccid, ":", System.currentTimeMillis() - now + " MS", "bytes:", nbytes);
                        logger.trace(methodName, did, "Service", duccid, "Store CQL:", servicePrepare.getQueryString());
                        long now1 = System.currentTimeMillis();
                        BoundStatement boundStatement = new BoundStatement(servicePrepare);
                        BoundStatement bound = boundStatement.bind(duccid, "service", true, bb);
                        DbHandle h = dbManager.open();

                        try {
                          h.execute(bound);
                        } catch (Exception e) {
                          logger.error(methodName,  did,  "Error:", e);
                        }
                        logger.trace(methodName, did, "Time to store service", duccid, "- Database update:", (System.currentTimeMillis() - now1) + " MS", "Total save time:", (System.currentTimeMillis() - now) + " MS");

                        ByteArrayInputStream bais = new ByteArrayInputStream(buf);
                        ObjectInputStream ois = new ObjectInputStream(bais);
                        Object o = ois.readObject();
                        ois.close();            
                        bais.close();

                        String type = null;
                        if ( ((IDuccWorkService)o).getServiceDeploymentType() == null ) {
                            logger.warn(methodName, did, "getServiceDeploymentType is null, not extracting details.");
                            continue;
                        }

                        switch ( ((IDuccWorkService)o).getServiceDeploymentType() ) 
                        {
                            case uima:
                            case custom:
                                type = "S";
                                break;
                            case other:
                                type = "A";
                                break;
                            default :
                                type = "?";
                                break;
                        }

                       if (saveDetails) { // Jira 4804
                        now = System.currentTimeMillis();
                        hmd.summarizeProcesses(h, (IDuccWork) o, type);
                        long delta = System.currentTimeMillis() - now;
                        logger.trace(methodName, did, "Time to store AP/Service Instance summaries for job", duccid, ":", delta);
                       }

                    } catch(Exception e) {
                        logger.error(methodName, did, e);
                    } finally {
                        closeStream(in);
                        closeStream(fis);
                        counter.getAndDecrement();
                    }
                } catch ( InterruptedException e ) {
                    return;
                } catch(Exception e) {
                    logger.error(methodName, did, e);
                } finally {

                    synchronized(ids) {
                        if ( nbytes > 0 ) {
                            ids.add(duccid);
                            svcBytes += nbytes;
                        }
                    }
                }
            }
        }
    }

    static PreparedStatement reservationPrepare = null;
    class ReservationLoader
        implements Runnable
    {
        BlockingQueue<File> queue;
        List<Long> ids;
        ReservationLoader(BlockingQueue<File> queue, List<Long> ids)
        	throws Exception
        {
            this.queue = queue;
            this.ids = ids;            
            DbHandle h = dbManager.open();
            synchronized(ReservationLoader.class) {
                if ( reservationPrepare == null ) {
                    reservationPrepare = h.prepare("INSERT INTO " + HistoryManagerDb.RES_HISTORY_TABLE + " (ducc_id, type, history, work) VALUES (?, ?, ?, ?);");
                }
            }
        }

        public void run()
        {
            String methodName = "ReservationLoader.run";
            while ( true ) {
                File f = null;
                long nbytes = 0;
                long duccid = 0;
                DuccId did = null;
                try {
                    f = queue.take();
                    FileInputStream fis = null;
                    ObjectInputStream in = null;

                    try {
                        long now = System.currentTimeMillis();

                        String s = f.getName();
                        int ndx = s.indexOf(".");
                        duccid = Long.parseLong(s.substring(0, ndx));
                        did = new DuccId(duccid);
                        nbytes = f.length();
                        if ( nbytes > 16*1024*1024) {
                            logger.warn(methodName, did, "Skipping outsized reservation", duccid, "length=", nbytes);
                            nbytes = 0;
                            continue;
                        }

                        byte[] buf = new byte[(int)nbytes];
                        fis = new FileInputStream(f);
                        fis.read(buf);

                        ByteBuffer bb = ByteBuffer.wrap(buf);
                        logger.trace(methodName, did, "Time to read reservation", duccid, ":", System.currentTimeMillis() - now+" MS", "bytes:", nbytes);
                        logger.trace(methodName, did, "Reservation", duccid, "Store CQL:", reservationPrepare.getQueryString());
                        long now1 = System.currentTimeMillis();
                        BoundStatement boundStatement = new BoundStatement(reservationPrepare);
                        BoundStatement bound = boundStatement.bind(duccid, "reservation", true, bb);
                        DbHandle h = dbManager.open();

                        try {
                          h.execute(bound);
                        } catch (Exception e) {
                          logger.error(methodName,  did,  "Error:", e);
                        }
                        logger.trace(methodName, did, "Time to store reservation", duccid, "- Database update:", (System.currentTimeMillis() - now1) + " MS", "Total save time:", (System.currentTimeMillis() - now) + " MS");

                       if (saveDetails) { // Jira 4804
                        ByteArrayInputStream bais = new ByteArrayInputStream(buf);
                        ObjectInputStream ois = new ObjectInputStream(bais);
                        Object o = ois.readObject();
                        ois.close();            
                        bais.close();

                        now = System.currentTimeMillis();
                        hmd.summarizeProcesses(h, (IDuccWork) o, "R"); // details on the "process" in the map
                        hmd.summarizeReservation(h, (IDuccWork) o);    // details on the reservaiton itself
                        logger.trace(methodName, did, "Time to store reservation summaries for job", duccid, ":", (System.currentTimeMillis() - now));
                       }

                    } catch(Exception e) {
                        e.printStackTrace();
                        logger.error(methodName, did, e);
                    } finally {
                        closeStream(in);
                        closeStream(fis);
                        counter.getAndDecrement();
                    }


                } catch ( InterruptedException e ) {
                    return;
                } catch ( Exception e ){
                    logger.error(methodName, null, e);
                } finally {
                    synchronized(ids) {
                        if ( nbytes > 0 ) {
                            ids.add(duccid);
                            resBytes += nbytes;
                        }
                    }
                }
            }
        }
    }


    class ServiceRegistrationLoader
        implements Runnable
    {
        BlockingQueue<Pair<String, Boolean>> queue;
        List<Long> ids;
        ServiceRegistrationLoader(BlockingQueue<Pair<String, Boolean>> queue, List<Long> ids)
        {
            this.queue = queue;
            this.ids = ids;
        }

        public void run()
        {
            String methodName = "ServiceRegistrationLoader.run";
            while ( true ) {
                Pair<String, Boolean> p = null;
                String id = null;
                int nbytes = 0;
                boolean isHistory;
                try {
                    logger.trace(methodName, null, "About to take (service id).");
                    p = queue.take();
                    id = p.first();
                    isHistory = p.second();
                } catch ( InterruptedException e ) {
                    return;
                }
                logger.trace(methodName, null, id, "Took a service id");
                FileInputStream svc_in = null;
                FileInputStream meta_in = null;
                try {
                    Properties svc_props = new Properties();
                    Properties meta_props = new Properties();
                    
                    String svc_name = id + ".svc";
                    String meta_name = id + ".meta";

                    File svc_file = new File(svc_name);
                    File meta_file = new File(meta_name);
                    nbytes += (svc_file.length() + meta_file.length());
                    svc_in = new FileInputStream(svc_file);
                    meta_in = new FileInputStream(meta_file);
                    svc_props.load(svc_in);
                    meta_props.load(meta_in);

                    String sid = meta_props.getProperty(IStateServices.SvcMetaProps.numeric_id.pname());
                    if ( sid == null ) {
                        logger.error(methodName, null, "Cannot find service id in meta file for", id, "skipping load.");
                        skippedServices.getAndIncrement();
                    } else {
                        if ( id.indexOf(sid) < 0 ) {
                            // must do index of because the 'id' is a full path, not just the numeric id.  so we
                            // are satisfied with making sure the id is in the path.
                            throw new IllegalStateException("Service id and internal id do not match.");
                        }
                        DuccId did = new DuccId(Long.parseLong(sid));                        
                        ssd.storeProperties(did, svc_props, meta_props);     // always stores as not history
                        if ( isHistory ) {
                            ssd.moveToHistory(did, svc_props, meta_props);       // updates a single column in each
                        }

                        synchronized(ids) {
                            ids.add(did.getFriendly());
                            svcRegBytes += nbytes;
                        }
                    }
                } catch(Exception e) {
                    logger.error(methodName, null, e);
                } finally {
                    closeStream(svc_in);
                    closeStream(meta_in);
                    counter.getAndDecrement();
                }

            }
        }
    }
    

}
