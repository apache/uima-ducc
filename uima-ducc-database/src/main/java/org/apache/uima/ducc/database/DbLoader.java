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

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
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

    int nthreads = 20;
    AtomicInteger counter = new AtomicInteger(0);

    //int joblimit         = 10;
    //int reservationlimit = 10;
    //int servicelimit     = 10;
    //int registrylimit    = 1;

    int joblimit = Integer.MAX_VALUE;
    int reservationlimit = Integer.MAX_VALUE;
    int servicelimit = Integer.MAX_VALUE;
    int registrylimit = Integer.MAX_VALUE;

    boolean dojobs         = false;
    boolean doreservations = false;
    boolean doservices     = false;
    boolean doregistry     = true;
    boolean docheckpoint   = false;

    long jobBytes = 0;
    long resBytes = 0;
    long svcBytes = 0;
    long svcRegBytes= 0;

    AtomicInteger skippedServices = new AtomicInteger(0);

    public DbLoader(String from, String to)
        throws Exception
    {
    	//String methodName = "<ctr>";
        DUCC_HOME = System.getProperty("DUCC_HOME");        
        if ( DUCC_HOME == null ) {
            System.out.println("System proprety -DDUCC_HOME must be set.");
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

        f = new File(to);
        if ( ! f.isDirectory() ) {
            System.out.println("'to' must be a directory");
            System.exit(1);
        }

        String databasedir =  to + "/database/databases";

        // We always use a non-networked version for loading
        //state_url = "plocal:" + databasedir + "/DuccState";
        state_url = "bluej538";
        System.setProperty("ducc.state.database.url", state_url);

        if ( state_url.startsWith("plocal") ) {
            f = new File(databasedir);
            if ( !f.exists() ) {
                try {            
                    if ( ! f.mkdirs() ) {
                        System.out.println("Cannot create database directory: " + databasedir);
                        System.exit(1);
                    }
                    System.out.println("Created database directory " + databasedir);
                } catch ( Exception e ) {
                    System.out.println("Cannot create database directory: " + databasedir + ":" + e.toString());
                    System.exit(1);
                }
            }
        }

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
        File dir = new File(jobHistory);
        if ( !dir.isDirectory() ) {
            logger.info(methodName, null, "Cannot find job history; skipping load of jobs.");
            return;
        }

        File[] files = dir.listFiles();
        if ( files.length == 0 ) {
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
                logger.info(methodName, null, "Loading file", c++, ":", f);
                queue.offer(f);
                counter.getAndIncrement();

                if ( c >= max_to_load ) break;
            } else {
                logger.info(methodName, null, "Can't find history file", f);
            }                              
        }

        while ( (c = counter.get()) != 0 ) {
            try { 
                logger.info(methodName, null, "Waiting for loads to finish, counter is", c, "(job).");
                Thread.sleep(1000); 
            } 
            catch ( Exception e ) {}
        }

        for ( int i = 0; i < nth; i++ ) {
            logger.info(methodName, null, "Intterupt thread (job)", i);
            threads[i].interrupt();
        }
                    
        for ( int i = 0; i < nth; i++ ) {
            logger.info(methodName, null, "Joining thread (job)", i);
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
        File dir = new File(reservationHistory);
        if ( ! dir.isDirectory() ) {
            logger.info(methodName, null, "No reservation directory found; skipping database load of reservations.");
            return;
        }

        File[] files = dir.listFiles();
        if ( files.length == 0 ) {
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
                logger.info(methodName, null, "Loading file", c++, ":", f);
                
                queue.offer(f);
                counter.getAndIncrement();

                if ( c >= max_to_load ) break;
            } else {
                logger.info(methodName, null, "Can't find history file", f);
            }                              

        }

        while ( (c = counter.get()) != 0 ) {
            try { 
                logger.info(methodName, null, "Waiting for reservation loads to finish, counter is", c);
                Thread.sleep(1000); 
            } 
            catch ( Exception e ) {}
        }

        for ( int i = 0; i < nth; i++ ) {
            logger.info(methodName, null, "Intterupt thread (reservations).", i);
            threads[i].interrupt();
        }
                    
        for ( int i = 0; i < nth; i++ ) {
            logger.info(methodName, null, "Joining thread (reservations).", i);
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
        File dir = new File(serviceHistory);
        if ( ! dir.isDirectory() ) {
            logger.info(methodName, null, "No service history directory found; skipping load of service history.");
            return;
        }

        File[] files = dir.listFiles();

        if ( files.length == 0 ) {
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
                logger.info(methodName, null, "Loading file", c++, ":", f);

                queue.offer(f);
                counter.getAndIncrement();

                if ( c >= max_to_load ) break;
            } else {
                logger.info(methodName, null, "Can't find history file", f);
            }                              
        }

        while ( (c = counter.get()) != 0 ) {
            try { 
                logger.info(methodName, null, "Waiting for loads to finish, counter is", c, "(service instances");
                Thread.sleep(1000); 
            } 
            catch ( Exception e ) {}
        }

        for ( int i = 0; i < nth; i++ ) {
            logger.info(methodName, null, "Intterupt thread (services).", i);
            threads[i].interrupt();
        }
                    
        for ( int i = 0; i < nth; i++ ) {
            logger.info(methodName, null, "Joining thread (services).", i);
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

        int c = 0;
        File dir = new File(registry);
        if ( ! dir.isDirectory() ) {
            logger.error(methodName, null, registry, "is not a directory and cannot be loaded.");
            return;
        }

        File[] files = dir.listFiles();
        
        if ( files.length == 0 ) {
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

        while ( (c = counter.get()) != 0 ) {
            try { 
                logger.info(methodName, null, "Waiting for service registry loads to finish, counter is", c);
                Thread.sleep(1000); 
            } 
            catch ( Exception e ) {}
        }

        for ( int i = 0; i < nth; i++ ) {
            logger.info(methodName, null, "Intterupt thread (service registry).", i);
            threads[i].interrupt();
        }
                    
        for ( int i = 0; i < nth; i++ ) {
            logger.info(methodName, null, "Joining thread (service registry).", i);
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
			fis.close();
			in.close();
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
        SimpleStatement s = new SimpleStatement("SELECT * from " + HistoryManagerDb.JOB_TABLE + " limit 5000");
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
                logger.info(methodName, null, "found", r.getLong("ducc_dbid"), "of type", r.getString("type"));
            }
            
            logger.info(methodName, null, "Found", counter, "results. Total bytes", nbytes);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        logger.info(methodName, null, "Time to select:", System.currentTimeMillis() - now);
    }


    void run()
    	throws Exception
    {
    	String methodName = "run";
        long now = System.currentTimeMillis();

        if ( false ) {
            try {
                dbManager = new DbManager(state_url, logger);
                dbManager.init();
                
                test();
                if ( true ) return;
            } finally {
                dbManager.shutdown();
            }
            return;

        }

        dbManager = new DbManager(state_url, logger);
        dbManager.init();

//        DbCreate cr = new DbCreate(state_url, logger);
//        if ( state_url.startsWith("plocal") ) {
//            cr.createPlocalDatabase();
//        } else {
//            cr.createDatabase();
//        }
        


        if ( true ) {
            try {

                hmd = new HistoryManagerDb();
                hmd.init(logger, dbManager);
                
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
        if ( args.length != 2 ) {
            System.out.println("USage: DbLoader from to");
            System.out.println("");
            System.out.println("Where:");
            System.out.println("   from");        
            System.out.println("      is the DUCC_HOME you wish to convert");
            System.out.println("   to");
            System.out.println("      is the DUCC_HOME contining the new database");
            System.out.println("");
            System.out.println("'from' and 'to' may be the same thing");
            System.exit(1);
        }

            
    	DbLoader dbl = null;
        try {
            dbl = new DbLoader(args[0], args[1]);
            dbl.run();
        } catch ( Exception e  ) {
            e.printStackTrace();
        } 
    }

    class JobLoader
        implements Runnable
    {
        PreparedStatement statement = null;
        BlockingQueue<File> queue;
        List<Long> ids;
        JobLoader(BlockingQueue<File> queue, List<Long> ids)
                throws Exception
        {
            this.queue = queue;
            this.ids = ids;

            DbHandle h = dbManager.open();
            statement = h.prepare("INSERT INTO " + HistoryManagerDb.JOB_TABLE + " (ducc_dbid, type, history, work) VALUES (?, ?, ?, ?);");            
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
                    ObjectInputStream in = null;

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
                        logger.info(methodName, did, "Time to read job:", System.currentTimeMillis() - now+" MS", "bytes:", nbytes);
                        logger.info(methodName, did, "Job", duccid, "Store CQL:", statement.getQueryString());
                        long now1 = System.currentTimeMillis();
                        BoundStatement boundStatement = new BoundStatement(statement);
                        BoundStatement bound = boundStatement.bind(duccid, "job", true, bb);
                        DbHandle h = dbManager.open();

                        h.execute(bound);
                        logger.info(methodName, did, "Time to store job", duccid, "- Database update:", (System.currentTimeMillis() - now1) + " MS", "Total save time:", (System.currentTimeMillis() - now) + " MS");
                    } catch(Exception e) {
                        logger.info(methodName, did, e);
                    } finally {
                        closeStream(in);
                        closeStream(fis);
                        counter.getAndDecrement();
                    }

                } catch ( InterruptedException e ) {
                    return;
                } catch(Exception e) {
                    logger.info(methodName, null, e);
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


    class ServiceLoader
        implements Runnable
    {
        PreparedStatement statement = null;
        BlockingQueue<File> queue;
        List<Long> ids;
        ServiceLoader(BlockingQueue<File> queue, List<Long> ids)
        	throws Exception
        {
            this.queue = queue;
            this.ids = ids;
            DbHandle h = dbManager.open();
            statement = h.prepare("INSERT INTO " + HistoryManagerDb.SVC_TABLE + " (ducc_dbid, type, history, work) VALUES (?, ?, ?, ?);");            
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
                        logger.info(methodName, did, "Time to read service", duccid, ":", System.currentTimeMillis() - now + " MS", "bytes:", nbytes);
                        logger.info(methodName, did, "Service", duccid, "Store CQL:", statement.getQueryString());
                        long now1 = System.currentTimeMillis();
                        BoundStatement boundStatement = new BoundStatement(statement);
                        BoundStatement bound = boundStatement.bind(duccid, "service", true, bb);
                        DbHandle h = dbManager.open();

                        h.execute(bound);
                        logger.info(methodName, did, "Time to store service", duccid, "- Database update:", (System.currentTimeMillis() - now1) + " MS", "Total save time:", (System.currentTimeMillis() - now) + " MS");
                    } catch(Exception e) {
                        logger.info(methodName, did, e);
                    } finally {
                        closeStream(in);
                        closeStream(fis);
                        counter.getAndDecrement();
                    }
                } catch ( InterruptedException e ) {
                    return;
                } catch(Exception e) {
                    logger.info(methodName, did, e);
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

    class ReservationLoader
        implements Runnable
    {
        PreparedStatement statement = null;
        BlockingQueue<File> queue;
        List<Long> ids;
        ReservationLoader(BlockingQueue<File> queue, List<Long> ids)
        	throws Exception
        {
            this.queue = queue;
            this.ids = ids;            
            DbHandle h = dbManager.open();
            statement = h.prepare("INSERT INTO " + HistoryManagerDb.RES_TABLE + " (ducc_dbid, type, history, work) VALUES (?, ?, ?, ?);");
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
                        logger.info(methodName, did, "Time to read reservation", duccid, ":", System.currentTimeMillis() - now+" MS", "bytes:", nbytes);
                        logger.info(methodName, did, "Reservation", duccid, "Store CQL:", statement.getQueryString());
                        long now1 = System.currentTimeMillis();
                        BoundStatement boundStatement = new BoundStatement(statement);
                        BoundStatement bound = boundStatement.bind(duccid, "reservation", true, bb);
                        DbHandle h = dbManager.open();

                        h.execute(bound);
                        logger.info(methodName, did, "Time to store reservation", duccid, "- Database update:", (System.currentTimeMillis() - now1) + " MS", "Total save time:", (System.currentTimeMillis() - now) + " MS");

                    } catch(Exception e) {
                        logger.info(methodName, did, e);
                    } finally {
                        closeStream(in);
                        closeStream(fis);
                        counter.getAndDecrement();
                    }


                } catch ( InterruptedException e ) {
                    return;
                } catch ( Exception e ){
                    logger.info(methodName, null, e);
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
                    logger.info(methodName, null, "About to take (service id).");
                    p = queue.take();
                    id = p.first();
                    isHistory = p.second();
                } catch ( InterruptedException e ) {
                    return;
                }
                logger.info(methodName, null, id, "Took a service id");
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
                    logger.info(methodName, null, e);
                } finally {
                    closeStream(svc_in);
                    closeStream(meta_in);
                    counter.getAndDecrement();
                }

            }
        }
    }
    

}
