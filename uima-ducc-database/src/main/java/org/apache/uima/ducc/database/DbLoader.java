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
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.uima.ducc.common.Pair;
import org.apache.uima.ducc.common.persistence.services.IStateServices;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.database.DbConstants.DbCategory;
import org.apache.uima.ducc.transport.event.common.DuccWorkMap;
import org.apache.uima.ducc.transport.event.common.IDuccWorkJob;
import org.apache.uima.ducc.transport.event.common.IDuccWorkReservation;
import org.apache.uima.ducc.transport.event.common.IDuccWorkService;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;

/**
 * Toy orientdb loader to load a historydb from ducc history
 */

public class DbLoader
{
    DuccLogger logger = DuccLogger.getLogger(DbLoader.class, "DBLOAD");
    String DUCC_HOME;

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
    String archive_key  = IStateServices.archive_key;
    String archive_flag = IStateServices.archive_flag;

    int nthreads = 20;
    AtomicInteger counter = new AtomicInteger(0);

    //int joblimit         = 10000;
    //int reservationlimit = 10000;
    //int servicelimit     = 10000;
    //int registrylimit    = 10000;

    int joblimit = Integer.MAX_VALUE;
    int reservationlimit = Integer.MAX_VALUE;
    int servicelimit = Integer.MAX_VALUE;
    int registrylimit = Integer.MAX_VALUE;

    boolean dojobs         = true;
    boolean doreservations = true;
    boolean doservices     = true;
    boolean doregistry     = true;
    boolean docheckpoint   = true;

    public DbLoader(String from, String to)
        throws Exception
    {
    	String methodName = "<ctr>";
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
        String databasename = databasedir + "/DuccState";
        // We always use a non-networked version for loading
        //state_url = "plocal:" + databasedir + "/DuccState";
        state_url = "remote:bluej538/DuccState";
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

    void run()
    	throws Exception
    {
    	String methodName = "run";
        long now = System.currentTimeMillis();

        DbManager dbm = new DbManager(state_url, logger);
        if ( dbm.checkForDatabase() ) {
            dbm.init();
            dbm.drop();
            dbm.shutdown();
        }

        DbCreate cr = new DbCreate(state_url, logger);
        if ( state_url.startsWith("plocal") ) {
            cr.createPlocalDatabase();
        } else {
            cr.createDatabase();
        }

        logger.info(methodName, null, "storage.useWAL", System.getProperty("storage.useWAL"));
        logger.info(methodName, null, "tx.useLog", System.getProperty("tx.useLog"));
        if ( true ) {
            try {

                OGlobalConfiguration.USE_WAL.setValue(false);
                OGlobalConfiguration.USE_LOG.setValue(false);

                OGlobalConfiguration.dumpConfiguration(System.out);

                hmd = new HistoryManagerDb(logger);

                long nowt = System.currentTimeMillis();
                if ( docheckpoint ) loadCheckpoint();
                logger.info(methodName, null, "***** Time to load checkpoint A ****", System.currentTimeMillis() - nowt);

                OGlobalConfiguration.dumpConfiguration(System.out);


                // ---------- Load job history
                nowt = System.currentTimeMillis();
                if ( dojobs ) loadJobs();            
                logger.info(methodName, null, "**** Time to load jobs**** ", System.currentTimeMillis() - nowt);

                // ---------- Load reservation history
                nowt = System.currentTimeMillis();
                if ( doreservations ) loadReservations();                         
                logger.info(methodName, null, "**** Time to load reservations ****", System.currentTimeMillis() - nowt);


                // ---------- Load service isntance and AP history
                nowt = System.currentTimeMillis();
                if ( doservices ) loadServices();
                logger.info(methodName, null, "**** Time to load service instances ****", System.currentTimeMillis() - nowt);

                // ---------- Load service registry
                if ( doregistry ) {
                    nowt = System.currentTimeMillis();
                    ssd = new StateServicesDb();
                    ssd.init(logger);
                    loadServiceRegistry(serviceRegistry, false);
                    try {
                        ssd.shutdown(); 
                    } catch ( Exception e ) {
                        e.printStackTrace();
                    }
                    logger.info(methodName, null, "**** Time to load Service registry ****", System.currentTimeMillis() - nowt);
                    
                    // ---------- Load service registry history
                    nowt = System.currentTimeMillis();
                    ssd = new StateServicesDb();
                    ssd.init(logger);
                    loadServiceRegistry(serviceRegistryHistory, true);                          
                    logger.info(methodName, null, "**** Time to load Service history ****", System.currentTimeMillis() - nowt);
                }

                nowt = System.currentTimeMillis();
                logger.info(methodName, null, "**** Total load time ****", System.currentTimeMillis() - now);

                if ( docheckpoint ) loadCheckpoint();
                logger.info(methodName, null, "**** Time to reload checkpoint B ****", System.currentTimeMillis() - nowt);

            } catch ( Exception e ) {
            	logger.error(methodName, null, e);

            } finally {
                if ( hmd != null ) hmd.shutdown();
                if ( ssd != null ) ssd.shutdown();
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
        BlockingQueue<File> queue;
        List<Long> ids;
        JobLoader(BlockingQueue<File> queue, List<Long> ids)
        {
            this.queue = queue;
            this.ids = ids;
        }

        public void run()
        {
            String methodName = "JobLoader.run";
            while ( true ) {
            	File  f = null;
                IDuccWorkJob job = null;
                try {
                    f = queue.take();

                    FileInputStream fis = null;
                    ObjectInputStream in = null;

                    try {
                        long now = System.currentTimeMillis();
                        fis = new FileInputStream(f);
                        in = new ObjectInputStream(fis);
                        job =  (IDuccWorkJob) in.readObject();
                        logger.info(methodName, job.getDuccId(), "Time to read job:", System.currentTimeMillis() - now);
                        hmd.saveJobUnsafe(job);
                    } catch(Exception e) {
                        logger.info(methodName, null, e);
                    } finally {
                        closeStream(in);
                        closeStream(fis);
                        counter.getAndDecrement();
                    }

                } catch ( InterruptedException e ) {
                    return;
                } catch(Exception e) {
                    logger.info(methodName, null, e);
                } 

                synchronized(ids) {
                    if ( job != null ) {
                        ids.add(job.getDuccId().getFriendly());
                    }
                }
            }
        }
    }


    class ServiceLoader
        implements Runnable
    {
        BlockingQueue<File> queue;
        List<Long> ids;
        ServiceLoader(BlockingQueue<File> queue, List<Long> ids)
        {
            this.queue = queue;
            this.ids = ids;
        }

        public void run()
        {
            String methodName = "ServiceLoader.run";
            while ( true ) {
            	IDuccWorkService svc = null;
                File f = null;
                try {
                    f = queue.take();
                    FileInputStream fis = null;
                    ObjectInputStream in = null;
                    
                    try {
                        long now = System.currentTimeMillis();
                        fis = new FileInputStream(f);
                        in = new ObjectInputStream(fis);
                        svc =  (IDuccWorkService) in.readObject();
                        logger.info(methodName, svc.getDuccId(), "Time to read service:", System.currentTimeMillis() - now);                        
                        hmd.saveServiceUnsafe(svc);
                    } catch(Exception e) {
                        logger.info(methodName, null, "Error reading or saving service:", f);
                        logger.info(methodName, null, e);
                    } finally {
                        closeStream(in);
                        closeStream(fis);
                        counter.getAndDecrement(); 
                    }

                } catch ( InterruptedException e ) {
                    return;
                } catch ( Exception e ){
                    logger.info(methodName, null, "Error reading or saving service:", f);
                    logger.info(methodName, null, e);
                }

                synchronized(ids) {
                    if ( svc != null ) {
                        ids.add(svc.getDuccId().getFriendly());
                    }
                }
            }
        }
    }

    class ReservationLoader
        implements Runnable
    {
        BlockingQueue<File> queue;
        List<Long> ids;
        ReservationLoader(BlockingQueue<File> queue, List<Long> ids)
        {
            this.queue = queue;
            this.ids = ids;
        }

        public void run()
        {
            String methodName = "ReservationLoader.run";
            while ( true ) {
            	IDuccWorkReservation res = null;
                File f = null;
                try {
                    f = queue.take();
                    FileInputStream fis = null;
                    ObjectInputStream in = null;

                    try {
                        long now = System.currentTimeMillis();
                        fis = new FileInputStream(f);
                        in = new ObjectInputStream(fis);
                        res =  (IDuccWorkReservation) in.readObject();
                        logger.info(methodName, res.getDuccId(), "Time to read reservation:", System.currentTimeMillis() - now);
                        hmd.saveReservationUnsafe(res);
                    } catch(Exception e) {
                        logger.info(methodName, null, e);
                    } finally {
                        closeStream(in);
                        closeStream(fis);
                        counter.getAndDecrement();
                    }


                } catch ( InterruptedException e ) {
                    return;
                } catch ( Exception e ){
                    logger.info(methodName, null, e);
                }

                synchronized(ids) {
                    if ( res != null ) {
                        ids.add(res.getDuccId().getFriendly());
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

                    svc_in = new FileInputStream(svc_name);
                    meta_in = new FileInputStream(meta_name);
                    svc_props.load(svc_in);
                    meta_props.load(meta_in);
                    if ( isHistory ) {
                        meta_props.setProperty(archive_key, archive_flag);
                    }

                    String sid = meta_props.getProperty(IStateServices.SvcProps.numeric_id.pname());
                    if ( sid == null ) {
                        logger.warn(methodName, null, "Cannot find service id in meta file for", id);
                    } else {
                        if ( id.indexOf(sid) < 0 ) {
                            // must do index of because the 'id' is a full path, not just the numeric id.  so we
                            // are satisfied with making sure the id is in the path.
                            throw new IllegalStateException("Service id and internal id do not match.");
                        }
                        DuccId did = new DuccId(Long.parseLong(sid));
                        
                        if ( isHistory ) {
                            ssd.storePropertiesUnsafe(did, svc_props, meta_props, DbCategory.History);
                        } else {
                            ssd.storePropertiesUnsafe(did, svc_props, meta_props, DbCategory.SmReg);
                        }
                        synchronized(ids) {
                            ids.add(did.getFriendly());
                        }
                    }
                    counter.getAndDecrement();
                } catch(Exception e) {
                    logger.info(methodName, null, e);
                } finally {
                    closeStream(svc_in);
                    closeStream(meta_in);
                }

            }
        }
    }
    

}
