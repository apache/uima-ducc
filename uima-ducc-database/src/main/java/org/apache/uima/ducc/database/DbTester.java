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
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.uima.ducc.common.persistence.services.IStateServices;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.database.DbConstants.DbCategory;
import org.apache.uima.ducc.transport.event.common.IDuccWorkJob;
import org.apache.uima.ducc.transport.event.common.IDuccWorkReservation;
import org.apache.uima.ducc.transport.event.common.IDuccWorkService;

/**
 * Toy orientdb loader to load a historydb from ducc history
 */

public class DbTester
{
    DuccLogger logger = DuccLogger.getLogger(DbTester.class, "DBLOAD");

    HistoryManagerDb hmd = null;
    StateServicesDb  ssd = null;

    String history_url = "remote:localhost/DuccHistory";
    String state_url   = "remote:localhost/DuccState";

    // String jobHistory = System.getProperty("user.home") + "/ducc_runtime/history/jobs";
    String jobHistory = "/home/ducc/ducc_runtime/history/jobs";

    // String reservationHistory = System.getProperty("user.home") + "/ducc_runtime/history/reservations";
    String reservationHistory = "/home/ducc/ducc_runtime/history/reservations";

    //String serviceHistory = System.getProperty("user.home") + "/ducc_runtime/history/services";
    String serviceHistory = "/home/ducc/ducc_runtime/history/services";

    //String serviceHistory = System.getProperty("user.home") + "/ducc_runtime/history/services";
    String serviceRegistryHistory = "/home/ducc/ducc_runtime/history/services-registry";

    //String serviceRegistry = System.getProperty("user.home") + "/ducc_runtime/state/services";
    String serviceRegistry = "/home/ducc/ducc_runtime/state/services";

    int nthreads = 20;
    AtomicInteger counter = new AtomicInteger(0);

    public DbTester()
        throws Exception
    {
        System.setProperty("ducc.history.database.url", history_url);
        System.setProperty("ducc.state.database.url", state_url);
    }

    void closeStream(InputStream in)
    {
        try { in.close(); } catch(Exception e) {}
    }

    public void loadJobs()
    {
        String methodName = "loadJobs";
        LinkedBlockingQueue<IDuccWorkJob> jobqueue = new LinkedBlockingQueue<IDuccWorkJob>();

        int max_to_load = 2;
        int nth = Math.min(nthreads, max_to_load);
        JobLoader[] loader = new JobLoader[nth];
        Thread[]    threads = new Thread[nth];        
        List<Long> ids = new ArrayList<Long>();

        for ( int i = 0; i < nth; i++ ) {
            loader[i] = new JobLoader(jobqueue, ids);
            threads[i] = new Thread(loader[i]);
            threads[i].start();
        }

        File dir = new File(jobHistory);
        File[] files = dir.listFiles();
        logger.info(methodName, null, "Reading", files.length, "jobs.");

        int c = 0;
        for ( File f : files) {
            String s = f.toString();
            if ( s.endsWith(".dwj") ) {
                logger.info(methodName, null, "Loading file", c++, ":", f);
                IDuccWorkJob job = null;
                FileInputStream fis = null;
                ObjectInputStream in = null;

                try {
                    long now = System.currentTimeMillis();
                    fis = new FileInputStream(f);
                    in = new ObjectInputStream(fis);
                    job =  (IDuccWorkJob) in.readObject();
                    logger.info(methodName, job.getDuccId(), "Time to read job:", System.currentTimeMillis() - now);
                    jobqueue.offer(job);
                    counter.getAndIncrement();
                } catch(Exception e) {
                    logger.info(methodName, null, e);
                } finally {
                    // restoreJob(job.getDuccId().getFriendly());
                    closeStream(in);
                    closeStream(fis);
                    if ( c >= max_to_load ) break;
                }
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

    }

    public void loadReservations()
    {
        String methodName = "loadReservations";

        LinkedBlockingQueue<IDuccWorkReservation> queue = new LinkedBlockingQueue<IDuccWorkReservation>();
        
        int max_to_load = Integer.MAX_VALUE;
        int nth = Math.min(nthreads, max_to_load);
        ReservationLoader[] loader = new ReservationLoader[nth];
        Thread[] threads = new Thread[nth];
        ArrayList<Long> ids = new ArrayList<Long>();

        for ( int i = 0; i < nth; i++ ) {
            loader[i] = new ReservationLoader(queue, ids);
            threads[i] = new Thread(loader[i]);
            threads[i].start();
        }

        File dir = new File(reservationHistory);
        int c   = 0;

        File[] files = dir.listFiles();
        logger.info(methodName, null, "Reading", files.length, "reservation instances.");
        for ( File f : dir.listFiles() ) {
            String s = f.toString();
            if ( s.endsWith(".dwr") ) {
                logger.info(methodName, null, "Loading file", c++, ":", f);
                IDuccWorkReservation res = null;
                FileInputStream fis = null;
                ObjectInputStream in = null;

                try {
                    long now = System.currentTimeMillis();
                    fis = new FileInputStream(f);
                    in = new ObjectInputStream(fis);
                    res =  (IDuccWorkReservation) in.readObject();
                    logger.info(methodName, res.getDuccId(), "Time to read reservation:", System.currentTimeMillis() - now);
                    
                    queue.offer(res);
                    counter.getAndIncrement();
                } catch(Exception e) {
                    logger.info(methodName, null, e);
                } finally {
                    // restoreJob(job.getDuccId().getFriendly());
                    closeStream(in);
                    closeStream(fis);
                    if ( c >= max_to_load ) break;
                }
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

        // try {
		// 	List<IDuccWorkReservation> ress = hmd.restoreReservations(c);
        //     logger.info(methodName, null, "Recovered", ress.size(), "reservations.");
		// } catch (Exception e) {
		// 	// TODO Auto-generated catch block
		// 	e.printStackTrace();
		// }
    }


    public void loadServices()
    {
        String methodName = "loadServices";
        LinkedBlockingQueue<IDuccWorkService> queue = new LinkedBlockingQueue<IDuccWorkService>();
        
        int max_to_load = Integer.MAX_VALUE;
        int nth = Math.min(nthreads, max_to_load);
        ServiceLoader[] loader = new ServiceLoader[nth];
        Thread[] threads = new Thread[nth];
        ArrayList<Long> ids = new ArrayList<Long>();

        for ( int i = 0; i < nth; i++ ) {
            loader[i] = new ServiceLoader(queue, ids);
            threads[i] = new Thread(loader[i]);
            threads[i].start();
        }

        File dir = new File(serviceHistory);
        int c   = 0;

        File[] files = dir.listFiles();
        logger.info(methodName, null, "Reading", files.length, "service instances.");
        for ( File f : files ) {
            String s = f.toString();
            if ( s.endsWith(".dws") ) {
                logger.info(methodName, null, "Loading file", c++, ":", f);
                IDuccWorkService svc = null;
                FileInputStream fis = null;
                ObjectInputStream in = null;

                try {
                    long now = System.currentTimeMillis();
                    fis = new FileInputStream(f);
                    in = new ObjectInputStream(fis);
                    svc =  (IDuccWorkService) in.readObject();
                    logger.info(methodName, svc.getDuccId(), "Time to read service:", System.currentTimeMillis() - now);
                    

                    queue.offer(svc);
                    counter.getAndIncrement();
                } catch(Exception e) {
                    logger.info(methodName, null, e);
                } finally {
                    // restoreJob(job.getDuccId().getFriendly());
                    closeStream(in);
                    closeStream(fis);
                    if ( c >= max_to_load ) break;
                }
            } else {
                logger.info(methodName, null, "Can't find history file", f);
            }                              
        }

        while ( (c = counter.get()) != 0 ) {
            try { 
                logger.info(methodName, null, "Waiting for service loads to finish, counter is", c);
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

        // try {
		// 	List<IDuccWorkService> services = hmd.restoreServices(c);
        //     logger.info(methodName, null, "Recovered", services.size(), "serves.");
		// } catch (Exception e) {
		// 	// TODO Auto-generated catch block
		// 	e.printStackTrace();
		// }

    }

    public void loadServiceRegistry(String registry)
    {
        String methodName = "loadServiceRegistry";

        LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<String>();

        int max_to_load = Integer.MAX_VALUE;
        int nth = Math.min(nthreads, max_to_load);
        ServiceRegistrationLoader[] loader = new ServiceRegistrationLoader[nth];
        Thread[] threads = new Thread[nth];
        ArrayList<Long> ids = new ArrayList<Long>();

        for ( int i = 0; i < nth; i++ ) {
            loader[i] = new ServiceRegistrationLoader(queue, ids);
            threads[i] = new Thread(loader[i]);
            threads[i].start();
        }

        int c = 0;
        File dir = new File(registry);
        File[] files = dir.listFiles();
        logger.info(methodName, null, "Reading", files.length, "service files (2 per instance).");
        for ( File f : files ) {
            String s = f.toString();
            if ( s.endsWith(".svc") ) {
                int ndx = s.indexOf(".svc");
                String numeric = s.substring(0, ndx);
                queue.offer(numeric);
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

    }

    void run()
    {
    	String methodName = "run";
        // load the history db
        if ( true ) {
            try {

                hmd = new HistoryManagerDb(logger);

                // ---------- Load job history
                loadJobs();            
                if ( true ) return;

                // ---------- Load reservation history
                loadReservations();                         
   
                // ---------- Load service isntance and AP history
                loadServices();

                // ---------- Load service registry
                ssd = new StateServicesDb();
                ssd.init(logger);
                loadServiceRegistry(serviceRegistry);
                try {
                    ssd.shutdown(); 
                } catch ( Exception e ) {
                    e.printStackTrace();
                }

                // ---------- Load service registry history
                ssd = new StateServicesDb();
                ssd.init(logger);
                loadServiceRegistry(serviceRegistryHistory);                
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
    	DbTester dbl = null;
        try {
            dbl = new DbTester();
            dbl.run();
        } catch ( Exception e  ) {
            e.printStackTrace();
        } 
    }

    class JobLoader
        implements Runnable
    {
        BlockingQueue<IDuccWorkJob> queue;
        List<Long> ids;
        JobLoader(BlockingQueue<IDuccWorkJob> queue, List<Long> ids)
        {
            this.queue = queue;
            this.ids = ids;
        }

        public void run()
        {
            String methodName = "JobLoader.run";
            while ( true ) {
            	IDuccWorkJob job = null;
                try {
                    logger.info(methodName, null, "About to take (job).");
                    job = queue.take();
                } catch ( InterruptedException e ) {
                    return;
                }
                logger.info(methodName, job.getDuccId(), "Took a job.");
                try {
                    //h = dbManager.open();
                    hmd.saveJob(job);
                    //h.close();
                    synchronized(ids) {
                        ids.add(job.getDuccId().getFriendly());
                    }
                    counter.getAndDecrement();
                } catch(Exception e) {
                    logger.info(methodName, null, e);
                } 
            }
        }
    }


    class ServiceLoader
        implements Runnable
    {
        BlockingQueue<IDuccWorkService> queue;
        List<Long> ids;
        ServiceLoader(BlockingQueue<IDuccWorkService> queue, List<Long> ids)
        {
            this.queue = queue;
            this.ids = ids;
        }

        public void run()
        {
            String methodName = "ServiceLoader.run";
            while ( true ) {
            	IDuccWorkService svc = null;
                try {
                    logger.info(methodName, null, "About to take (service).");
                    svc = queue.take();
                } catch ( InterruptedException e ) {
                    return;
                }
                logger.info(methodName, svc.getDuccId(), "Took a Service");
                try {
                    //h = dbManager.open();
                    hmd.saveServiceUnsafe(svc);
                    //h.close();
                    synchronized(ids) {
                        ids.add(svc.getDuccId().getFriendly());
                    }
                    counter.getAndDecrement();
                } catch(Exception e) {
                    logger.info(methodName, null, e);
                } 
            }
        }
    }

    class ReservationLoader
        implements Runnable
    {
        BlockingQueue<IDuccWorkReservation> queue;
        List<Long> ids;
        ReservationLoader(BlockingQueue<IDuccWorkReservation> queue, List<Long> ids)
        {
            this.queue = queue;
            this.ids = ids;
        }

        public void run()
        {
            String methodName = "ReservationLoader.run";
            while ( true ) {
            	IDuccWorkReservation res = null;
                try {
                    logger.info(methodName, null, "About to take (reservation).");
                    res = queue.take();
                } catch ( InterruptedException e ) {
                    return;
                }
                logger.info(methodName, res.getDuccId(), "Took a Service");
                try {
                    //h = dbManager.open();
                    hmd.saveReservationUnsafe(res);
                    //h.close();
                    synchronized(ids) {
                        ids.add(res.getDuccId().getFriendly());
                    }
                    counter.getAndDecrement();
                } catch(Exception e) {
                    logger.info(methodName, null, e);
                } 
            }
        }
    }


    class ServiceRegistrationLoader
        implements Runnable
    {
        BlockingQueue<String> queue;
        List<Long> ids;
        ServiceRegistrationLoader(BlockingQueue<String> queue, List<Long> ids)
        {
            this.queue = queue;
            this.ids = ids;
        }

        public void run()
        {
            String methodName = "ServiceRegistrationLoader.run";
            while ( true ) {
                String id = null;
                try {
                    logger.info(methodName, null, "About to take (service id).");
                    id = queue.take();
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
                        
                        ssd.storePropertiesUnsafe(did, svc_props, meta_props, DbCategory.SmReg);
                        
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
