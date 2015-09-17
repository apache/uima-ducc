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

    HistoryManagerDb hmd = null;
    StateServicesDb  ssd = null;

    // String history_url = "remote:localhost/DuccHistory";
    // String state_url   = "plocal:/home/challngr/ducc_runtime_db/database/databases/DuccHistoryT";
    String state_url   = "plocal:/users/challngr/DuccHistoryT";

    // String jobHistory = System.getProperty("user.home") + "/ducc_runtime_db/history/jobs";
    String jobHistory = "/home/ducc/ducc_runtime/history/jobs";

    // String reservationHistory = System.getProperty("user.home") + "/ducc_runtime/history/reservations";
    String reservationHistory = "/home/ducc/ducc_runtime/history/reservations";

    //String serviceHistory = System.getProperty("user.home") + "/ducc_runtime/history/services";
    String serviceHistory = "/home/ducc/ducc_runtime/history/services";

    //String serviceHistory = System.getProperty("user.home") + "/ducc_runtime/history/services";
    String serviceRegistryHistory = "/home/ducc/ducc_runtime/history/services-registry";

    //String serviceRegistry = System.getProperty("user.home") + "/ducc_runtime/state/services";
    String serviceRegistry = "/home/ducc/ducc_runtime/state/services";

    String checkpointFile = "/home/ducc/ducc_runtime/state/orchestrator.ckpt";
    String archive_key  = IStateServices.archive_key;
    String archive_flag = IStateServices.archive_flag;

    int nthreads = 40;
    AtomicInteger counter = new AtomicInteger(0);

    public DbLoader()
        throws Exception
    {
        // System.setProperty("ducc.history.database.url", history_url);
        System.setProperty("ducc.state.database.url", state_url);
    }

    void closeStream(InputStream in)
    {
        try { in.close(); } catch(Exception e) {}
    }

    public void loadJobs()
    {
        String methodName = "loadJobs";
        LinkedBlockingQueue<File> queue = new LinkedBlockingQueue<File>();

        // int max_to_load = Integer.MAX_VALUE;  // or Integer.MAX_VALUE for 'all of them'
        int max_to_load = 1000;  // or Integer.MAX_VALUE for 'all of them'
        int nth = Math.min(nthreads, max_to_load);
        JobLoader[] loader = new JobLoader[nth];
        Thread[]    threads = new Thread[nth];        
        List<Long> ids = new ArrayList<Long>();

        for ( int i = 0; i < nth; i++ ) {
            loader[i] = new JobLoader(queue, ids);
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

    }

    public void loadReservations()
    {
        String methodName = "loadReservations";
        LinkedBlockingQueue<File> queue = new LinkedBlockingQueue<File>();
        
        //int max_to_load = Integer.MAX_VALUE;
        int max_to_load = 1000;
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
        File[] files = dir.listFiles();
        logger.info(methodName, null, "Reading", files.length, "reservation instances.");

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
    }


    public void loadServices()
    {
        String methodName = "loadServices";
        LinkedBlockingQueue<File> queue = new LinkedBlockingQueue<File>();
        
        // int max_to_load = Integer.MAX_VALUE;
        int max_to_load = 1000;
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
        File[] files = dir.listFiles();
        logger.info(methodName, null, "Reading", files.length, "service instances.");

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

    }

    public void loadServiceRegistry(String registry, boolean isHistory)
    {
        String methodName = "loadServiceRegistry";

        LinkedBlockingQueue<Pair<String, Boolean>> queue = new LinkedBlockingQueue<Pair<String, Boolean>>();

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

    }

    void loadCheckpoint()
    	throws Exception
    {
        String methodName = "loadCheckpoint";
        
        //Checkpointable obj = null;
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
        
    }    

    void run()
    	throws Exception
    {
    	String methodName = "run";

        DbCreate cr = new DbCreate(state_url);
        cr.createPlocalDatabase();

        logger.info(methodName, null, "storage.useWAL", System.getProperty("storage.useWAL"));
        logger.info(methodName, null, "tx.useLog", System.getProperty("tx.useLog"));
        if ( true ) {
            try {

                OGlobalConfiguration.dumpConfiguration(System.out);

                hmd = new HistoryManagerDb(logger);

                if ( true ) loadCheckpoint();

                OGlobalConfiguration.USE_WAL.setValue(false);

                OGlobalConfiguration.dumpConfiguration(System.out);


                // ---------- Load job history
                if ( true ) loadJobs();            

                // ---------- Load reservation history
                if ( true ) loadReservations();                         


                // ---------- Load service isntance and AP history
                if ( true ) loadServices();

                // ---------- Load service registry
                ssd = new StateServicesDb();
                ssd.init(logger);
                loadServiceRegistry(serviceRegistry, false);
                try {
                    ssd.shutdown(); 
                } catch ( Exception e ) {
                    e.printStackTrace();
                }

                // ---------- Load service registry history
                ssd = new StateServicesDb();
                ssd.init(logger);
                loadServiceRegistry(serviceRegistryHistory, true);      

                OGlobalConfiguration.USE_WAL.setValue(true);
                if ( true ) loadCheckpoint();



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
    	DbLoader dbl = null;
        try {
            dbl = new DbLoader();
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
                    } catch(Exception e) {
                        logger.info(methodName, null, e);
                    } finally {
                        closeStream(in);
                        closeStream(fis);
                    }
                    hmd.saveJobUnsafe(job);

                } catch ( InterruptedException e ) {
                    return;
                } catch(Exception e) {
                    logger.info(methodName, null, e);
                } 

                synchronized(ids) {
                    ids.add(job.getDuccId().getFriendly());
                }
                counter.getAndDecrement();
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
                    } catch(Exception e) {
                        logger.info(methodName, null, e);
                    } finally {
                        closeStream(in);
                        closeStream(fis);
                    }
                    hmd.saveServiceUnsafe(svc);

                } catch ( InterruptedException e ) {
                    return;
                } catch ( Exception e ){
                    logger.info(methodName, null, e);
                }

                synchronized(ids) {
                    ids.add(svc.getDuccId().getFriendly());
                }
                counter.getAndDecrement(); 
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
                    } catch(Exception e) {
                        logger.info(methodName, null, e);
                    } finally {
                        closeStream(in);
                        closeStream(fis);
                    }
                    hmd.saveReservationUnsafe(res);

                } catch ( InterruptedException e ) {
                    return;
                } catch ( Exception e ){
                    logger.info(methodName, null, e);
                }

                synchronized(ids) {
                    ids.add(res.getDuccId().getFriendly());
                }
                counter.getAndDecrement();
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
