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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.lang.reflect.Field;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.common.DuccWorkMap;
import org.apache.uima.ducc.transport.event.common.IDuccWorkJob;
import org.apache.uima.ducc.transport.event.common.IDuccWorkReservation;
import org.apache.uima.ducc.transport.event.common.IDuccWorkService;

/**
 * Toy orientdb loader to load a historydb from ducc history
 */

public class DbListLoader
{
    DuccLogger logger = DuccLogger.getLogger(DbLoader.class, "DBLOAD");
    String DUCC_HOME;

    HistoryManagerDb hmd = null;
    StateServicesDb  ssd = null;

    // String history_url = "remote:localhost/DuccHistory";
    // String state_url   = "plocal:/home/challngr/ducc_runtime_db/database/databases/DuccHistoryT";
    String state_url   = null;
    String input_list = null;
    int nthreads = 20;
    AtomicInteger counter = new AtomicInteger(0);

    
    public DbListLoader(String from, String to)
        throws Exception
    {
    	//String methodName = "<ctr>";
        DUCC_HOME = System.getProperty("DUCC_HOME");        
        if ( DUCC_HOME == null ) {
            System.out.println("System proprety -DDUCC_HOME must be set.");
            System.exit(1);
        }
        
        File f = new File(from);
        if ( ! f.exists() ) {
            System.out.println("Input file does not exist or cannot be read.");
            System.exit(1);
        }
        input_list = from;

        f = new File(to);
        if ( ! f.isDirectory() ) {
            System.out.println("'to' must be a directory");
            System.exit(1);
        }

        String databasedir =  to + "/database/databases";
        //String databasename = databasedir + "/DuccState";

        state_url = "plocal:" + databasedir + "/DuccState";
        state_url = "remote:bluej538/DuccState";
        System.setProperty("ducc.state.database.url", state_url);
    }

    void closeStream(InputStream in)
    {
        try { in.close(); } catch(Exception e) {}
    }

    void loadJob(String f)
    {
    	String methodName = "loadJob";
        IDuccWorkJob job = null;

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
        }
    }

    void loadReservation(String f)
    {
    	String methodName = "loadReservation";
        IDuccWorkReservation res = null;
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
    }

    void loadService(String f)
    {
    	String methodName = "loadService";
        IDuccWorkService svc = null;

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
        }            
    }

	void loadCheckpoint(String ckpt)
    	throws Exception
    {
        String methodName = "loadCheckpoint";

        File f = new File(ckpt);
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
			fis = new FileInputStream(ckpt);
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
    	//String methodName = "run";
        //long now = System.currentTimeMillis();

        hmd = new HistoryManagerDb(logger);

        BufferedReader br = new BufferedReader(new FileReader(input_list));
        String f = null;
        while ( (f = br.readLine()) != null ) {
            if ( f.endsWith(".dwj") ) { loadJob(f); continue; }
            if ( f.endsWith(".dwr") ) { loadReservation(f); continue; }
            if ( f.endsWith(".dws") ) { loadService(f); continue; }
            if ( f.endsWith(".ckpt") ) { loadCheckpoint(f); continue; }
        }
        br.close();
    }

    public static void main(String[] args)
    {
        if ( args.length != 2 ) {
            System.out.println("USage: DbLoader from to");
            System.out.println("");
            System.out.println("Where:");
            System.out.println("   from");        
            System.out.println("      is a file with the fully-qualified names of files you want loaded into he db.");
            System.out.println("   to");
            System.out.println("      is the DUCC_HOME contining the new database");
            System.out.println("");
            System.out.println("THe database must be started and initialized with the correct schema.");
            System.exit(1);
        }

            
    	DbListLoader dbl = null;
        try {
            dbl = new DbListLoader(args[0], args[1]);
            dbl.run();
        } catch ( Exception e  ) {
            e.printStackTrace();
        } 
    }

}
