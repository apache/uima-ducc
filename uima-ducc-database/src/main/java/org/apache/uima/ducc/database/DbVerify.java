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
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.uima.ducc.common.persistence.services.StateServicesDirectory;
import org.apache.uima.ducc.common.persistence.services.StateServicesSet;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.id.DuccId;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SimpleStatement;

/**
 * Toy orientdb loader to load a historydb from ducc history
 */

public class DbVerify
{
    DuccLogger logger = DuccLogger.getLogger(DbLoader.class, "DBVERIFY");
    String DUCC_HOME;

    DbManager dbManager = null;
    long total_bytes = 0;

    public DbVerify()
        throws Exception
    {
    	//String methodName = "<ctr>";
        DUCC_HOME = System.getProperty("DUCC_HOME");        
        if ( DUCC_HOME == null ) {
            System.out.println("System proprety -DDUCC_HOME must be set.");
            System.exit(1);
        }        
    }


    void verify(String table)
    	throws Exception
    {
    	String methodName = "verify";
        DbHandle h = dbManager.open();
        SimpleStatement s = new SimpleStatement("SELECT * from " + table);
        //SimpleStatement s = new SimpleStatement("SELECT * from " + table + " LIMIT 10"); // for test and debug
        logger.info(methodName, null, "Fetch size", s.getFetchSize());
        s.setFetchSize(100);
        long now = System.currentTimeMillis();

        int counter = 0;
        int nbytes = 0;
        try {
            ResultSet rs = h.execute(s);
            for ( Row r : rs ) {
                counter++;
                ByteBuffer b = r.getBytes("work");
                byte[] bytes = b.array();
                nbytes += bytes.length;
                total_bytes += bytes.length;;

                ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
                ObjectInputStream ois = new ObjectInputStream(bais);
                Object o = ois.readObject();
                ois.close();            
                DuccId did = new DuccId(r.getLong("ducc_dbid"));
                
                logger.info(methodName, did, "found object class", o.getClass().getName(), "of type", r.getString("type"), "in table", table, "of size", bytes.length);
            }
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        logger.info(methodName, null, "Found", counter, "results. Total bytes", nbytes);
        logger.info(methodName, null, "Total time for", table, System.currentTimeMillis() - now);
    }
    
    void verifyServices()
    	throws Exception
    {
    	String methodName = "verify";
        int live = 0;
        int archived = 0;
        StateServicesDb sdb = new StateServicesDb();
        sdb.init(logger,dbManager);

        StateServicesDirectory ssd = sdb.fetchServices(true);          // first the archived stuff
        Map<Long, StateServicesSet>  svcmap = ssd.getMap();
        for ( Long id : svcmap.keySet() ) {
            DuccId did = new DuccId(id);
            archived++;
            logger.info(methodName, did, "Found an archived service.");
        }

        ssd = sdb.fetchServices(false);          // first the archived stuff
        svcmap = ssd.getMap();
        for ( Long id : svcmap.keySet() ) {
            DuccId did = new DuccId(id);
            logger.info(methodName, did, "Found a live service.");
            live++;
        }
        logger.info(methodName, null, "Found", live, "live services and", archived, "archived services.");

    }

    void run()
        throws Exception
    {
        String methodName = "run";
        long now = System.currentTimeMillis();
        String state_url = "bluej538";
        try {
            dbManager = new DbManager(state_url, logger);
            dbManager.init();

            verifyServices();

            if ( false ) verify("ducc.res_history");
            if ( false ) verify("ducc.svc_history");                
            if ( false ) verify("ducc.job_history");
            
        } finally {
            dbManager.shutdown();
        }
        logger.info(methodName, null, "Read", total_bytes, "bytes in",  System.currentTimeMillis() - now, "MS");
    }

    
    public static void main(String[] args)
    {
        DbVerify v = null;
        try {
            v = new DbVerify();
            v.run();
        } catch ( Exception e  ) {
            e.printStackTrace();
        } 
    }
}
