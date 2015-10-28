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

import java.util.HashMap;
import java.util.Map;

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.common.DuccWorkMap;

/**
 * A test routine, read and verify the OR checkpoint file.
 */

public class ReadCkpt
{

    DuccLogger logger = DuccLogger.getLogger(DbLoader.class, "DBLOAD");
    String DUCC_HOME;
    DbManager dbManager = null;
    HistoryManagerDb hmd = null;

    public ReadCkpt()
    	throws Exception
    {
        DUCC_HOME = System.getProperty("DUCC_HOME");        
        if ( DUCC_HOME == null ) {
            System.out.println("System proprety -DDUCC_HOME must be set.");
            System.exit(1);
        }
        
        String state_url = "bluej538";
        System.setProperty("ducc.state.database.url", state_url);

        dbManager = new DbManager(state_url, logger);
        dbManager.init();

        hmd = new HistoryManagerDb();
    }

    public void run()
    {
        DuccWorkMap work = new DuccWorkMap();
        Map<DuccId, DuccId> processToJob = new HashMap<DuccId, DuccId>();

    }

    public static void main(String [] args)
    {
        try {
			ReadCkpt rc = new ReadCkpt();
			rc.run();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}


    }

}
