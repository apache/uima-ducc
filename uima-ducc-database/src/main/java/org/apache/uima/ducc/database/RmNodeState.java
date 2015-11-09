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

import java.util.Map;

import org.apache.uima.ducc.common.persistence.rm.IRmPersistence;
import org.apache.uima.ducc.common.persistence.rm.RmPersistenceFactory;
import org.apache.uima.ducc.common.utils.DuccLogger;

public class RmNodeState
{
    DuccLogger logger = DuccLogger.getLogger(RmNodeState.class, "State");
    String dburl = null;
    
    RmNodeState(String dburl)
    {
        this.dburl = dburl;
    }

    void run()
        throws Exception
    {
        IRmPersistence persistence = RmPersistenceFactory.getInstance(this.getClass().getName(), "RM");
        
        try {
			Map<String, Map<String, Object>> state = persistence.getAllMachines();
			for ( String node : state.keySet() ) {
			    StringBuffer buf = new StringBuffer(node);
			    buf.append(": ");
			    Map<String, Object> st = state.get(node);
			    for ( String k : st.keySet() ) {
			        buf.append(k);
			        buf.append("[");
			        buf.append(st.get(k).toString());
			        buf.append("] ");
			    }
			    System.out.println(buf.toString());
			}
		} finally {
            persistence.close();
        }
    }

    public static void main(String[] args)
    {
        if ( args.length != 1 ) {
            System.out.println("Usage: RmNodeState <dburl>");
            System.exit(1);
        }
        System.setProperty("ducc.state.database.url", args[0]);

        RmNodeState rns = new RmNodeState(args[0]);
        try {
            rns.run();
        } catch ( Exception e ) {
            e.printStackTrace();
        }
    }
}
