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

import org.apache.uima.ducc.common.persistence.IDbProperty;
import org.apache.uima.ducc.common.persistence.rm.IRmPersistence;
import org.apache.uima.ducc.common.persistence.rm.IRmPersistence.RmShares;
import org.apache.uima.ducc.common.persistence.rm.NullRmStatePersistence;
import org.apache.uima.ducc.common.persistence.rm.RmPersistenceFactory;
import org.apache.uima.ducc.common.utils.DuccLogger;


public class RmShareState
{
    DuccLogger logger = DuccLogger.getLogger(RmShareState.class, "State");
    String dburl = null;
    
    RmShareState(String dburl)
    {
        this.dburl = dburl;
    }
    
    void run()
        throws Exception
    {
        System.setProperty(DbManager.NOISE_PROPERTY, "false");
        IRmPersistence persistence = RmPersistenceFactory.getInstance(this.getClass().getName(), "RM");
        if ( persistence instanceof NullRmStatePersistence ) {
            System.out.println("Cannot get viable RM Persistance isntance.");
            return;
        }
        
        try {
            Map<String, Map<String, Object>> state = persistence.getAllShares();
            System.out.println(toJson(state));
        } catch ( Exception e ) {
            e.printStackTrace();
		} finally {
            // In "real life" you don't need to, and shouldn't, close the persistence until the process is ready to exit.
            persistence.close();
        }
    }

    String toJson(Map<String, Map<String, Object>> nodes)
    {
        StringBuffer buf = new StringBuffer("[");
        for ( Map<String, Object> vals : nodes.values() ) {
            buf.append("{");
            for ( IDbProperty p : RmShares.values() ) {
                if ( p.isMeta() ) continue;
                if ( p.isPrivate() ) continue;
                buf.append("'");
                buf.append(p.pname());
                buf.append("'");
                buf.append(":");
                switch(p.type()) {
                    case String:
                    case UUID:
                        buf.append("'");           // must quote strings
                        buf.append(vals.get(p.columnName()));
                        buf.append("'");                                   
                        break;
                    case Boolean:
                        boolean bv = (boolean) vals.get(p.columnName());
                        buf.append(bv ? "True" : "False"); // must pythonify the booleans
                        break;
                    case Integer:
                    case Long:
                    case Double:
                    	buf.append(vals.get(p.columnName()).toString());
                        break;
                    default:
                        // RmNodes doesn't use other types
                        break;
                }
                buf.append(",");
            }
            buf.append("},\n");            
        }    
        buf.append("]");
        return buf.toString();
    }

    public static void main(String[] args)
    {
        if ( args.length != 1 ) {
            System.out.println("Usage: RmShareState <dburl>");
            System.exit(1);
        }
        System.setProperty(DbManager.URL_PROPERTY, args[0]);

        RmShareState rns = new RmShareState(args[0]);
        try {
            rns.run();
        } catch ( Exception e ) {
            e.printStackTrace();
        }
    }
}
