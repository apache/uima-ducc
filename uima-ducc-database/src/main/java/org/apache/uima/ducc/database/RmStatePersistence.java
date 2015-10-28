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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.uima.ducc.common.persistence.rm.IRmPersistence;
import org.apache.uima.ducc.common.utils.DuccLogger;

import com.datastax.driver.core.SimpleStatement;

/**
 * Manage saving and fetching of transient RM state.  The primary consumer is
 * intended to be the WS. 
 */
public class RmStatePersistence
    implements IRmPersistence
{

    DbManager dbManager = null;
    DuccLogger logger = null;
    public RmStatePersistence()
    {
    }

    private boolean init(String dburl)
        throws Exception
    {
        boolean ret = false;
        try {
            dbManager = new DbManager(dburl, logger);
            dbManager.init();
            ret = true;
        } catch ( Exception e ) {
            throw e;
        }
        return ret;
    }

    public void init(DuccLogger logger)
    	throws Exception
    {
    	this.logger = logger;
        String stateUrl = System.getProperty("ducc.state.database.url");
        init(stateUrl);
    }

    public void shutdown()
    {
        dbManager.shutdown();
    }

    public void clear()
        throws Exception
    {
        String methodName = "clear";
        DbHandle h = null;
        try {
            h = dbManager.open();
            h.execute("TRUNCATE ducc.rmnodes");
        } catch ( Exception e ) {
            logger.error(methodName, null, "Cannot clear the database.", e);
        } 
    }

    static List<SimpleStatement> mkSchema()
    	throws Exception
    {
        List<SimpleStatement> ret = new ArrayList<SimpleStatement>();

        StringBuffer buf = new StringBuffer("CREATE TABLE IF NOT EXISTS ducc." + RmProperty.TABLE_NAME.pname() + " (");
        buf.append(DbUtil.mkSchema(RmProperty.values()));
        buf.append(")");    
        ret.add(new SimpleStatement(buf.toString()));
        return ret;
    }

    // static String[] mkSchemaItems()
    // {
    //     int size = RmProperty.values().length;
    //     String[] ret = new String[size];
    //     int ndx = 0;

    //     for ( RmProperty n: RmProperty.values() ) {
    //         String s = n.pname();
    //         s = s + " " + DbUtil.typeToString(n.type());
    //         if ( n.isPrimaryKey() ) {
    //             s = s + " PRIMARY KEY";
    //         }
    //         ret[ndx++] = s;
    //     }
    //     return ret;
    // }

    public void createMachine(String m, Map<RmProperty, Object> props)
    	throws Exception
    {
        String methodName = "createMachine";
        DbHandle h = dbManager.open();
        try {           
            String cql = DbUtil.mkInsert("ducc.rmnodes", props);
            h.execute(cql);
        } catch ( Exception e ) {
            logger.error(methodName, null, "Error creating new record:", e);
        } 
    }

    public void setProperties(String node, Object... props)
    	throws Exception
    {
        String methodName = "setProperties";

        long now = System.currentTimeMillis();
        if (( props.length % 2) != 0 ) {
            throw new IllegalStateException("Set properties: number of properties must be even, instead was " + props.length);
        }

        DbHandle h = dbManager.open();

        try {           
            h.updateProperties("ducc.rmnodes", "WHERE name='" + node + "'", props);
        } catch ( Exception e ) {
            logger.error(methodName, null, "Problem setting properties");
        } finally {           
            logger.info(methodName, null, "Total time to update properties on", System.currentTimeMillis() - now);

        }
        
    }

    public void setProperty(String node, RmProperty k, Object v)
    	throws Exception
    {
        String methodName = "setProperty";

        DbHandle h = dbManager.open();

        try {           
            h.updateProperty("ducc.rmnodes", "name='" + node + "'", k.columnName(), v);
        } catch ( Exception e ) {
            logger.error(methodName, null, "Problem setting properties.");
        } 
        
    }
    
    public Properties getMachine(String m)
    	throws Exception
    {
    	return null;
    }
    
    public Map<String, Properties> getAllMachines()
    	throws Exception
   {
    	return new HashMap<String, Properties>();
   }
    
    public static void main(String[] args)
    {
    }

}
