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
import org.apache.uima.ducc.common.utils.id.DuccId;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
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
    static final String RM_NODE_TABLE = RmNodes.TABLE_NAME.pname();

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

    public void close()
    {
        dbManager.shutdown();
        dbManager = null;
    }

    public void clear()
        throws Exception
    {
        String methodName = "clear";
        DbHandle h = null;
        try {
            h = dbManager.open();
            h.execute("TRUNCATE " + RM_NODE_TABLE);
        } catch ( Exception e ) {
            logger.error(methodName, null, "Cannot clear the database.", e);
        } 
    }

    static List<SimpleStatement> mkSchema()
    	throws Exception
    {
        List<SimpleStatement> ret = new ArrayList<SimpleStatement>();
        
        StringBuffer buf = new StringBuffer("CREATE TABLE IF NOT EXISTS " + RM_NODE_TABLE + " (");
        buf.append(DbUtil.mkSchema(RmNodes.values()));
        buf.append(")");
        ret.add(new SimpleStatement(buf.toString()));
        List<String> indexes = DbUtil.mkIndices(RmNodes.values(), RM_NODE_TABLE);
        for ( String s : indexes ) {
            ret.add(new SimpleStatement(s));
        }
        return ret;
    }

    public void createMachine(String m, Map<RmNodes, Object> props)
    	throws Exception
    {
        String methodName = "createMachine";
        DbHandle h = dbManager.open();
        try {           
            String cql = DbUtil.mkInsert(RM_NODE_TABLE, props);
            h.execute(cql);
        } catch ( Exception e ) {
            logger.error(methodName, null, "Error creating new record:", e);
        } 
    }

    public void setNodeProperties(String node, Object... props)
    	throws Exception
    {
        String methodName = "setProperties";

        long now = System.currentTimeMillis();
        if (( props.length % 2) != 0 ) {
            throw new IllegalStateException("Set properties: number of properties must be even, instead was " + props.length);
        }

        DbHandle h = dbManager.open();

        try {           
            h.updateProperties(RM_NODE_TABLE, "name='" + node + "'", props);
        } catch ( Exception e ) {
            logger.error(methodName, null, "Problem setting properties", e);
        } finally {           
            logger.info(methodName, null, "Total time to update properties on", System.currentTimeMillis() - now);

        }
        
    }

    public void setNodeProperty(String node, RmNodes k, Object v)
    	throws Exception
    {
        String methodName = "setProperty";

        DbHandle h = dbManager.open();

        try {           
            h.updateProperty(RM_NODE_TABLE, "name='" + node + "'", k.columnName(), v);
        } catch ( Exception e ) {
            logger.error(methodName, null, "Problem setting properties:", e);
        } 
        
    }

    public void addAssignment(String node, DuccId jobid, DuccId shareid)
    {
    }

    public void removeAssignment(String node, DuccId jobid, DuccId shareid)
    {
    }

    public Properties getMachine(String m)
    	throws Exception
    {
    	return null;
    }
    
    public Map<String, Map<String, Object>> getAllMachines()
    	throws Exception
    {
    	String methodName = "getAllMachiens";
        Map<String, Map<String, Object>> ret = new HashMap<String, Map<String, Object>>();
        String cql = "SELECT * FROM " + RM_NODE_TABLE;
        DbHandle h = dbManager.open();
        ResultSet rs = h.execute(cql);
        for ( Row r : rs ) {
            Map<String, Object> mach = new HashMap<String, Object>();
            // We don't expect any nulls in this table
            for ( RmNodes n : RmNodes.values() ) {
                if ( n.isPrivate() ) continue;
                if ( n.isMeta()    ) continue;
                switch ( n.type() ) {

                    case String: {
                        String  v = r.getString(n.columnName());
                        mach.put(n.pname(), v);
                        if ( n == RmNodes.Name ) {
                            ret.put(v, mach);                            
                        }
                    }
                    break;

                    case Integer: {
                        int v = r.getInt(n.columnName());
                        mach.put(n.pname(), v);
                    }
                    break;

                    case Boolean: {
                        boolean v = r.getBool(n.columnName());
                        mach.put(n.pname(), v);
                    }
                    break;

                    default:
                        logger.warn(methodName, null, "Unexpected value in db:", n.pname(), "type", n.type(), "is not recognized.");
                        break;
                }
            }
        }
        return ret;
   }
    
    public static void main(String[] args)
    {
    }

}
