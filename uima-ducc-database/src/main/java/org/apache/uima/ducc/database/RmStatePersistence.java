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
import java.util.Properties;

import org.apache.uima.ducc.common.persistence.rm.IRmPersistence;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.database.DbConstants.DbCategory;
import org.apache.uima.ducc.database.DbConstants.DbVertex;

import com.google.gson.Gson;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

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

    public void clear()
        throws Exception
    {
        String methodName = "clear";
        DbHandle h = null;
        try {
            h = dbManager.open();
            h.execute("DELETE VERTEX V where " + DbConstants.DUCC_DBCAT + "='" + DbCategory.RmState.pname() + "'");
        } catch ( Exception e ) {
            logger.error(methodName, null, "Cannot clear the database.", e);
        } finally {
            if ( h != null ) h.close();
        }
    }

    public String toGson(Object o)
    {
        // We need to define Instance creators and such so we do it in a common place
        Gson g = DbHandle.mkGsonForJob();
        return g.toJson(o);
    }

    public Object createMachine(String m, Properties props)
    	throws Exception
    {
        String methodName = "createMachine";
        DbHandle h = dbManager.open();
        Object ret = null;
        try {           
            OrientVertex v = h.createProperties(DbConstants.DUCC_DBNODE, m, DbVertex.RmNode,  DbCategory.RmState, props);
            ret = v.getId();
            h.commit();
        } catch ( Exception e ) {
            logger.error(methodName, null, "Update", m, "ROLLBACK: ", e);
            if ( h != null ) h.rollback();
        } finally {
            if ( h != null ) h.close();
        }
        return ret;
    }

    public void setProperties(Object dbid, String dbk, Object... props)
    	throws Exception
    {
        String methodName = "setProperties";

        long now = System.currentTimeMillis();
        if (( props.length % 2) != 0 ) {
            throw new IllegalStateException("Set properties: number of properties must be even, instead was " + props.length);
        }

        DbHandle h = dbManager.open();

        try {           
            h.updateProperties(dbid, props);
            h.commit();
        } catch ( Exception e ) {
            logger.error(methodName, null, "Update", dbk, "ROLLBACK: ", e);
            if ( h != null ) h.rollback();
        } finally {
            if ( h != null ) h.close();
            logger.info(methodName, null, "Total time to update properties on", dbid.toString(), System.currentTimeMillis() - now);

        }
        
    }

    public void setProperty(Object dbid, String dbk, RmPropName k, Object v)
    	throws Exception
    {
        String methodName = "setProperty";
        long now = System.currentTimeMillis();

        DbHandle h = dbManager.open();

        try {           
            h.updateProperty(dbid, k.pname(), v);
            h.commit();
        } catch ( Exception e ) {
            logger.error(methodName, null, "Update", dbk, "ROLLBACK: ", e);
            if ( h != null ) h.rollback();
        } finally {
            if ( h != null ) h.close();
            logger.info(methodName, null, "Total time to update property on", dbid.toString(), System.currentTimeMillis() - now);
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

/**
    String name;
    String nodepoolId;
    long memory;
    int order;
    boolean blacklisted;                                         // UIMA-4142
    boolean online;                                              // UIMA-4234
    boolean responsive;                                          // UIMA-4234




   Properties file for a node
   name = string
   ip   = string
   state = <state>
      states: vary status: online | offline
              reporting  : present | absent
   nodepool = string
   quantum = string
   class = string
   scheduling policy = string
   scheduled work = list of duccids of work on the node
 */
