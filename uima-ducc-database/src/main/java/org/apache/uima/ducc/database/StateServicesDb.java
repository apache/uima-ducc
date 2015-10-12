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
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.uima.ducc.common.persistence.services.IStateServices;
import org.apache.uima.ducc.common.persistence.services.StateServicesDirectory;
import org.apache.uima.ducc.common.persistence.services.StateServicesSet;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccProperties;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.database.DbConstants.DbCategory;
import org.apache.uima.ducc.database.DbConstants.DbVertex;

import com.tinkerpop.blueprints.impls.orient.OrientVertex;

public class StateServicesDb
    implements IStateServices
{
	private DuccLogger logger = null;
    private DbManager dbManager;

    public StateServicesDb()
    {
    }

    private boolean init(String dburl)
        throws Exception
    {
        // log4j issue - the caller must pass in a logger configured correctly for the component
        //               to which it belongs or it won't get sent to the right appender.

        boolean ret = false;
        try {
            dbManager = new DbManager(dburl, logger);
            dbManager.init();
            ret = true;
        } catch ( Exception e ) {
            // logger.error("StateServicesDb", null, "Cannot open the service database:", e);
            throw e;
        }
        return ret;
    }

    public boolean init(DuccLogger logger)
    	throws Exception
    {
    	this.logger = logger;
        String stateUrl = System.getProperty("ducc.state.database.url");
        return init(stateUrl);
    }

    /** 
     * Helper for restoring service registrations.  This looks in the non-history part of the DB.
     */
    private List<Long> getListOfType(DbVertex type)
    {
        String methodName = "getSvcList";

        if ( dbManager == null ) {
            logger.error(methodName, null, "Service database is not initialized.");
            return new ArrayList<Long>();    // avoid NPE in caller
        }

        List<Long> ret = null;
        DbHandle h = null;
		try {
			h = dbManager.open();
            ret = h.listObjectsOfType(type, DbCategory.SmReg);
		} catch (Throwable e) {
            logger.error(methodName, null, e);
		} finally {
            if ( h != null ) h.close();
        }

        return ret;
    }

    /**
     * Return a list of service property file names.  Must query the db every time.
     */
    public List<Long> getSvcList()
    {
        return getListOfType(DbVertex.ServiceReg);
    }
    
    /**
     * Return a list of sersvice meta file names.  Must query the db every time.
     */
    public List<Long> getMetaList()
    {
        return getListOfType(DbVertex.ServiceMeta);
    }

    /**
     * This is adapted from the file-based version and as such, perhaps should be named better.
     *
     * This reads the entire (live, non-history) service registration into an object called
     * StateServicesDirectory.  This in turn contains a map of StateServicesSet objects.  Each
     * StateServiceSet contains two properties files, one for the submitted properties, and one
     * for the service meta properties (SM state).
     */
    public StateServicesDirectory getStateServicesDirectory()
        throws Exception
    {
        String methodName = "getStateServicesDirectory";
        long now = System.currentTimeMillis();

        StateServicesDirectory ret = new StateServicesDirectory();

        if ( dbManager== null ) {
            logger.error(methodName, null, "Service database is not initialized.");
            return ret;    // avoid NPE in caller
        }

        DbHandle h = dbManager.openNoTx();
        try {
            Map<Long, Properties> svcset  = h.getPropertiesForTypeSel(DbVertex.ServiceReg , DbCategory.SmReg);
            Map<Long, Properties> metaset = h.getPropertiesForTypeSel(DbVertex.ServiceMeta, DbCategory.SmReg);

            for ( Long k : svcset.keySet() ) {
                logger.trace(methodName, null, "Handling key", k);
                DuccProperties sp = new DuccProperties(svcset.get(k));                
                DuccProperties mp = new DuccProperties(metaset.get(k));
                StateServicesSet sss = new StateServicesSet();
                sss.put(svc, sp);
                sss.put(meta, mp);
                
                ret.put(k, sss);            
            }
        } finally {
            if ( h != null ) h.close();
        }
        
        logger.info(methodName, null, "Time to read service registy", System.currentTimeMillis() - now);
        return ret;
    }

    /**
     * Save the src and meta propeties into the non-history part of the DB. 
     *
     * @param serviceID The SM-assigned duccid for the service registration.
     * @param svc_props The "user-submitted" properties set defining the service.
     * @param meta-props The SM-generated properties contain service state
     * @param safe This is for the loader.  If 'true', then don't do anything if
     *             there is already something in the DB for the service.  If 'false
     *             just blindly put it into the DB.
     * @NOTE The OrientDb SQL has a create-or-modify primitive.  Is there something
     *       equivalent in the Java interface?  If so, we should modify this to use it
     *       and can then eliminate the 'safe' flag.
     */
    boolean storePropertiesInternal (DuccId serviceId, Properties svc_props, Properties meta_props, boolean safe, DbCategory category) 
    {
        String methodName = "storePropertiesInternal";
        DbHandle h = null;

        try {
            if ( safe ) {
                h = dbManager.open(); 
            } else {
                h = dbManager.openNoTx();
            }

            if ( meta_props.containsKey("meta_dbid")) return false; // if it's assigned, it came from the db so we know it's already there            

            Long id = serviceId.getFriendly();            
            OrientVertex ov_svc = h.createProperties(DbConstants.DUCCID, id, DbVertex.ServiceReg , category, svc_props);
            OrientVertex ov_meta = h.createProperties(DbConstants.DUCCID, id, DbVertex.ServiceMeta, category, meta_props);

            Object dbid = ov_svc.getId();
            meta_props.put("svc_dbid", dbid);
            ov_meta.setProperty("svc_dbid", dbid);

            dbid = ov_meta.getId();
            meta_props.put("meta_dbid", dbid);
            ov_meta.setProperty("meta_dbid", dbid);

            h.commit();
            return true;
        } catch ( Exception e ) {
            logger.error(methodName, serviceId, "ROLLBACK: ", e);
            if ( h != null ) h.rollback();
            return false;
        } finally {
            if ( h != null ) h.close();
        }
    }

    /**
     * Save the props into the database, don't check to see if they're there already.  Used by the
     * loader for converting old registries to the db.
     */
    public boolean storePropertiesUnsafe(DuccId serviceId, Properties svc_props, Properties meta_props, DbCategory category) 
    {
        return storePropertiesInternal(serviceId, svc_props, meta_props, false, category);
    }

    /**
     * Save the props into the db.  If the object exists don't overwrite it, and return an error.
     * The only non-error return is if the object doesn't already exist, and it is safely committed.
     *
     * This is used by the SM on initial service registration only.
     */
    public boolean storeProperties(DuccId serviceId, Properties svc_props, Properties meta_props) 
    {
        return storePropertiesInternal(serviceId, svc_props, meta_props, true, DbCategory.SmReg);
    }


    /**
     * The registration is removed, move it to the history part of the DB.
     */
    public void  moveToHistory(DuccId serviceId, Properties job_props, Properties meta_props)
    {
        // All we need to do is re-sync the final properties, and be sure to set DUCC_HISTORY to false
        String methodName = "moveToHistory";
        DbHandle h = null;
        try {
        
            h = dbManager.open();        // get new connection from the pool
            Object svc_dbid = meta_props.get("svc_dbid");
            Object meta_dbid = meta_props.get("meta_dbid");
            OrientVertex obj_reg  = h.updateProperties(svc_dbid, job_props);
            OrientVertex obj_meta = h.updateProperties(meta_dbid, meta_props);

            h.changeCategory(obj_reg,  DbCategory.History);
            h.changeCategory(obj_meta, DbCategory.History);
            h.commit();

            // h.syncProperties(job_props, DbVertex.ServiceReg, id, DbCategory.History);
            // h.syncProperties(meta_props, DbVertex.ServiceMeta, id, DbCategory.History);
        }  catch ( Exception e ) {
            logger.error(methodName, serviceId, "ROLLBACK: ", e);
            if ( h != null ) h.rollback();
        } finally {
            if ( h != null ) h.close();
        }
    }
    
    /**
     * Helper method to Update the indicated properties file, in the non-history part of the db.
     * This is most often called by SM to update the service meta after state changes.
     *
     * @param serviceId The SM-assigned DUCC ID for the service registration.
     * @param props The properties file to save.  Usually it's just the meta but if
     *              the service is being modified, it could also be the registration.
     * @param type The type enum, ususally Service or ServiceMeta.
     */
    private boolean updateProperties(Object dbid, DuccId serviceId, Properties props, DbVertex type)
    {
        String methodName = "updatePropeties";
        DbHandle h = null;
        try {            
            h = dbManager.open();
            h.updateProperties(dbid, props);
            // h.synchronizeProperties(DbConstants.DUCCID, serviceId.getFriendly(), type,  DbCategory.SmReg, props);
            // h.synchronizeProperties(dbid, props);

            h.commit();
            // h.syncProperties(props, type, serviceId.getFriendly(), DbCategory.SmReg);            
            return true;
        } catch ( Exception e ) {
            logger.error(methodName, serviceId, "ROLLBACK:", e);
            if ( h != null ) h.rollback();
            return false;            
        } finally {            
            if ( h != null ) h.close();
        }
    }
    
    /**
     * Update the service registration.
     */
    public boolean updateJobProperties(Object dbid, DuccId serviceId, Properties props) 
    {
        return updateProperties(dbid, serviceId, props, DbVertex.ServiceReg);
    }

    /**
     * Update the service meta data.
     */
    public boolean updateMetaProperties(Object dbid, DuccId serviceId, Properties props) 
    {
        return updateProperties(dbid, serviceId, props, DbVertex.ServiceMeta);
    }

    /**
     * Close and discard the database connection pool.
     */
    public void shutdown() 
    {
        dbManager.shutdown();
    } 
}
