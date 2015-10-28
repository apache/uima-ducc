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
import java.util.UUID;

import org.apache.uima.ducc.common.persistence.IDbProperty;
import org.apache.uima.ducc.common.persistence.services.IStateServices;
import org.apache.uima.ducc.common.persistence.services.StateServicesDirectory;
import org.apache.uima.ducc.common.persistence.services.StateServicesSet;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccProperties;
import org.apache.uima.ducc.common.utils.id.DuccId;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SimpleStatement;

public class StateServicesDb
    implements IStateServices
{
	private DuccLogger logger = null;
    private DbManager dbManager;

    private static final String SVC_TABLE = "ducc." + SvcRegProps.TABLE_NAME.pname();
    private static final String META_TABLE = "ducc." + SvcMetaProps.TABLE_NAME.pname();
    // (My) db conventions are that everything must follow the conventions of IDbProperty.  SM
    // uses properties directly.  Maybe we'll change this some time.  For now, we need to efficiently
    // convert a Properties object into a Map keyed on IDbProperty; hence these two convenient maps
    // from string to property.
    Map<String, SvcRegProps> s2regProps = new HashMap<String, SvcRegProps>();
    Map<String, SvcMetaProps> s2metaProps = new HashMap<String, SvcMetaProps>();
    public StateServicesDb()
    {
        for ( SvcRegProps p : SvcRegProps.values() ) {
            s2regProps.put(p.pname(), p);
        }
        for ( SvcMetaProps p : SvcMetaProps.values() ) {
            s2metaProps.put(p.pname(), p);
        }
    }

    private boolean init(String dburl, DbManager dbm)
        throws Exception
    {
    	String methodName = "init";
        // log4j issue - the caller must pass in a logger configured correctly for the component
        //               to which it belongs or it won't get sent to the right appender.

        boolean ret = true;
        if ( dbm != null ) {
            this.dbManager = dbm;
        } else {
            try {
                dbManager = new DbManager(dburl, logger);
                dbManager.init();
                ret = true;
            } catch ( Exception e ) {
                logger.error(methodName, null, "Cannot open the state database:", e);
                throw e;
            }
        }

        return ret;
    }

    public boolean init(DuccLogger logger)
    	throws Exception
    {
    	this.logger = logger;
        String stateUrl = System.getProperty("ducc.state.database.url");
        return init(stateUrl, null);
    }

    // package only, for the loader
    boolean init(DuccLogger logger, DbManager dbManager)
    	throws Exception
    {
    	this.logger = logger;
        String stateUrl = System.getProperty("ducc.state.database.url");
        return init(stateUrl, dbManager);
    }

    private Map<Long, DuccProperties> getProperties(String tableid, IDbProperty[] props, boolean active)
    	throws Exception
    {
    	String methodName = "getProperties";
        Map<Long, DuccProperties> ret = new HashMap<Long, DuccProperties>();
        
        SimpleStatement s = new SimpleStatement( "SELECT * FROM " + tableid + " WHERE is_archived=" + active);
        s.setFetchSize(100);
        DbHandle h = dbManager.open();
        ResultSet rs = h.execute(s);
        for ( Row r: rs ) {
            long id = Long.parseLong(r.getString("numeric_id"));
            DuccId did = new DuccId(id);
            logger.debug(methodName, did, "Found properties in table", tableid);
            DuccProperties dp = new DuccProperties();
            for ( IDbProperty p : props ) {
                String val = null;
                if ( logger.isTrace() ) {
                    logger.info(methodName, null, "Fetching", p.pname(), "from", tableid);
                }
                if ( !(p.isPrivate() || p.isMeta()) ) {    // collect non-private / non-meta properties into dp
                    val = r.getString(p.columnName());                    
                    if ( val != null ) {
                        dp.put(p.pname(), val);
                    }
                }
                if ( p.isPrimaryKey() ) {                  // once we find the key we set the dp int return val
                	String k = null;
                    if ( val == null ) {
                        k = r.getString(p.columnName());
                    } else {
                        k = val;
                    }
                    ret.put(Long.parseLong(k), dp);
                }
            }
        }
    	return ret;
    }

    StateServicesDirectory fetchServices(boolean isArchived)      // pkg protection, for db utilities to use
        throws Exception
    {
        String methodName = "getStateServicesDirectory";
        long now = System.currentTimeMillis();

        StateServicesDirectory ret = new StateServicesDirectory();

        if ( dbManager== null ) {
            logger.error(methodName, null, "Service database is not initialized.");
            return ret;    // avoid NPE in caller
        }

        DbHandle h = null;
        try {
            h = dbManager.open();
            Map<Long, DuccProperties> svcset  = getProperties(SVC_TABLE, IStateServices.SvcRegProps.values(), isArchived);
            Map<Long, DuccProperties> metaset = getProperties(META_TABLE, IStateServices.SvcMetaProps.values(), isArchived);
            
            for ( Long k : svcset.keySet() ) {
                logger.trace(methodName, null, "Handling key", k);
                DuccProperties sp = svcset.get(k);                
                DuccProperties mp = metaset.get(k);
                StateServicesSet sss = new StateServicesSet();
                sss.put(svc, sp);
                sss.put(meta, mp);
                
                ret.put(k, sss);            
            }
        } catch ( Exception e ) {
            logger.error(methodName, null, "Cannot read service directory:", e);
        }

        logger.info(methodName, null, "Time to read service registy", System.currentTimeMillis() - now);
        return ret;
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
        return fetchServices(false);          // get the non-archived stsuff
    }

    Map<IDbProperty, Object> mkMap(DuccId did, String table, Map<String, ? extends IDbProperty> converter, Properties props)
    {
    	String methodName = "mkMap";

        // hmmm  - some old registrations didn't seem to get converted - we'll do it now
        String kk = "process_failures_limit";
        if ( props.containsKey(kk) ) {
            Object v = props.remove(kk);
            kk = SvcRegProps.instance_failures_limit.columnName();
            props.put(kk, v);
        }
        kk = "process_DD";
        if ( props.containsKey(kk) ) {
            Object v = props.remove(kk);
            kk = SvcRegProps.process_dd.columnName();
            props.put(kk, v);
        }
        kk = "process_classpath";
        if ( props.containsKey(kk) ) {
            Object v = props.remove(kk);
            kk = SvcRegProps.classpath.columnName();
            props.put(kk, v);
        }
        kk = "jvm_args";
        if ( props.containsKey(kk) ) {
            Object v = props.remove(kk);
            kk = SvcRegProps.process_jvm_args.columnName();
            props.put(kk, v);
        }
        
        Map<IDbProperty, Object> ret = new HashMap<IDbProperty, Object>();
        for ( Object k : props.keySet() ) {
            IDbProperty p = converter.get((String) k);
            if (p == null ) {
                logger.error(methodName, did, "Unrecognized property", k, "for table", table);
                continue;
            }
            String val = (String) props.get(k);
            val = val.replace("'", "''");          // must escape single quotes - this is how CQL and SQL do it
            ret.put(p, val);
        }
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
    public boolean storeProperties (DuccId serviceId, Properties svc_props, Properties meta_props)
    {
        String methodName = "storePropertiesInternal";
        DbHandle h = null;

        boolean ret = false;
        long now = System.currentTimeMillis();
        try {
            h = dbManager.open();
            long numeric = serviceId.getFriendly();
            UUID uuid =    serviceId.getUUID();
            // the utils want Maps of IDbProperty which give hints how to form the cql; properties don't
            Map<IDbProperty, Object> svc_map = mkMap(serviceId, SVC_TABLE, s2regProps, svc_props);
            Map<IDbProperty, Object> meta_map = mkMap(serviceId, META_TABLE, s2metaProps, meta_props);
            svc_map.put(SvcRegProps.numeric_id, numeric);
            svc_map.put(SvcRegProps.uuid, uuid.toString());

            svc_map.put(SvcRegProps.is_archived, "false");        // never archived when first put into db
            meta_map.put(SvcMetaProps.is_archived, "false");
            
            // in Cassandra, we use "batch" mode to get multiple inserts managed as a single transaction
            String cql = "";
            StringBuffer buf = new StringBuffer("BEGIN BATCH ");
            cql = DbUtil.mkInsert(SVC_TABLE, svc_map);
            buf.append(cql);
            buf.append("; ");
            cql = DbUtil.mkInsert(META_TABLE, meta_map);
            buf.append(cql);
            buf.append("; ");
            buf.append("APPLY BATCH");
            h.execute(buf.toString());
            
        } catch ( Exception e ) {
            logger.error(methodName, null, "Error storing props for new registration:", e);
            ret = false;
        } finally {
            logger.info(methodName, serviceId, "Time to create (2) proeprties files:", System.currentTimeMillis() - now);
        }
        return ret;
    }
    

    /**
     * The registration is removed, move it to the history part of the DB.
     */
    public boolean  moveToHistory(DuccId serviceId, Properties job_props, Properties meta_props)
    {
       // All we need to do is re-sync the final properties, and be sure to set DUCC_HISTORY to false
        String methodName = "moveToHistory";
        DbHandle h = null;
        try {
            
            h = dbManager.open();        // get new connection from the pool

            job_props.put(SvcRegProps.is_archived.pname(), "true");
            meta_props.put(SvcRegProps.is_archived.pname(), "true");
            StringBuffer buf = new StringBuffer("BEGIN BATCH ");
            buf.append("UPDATE ");
            buf.append(SVC_TABLE);
            buf.append(" SET ");
            buf.append(SvcRegProps.is_archived.columnName());
            buf.append("=true WHERE numeric_id='");
            buf.append(Long.toString(serviceId.getFriendly()));
            buf.append("';");

            buf.append("UPDATE ");
            buf.append(META_TABLE);
            buf.append(" SET ");
            buf.append(SvcMetaProps.is_archived.columnName());
            buf.append("=true WHERE numeric_id='");
            buf.append(Long.toString(serviceId.getFriendly()));
            buf.append("';");

            buf.append("APPLY BATCH");
            h.execute(buf.toString());

            return true;
        }  catch ( Exception e ) {
            logger.error(methodName, serviceId, "Error moving registration to history:", e);
            return false;
        } 
    }
    
    /**
     * Helper method to Update the indicated properties file, in the non-history part of the db.
     * This is most often called by SM to update the service meta after state changes.
     *
     * @param keyField - the IDbProperty identifying the row key
     * @parem table    - the String name of the table
     * @param key      - the String value of the key identifying the row
     * @param converter - either s2regProps or s2metaProps, identifying the String - to - IDbProperty converter
     * @param props The properties file to save.  Usually it's just the meta but if
     *              the service is being modified, it could also be the registration being updated.
     */
    private boolean updateProperties(DuccId serviceId, String table, String key, Map<String, ? extends IDbProperty> converter, Properties props)
    {
        String methodName = "updatePropeties";
        DbHandle h = null;
        try {            
            h = dbManager.open();
            Map<IDbProperty, Object> map = mkMap(serviceId, table, converter, props);
            Object[] updates = new Object[props.size()*2];
            int i = 0;
            for ( IDbProperty k : map.keySet() ) {
            	logger.info(methodName, null, "Updating", k.columnName(), "with", map.get(k));
                updates[i++] = k;
                updates[i++] = map.get(k);
            }
            
            h.updateProperties(table, key, updates);            
            return true;
        } catch ( Exception e ) {
            logger.error(methodName, null, "Unable to update properties for service", key, "table", table, ":", e);
            return false;            
        } 
    }
    
    /**
     * Update the service registration.
     */
    public boolean updateJobProperties(DuccId serviceId, Properties props) 
    {
        return updateProperties(serviceId, SVC_TABLE, "numeric_id='" + Long.toString(serviceId.getFriendly()) + "'", s2regProps, props);
    }

    /**
     * Update the service meta data.
     */
    public boolean updateMetaProperties(DuccId serviceId, Properties props) 
    {
        return updateProperties(serviceId, META_TABLE, "numeric_id='" + Long.toString(serviceId.getFriendly()) + "'", s2metaProps, props);
    }

    public void shutdown()
    {
        dbManager.shutdown();
    }

    static List<SimpleStatement> mkSchema()
    	throws Exception
    {
        List<SimpleStatement> ret = new ArrayList<SimpleStatement>();


        StringBuffer buf = new StringBuffer("CREATE TABLE IF NOT EXISTS " + SVC_TABLE + " (");
        buf.append(DbUtil.mkSchema(SvcRegProps.values()));
        buf.append(")");    
        ret.add(new SimpleStatement(buf.toString()));
        ret.add(new SimpleStatement("CREATE INDEX IF NOT EXISTS ON " + SVC_TABLE + "(" + SvcRegProps.is_archived.columnName() + ")"));

        buf = new StringBuffer("CREATE TABLE IF NOT EXISTS " + META_TABLE + " (");
        buf.append(DbUtil.mkSchema(SvcMetaProps.values()));
        buf.append(")");    
        ret.add(new SimpleStatement(buf.toString()));
        ret.add(new SimpleStatement("CREATE INDEX IF NOT EXISTS ON " + META_TABLE + "(" + SvcMetaProps.is_archived.columnName() +")"));

        return ret;
    }

    // static String[] mkSchemaForReg()
    // {

    //     int size = SvcRegProps.values().length;
    //     String[] ret = new String[size];

    //     int ndx = 0;

    //     for ( SvcRegProps p: SvcRegProps.values() ) {
    //         String s = p.pname();
    //         s = s + " " + DbUtil.typeToString(p.type());
    //         if ( p.isPrimaryKey() ) {
    //             s = s + " PRIMARY KEY";
    //         }
    //         ret[ndx++] = s;
    //     }
    //     return ret;

    // }

    // static String[] mkSchemaForMeta()
    // {
    //     int size = SvcMetaProps.values().length;
    //     String[] ret = new String[size];

    //     int ndx = 0;

    //     for ( SvcMetaProps p: SvcMetaProps.values() ) {
    //         String s = p.pname();
    //         s = s + " " + DbUtil.typeToString(p.type());
    //         if ( p.isPrimaryKey() ) {
    //             s = s + " PRIMARY KEY";
    //         }
    //         ret[ndx++] = s;
    //     }

    //     return ret;

    // }

}
