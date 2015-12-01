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

import org.apache.uima.ducc.common.persistence.rm.IDbJob;
import org.apache.uima.ducc.common.persistence.rm.IDbShare;
import org.apache.uima.ducc.common.persistence.rm.IRmPersistence;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.id.DuccId;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.exceptions.NoHostAvailableException;

/**
 * Manage saving and fetching of transient RM state.  The primary consumer is
 * intended to be the WS. 
 */
public class RmStatePersistence
    implements IRmPersistence
{

    DbManager dbManager = null;
    DuccLogger logger = null;
    static final String RM_NODE_TABLE  = RmNodes.TABLE_NAME.pname();
    static final String RM_SHARE_TABLE = RmShares.TABLE_NAME.pname();
    static final String RM_LOAD_TABLE  = RmLoad.TABLE_NAME.pname();

    // Prepared statements to manage the RmNodes table

    // Prepared statements to manage the RmShares table
    PreparedStatement shareAddPrepare = null;
    PreparedStatement shareDelPrepare = null;
    PreparedStatement updateFixedPrepare = null;
    PreparedStatement updatePurgedPrepare = null;
    PreparedStatement updateEvictedPrepare = null;
    PreparedStatement updateSharePrepare = null;

    // Prepared statements to manage the RmLoad table
    PreparedStatement addJobPrepare = null;
    PreparedStatement deleteJobPrepare = null;
    PreparedStatement updateDemandPrepare = null;

    public RmStatePersistence()
    {
    }

    private boolean init(String dburl)
        throws Exception
    {
    	String methodName = "init";
        boolean ret = false;
        while ( true ) {
            try {
                dbManager = new DbManager(dburl, logger);
                dbManager.init();
                ret = true;
                break;
            } catch ( NoHostAvailableException e ) {
                logger.error(methodName, null, "Cannot contact database.  Retrying in 5 seconds.");
                Thread.sleep(5000);
            } catch ( Exception e ) {
                logger.error(methodName, null, "Errors contacting database.  No connetion made.", e);
                ret = false;
                break;
            }
        }
        return ret;
    }

    public void init(DuccLogger logger)
    	throws Exception
    {
    	this.logger = logger;
        String stateUrl = System.getProperty("ducc.state.database.url");
        init(stateUrl);
        DbHandle h = dbManager.open();

        // For creating a new share
        // These are upserts - sometimes the shares are updated before they're actually added to the DB.
        shareAddPrepare = h.prepare("UPDATE " + RM_SHARE_TABLE + " SET  uuid=?, share_order=?, blacklisted=?, evicted=?, fixed=?, purged=?, quantum=?, jobtype=? WHERE node=? AND ducc_dbid=? and job_id=?");
        shareDelPrepare = h.prepare("DELETE FROM " + RM_SHARE_TABLE + " WHERE node = ? and ducc_dbid = ? and job_id = ?;");
        updateFixedPrepare = h.prepare("UPDATE " + RM_SHARE_TABLE + " SET fixed = ? WHERE node = ? AND ducc_dbid = ? and job_id = ?");
        updatePurgedPrepare = h.prepare("UPDATE " + RM_SHARE_TABLE + " SET purged = ? WHERE node = ? AND ducc_dbid = ? and job_id = ?");
        updateEvictedPrepare = h.prepare("UPDATE " + RM_SHARE_TABLE + " SET evicted = ? WHERE node = ? AND ducc_dbid = ? and job_id = ?");
        updateSharePrepare = h.prepare("UPDATE " + RM_SHARE_TABLE + " SET investment = ?, state = ?, init_time = ?, pid = ?  WHERE node = ? AND ducc_dbid = ? and job_id = ?");

        // An upsert
        addJobPrepare = h.prepare("UPDATE " + RM_LOAD_TABLE + " SET class = ?, user = ?, memory = ?, jobtype = ? WHERE job_id = ?");
        deleteJobPrepare = h.prepare("DELETE FROM " + RM_LOAD_TABLE + " WHERE job_id=?");
        updateDemandPrepare = h.prepare("UPDATE " + RM_LOAD_TABLE + " SET demand = ?, occupancy = ?, state = ? WHERE job_id=?");
    }

    public void close()
    {
        if ( dbManager != null ) dbManager.shutdown();
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
            h.execute("TRUNCATE " + RM_SHARE_TABLE);
            h.execute("TRUNCATE " + RM_LOAD_TABLE);
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

        buf = new StringBuffer("CREATE TABLE IF NOT EXISTS " + RM_SHARE_TABLE + " (");
        buf.append(DbUtil.mkSchema(RmShares.values()));
        buf.append(")");
        ret.add(new SimpleStatement(buf.toString()));
        indexes = DbUtil.mkIndices(RmShares.values(), RM_SHARE_TABLE);
        for ( String s : indexes ) {
            ret.add(new SimpleStatement(s));
        }

        buf = new StringBuffer("CREATE TABLE IF NOT EXISTS " + RM_LOAD_TABLE + " (");
        buf.append(DbUtil.mkSchema(RmLoad.values()));
        buf.append(")");
        ret.add(new SimpleStatement(buf.toString()));
        indexes = DbUtil.mkIndices(RmShares.values(), RM_SHARE_TABLE);
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

    public void addAssignment(String node, DuccId jobid, IDbShare s, int quantum, String type)
    	throws Exception
    {
        DbHandle h = dbManager.open();
        h.saveObject(shareAddPrepare, s.getId().getUUID(), s.getShareOrder(), s.isBlacklisted(), s.isEvicted(), s.isFixed(), s.isPurged(), quantum, type, node, s.getId().getFriendly(), jobid.getFriendly() ); 
    }

    public void removeAssignment(String node, DuccId jobid, IDbShare s)
    	throws Exception
    {
    	DbHandle h = dbManager.open();
        h.execute(shareDelPrepare, node, s.getId().getFriendly(), jobid.getFriendly());
    }

    public void setFixed(String node, DuccId shareId, DuccId jobId, boolean val) 
        throws Exception
    {
    	DbHandle h = dbManager.open();
        h.execute(updateFixedPrepare, val, node, shareId.getFriendly(), jobId.getFriendly());
    }

    public void setPurged(String node, DuccId shareId, DuccId jobId, boolean val) 
        throws Exception
    {
    	DbHandle h = dbManager.open();
        h.execute(updatePurgedPrepare, val, node, shareId.getFriendly(), jobId.getFriendly());
    }

    public void setEvicted(String node, DuccId shareId, DuccId jobId, boolean val) 
        throws Exception
    {
    	DbHandle h = dbManager.open();
        h.execute(updateEvictedPrepare, val, node, shareId.getFriendly(), jobId.getFriendly());
    }

    public void updateShare(String node, DuccId shareid, DuccId jobid, long investment, String state, long init_time, long pid) 
        throws Exception
    {
    	DbHandle h = dbManager.open();
        h.execute(updateSharePrepare, investment, state, init_time, pid, node, shareid.getFriendly(), jobid.getFriendly());
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
            Map<String, Object> mach = DbUtil.getProperties(RmNodes.values(), r);
            ret.put((String)mach.get(RmNodes.Name.pname()), mach);
        }
        return ret;
   }

    public void addJob(IDbJob j) 
        throws Exception
    {
    	DbHandle h = dbManager.open();
        h.execute(addJobPrepare, j.getClassName(), j.getUserName(), j.getMemory(), j.getShortType(), j.getFriendlyId());
    }

    public void deleteJob(IDbJob j) 
        throws Exception
    {
    	DbHandle h = dbManager.open();
        h.execute(deleteJobPrepare, j.getFriendlyId());        
    }

    public void updateDemand(IDbJob j)
    	throws Exception
    {
    	DbHandle h = dbManager.open();
        // queryDemand returns the number of processes wanted by the job, of the job's memory size
        // The occupancy is converted from qshares to nshares (processes) for the db.
        h.execute(updateDemandPrepare, j.queryDemand(), (j.countOccupancy() / j.getShareOrder()), j.getState(), j.getFriendlyId());
    }

    public static void main(String[] args)
    {
    }

}
