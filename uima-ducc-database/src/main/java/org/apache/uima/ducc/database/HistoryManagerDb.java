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
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.uima.ducc.common.Pair;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.common.DuccWorkMap;
import org.apache.uima.ducc.transport.event.common.DuccWorkReservation;
import org.apache.uima.ducc.transport.event.common.IDuccTypes.DuccType;
import org.apache.uima.ducc.transport.event.common.IDuccWork;
import org.apache.uima.ducc.transport.event.common.IDuccWorkJob;
import org.apache.uima.ducc.transport.event.common.IDuccWorkReservation;
import org.apache.uima.ducc.transport.event.common.IDuccWorkService;
import org.apache.uima.ducc.transport.event.common.history.IHistoryPersistenceManager;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;


public class HistoryManagerDb 
    implements IHistoryPersistenceManager 
{

	
	private DuccLogger logger = null;
    private DbManager dbManager;

    PreparedStatement jobPrepare = null;
    PreparedStatement reservationPrepare = null;
    PreparedStatement servicePrepare = null;
    PreparedStatement ckptPrepare = null;
    static final String JOB_TABLE = "ducc." + OrWorkProps.JOB_TABLE.pname();
    static final String RES_TABLE = "ducc." + OrWorkProps.RESERVATION_TABLE.pname();
    static final String SVC_TABLE = "ducc." + OrWorkProps.SERVICE_TABLE.pname();
    static final String CKPT_TABLE = "ducc." + OrCkptProps.CKPT_TABLE.pname();
		
    public HistoryManagerDb()
    {
    }
    
    
	private boolean init(String dburl, DbManager dbm)
        throws Exception
    {        
		String methodName = "init";
        boolean ret = true;
        logger.info(methodName, null, "Initializing OR persistence over the database");
        while ( true ) {
            try {
                if ( dbm != null ) {
                    this.dbManager = dbm;
                } else {
                    dbManager = new DbManager(dburl, logger);
                    dbManager.init();
                }
                
                // prepare some statements
                DbHandle h = dbManager.open();
                jobPrepare         = h.prepare("INSERT INTO " + JOB_TABLE + " (ducc_dbid, type, history, work) VALUES (?, ?, ?, ?) IF NOT EXISTS;");            
                reservationPrepare = h.prepare("INSERT INTO " + RES_TABLE + " (ducc_dbid, type, history, work) VALUES (?, ?, ?, ?) IF NOT EXISTS;");            
                servicePrepare     = h.prepare("INSERT INTO " + SVC_TABLE + " (ducc_dbid, type, history, work) VALUES (?, ?, ?, ?) IF NOT EXISTS;");            
                ckptPrepare        = h.prepare("INSERT INTO " + CKPT_TABLE + " (id, work, p2jmap) VALUES (?, ?, ?);");            
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

    public boolean init(DuccLogger logger)
        throws Exception
    {
        this.logger = logger;
        String historyUrl = System.getProperty("ducc.state.database.url");
        return init(historyUrl, null);
    }

    // package only, for the loader
    boolean init(DuccLogger logger, DbManager dbManager)
    	throws Exception
    {
    	this.logger = logger;
        String stateUrl = System.getProperty("ducc.state.database.url");
        return init(stateUrl, dbManager);
    }


    /**
     * Schema gen.  Do anything you want to make the schema, but notice that DbUtil has a few convenience methods if
     * you want to define your schema in a magic enum.
     */
    static ArrayList<SimpleStatement> mkSchema(String tablename)
    	throws Exception
    {
        ArrayList<SimpleStatement> ret = new ArrayList<SimpleStatement>();

        StringBuffer buf = new StringBuffer("CREATE TABLE IF NOT EXISTS " + tablename + " (");
        buf.append(DbUtil.mkSchema(OrWorkProps.values()));
        buf.append(")");
        buf.append("WITH CLUSTERING ORDER BY (ducc_dbid desc)");

        ret.add(new SimpleStatement(buf.toString()));
        ret.add(new SimpleStatement("CREATE INDEX IF NOT EXISTS ON " +  tablename + "(ducc_dbid)"));
        ret.add(new SimpleStatement("CREATE INDEX IF NOT EXISTS ON " +  tablename + "(history)"));

        return ret;
    }

    static ArrayList<SimpleStatement> mkSchema()
    	throws Exception
    {
        ArrayList<SimpleStatement> ret = new ArrayList<SimpleStatement>();

        ret.addAll(mkSchema(JOB_TABLE));
        ret.addAll(mkSchema(RES_TABLE));
        ret.addAll(mkSchema(SVC_TABLE));

        StringBuffer buf = new StringBuffer("CREATE TABLE IF NOT EXISTS " + CKPT_TABLE + " (");
        buf.append(DbUtil.mkSchema(OrCkptProps.values()));
        buf.append(")");
        ret.add(new SimpleStatement(buf.toString()));

        return ret;
    }

    // ----------------------------------------------------------------------------------------------------
    // Jobs section

    void saveWork(PreparedStatement s, IDuccWork w, boolean isHistory)
        throws Exception
    {
    	String methodName = "saveWork";
        Long nowP =  System.currentTimeMillis();
        String type = null;
        if ( w instanceof IDuccWorkJob ) {
            type = "job";
        } else if ( w instanceof IDuccWorkReservation ) {
            type = "reservation";
        } else if ( w instanceof IDuccWorkService ) {
            type = "service";
        } else {
        	throw new IllegalArgumentException("Improper object passed to saveWork");
        }

        logger.info(methodName, w.getDuccId(), "-------- saving " + type);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream out = new ObjectOutputStream(baos);
        out.writeObject(w);
        out.close();
        byte[] bytes = baos.toByteArray();
        ByteBuffer buf = ByteBuffer.wrap(bytes);

        DbHandle h = dbManager.open();
        h.saveObject(s,  w.getDuccId().getFriendly(), type, isHistory, buf);

        logger.info(methodName, w.getDuccId(), "----------> Time to save", type, ":", System.currentTimeMillis() - nowP, "Size:", bytes.length, "bytes.");        
    }

    /**
     * Part of history management, recover ths indicated job from history.
     */
    <T> T restoreWork(Class<T> cl, String tablename, long friendly_id)
        throws Exception
    {
    	String methodName = "restoreWork";
        T ret = null;
        DbHandle h = null;

        h = dbManager.open();
        String cql = "SELECT WORK FROM " + tablename + " WHERE DUCC_DBID=" + Long.toString(friendly_id);
        ResultSet rs = h.execute(cql);
        for ( Row r : rs ) {
            logger.info(methodName, null, "----- Restoring", friendly_id); 
            ByteBuffer bbWork = r.getBytes("work");
            
            byte[] workbytes = bbWork.array();
            ByteArrayInputStream bais = new ByteArrayInputStream(workbytes);
            ObjectInputStream ois = new ObjectInputStream(bais);
            ret= (T) ois.readObject();
            ois.close();            
        } 
        
        return ret;
    }
    
    /**
     * Part of history management, recover ths indicated jobs from history.
     *
     * Reminder to self, we need to pass Clas<T> cl so compiler can infer T.
     */
    public <T> ArrayList<T> restoreSeveralThings(Class<T> cl, String tablename, long max)
        throws Exception
    {
    	String methodName = "restoreSeveralThings";

        ArrayList<T> ret = new ArrayList<T>();
        DbHandle h = dbManager.open();
        SimpleStatement s = new SimpleStatement("SELECT * from " + tablename + " limit " + max);
        s.setFetchSize(100);
        long now = System.currentTimeMillis();

        try {
            int count = 0;
            int nbytes = 0;
            ResultSet rs = h.execute(s);
            for ( Row r : rs ) {
                count++;
                ByteBuffer b = r.getBytes("work");
                byte[] workbytes = b.array();
                nbytes += workbytes.length;

                ByteArrayInputStream bais = new ByteArrayInputStream(workbytes);
                ObjectInputStream ois = new ObjectInputStream(bais);
                ret.add( (T) ois.readObject());
                ois.close();            
                count++;
            }
            
            logger.info(methodName, null, "Found", count, "results. Total bytes", nbytes, "Time:",  System.currentTimeMillis() - now);
		} catch (Exception e) {
            logger.error(methodName, null, "Error fetching history:", e);
		}
        return ret;
    }


    /**
     * For use by normal operation: forces an existence check.  This saves history only.
     */
	public void saveJob(IDuccWorkJob j)
        throws Exception 
    {
        saveWork(jobPrepare, j, true);
    }

	
    /**
     * Part of history management, recover ths indicated job from history.
     */
    public IDuccWorkJob restoreJob(long friendly_id)
        throws Exception
    {
        return (IDuccWorkJob) restoreWork(IDuccWorkJob.class, JOB_TABLE, friendly_id);
    }
    
    /**
     * Part of history management, recover ths indicated jobs from history.
     */
    public ArrayList<IDuccWorkJob> restoreJobs(long max)
        throws Exception
    {
        return restoreSeveralThings(IDuccWorkJob.class, JOB_TABLE, max);
    }
    // End of jobs section
    // ----------------------------------------------------------------------------------------------------


    // ----------------------------------------------------------------------------------------------------
    // Reservations section

    // Save to history only
	public void saveReservation(IDuccWorkReservation r) 
        throws Exception 
    {
        saveWork(reservationPrepare, r, true);
    }

    /**
     * Part of history management, recover ths indicated reservation from history.
     */
	public IDuccWorkReservation restoreReservation(long duccid)
        throws Exception
    {
        return (IDuccWorkReservation) restoreWork(IDuccWorkReservation.class, RES_TABLE, duccid);
    }
	
    /**
     * Part of history management, recover ths indicated reservations from history.
     */
	public ArrayList<IDuccWorkReservation> restoreReservations(long max) 
		throws Exception
    {
        return restoreSeveralThings(IDuccWorkReservation.class, RES_TABLE, max);
    }

    // End of reservations section
    // ----------------------------------------------------------------------------------------------------
	

    // ----------------------------------------------------------------------------------------------------
    // Services section

    public void saveService(IDuccWorkService s)
    	throws Exception
    {
        saveWork(servicePrepare, s, true);
    }

	
    /**
     * Part of history management, recover ths indicated service instance from history.
     */
	public IDuccWorkService restoreService(long duccid)
		throws Exception
    {
        return (IDuccWorkService) restoreWork(IDuccWorkService.class, SVC_TABLE, duccid);
	}
	
    /**
     * Part of history management, recover ths indicated service instances from history.
     */
	public ArrayList<IDuccWorkService> restoreServices(long max) 
		throws Exception
    {
        return restoreSeveralThings(IDuccWorkService.class, SVC_TABLE, max);
	}
    // End of services section
    // ----------------------------------------------------------------------------------------------------
    
    // ----------------------------------------------------------------------------------------------------
    // Orchecstrator Checkpoint save and restore.  We save as discrete objects (unlike file-based checkpoint)
    // so they can be included in queries.

    /**
     * Orchestrator checkpoint, save the live orchestrator state to the database.
     *
     * @param work A map of all 'live' work, Jobs, Reservations, APs, Service instances
     * @param processToJob Maps each specific process to the controlling work (job, reservation, ap, service instance)
     *
     * @TODO Do we even need processToJob?  Can't it be derived from the work map?  This question needs to be
     *       resolved by the owner of the OR.  For now we just do it.
     */
    public boolean checkpoint(DuccWorkMap work, Map<DuccId, DuccId> processToJob)
        throws Exception
    {
        String methodName = "checkpoint";
        long now = System.currentTimeMillis();
        boolean ret = true;

        // We transactionally delete the old checkpoint, and then save the new one.  If something gows wrong we
        // rollback and thus don't lose stuff.  In theory.
        
        // TODO: make the truncate and insert transactional
        DbHandle h = dbManager.open();
        h.truncate("ducc.orckpt");

        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream out = new ObjectOutputStream(baos);
            out.writeObject(work);
            out.close();
            byte[] bytes = baos.toByteArray();
            ByteBuffer workbuf = ByteBuffer.wrap(bytes);

            if ( logger.isTrace() ) {
                ConcurrentHashMap<DuccId, IDuccWork> map = work.getMap();
                for ( DuccId id : map.keySet() ) {
                    IDuccWork w = map.get(id);
                    logger.trace(methodName, id, "Checkpointing", w.getClass());
                }
            }

            baos = new ByteArrayOutputStream();
            out= new ObjectOutputStream(baos);
            out.writeObject(processToJob);
            out.close();
            bytes = baos.toByteArray();
            ByteBuffer mapbuf = ByteBuffer.wrap(bytes);
            
            h = dbManager.open();
            h.saveObject(ckptPrepare, 0, workbuf, mapbuf);       

        } catch ( Exception e ) {
            logger.error(methodName, null, "Cannot save ProcessToJob map", e);
            ret = false;
        } finally {
            if ( ret ) logger.info(methodName, null, "Saved Orchestrator Checkpoint");
        }

        logger.info(methodName, null, "Total time to save checkpoint:", System.currentTimeMillis() - now);
        return ret;
    }

    /**
     * Orchestrator checkpoint.  Restore the checkpoint from the DB.  Caller must initialize
     * empty maps, which we fill in.
     */
    public Pair<DuccWorkMap, Map<DuccId, DuccId>>  restore()
        throws Exception
    {
        String methodName = "restore";
        DbHandle h = null;
        Pair<DuccWorkMap, Map<DuccId, DuccId>> ret = new Pair<DuccWorkMap, Map<DuccId, DuccId>>();
        try {
            h = dbManager.open();
            String cql = "SELECT * FROM ducc.orckpt WHERE id=0";
            ResultSet rs = h.execute(cql);
            for ( Row r : rs ) {
                logger.info(methodName, null, "Found checkpoint.");
                ByteBuffer bbWork = r.getBytes("work");
                ByteBuffer bbmap = r.getBytes("p2jmap");

                byte[] workbytes = bbWork.array();
                ByteArrayInputStream bais = new ByteArrayInputStream(workbytes);
                ObjectInputStream ois = new ObjectInputStream(bais);
                DuccWorkMap work = (DuccWorkMap) ois.readObject();
                ois.close();

                workbytes = bbmap.array();
                bais = new ByteArrayInputStream(workbytes);
                ois = new ObjectInputStream(bais);
                Map<DuccId, DuccId> processToJob = (Map<DuccId, DuccId>) ois.readObject();
                ois.close();

                // hack because java serializion is stupid and won't call the no-args constructor - need
                // to restore sometransient fields
                Set<DuccId> ids = work.getReservationKeySet();
                for ( DuccId id : ids ) {
                    DuccWorkReservation res = (DuccWorkReservation) work.findDuccWork(DuccType.Reservation, ""+id.getFriendly());
                    if ( r != null ) res.initLogger();
                }

                // only gets called once per boot and might be useful, let's leave at info
                ConcurrentHashMap<DuccId, IDuccWork> map = work.getMap();
                for ( DuccId id : map.keySet() ) {
                    IDuccWork w = map.get(id);
                    logger.info(methodName, id, "Restored", w.getClass());
                }
                
                ret = new Pair(work, processToJob);
            }

       } catch ( Exception e ) {
            logger.error(methodName, null, "Error restoring checkpoint:", e);
        } 
        
        return ret;
    }
    
    // End of OR checkpoint save and restore
    // ----------------------------------------------------------------------------------------------------
    
    // ----------------------------------------------------------------------------------------------------
    // Stuff common to everything
    JsonObject mkJsonObject(String json)
    {
        // This method lets us munge the json before using it, if we need to
        JsonParser parser = new JsonParser();
        JsonObject jobj = parser.parse(json).getAsJsonObject();
        
        return jobj;
    }

    public void shutdown()
    {
        dbManager.shutdown();
    }

    // End of common
    // ----------------------------------------------------------------------------------------------------
		
}
