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
import java.util.concurrent.ConcurrentHashMap;

import org.apache.uima.ducc.common.SizeBytes;
import org.apache.uima.ducc.common.main.DuccService;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.database.DbConstants.DbCategory;
import org.apache.uima.ducc.database.DbConstants.DbEdge;
import org.apache.uima.ducc.database.DbConstants.DbVertex;
import org.apache.uima.ducc.transport.event.common.ADuccWork;
import org.apache.uima.ducc.transport.event.common.DuccProcess;
import org.apache.uima.ducc.transport.event.common.DuccProcessMap;
import org.apache.uima.ducc.transport.event.common.DuccReservation;
import org.apache.uima.ducc.transport.event.common.DuccWorkJob;
import org.apache.uima.ducc.transport.event.common.DuccWorkMap;
import org.apache.uima.ducc.transport.event.common.DuccWorkPopDriver;
import org.apache.uima.ducc.transport.event.common.DuccWorkReservation;
import org.apache.uima.ducc.transport.event.common.IDuccCompletionType.JobCompletionType;
import org.apache.uima.ducc.transport.event.common.IDuccCompletionType.ReservationCompletionType;
import org.apache.uima.ducc.transport.event.common.IDuccPerWorkItemStatistics;
import org.apache.uima.ducc.transport.event.common.IDuccProcess;
import org.apache.uima.ducc.transport.event.common.IDuccProcessMap;
import org.apache.uima.ducc.transport.event.common.IDuccReservation;
import org.apache.uima.ducc.transport.event.common.IDuccReservationMap;
import org.apache.uima.ducc.transport.event.common.IDuccState.JobState;
import org.apache.uima.ducc.transport.event.common.IDuccState.ReservationState;
import org.apache.uima.ducc.transport.event.common.IDuccWork;
import org.apache.uima.ducc.transport.event.common.IDuccWorkJob;
import org.apache.uima.ducc.transport.event.common.IDuccWorkReservation;
import org.apache.uima.ducc.transport.event.common.IDuccWorkService;
import org.apache.uima.ducc.transport.event.common.JdReservationBean;
import org.apache.uima.ducc.transport.event.common.history.IHistoryPersistenceManager;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientEdge;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;


public class HistoryManagerDb 
    implements IHistoryPersistenceManager 
{

	
	private DuccLogger logger = null;
    private String dburl;
    private DbManager dbManager;
		
    public HistoryManagerDb()
    {
        this(DuccService.getDuccLogger(HistoryManagerDb.class.getName()));
    }

	public HistoryManagerDb(DuccLogger logger) 
    {
        
        this.logger = logger;
        dburl = System.getProperty("ducc.state.database.url");
        try {
            dbManager = new DbManager(dburl, logger);
            dbManager.init();
            // TODO TODO
            dbManager.declareIntent(new OIntentMassiveInsert());
            logger.warn("<CTR>.HistoryManagerDb", null, "****MUST FIX DECLARE INTENT****");
        } catch ( Exception e ) {
            logger.error("HisstoryManagerDb", null, "Cannot open the history database:", e);
        }        
	}

    public void setLogger(DuccLogger logger)
    {
        this.logger = logger;
    }

    // ----------------------------------------------------------------------------------------------------
    // Jobs section

    /**
     * Common code to save a job in an open handle.  Caller will commit or fail as needed.
     */
    void saveJobNoCommit(DbHandle h, IDuccWorkJob j, DbVertex type, DbCategory dbcat)
        throws Exception
    {
    	String methodName = "saveJobNoCommit";
        Long nowP =  System.currentTimeMillis();
        // Nuke the command lines
        DuccWorkPopDriver driver = j.getDriver();
        //ICommandLine driverCl = null;
        IDuccProcessMap jdProcessMap = null;

        int size = 0;

        if ( driver != null ) {
            //driverCl = driver.getCommandLine();
            //driver.setCommandLine(null);
            jdProcessMap =  driver.getProcessMap();
            driver.setProcessMap(null);
        }

        //ICommandLine jobCl    = j.getCommandLine();
        //j.setCommandLine(null);

        IDuccPerWorkItemStatistics stats = j.getSchedulingInfo().getPerWorkItemStatistics();

        if ( stats != null ) {
            if (Double.isNaN(stats.getStandardDeviation()) ) {
                stats.setStandardDeviation(0.0);
            }
        }

        // Pull process map so we can put processes in their own records
        IDuccProcessMap processMap = j.getProcessMap();
        j.setProcessMap(null);

        Gson g = DbHandle.mkGsonForJob();

        String dbJob = g.toJson(j);
        size += dbJob.length();

        // Must repair these things because OR continues to use the job after it has been
        // written to history.
        j.setProcessMap(processMap);
        //j.setCommandLine(jobCl);
        if ( driver != null ) {
            //driver.setCommandLine(driverCl);
            driver.setProcessMap(jdProcessMap);
        }
        
        OrientVertex savedJob = h.saveObject(type, j.getDuccId().getFriendly(), dbJob, dbcat);
    
        List<OrientVertex> savedJPs = new ArrayList<OrientVertex>();
        List<OrientVertex> savedJDs = new ArrayList<OrientVertex>();
        for (DuccId did : processMap.keySet()) {
            Long pid = did.getFriendly();
            
            IDuccProcess p = processMap.get(did);
            String proc = g.toJson(p);
            size += proc.length();
            
            savedJPs.add(h.saveObject(DbVertex.Process, pid, proc, dbcat));
            // logger.info(methodName, p.getDuccId(), "2 ----------> Time to save process", System.currentTimeMillis() - nowP);
                         
        }
        
        if ( driver != null ) {
            for (DuccId did : jdProcessMap.keySet()) {
                Long pid = did.getFriendly();
                
                IDuccProcess p = jdProcessMap.get(did);
                String proc = g.toJson(p);
                size += proc.length();
                
                savedJDs.add(h.saveObject(DbVertex.Process, pid, proc, dbcat));
                // logger.info(methodName, p.getDuccId(), "2 ----------> Time to save process", System.currentTimeMillis() - nowP);
                
            }
            h.addEdges(savedJob, savedJDs, DbEdge.JdProcess);
        }
        
        h.addEdges(savedJob, savedJPs, DbEdge.JpProcess);

        logger.info(methodName, j.getDuccId(), "----------> Time to save job", System.currentTimeMillis() - nowP, "json size", size, "nprocesses", processMap.size());
        
    }
    
    private void saveJobInternal(IDuccWorkJob j, DbVertex type, boolean safe, DbCategory dbcat)
        throws Exception 
    {

        // It seems that services instances are represented as jobs without drivers.  So we use
        // the common code here, passing in the vertex type, for both jobs and services.

        String methodName = "saveJob";
        logger.info(methodName, j.getDuccId(), "Saving: type:", type.pname(), "safe:", safe, "DbCategory:", dbcat);

		Long id = j.getDuccId().getFriendly();
        DbHandle h = null;
        try {

            if ( safe ) {
                h = dbManager.open(); 
            } else {
                h = dbManager.openNoLog();
            }

            if ( safe && h.thingInDatabase(id, type, dbcat) ) {
                logger.warn(methodName, j.getDuccId(), "Not overwriting saved job.");
                h.close();
                return;
            } 
        } catch ( Exception e ) {
            if ( h != null ) h.close();
            throw e;
        }

        try {
            saveJobNoCommit(h, j, type, dbcat);
        } catch ( Exception e ) {
            h.rollback();
            logger.error(methodName, j.getDuccId(), "Cannot store job", e);
            throw e;
        } finally {
            Long nowP =  System.currentTimeMillis();
            h.commit();
            logger.info(methodName, j.getDuccId(), "Time to commit", System.currentTimeMillis() - nowP);
            h.close();
        }
	}

    /**
     * For use by the loader, load it without the existence check; the assumption this is a first-time load
     * and the check isn't needed.  This saves history only.
     */
    public void saveJobUnsafe(IDuccWorkJob j)
        throws Exception
    {
        saveJobInternal(j, DbVertex.Job, false, DbCategory.History);
    }

    /**
     * For use by normal operation: forces an existence check.  This saves history only.
     */
	public void saveJob(IDuccWorkJob j)
        throws Exception 
    {
        saveJobInternal(j, DbVertex.Job, true, DbCategory.History);
    }

	
    private IDuccWorkJob restoreJobInternal(DbHandle h, OrientVertex v)
        throws Exception
    {
        IDuccWorkJob j = null;

        ODocument d = v.getRecord();
        String json = d.toJSON();
        JsonObject jo = mkJsonObject(json);

        Gson g = DbHandle.mkGsonForJob();        
        j      = g.fromJson(jo, DuccWorkJob.class);

        // System.out.println(g.toJson(jo));
        
        IDuccProcessMap pm = j.getProcessMap();              // seems to get set by default when job is recovered
        Iterable<Edge> ed = v.getEdges(Direction.OUT, DbEdge.JpProcess.pname());
        for ( Edge e : ed ) {
            OrientEdge   oe = (OrientEdge) e;
            OrientVertex ov = oe.getVertex(Direction.IN);
            
            ODocument    pd    = ov.getRecord();
            String       pjson = pd.toJSON();
            
            IDuccProcess pe = g.fromJson(pjson, DuccProcess.class);
            pm.addProcess(pe);
        }

        DuccWorkPopDriver driver = j.getDriver();
        if ( driver != null ) {
            pm = new DuccProcessMap();
            driver.setProcessMap(pm);                     // seems NOT to get set when driver is reconstituted
            ed = v.getEdges(Direction.OUT, DbEdge.JdProcess.pname());
            for ( Edge e : ed ) {
                OrientEdge   oe = (OrientEdge) e;
                OrientVertex ov = oe.getVertex(Direction.IN);
                
                ODocument    pd    = ov.getRecord();
                String       pjson = pd.toJSON();
                
                IDuccProcess pe = g.fromJson(pjson, DuccProcess.class);
                pm.addProcess(pe);
            }
        }

        // Now must hack around becase this 'n that and JSON can't work out some things
        String ct = (String) ((ADuccWork)j).getCompletionTypeObject();
        j.setCompletionType(JobCompletionType.valueOf(ct));

        String so = (String) ((ADuccWork)j).getStateObject();
        j.setJobState(JobState.valueOf(so));
        
        return j;
    }

    /**
     * Part of history management, recover ths indicated job from history.
     */
    public IDuccWorkJob restoreJob(long friendly_id)
        throws Exception
    {
        DuccWorkJob ret = null;
        DbHandle h = null;
        try {
            h = dbManager.open();
            Iterable<Vertex> q =  h.select("SELECT * FROM " + DbVertex.Job.pname() + " WHERE ducc_dbid=" + friendly_id + 
                                           " AND " + DbConstants.DUCC_DBCAT + "='" + DbCategory.History.pname() + "'");
            for ( Vertex v : q ) {
                // There's only 1 unless db is broken.
                return restoreJobInternal(h, (OrientVertex) v);
            }
        } finally {
            h.close();
        }

        return ret;
    }
    
    /**
     * Part of history management, recover ths indicated jobs from history.
     */
    public ArrayList<IDuccWorkJob> restoreJobs(long max)
        throws Exception
    {
        ArrayList<IDuccWorkJob> ret = new ArrayList<IDuccWorkJob>();
        DbHandle h = null;
        try {
            h = dbManager.open();
            Iterable<Vertex> q =  h.select("SELECT * FROM " + DbVertex.Job.pname() + 
                                           " where " + DbConstants.DUCC_DBCAT +"='" + DbCategory.History.pname() + 
                                           "' ORDER BY ducc_dbid DESC LIMIT "+ max);
            for ( Vertex v : q ) {
                IDuccWorkJob j = restoreJobInternal(h, (OrientVertex) v);
                ret.add(j);
            }
        } finally {
            h.close();
        }

        return ret;
    }
    // End of jobs section
    // ----------------------------------------------------------------------------------------------------


    // ----------------------------------------------------------------------------------------------------
    // Reservations section

	private void saveReservationNoCommit(DbHandle h, IDuccWorkReservation r, DbCategory dbcat)
        throws Exception 
    {
        String methodName = "saveReservationNoCommit";
        long now = System.currentTimeMillis();

        List<JdReservationBean> l = r.getJdReservationBeanList();
        if ( l != null ) {
            for (JdReservationBean b : l ) {
                ConcurrentHashMap<DuccId, SizeBytes> map = b.getMap();
                for ( DuccId k : map.keySet() ) {
                    logger.info(methodName, null, "SAVE ===> " + k.getFriendly() + " " + k.getUnique() + " : " + map.get(k));
                }
            }
        }




		Long id = r.getDuccId().getFriendly();
        logger.info(methodName, r.getDuccId(), "Saving.");
   
        // Nuke the command lines

        IDuccReservationMap resmap = r.getReservationMap();
        r.setReservationMap(null);

        Gson g = DbHandle.mkGsonForJob();

        String dbres = g.toJson(r);
        // logger.info(methodName, null, "------------------- Reservation JSON: " + dbres);
        
        // Must repair these things because OR continues to use the job after it has been
        // written to history.
        r.setReservationMap(resmap);
        
        OrientVertex savedRes = h.saveObject(DbVertex.Reservation, id, dbres, dbcat);
        
        List<OrientVertex> savedHosts = new ArrayList<OrientVertex>();
        for (DuccId did : resmap.keySet()) {
            Long pid = did.getFriendly();
            
            IDuccReservation p = resmap.get(did);
            String proc = g.toJson(p);
            
            savedHosts.add(h.saveObject(DbVertex.Process, pid, proc, dbcat));
            // logger.info(methodName, p.getDuccId(), "2 ----------> Time to save process", System.currentTimeMillis() - nowP);
            
        }
        
        h.addEdges(savedRes, savedHosts, DbEdge.JpProcess);
        logger.info(methodName, r.getDuccId(), "----------> Total reservation save time:", System.currentTimeMillis() - now, "nPE", resmap.size());

	}

    private void saveReservationInternal(IDuccWorkReservation r, boolean safe, DbCategory dbcat)
        throws Exception 
    {
        String methodName = "saveReservation";

		Long id = r.getDuccId().getFriendly();
        DbHandle h = null;
        try {
            if ( safe ) {
                h = dbManager.open(); 
            } else {
                h = dbManager.openNoTx();
            }
            if ( safe && h.thingInDatabase(id, DbVertex.Reservation, dbcat) ) {
                h.close();
                return;
            } 
        } catch ( Exception e ) {
            logger.warn(methodName, r.getDuccId(), e);
            h.close();
            return;
        }

        try {
            saveReservationNoCommit(h, r, dbcat);
        } catch ( Exception e ) {
            h.rollback();
            logger.error(methodName, r.getDuccId(), "Cannot store reservation:", e);
        } finally {
            h.commit();
            h.close();
        }

	}

    // Save to history only
	public void saveReservation(IDuccWorkReservation r) 
        throws Exception 
    {
        saveReservationInternal(r, true, DbCategory.History);
    }

	public void saveReservationUnsafe(IDuccWorkReservation r) 
        throws Exception 
    {
        saveReservationInternal(r, false, DbCategory.History);
    }

    private IDuccWorkReservation restoreReservationInternal(DbHandle h, OrientVertex v)
        throws Exception
    {
        // String methodName = "restoreReservationInternal";
        IDuccWorkReservation r = null;

        ODocument d = v.getRecord();
        String json = d.toJSON();
        JsonObject jo = mkJsonObject(json);

        Gson g = DbHandle.mkGsonForJob();        
        // logger.info(methodName, null, g.toJson(jo));

        r      = g.fromJson(jo, DuccWorkReservation.class);

        //List<JdReservationBean> l = r.getJdReservationBeanList();
        //if ( l != null ) {
            //for (JdReservationBean b : l ) {
                //ConcurrentHashMap<DuccId, SizeBytes> map = b.getMap();
                //for ( DuccId k : map.keySet() ) {
                //    logger.info(methodName, null, "REST ===> " + k.getFriendly() + " " + k.getUnique() + " : " + map.get(k));
                //}
            //}
        //}
        
        IDuccReservationMap rm = r.getReservationMap();              // seems to get set by default when job is recovered
        Iterable<Edge> ed = v.getEdges(Direction.OUT, DbEdge.JpProcess.pname());
        for ( Edge e : ed ) {
            OrientEdge   oe = (OrientEdge) e;
            OrientVertex ov = oe.getVertex(Direction.IN);
            
            ODocument    pd    = ov.getRecord();
            String       pjson = pd.toJSON();
            
            IDuccReservation rr = g.fromJson(pjson, DuccReservation.class);
            rm.addReservation(rr);
        }

        // Now must hack around becase this 'n that and JSON can't work out some things
        String ct = (String) ((ADuccWork)r).getCompletionTypeObject();
        r.setCompletionType(ReservationCompletionType.valueOf(ct));

        String so = (String) ((ADuccWork)r).getStateObject();
        r.setReservationState(ReservationState.valueOf(so));
        
        return r;
    }
	
    /**
     * Part of history management, recover ths indicated reservation from history.
     */
	public IDuccWorkReservation restoreReservation(long duccid)
        throws Exception
    {
        DuccWorkReservation ret = null;
        DbHandle h = null;
        try {
            h = dbManager.open();
            Iterable<Vertex> q =  h.select("SELECT * FROM " + DbVertex.Reservation.pname() + " WHERE ducc_dbid=" + duccid + 
                                           " AND " + DbConstants.DUCC_DBCAT +"='" + DbCategory.History.pname() + "'");
            for ( Vertex v : q ) {
                // There's only 1 unless db is broken.
                return restoreReservationInternal(h, (OrientVertex) v);
            }
        } finally {
            h.close();
        }

        return ret;
	}
	
    /**
     * Part of history management, recover ths indicated reservations from history.
     */
	public ArrayList<IDuccWorkReservation> restoreReservations(long max) 
		throws Exception
    {
        ArrayList<IDuccWorkReservation> ret = new ArrayList<IDuccWorkReservation>();
        DbHandle h = null;
        try {
            h = dbManager.open();
            Iterable<Vertex> q =  h.select("SELECT * FROM " + DbVertex.Reservation.pname() + 
                                           " WHERE " + DbConstants.DUCC_DBCAT + "='" + DbCategory.History.pname() + "'" + 
                                           " ORDER BY ducc_dbid DESC LIMIT "+ max);
            for ( Vertex v : q ) {
                IDuccWorkReservation j = restoreReservationInternal(h, (OrientVertex) v);
                ret.add(j);
            }
        } finally {
            h.close();
        }

        return ret;
	}
    // End of reservations section
    // ----------------------------------------------------------------------------------------------------
	

    // ----------------------------------------------------------------------------------------------------
    // Services section
    // public void serviceSave(IDuccWorkService s) 
    //     throws Exception
    // {
    // }

    public void saveService(IDuccWorkService s)
    	throws Exception
    {
        saveJobInternal((IDuccWorkJob)s, DbVertex.ServiceInstance, true, DbCategory.History);
    }

    public void saveServiceUnsafe(IDuccWorkService s)
    	throws Exception
    {
        saveJobInternal((IDuccWorkJob)s, DbVertex.ServiceInstance, false, DbCategory.History);
    }

	
    /**
     * Part of history management, recover ths indicated service instance from history.
     */
	public IDuccWorkService restoreService(long duccid)
		throws Exception
    {
        DbHandle h = null;
        try {
            h = dbManager.open();
            Iterable<Vertex> q =  h.select("SELECT * FROM " + DbVertex.ServiceInstance.pname() + " WHERE ducc_dbid=" + duccid + 
                                           " AND " + DbConstants.DUCC_DBCAT + "='" + DbCategory.History.pname() + "'");
            for ( Vertex v : q ) {
                return restoreJobInternal(h, (OrientVertex) v);
            }
        } finally {
            h.close();
        }

        return null;
	}
	
    /**
     * Part of history management, recover ths indicated service instances from history.
     */
	public ArrayList<IDuccWorkService> restoreServices(long max) 
		throws Exception
    {
        ArrayList<IDuccWorkService> ret = new ArrayList<IDuccWorkService>();
        DbHandle h = null;
        try {
            h = dbManager.open();
            Iterable<Vertex> q =  h.select("SELECT * FROM " + DbVertex.ServiceInstance.pname() + 
                                           " WHERE " + DbConstants.DUCC_DBCAT +"='" + DbCategory.History.pname() + "'" +
                                           " ORDER BY ducc_dbid DESC LIMIT "+ max);
            for ( Vertex v : q ) {
                IDuccWorkService j = restoreJobInternal(h, (OrientVertex) v);
                ret.add(j);
            }
        } finally {
            h.close();
        }

        return ret;

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

        DbHandle h = null;
        try {
            h = dbManager.open(); 
        } catch ( Exception e ) {
            logger.warn(methodName, null, "Cannot open database.", e);
            if ( h != null ) h.close();
            return false;
        }

        // We transactionally delete the old checkpoint, and then save the new one.  If something gows wrong we
        // rollback and thus don't lose stuff.  In theory.
       try {
           h.execute("DELETE VERTEX V where " + DbConstants.DUCC_DBCAT + "='" + DbCategory.Checkpoint.pname() + "'");
           Map<DuccId, IDuccWork> map = work.getMap();
           for (IDuccWork w : map.values() ) {
               switch(w.getDuccType()) {
               case Job:
                   saveJobNoCommit(h, (IDuccWorkJob) w, DbVertex.Job, DbCategory.Checkpoint);
                   break;
               case Service:
                   saveJobNoCommit(h, (IDuccWorkJob) w, DbVertex.ServiceInstance, DbCategory.Checkpoint);
                   break;
               case Reservation:
                   if ( w.getDuccId().getFriendly() == 282282 ) {
                       int x = 0;
                       x++;
                   }
                   saveReservationNoCommit(h, (IDuccWorkReservation) w, DbCategory.Checkpoint);
                   break;
               default:
                   break;
               }
           } 
           
           Gson g = DbHandle.mkGsonForJob();
           ProcessToJobList l = new ProcessToJobList(processToJob);
           String json = g.toJson(l, l.getClass());
           // logger.info(methodName, null, "ProcessToJob:", json);
           h.saveObject(DbVertex.ProcessToJob, null, json, DbCategory.Checkpoint);
           h.commit();
        } catch ( Exception e ) {
            if ( h != null ) h.rollback();
            logger.error(methodName, null, "Cannot save ProcessToJob map", e);
            ret = false;
        } finally {
            if ( h != null ) h.close();
            if ( ret ) logger.info(methodName, null, "Saved Orchestrator Checkpoint");
        }

       logger.info(methodName, null, "Total time to save checkpoint:", System.currentTimeMillis() - now);
       return ret;
    }

    /**
     * Orchestrator checkpoint.  Restore the checkpoint from the DB.  Caller must initialize
     * empty maps, which we fill in.
     */
    public boolean restore(DuccWorkMap work, Map<DuccId, DuccId> processToJob)
        throws Exception
    {
    	String methodName = "restore";
        DbHandle h = null;
        boolean ret = true;
        try {
            h = dbManager.open();
            // Select all the "top level" objects ith DUCC_LIVE=true.  When they get restored the
            // attached object will get collected.

            Iterable<Vertex> q =  h.select("SELECT * FROM V WHERE (" +
                                            "@CLASS ='" + DbVertex.Job.pname() +
                                            "' OR " + 
                                            "@CLASS ='" + DbVertex.Reservation.pname() + 
                                            "' OR " +  
                                            "@CLASS ='" + DbVertex.ServiceInstance.pname() + 
                                            "') AND "   + DbConstants.DUCC_DBCAT + "='" + DbCategory.Checkpoint.pname() + "'");

            IDuccWork w = null;
            for ( Vertex v : q ) {
                String l = ((OrientVertex) v).getLabel();
                if ( l.equals(DbVertex.Job.pname()) || l.equals(DbVertex.ServiceInstance.pname()) ) {
                    w = restoreJobInternal(h, (OrientVertex) v);
                } else if ( l.equals(DbVertex.Reservation.pname()) ) {
                    w = restoreReservationInternal(h, (OrientVertex) v);
                } else {
                    logger.warn(methodName, null, "Unexpected record of type", l, "in checkpoint restore.");
                }
                
                work.addDuccWork(w);
            }

            q = h.select("SELECT FROM " + DbVertex.ProcessToJob.pname());
            
            int count = 0;
            for ( Vertex vv : q ) {
                if ( count > 1 ) {
                    logger.error(methodName, null, "Oops - we have multiple ProcessToJob records.  Using the first one but it may be wrong.");
                    break;
                }

                OrientVertex v = (OrientVertex) vv;
                ODocument d = v.getRecord();
                String json = d.toJSON();
                logger.info(methodName, null, json);

                Gson g = DbHandle.mkGsonForJob();

                ProcessToJobList l = g.fromJson(json, ProcessToJobList.class);
                l.fill(processToJob);
            }

        } catch ( Exception e ) {
            logger.error(methodName, null, "Error restoring checkpoint:", e);
            ret = false;
        } finally {
            if ( h != null ) h.close();
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
