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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.uima.ducc.common.Pair;
import org.apache.uima.ducc.common.node.metrics.ProcessGarbageCollectionStats;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.common.ADuccWorkExecutable;
import org.apache.uima.ducc.transport.event.common.DuccWorkMap;
import org.apache.uima.ducc.transport.event.common.DuccWorkReservation;
import org.apache.uima.ducc.transport.event.common.IDuccProcess;
import org.apache.uima.ducc.transport.event.common.IDuccProcessMap;
import org.apache.uima.ducc.transport.event.common.IDuccReservation;
import org.apache.uima.ducc.transport.event.common.IDuccReservationMap;
import org.apache.uima.ducc.transport.event.common.IDuccSchedulingInfo;
import org.apache.uima.ducc.transport.event.common.IDuccStandardInfo;
import org.apache.uima.ducc.transport.event.common.IDuccTypes.DuccType;
import org.apache.uima.ducc.transport.event.common.IDuccWork;
import org.apache.uima.ducc.transport.event.common.IDuccWorkJob;
import org.apache.uima.ducc.transport.event.common.IDuccWorkReservation;
import org.apache.uima.ducc.transport.event.common.IDuccWorkService;
import org.apache.uima.ducc.transport.event.common.ITimeWindow;
import org.apache.uima.ducc.transport.event.common.history.IHistoryPersistenceManager;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.exceptions.NoHostAvailableException;


public class HistoryManagerDb 
    implements IHistoryPersistenceManager 
{

	
	private DuccLogger logger = null;
    private DbManager dbManager;

    PreparedStatement jobBlobPrepare = null;
    PreparedStatement reservationBlobPrepare = null;
    PreparedStatement serviceBlobPrepare = null;
    PreparedStatement ckptPrepare = null;

    PreparedStatement processDetailsPrepare = null;        // "process" for things that aren't "reservations"
    PreparedStatement reservationAllocPrepare = null;      // "process" for things that are    "reservaitons" 

    PreparedStatement jobDetailsPrepare = null;
    PreparedStatement reservationDetailsPrepare = null;

    static final String JOB_HISTORY_TABLE  = OrWorkProps.JOB_HISTORY_TABLE.pname();
    static final String RES_HISTORY_TABLE  = OrWorkProps.RESERVATION_HISTORY_TABLE.pname();
    static final String SVC_HISTORY_TABLE  = OrWorkProps.SERVICE_HISTORY_TABLE.pname();
    static final String CKPT_TABLE = OrCkptProps.CKPT_TABLE.pname();
    static final String PROCESS_TABLE = OrProcessProps.TABLE_NAME.pname();
    static final String JOB_TABLE = OrJobProps.TABLE_NAME.pname();
    static final String RESERVATION_TABLE = OrReservationProps.TABLE_NAME.pname();
		
    static String[] alltables = {JOB_HISTORY_TABLE,
                                 RES_HISTORY_TABLE,
                                 SVC_HISTORY_TABLE,
                                 CKPT_TABLE,
                                 PROCESS_TABLE,
                                 JOB_TABLE,
                                 RESERVATION_TABLE}
        ;

    // Jira 4804 For now don't save details in tables: jobs, reservations, & processes
    static final boolean saveDetails  = System.getenv("SAVE_DB_DETAILS") == null ? false : true;
    
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
                jobBlobPrepare          = h.prepare("INSERT INTO " + JOB_HISTORY_TABLE + " (ducc_id, type, history, work) VALUES (?, ?, ?, ?) ;");            
                reservationBlobPrepare  = h.prepare("INSERT INTO " + RES_HISTORY_TABLE + " (ducc_id, type, history, work) VALUES (?, ?, ?, ?) ;");            
                serviceBlobPrepare      = h.prepare("INSERT INTO " + SVC_HISTORY_TABLE + " (ducc_id, type, history, work) VALUES (?, ?, ?, ?) ;");            
                ckptPrepare             = h.prepare("INSERT INTO " + CKPT_TABLE + " (id, work, p2jmap) VALUES (?, ?, ?);");
               if (saveDetails) { // Jira-4804
                processDetailsPrepare   = h.prepare("INSERT INTO " + PROCESS_TABLE + " (host, ducc_id, share_id, type, user, memory, start, stop, class, pid, reason_agent, exit_code, reason_scheduler, cpu, swap_max, run_time, init_time, initialized, investment, major_faults, gc_count, gc_time) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ;");
                reservationAllocPrepare = h.prepare("INSERT INTO " + PROCESS_TABLE + " (host, ducc_id, share_id, type, user, memory, start, stop, class, run_time) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ;");
                jobDetailsPrepare = h.prepare("INSERT INTO " + JOB_TABLE + " (user, class, ducc_id, submission_time, duration, memory, reason, init_fails, errors, pgin, swap, total_wi, retries, preemptions, description) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ;");
                reservationDetailsPrepare = h.prepare("INSERT INTO " + RESERVATION_TABLE + " (user, class, ducc_id, submission_time, duration, memory, reason, processes, state, type, hosts, description) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");
               }  // Jira-4804
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
        String historyUrl = System.getProperty(DbManager.URL_PROPERTY);
        return init(historyUrl, null);
    }

    // package only, for the loader
    boolean init(DuccLogger logger, DbManager dbManager)
    	throws Exception
    {
    	this.logger = logger;
        String stateUrl = System.getProperty(DbManager.URL_PROPERTY);
        return init(stateUrl, dbManager);
    }

    /**
     * For bulk loader, we drop some of the indices during loading.
     *
     * Some of the tables are not a problem during bulk loading so we only externalize the
     * indexes on some tables.
     */
    static ArrayList<SimpleStatement> dropIndices()
    {
        ArrayList<SimpleStatement> ret = new ArrayList<SimpleStatement>();

        List<String> indexes = DbUtil.dropIndices(OrProcessProps.values(), PROCESS_TABLE);
        for ( String s : indexes ) {
            ret.add(new SimpleStatement(s));
        }

        indexes = DbUtil.dropIndices(OrJobProps.values(), JOB_TABLE);
        for ( String s : indexes ) {
            ret.add(new SimpleStatement(s));
        }

        indexes = DbUtil.dropIndices(OrReservationProps.values(), RESERVATION_TABLE);
        for ( String s : indexes ) {
            ret.add(new SimpleStatement(s));
        }

        return ret;
    }

    /**
     * For bulk loader, we must recreate indices
     *
     * Some of the tables are not a problem during bulk loading so we only externalize the
     * indexes on some tables.
     */
    static ArrayList<SimpleStatement>  createIndices()
    {
        ArrayList<SimpleStatement> ret = new ArrayList<SimpleStatement>();

        List<String> indexes = DbUtil.mkIndices(OrProcessProps.values(), PROCESS_TABLE);
        for ( String s : indexes ) {
            ret.add(new SimpleStatement(s));
        }

        indexes = DbUtil.mkIndices(OrJobProps.values(), JOB_TABLE);
        for ( String s : indexes ) {
            ret.add(new SimpleStatement(s));
        }

        indexes = DbUtil.mkIndices(OrReservationProps.values(), RESERVATION_TABLE);
        for ( String s : indexes ) {
            ret.add(new SimpleStatement(s));
        }

        return ret;
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
        buf.append("WITH CLUSTERING ORDER BY (ducc_id desc)");
        ret.add(new SimpleStatement(buf.toString()));

        List<String> indexes = DbUtil.mkIndices(OrWorkProps.values(), tablename);
        for ( String s : indexes ) {
            ret.add(new SimpleStatement(s));
        }

        return ret;
    }

    static ArrayList<SimpleStatement> mkSchema()
    	throws Exception
    {
        ArrayList<SimpleStatement> ret = new ArrayList<SimpleStatement>();

        ret.addAll(mkSchema(JOB_HISTORY_TABLE));
        ret.addAll(mkSchema(RES_HISTORY_TABLE));
        ret.addAll(mkSchema(SVC_HISTORY_TABLE));

        StringBuffer buf = new StringBuffer("CREATE TABLE IF NOT EXISTS " + CKPT_TABLE + " (");
        buf.append(DbUtil.mkSchema(OrCkptProps.values()));
        buf.append(")");
        ret.add(new SimpleStatement(buf.toString()));

       if (saveDetails) {   // Jira 4804
          
        buf = new StringBuffer("CREATE TABLE IF NOT EXISTS " + PROCESS_TABLE + " (");
        buf.append(DbUtil.mkSchema(OrProcessProps.values()));
        buf.append(")");
        ret.add(new SimpleStatement(buf.toString()));

        buf = new StringBuffer("CREATE TABLE IF NOT EXISTS " + JOB_TABLE + " (");
        buf.append(DbUtil.mkSchema(OrJobProps.values()));
        buf.append(")");
        ret.add(new SimpleStatement(buf.toString()));


        buf = new StringBuffer("CREATE TABLE IF NOT EXISTS " + RESERVATION_TABLE + " (");
        buf.append(DbUtil.mkSchema(OrReservationProps.values()));
        buf.append(")");
        ret.add(new SimpleStatement(buf.toString()));
        
        // PLEASE NOTE: The process, job, and reservation tables can have 10000s, 100000s or 1000000s of records during bulk
        // load of a system that has been running a while during execution of DbLoader.  The DbLoader will drop the indexes
        // on these three tables and then recreate them.  To support this we break out the creation into another
        // routine that can be called from the loader.
        ret.addAll(createIndices());
       }   // Jira 4804
       
        return ret;
    }

    // ----------------------------------------------------------------------------------------------------

    int toInt(String i)
    {
        if ( i == null ) return 0;
        try {
            return Integer.parseInt(i);
        } catch ( Exception e ) {
            return 0;
        }
    }

    String getString(String s)
    {
        return s == null ? "<none>" : s;
    }

    void summarizeJob(DbHandle h, IDuccWork w, String type)
    	throws Exception
    {
        IDuccWorkJob j = (IDuccWorkJob) w;
        // need duccid, user, class, submission-time, duration, memory, exit-reason, init-fails, pgin, swap, total-wi, retries, preemptions, description

        long ducc_id  = j.getDuccId().getFriendly();
        
        IDuccStandardInfo dsi = j.getStandardInfo();
        IDuccSchedulingInfo dsx = j.getSchedulingInfo();
        
        String user = dsi.getUser();
        String jclass = getString(dsx.getSchedulingClass());

        int memory = toInt(dsx.getMemorySizeRequested());
        long submission = dsi.getDateOfSubmissionMillis();
        long completion = dsi.getDateOfCompletionMillis();
        long duration = Math.max(0, completion - submission);
        String reason = getString(j.getCompletionType().toString());
        int init_fails = (int) j.getProcessInitFailureCount();
        long pgin = j.getPgInCount();
        long swap = (long) j.getSwapUsageGbMax();
        int wi = (int) j.getWiTotal();
        int errors = toInt(dsx.getWorkItemsError());
        int retries = toInt(dsx.getWorkItemsRetry());
        int preemptions = toInt(dsx.getWorkItemsPreempt());
        String description = getString(dsi.getDescription());
        h.execute(jobDetailsPrepare, user, jclass, ducc_id, submission, duration, memory, reason, init_fails, errors, pgin, swap, wi, retries, preemptions, description);
    }

    void summarizeProcesses(DbHandle h, IDuccWork w, String type)
    	throws Exception
    {
        // Loop through the processes on w saving useful things:
        //    jobid, processid, node, user, type of job, PID, duration, start timestamp, stop timestamp, exit state,
        //    memory, CPU, exit code, swap max, investment, init time
        long work_id = w.getDuccId().getFriendly();
        switch ( w.getDuccType() ) {
            case Job:
            case Service:
            case Pop:
                {
                    IDuccProcessMap m = ((ADuccWorkExecutable)w).getProcessMap();
                    Map<DuccId, IDuccProcess> map = m.getMap();
                    IDuccStandardInfo dsi = w.getStandardInfo();
                    IDuccSchedulingInfo dsx = w.getSchedulingInfo();
                    
                    String user = dsi.getUser();
                    int memory = toInt(dsx.getMemorySizeRequested());
                    String sclass = dsx.getSchedulingClass();

                    for ( IDuccProcess idp : map.values() ) {
                        long share_id = idp.getDuccId().getFriendly();
                        long pid = toInt(idp.getPID());
                        String node = getString(idp.getNodeIdentity().getName());
                        String reason_agent = getString(idp.getReasonForStoppingProcess()); // called "reason" in duccprocess but not in ws
                        String extended_reason_agent = getString(idp.getExtendedReasonForStoppingProcess()); 
                        String reason_scheduler = getString(idp.getProcessDeallocationType().toString()); // called "processDeallocationType" in duccprocess but not in ws
                        int exit_code = idp.getProcessExitCode();
                        long cpu = idp.getCurrentCPU();
                        long swap = idp.getSwapUsageMax();
                        
                        ITimeWindow itw = idp.getTimeWindowInit();
                        long processStart = 0;
                        long initTime = 0;
                        if ( itw != null ) {
                            processStart = itw.getStartLong();
                            initTime = itw.getElapsedMillis();
                        }
                        itw = idp.getTimeWindowRun();
                        long processEnd = 0;
                        if ( itw != null ) {
                            processEnd = idp.getTimeWindowRun().getEndLong();
                        }
                        boolean initialized = idp.isInitialized();
                        long investment = idp.getWiMillisInvestment();
                        long major_faults = idp.getMajorFaults();
                        long gccount = 0;
                        long gctime = 0;
                        ProcessGarbageCollectionStats gcs = idp.getGarbageCollectionStats();
                        if ( gcs != null ) {
                        	gccount = gcs.getCollectionCount();
                            gctime = gcs.getCollectionTime();
                        }
                        h.execute(processDetailsPrepare, node, work_id, share_id, type, user, memory, processStart, processEnd, sclass,
                                  pid, reason_agent, extended_reason_agent, exit_code, reason_scheduler, cpu, swap, Math.max(0, (processEnd-processStart)), initTime,
                                  initialized, investment, major_faults, gccount, gctime);
                    }

                }
                break;
 
            case Reservation:
                {
                	IDuccReservationMap m = ((IDuccWorkReservation)w).getReservationMap();
                    Map<DuccId, IDuccReservation> map = m.getMap();
                    IDuccStandardInfo dsi = w.getStandardInfo();
                    IDuccSchedulingInfo dsx = w.getSchedulingInfo();
                    long start = dsi.getDateOfCompletionMillis();
                    long stop = dsi.getDateOfSubmissionMillis();
                    int memory_size = 0;
                    if ( dsx.getMemorySizeRequested() == null ) {
                    	memory_size = toInt(dsx.getMemorySizeRequested());
                    }
                    for ( IDuccReservation idr : map.values() ) {
                        String node = "<none>";
                        if ( idr.getNode() != null ) {
                            node = idr.getNode().getNodeIdentity().getName();
                        }
                    	try {
							h.execute(reservationAllocPrepare, node, work_id,
							          idr.getDuccId().getFriendly(), type, getString(dsi.getUser()), memory_size, 
							          start, stop, getString(dsx.getSchedulingClass()), Math.max(0, (stop-start)) );
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
                    }
                    
                }
                break;
                
            default :
                break;
        }
    }

    void summarizeReservation(DbHandle h, IDuccWork w)
    	throws Exception
    {
        DuccWorkReservation r = (DuccWorkReservation) w; // cannot use the interface because it is incomplete

        long ducc_id  = r.getDuccId().getFriendly();

        IDuccStandardInfo dsi = r.getStandardInfo();
        IDuccSchedulingInfo dsx = r.getSchedulingInfo();
        
        String user = dsi.getUser();
        String jclass = dsx.getSchedulingClass();

        int memory = toInt(dsx.getMemorySizeRequested());
        long submission = dsi.getDateOfSubmissionMillis();
        long completion = dsi.getDateOfCompletionMillis();
        long duration = Math.max(0, completion - submission);
        String reason = getString(r.getCompletionType().toString());
        String description = getString(dsi.getDescription());

        List<String> nodes = r.getNodes();
        int processes = nodes.size();
        StringBuffer buf = new StringBuffer("");
        for ( int i = 0; i < processes; i++ ) {
            buf.append(nodes.get(i));
            if ( i < (processes-1) ) buf.append(" ");
        }
        String hosts = buf.toString();
        String type = "R";

        String state = r.getReservationState().toString();

        h.execute(reservationDetailsPrepare, user, jclass, ducc_id, submission, duration, memory, reason, processes, state, type, hosts, description);

    }

    void saveWork(PreparedStatement s, IDuccWork w, boolean isHistory)
        throws Exception
    {
    	String methodName = "saveWork";
        Long nowP =  System.currentTimeMillis();
        String type = null;
        String processType = null;

        switch ( w.getDuccType() ) {
        case Job:
            type = "job";
            processType = "J";
            break;
        case Service:
        case Pop:
                switch ( ((IDuccWorkService)w).getServiceDeploymentType() ) 
                    {
                    case uima:
                    case custom:
                        type = "service";
                        processType = "S";
                        break;
                    case other:
                        type = "AP";
                        processType = "A";
                        break;
                    default :
                        break;
                    }
                break;
            case Reservation:
                type = "reservation";
                processType = "R";
                // ?? why are unmanaged reservations saved in processes?  because they have a share/rm-id ?
                break;
            default:
                // illegal - internal error if this happens
                logger.error(methodName, w.getDuccId(), "Unknown job type", w.getDuccType(), "Cannot save to database.");
                return;
        }
        logger.info(methodName, w.getDuccId(), "saving " + type);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream out = new ObjectOutputStream(baos);
        out.writeObject(w);
        out.close();
        byte[] bytes = baos.toByteArray();
        ByteBuffer buf = ByteBuffer.wrap(bytes);

        DbHandle h = dbManager.open();
        h.saveObject(s,  w.getDuccId().getFriendly(), type, isHistory, buf);

       if (saveDetails) {  // Jira-4804
        switch ( w.getDuccType() ) {
            case Job:
                summarizeJob(h, w, "J");
                break;
            case Service:
            case Pop:       
                break;
            case Reservation:
                break;
            default:
                break;      // Can't get here, we'd abort above in this case
        }

        summarizeProcesses(h, w, processType);    // summarize each process of the work 
       } // Jira-4804
        logger.trace(methodName, w.getDuccId(), "----------> Time to save", type, ":", System.currentTimeMillis() - nowP, "Size:", bytes.length, "bytes.");        
    }

    /**
     * Part of history management, recover the indicated job from history.
     */
    @SuppressWarnings("unchecked")
	<T> T restoreWork(Class<T> cl, String tablename, long friendly_id)
        throws Exception
    {
    	String methodName = "restoreWork";
        T ret = null;
        DbHandle h = null;

        h = dbManager.open();
        String cql = "SELECT WORK FROM " + tablename + " WHERE DUCC_ID=" + Long.toString(friendly_id);
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
    @SuppressWarnings("unchecked")
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
        saveWork(jobBlobPrepare, j, true);
    }

	
    /**
     * Part of history management, recover ths indicated job from history.
     */
    public IDuccWorkJob restoreJob(long friendly_id)
        throws Exception
    {
        return (IDuccWorkJob) restoreWork(IDuccWorkJob.class, JOB_HISTORY_TABLE, friendly_id);
    }
    
    /**
     * Part of history management, recover ths indicated jobs from history.
     */
    public ArrayList<IDuccWorkJob> restoreJobs(long max)
        throws Exception
    {
        return restoreSeveralThings(IDuccWorkJob.class, JOB_HISTORY_TABLE, max);
    }
    // End of jobs section
    // ----------------------------------------------------------------------------------------------------


    // ----------------------------------------------------------------------------------------------------
    // Reservations section

    // Save to history only
	public void saveReservation(IDuccWorkReservation r) 
        throws Exception 
    {
        saveWork(reservationBlobPrepare, r, true);
    }

    /**
     * Part of history management, recover ths indicated reservation from history.
     */
	public IDuccWorkReservation restoreReservation(long duccid)
        throws Exception
    {
        return (IDuccWorkReservation) restoreWork(IDuccWorkReservation.class, RES_HISTORY_TABLE, duccid);
    }
	
    /**
     * Part of history management, recover ths indicated reservations from history.
     */
	public ArrayList<IDuccWorkReservation> restoreReservations(long max) 
		throws Exception
    {
        return restoreSeveralThings(IDuccWorkReservation.class, RES_HISTORY_TABLE, max);
    }

    // End of reservations section
    // ----------------------------------------------------------------------------------------------------
	

    // ----------------------------------------------------------------------------------------------------
    // Services section

    public void saveService(IDuccWorkService s)
    	throws Exception
    {
        saveWork(serviceBlobPrepare, s, true);
    }

	
    /**
     * Part of history management, recover ths indicated service instance from history.
     */
	public IDuccWorkService restoreService(long duccid)
		throws Exception
    {
        return (IDuccWorkService) restoreWork(IDuccWorkService.class, SVC_HISTORY_TABLE, duccid);
	}
	
    /**
     * Part of history management, recover ths indicated service instances from history.
     */
	public ArrayList<IDuccWorkService> restoreServices(long max) 
		throws Exception
    {
        return restoreSeveralThings(IDuccWorkService.class, SVC_HISTORY_TABLE, max);
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
            
            // Just insert/update the one row of checkpoint data - Jira 4892 - don't truncate as it creates snapshots
            DbHandle h = dbManager.open();
            h.saveObject(ckptPrepare, 0, workbuf, mapbuf);       

        } catch ( Exception e ) {
            logger.error(methodName, null, "Cannot save ProcessToJob map", e);
            ret = false;
        } finally {
            if ( ret ) logger.trace(methodName, null, "Saved Orchestrator Checkpoint");
        }

        logger.trace(methodName, null, "Total time to save checkpoint:", System.currentTimeMillis() - now);
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
                if(r == null) {
                	continue;
                }
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
                @SuppressWarnings("unchecked")
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
                
                ret = new Pair<DuccWorkMap, Map<DuccId, DuccId>>(work, processToJob);
            }

       } catch ( Exception e ) {
            logger.error(methodName, null, "Error restoring checkpoint:", e);
        } 
        
        return ret;
    }
    
    // End of OR checkpoint save and restore
    // ----------------------------------------------------------------------------------------------------
    
    public void shutdown()
    {
        dbManager.shutdown();
    }

    // End of common
    // ----------------------------------------------------------------------------------------------------
		
}
