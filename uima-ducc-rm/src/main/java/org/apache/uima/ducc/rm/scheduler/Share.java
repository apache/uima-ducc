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
package org.apache.uima.ducc.rm.scheduler;

import java.util.HashMap;
import java.util.Map;

import org.apache.uima.ducc.common.Node;
import org.apache.uima.ducc.common.NodeIdentity;
import org.apache.uima.ducc.common.persistence.rm.IDbShare;
import org.apache.uima.ducc.common.persistence.rm.IRmPersistence;
import org.apache.uima.ducc.common.persistence.rm.RmPersistenceFactory;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.common.IProcessState.ProcessState;
import org.apache.uima.ducc.transport.event.common.ITimeWindow;
import org.apache.uima.ducc.transport.event.common.TimeWindow;



/**
 * This may more correctly thought of as representing a Process.
 *
 * A share is ALWAYS associated with a Machine.
 */
public class Share
	implements SchedConstants, 
               IDbShare
{
    private transient DuccLogger logger = DuccLogger.getLogger(Share.class, COMPONENT_NAME);
    
    private transient Machine machine;               // machine associatede with this share, assigned after "what-of"
    private DuccId id = null;              // unique *within this machine*         assigned after "what-of"
    private transient IRmJob job = null;;            // job executing in this share, if any, assigned after "what-of"
    private DuccId bljobid = null;         // UIMA-4142 ID of blacklisted job
    private int share_order;               // may not be same as machine's order

    private ITimeWindow init_time = null;  // how much time this process spends initializing

    // private HashMap<Integer, Long> activeQuestions = new HashMap<Integer, Long>();

    private boolean evicted = false;      // during preemption, remember this share has already been removed
                                          // (multiple classes might try to evict, this prevents multiple eviction)
    private boolean purged = false;       // share is forcibly removed because it's machine died
    private boolean fixed = false;        // if true, can't be preempted

    private long investment = 0;          // Current time for all ACTIVE work items in the process

    // note this returns a global static instance, no need to staticisze it here
    private IRmPersistence persistence = null;

     @SuppressWarnings("unused")
 	private long resident_memory = 0;
     @SuppressWarnings("unused")
 	private ProcessState state = ProcessState.Undefined;
     @SuppressWarnings("unused")
 	private String pid = "<none>";

    Map<String, Object> getShareProperties()
    {
        Map<String, Object> ret = new HashMap<String, Object>();
        ret.put("numeric_id", id.getFriendly());
        ret.put("uuid", id.getUnique());
        ret.put("share_order", share_order);
        ret.put("init_time", init_time.getElapsedMillis());
        ret.put("evicted", evicted);
        ret.put("purged", purged);
        ret.put("fixed", fixed);
        ret.put("investment", investment);
        return ret;
    }

    /**
     * This constructor is used during recovery ONLY.
     */
    public Share(DuccId id, Machine machine, IRmJob job, int share_order)
    {
        this.id = id;
        this.machine = machine;
        this.job = job;
        this.bljobid = null;        // UIMA-4142
        this.share_order = share_order;
        this.persistence = RmPersistenceFactory.getInstance(this.getClass().getName(), "RM");
    }

    /**
     * UIMA-4142
     * This constructor is for a blacklisted share.  We can't make a job but we know what the
     * job's id would have been if we could.
     */
    public Share(DuccId id, Machine machine, DuccId jobid, int share_order)
    {
        this.id = id;
        this.machine = machine;
        this.job = null;
        this.bljobid = jobid;       // UIMA-4142
        this.share_order = share_order;
        this.persistence = RmPersistenceFactory.getInstance(this.getClass().getName(), "RM");
    }

    /**
     * Normal constructor.
     */
    public Share(Machine machine, IRmJob job, int share_order)
    {
        this.machine = machine;
        this.id = Scheduler.newId();
        this.job = job;
        this.bljobid = null;        // UIMA-4142
        this.share_order = share_order;
        this.persistence = RmPersistenceFactory.getInstance(this.getClass().getName(), "RM");
    }

//     /**
//      * Simulation only.
//      */
//     void questionStarted(WorkItem q)
//     {
//         if ( activeQuestions.containsKey(q.getId()) ) {
//             throw new SchedulingException(job.getId(), "Share.questionStarted: work item " + q.getId() + " already running in share " + toString());
//         } 
        
//         activeQuestions.put(q.getId(), System.currentTimeMillis());
//     }

//     /**
//      * Simulation only.
//      */
//     void questionComplete(WorkItem q)
//     {
//     	String methodName = "questionComplete";
//         if ( !activeQuestions.containsKey(q.getId()) ) {
//             throw new SchedulingException(job.getId(), "Share.questionComplete: work item " + q.getId() + " not found in share " + toString());
//         }

//         logger.info(methodName, job.getId(), q.toString(), " completes in ", 
//                    ""+(System.currentTimeMillis() - activeQuestions.get(q.getId())), " milliseconds in share ", toString());
//         activeQuestions.remove(q.getId());
//     }

    public NodePool getNodepool()
    {
        return machine.getNodepool();
    }

    public int getNodepoolDepth()
    {
        return getNodepool().getDepth();
    }

    public String getNodepoolId()
    {
        return getNodepool().getId();
    }

    public IRmJob getJob()
    {
        return job;
    }

    public DuccId getId()
    {
        return id;
    }

    // UIMA-4142
    public boolean isBlacklisted()
    {
        return bljobid != null;
    }

    // UIMA-4142
    public DuccId getBlJobId()
    {
        return bljobid;
    }

    Machine getMachine()
    {
        return machine;
    }


    long getHostMemory()
    {
        if ( machine != null ) return machine.getMemory();
        else                   return 0L;        // no clue what it is; don't npe
    }

    boolean isPending()
    {
        return job.isPendingShare(this);
    }

    /**
     * Defrag - this (pending) share is given to a different job before OR learns about it.
     */
    void reassignJob(IRmJob job)
    {
        this.job = job;
    }

    public NodeIdentity getNodeIdentity()
    {
        return machine.getNodeIdentity();
    }

    public Node getNode()
    {
    	return machine.getNode();
    }
    
    /**
     * The order of the share itself.
     */
    public int getShareOrder()
    {
        return share_order;
    }

    /**
     * Update the share order, used during recovery.
     */
    void setShareOrder(int o)
    {
        this.share_order = o;
    }

    /**
     * The size of the machine the share resides on.
     */
    int getMachineOrder()
    {
        return machine.getShareOrder();
    }

    /**
     * It's forceable if it's not a permanent share and it's not already evicted for some reason.
     */
    boolean isForceable()
    {
        if ( evicted ) return false;
        if ( purged )  return false;
        if ( fixed )   return false;
        return true;
    }

    /**
     * It's preemptable if:
     *   - it's not yet preempted
     *   - it belongs to a job that has a "loser" count > 0
     *   - it's fair-share share
     */
    boolean isPreemptable()
    {
        return ( isForceable() && (( job.countNShares() - job.countNSharesGiven()) > 0 ));
        // This last works because if the job has preempted shares they count against the countNShares count
        // so we don't end up counting this job more than it deserves.
    }

    public boolean update(DuccId jobid, long mem, long investment, ProcessState state, ITimeWindow init_time, String pid)
    {
    	String methodName = "update";
        if ( ! jobid.equals(job.getId()) ) return false;      // something has gone horribly wrong
        
        this.resident_memory = mem;
        this.investment = investment;
        this.state = state;
        this.pid = pid;
        this.init_time = init_time;
        try {
            long npid = -1L;               
            if ( pid != null ) {              // OR sends junk here for a while
                npid = Long.parseLong(pid);
            }

			persistence.updateShare(getNode().getNodeIdentity().getName(), id, jobid, investment, state.toString(), getInitializationTime(), npid);
		} catch (Exception e) {
            logger.warn(methodName, job.getId(), "Cannot update share statistics in database for share", id, e);
		}
        logger.info(methodName, jobid, "UPDATE:", investment, state, getInitializationTime(), pid);
        return true;
    }

//    /**
//     * Calculate the "investment" this share has in the questions it's running.
//     * Currently, this will be the longest time any of the current questions has been running.
//     *
//     * We have to separete this from "getting" so we can freeze the timestamp - in a sort the
//     * getter could be called multiple times, and get inconsistent results.
//     */
//    synchronized void calculateInvestment()
//    {    
//    	investment = 0;
//        long now = System.currentTimeMillis();
//        for ( Long elapsed : activeQuestions.values() ) {
//            investment = Math.max(investment, (now - elapsed));
//        }
//    }

    /**
     * Caller must always call calculateInvestment before retrieving the investment, or it is
     * likely to be wrong.
     *
     * Investment is a combination of initialization time and execution time.  We always
     * want shares that aren't yet initialized to sort LOWER than shares that are initialized.
     *
     * For now we only take into account initialzation time.
     *
     * NOTE: We no longer set investment directly.  Instead each state update refreshes the
     *      init_time and run_time structures and we calculate investment from that.  This
     *      affects the RM simulator, which will need updating if we decide to revive it.     
     */
    long getInvestment()
    {
        return investment;
    }

    /**
     * Returns only initialization time.  Eventually getInvestment() may take other things into
     * consideration so we separate these two (even though currently they do the same thing.)
     */
    public long getInitializationTime()
    {
        if ( init_time == null ) return 0;
        return init_time.getElapsedMillis();        
    }

    public void setInitializationTime(long millis)
    {
    	String methodName = "setInitializationTme";
        logger.info(methodName, null, "SET INIT TIME", "shareid", id, millis);

        init_time = new TimeWindow();
        init_time.setStartLong(0);
        init_time.setEndLong(millis);
    }

    boolean isInitialized()
    {
        if ( init_time == null ) return false;

        return (init_time.getEnd() != null);
    }

    public void setFixed()
    {
    	String methodName = "setFixed";
        fixed = true;
        try {
			persistence.setFixed(getNode().getNodeIdentity().getName(), id, job.getId(), true);
		} catch (Exception e) {
            logger.warn(methodName, job.getId(), "Cannot update 'fixed' in database for share", id, e);
		}
    }

    public boolean isFixed()
    {
        return fixed;
    }

    void evict()
    {
    	String methodName = "evicted";
        evicted = true;
        try {
			persistence.setEvicted(getNode().getNodeIdentity().getName(), id, job.getId(), true);
		} catch (Exception e) {
            logger.warn(methodName, job.getId(), "Cannot update 'evicted' in database for share", id, e);
		}
    }

    public boolean isEvicted()
    {
        return evicted || purged;
    }

    void purge()
    {
    	String methodName = "purge";
        purged = true;
        try {
			persistence.setPurged(getNode().getNodeIdentity().getName(), id, job.getId(), true);
		} catch (Exception e) {
            logger.warn(methodName, job.getId(), "Cannot update 'purge bit' in database for share", id, e);
		}
    }

    public boolean isPurged()
    {
        return purged;
    }

    @Override
    public int hashCode()
    {
        return id.hashCode();
    }

    @Override
    public boolean equals(Object o)
    {
        if ( o == null ) return false;
        if ( this == o ) return true;
        if ( this.getClass() != o.getClass() ) return false;

        Share s = (Share) o;
        //return (id.equals(m.getId()));
    	return this.id.equals(s.getId());
    }

    public String toString()
    {
        return machine.getId() + "." + getId();
    }

}
            
