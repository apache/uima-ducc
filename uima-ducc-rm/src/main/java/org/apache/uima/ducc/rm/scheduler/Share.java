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

import org.apache.uima.ducc.common.Node;
import org.apache.uima.ducc.common.NodeIdentity;
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
	implements SchedConstants
{
    //private transient DuccLogger logger = DuccLogger.getLogger(Share.class, COMPONENT_NAME);
    private Machine machine;               // machine associatede with this share, assigned after "what-of"
    private DuccId id;                     // unique *within this machine*         assigned after "what-of"
    private IRmJob job;                    // job executing in this share, if any, assigned after "what-of"
    private int share_order;               // may not be same as machine's order

    private ITimeWindow init_time = null;  // how much time this process spends initializing

    // private HashMap<Integer, Long> activeQuestions = new HashMap<Integer, Long>();

    private boolean evicted = false;      // during preemption, remember this share has already been removed
                                          // (multiple classes might try to evict, this prevents multiple eviction)
    private boolean purged = false;       // share is forcibly removed because it's machine died
    private boolean fixed = false;        // if true, can't be preempted

    private long investment = 0;          // Current time for all ACTIVE work items in the process

     @SuppressWarnings("unused")
 	private long resident_memory = 0;
     @SuppressWarnings("unused")
 	private ProcessState state = ProcessState.Undefined;
     @SuppressWarnings("unused")
 	private String pid = "<none>";


    /**
     * This constructor is used during recovery ONLY.
     */
    public Share(DuccId id, Machine machine, IRmJob job, int share_order)
    {
        this.id = id;
        this.machine = machine;
        this.job = job;
        this.share_order = share_order;
    }

    /**
     * Normal constructor.
     */
    public Share(Machine machine, IRmJob job, int share_order)
    {
        this.machine = machine;
        this.id = Scheduler.newId();
        this.job = job;
        this.share_order = share_order;
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

    Machine getMachine()
    {
        return machine;
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
        if ( ! jobid.equals(job.getId()) ) return false;      // something has gone horribly wrong
        
        this.resident_memory = mem;
        this.investment = investment;
        this.state = state;
        this.pid = pid;
        this.init_time = init_time;
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
        fixed = true;
    }

    boolean isFixed()
    {
        return fixed;
    }

    void evict()
    {
        evicted = true;
    }

    boolean isEvicted()
    {
        return evicted || purged;
    }

    void purge()
    {
        purged = true;
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
            
