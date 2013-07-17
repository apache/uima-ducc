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

/**
 * This class represents a job inside the scheduler.
 */
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.SystemPropertyResolver;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.common.IDuccTypes.DuccType;


public class RmJob
	implements SchedConstants, 
               IRmJob
{
    DuccLogger     logger = DuccLogger.getLogger(RmJob.class, COMPONENT_NAME);
	static final int DEFAULT_NTHREADS = 4;
	
    protected DuccId   id;                            // sched-assigned id (maybe delegate to job manager eventually)
    protected DuccType ducc_type;                     // for messages so we can tell what kind of job
    protected String name;                            // user's name for job
    protected String resource_class_name;             // Name of the res class, from incoming job parms
    protected ResourceClass resource_class;           // The actual class, assigned as job is received in scheduler.
    protected int    user_priority;                   // user "priority", really apportionment 

    protected int n_machines;                         // RESERVE:     minimum machines to allocate
    protected int min_shares;                         // FIXED_SHARE: minimum N shares to allocate
    protected int max_shares;                         // FAIR_SHARE:  maximum N shares to allocate
    protected boolean is_reservation = false;

    protected int threads;                            // threads per process

    protected int memory;                             // estimated memory usage
    protected int nquestions;                         // number of work-items in total
    protected int nquestions_remaining;               // number of uncompleted work items
    protected double time_per_item = 0.0;             // from OR - mean time per work item

    protected int share_order = 0;                    // How many shares per process this job requires (calculated on submission)

    protected int share_cap = Integer.MAX_VALUE;      // initially; scheduler policy will reset as the job ages
    protected int pure_fair_share = 0;                // pure uncapped un-bonused share for this job

    protected long submit_time;                       // when job is submitted ( sched or job-manager sets this )

    protected String username;
    protected User user;                              // user id, enforced by submit and job manager. we just believe it in sched.

    //
    // We keep track of three things related to machines:
    // 1.  All the machines the job is running on.
    // 2.  The machines the job will be exanded to run on but which aren't yet dispatched
    // 3.  The machines to be removed from the job, but which the job is still running on
    //
    protected HashMap<Share, Share> assignedShares;      // job is dispatched to these
    protected HashMap<Share, Share> pendingShares ;      // job is scheduled for these but not yet confirmed
    protected HashMap<Share, Share> pendingRemoves;      // job is scheduled to remove these but not confirmed
    protected HashMap<Share, Share> recoveredShares;     // recovery after bounce, need to reconnect these

    // track shares by machine, and machines, to help when we have to give stuff away
    Map<Machine, Map<Share, Share>> sharesByMachine = new HashMap<Machine, Map<Share, Share>>();
    Map<Machine, Machine> machineList = new HashMap<Machine, Machine>();

    // protected int shares_given;                       // during scheduling, how many N-shares we get
    int[] given_by_order;                                // during scheduling, how many N-shares we get
    int[] wanted_by_order;                               // during scheduling, how many N-shares we we want - volatile, changes during countJobsByOrder
    protected boolean init_wait;                         // If True, we're waiting for orchestrator to tell us that init is successful.
                                                         // Until then, we give out only a very small share.
    protected boolean completed = false;                 // RmJob can linger a bit after completion for the
                                                         // defrag code - must mark it complete

    // For predicting end of job based on current rate of completion
    protected int orchestrator_epoch;
    protected int rm_rate;
    protected int ducc_epoch;                           

    protected Properties jobprops;                       // input that job is constructed from. currently is condensed from Blade logs (simulation only)

    protected String refusalReason = null;               // if refused, this is why, for the message

    private static Comparator<IEntity> apportionmentSorter = new ApportionmentSorterCl();

    protected RmJob()
    {
    }

    public RmJob(DuccId id)
    {
        this.id = id;

        orchestrator_epoch = SystemPropertyResolver.getIntProperty("ducc.orchestrator.state.publish.rate", 10000);
        rm_rate            = SystemPropertyResolver.getIntProperty("ducc.rm.state.publish.ratio", 4);
        ducc_epoch         = orchestrator_epoch * rm_rate;
    }
    
    // public RmJob(DuccId id, Properties properties)
    // {
    // 	this.jobprops = properties;
    //     this.id = id;
    // }
    
    /**
     * read the props file and set inital values for internal fields
     */
    public void init()
    {
        assignedShares  = new HashMap<Share, Share>();
        pendingShares   = new HashMap<Share, Share>();
        pendingRemoves  = new HashMap<Share, Share>();
        recoveredShares = new HashMap<Share, Share>();

        if ( max_shares == 0 ) max_shares = Integer.MAX_VALUE;
    }

    public DuccId getId()
    {
        return id;
    }

    public void setId(DuccId id)
    {
        this.id = id;
    }

    public long getFriendlyId()
    {
    	return id.getFriendly();
    }

    public String getName()
    {
    	return id.toString();
    }
    
    public void setJobName(String name)
    {
        this.name = name;
    }
    
    public void setReservation()
    {
        this.is_reservation = true;
    }

    public boolean isReservation()
    {
        return is_reservation;
    }

    public void markComplete()
    {
        completed = true;
    }

    public boolean isCompleted()
    {
        return completed;
    }

    /**
     *    The matrix:
     *    IW = am I in initializaton wait
     *    RU = am I runnable
     *    Resched = do we need to reschedule
     *    Reset   = set IW to this
     *    x       = don't care
     *
     *    IW   RU    Resched    Reset
     *     T    T       T         F
     *     T    F       F         T
     *     F    T       F         F
     *     F    F       x         x
     *
     *     So resched = IR & RU
     *        IW      = !RU
     *
     *    We return resched so caller knows the tickle the scheduler
     */
    public boolean setInitWait(boolean is_running)
    {
        boolean resched = init_wait & is_running;
        init_wait = !is_running;
        return resched;
    }

    /**
     * Save ref to the class we are in, and init class-based structures.
     */
    public void setResourceClass(ResourceClass cl)
    {
        this.resource_class = cl;
    }

    /**
     * Number of questions submitted, all must be answered.   This is used by job manager
     * to know when they've all been dealt with.
     */
    public int nQuestions()
    {
    	return nquestions;
    }

    public void setNQuestions(int allq, int remainingq, double time_per_item)
    {
        this.nquestions = allq;
        this.nquestions_remaining = remainingq;
        this.time_per_item = time_per_item;
    }

    /**
     * Number of questions still to be answered.  Used by scheduler to determing current
     * machine requirement.
     */
    public int nQuestionsRemaining()
    {
    	return nquestions_remaining;
    }


    public Map<Machine, Map<Share, Share>> getSharesByMachine()
    {
        return sharesByMachine;
    }

    public Map<Machine, Machine> getMachines()
    {
        return machineList;
    }

    /**
     * There are a fair number of piddling little methods to manage shares.  This high granularity is
     * needed in order to manage bits and pieces of the bookkeeping from different threads and queues
     * without blocking.
     *
     * TODO: maybe we can consolidate some of this after it's all worked out.
     */

    /**
     * Before each scheduling epoch, clear the counting from the last time.
     */
    public void clearShares()
    {
        // this.shares_given = 0;
        given_by_order = null;
    }

    /**
    public void addQShares(int s)
    {
        this.shares_given += ( s / share_order ) ;    // convert to N-shares == processes
    }
    */

    public void setPureFairShare(int pfs)
    {
        this.pure_fair_share = pfs;
    }

    public int getPureFairShare()
    {
        return pure_fair_share;
    }

    public int[] getGivenByOrder()
    {
        return given_by_order;
    }

    public void setGivenByOrder(int[] gbo)
    {
        this.given_by_order = gbo;
    }

    public int getShareWeight()
    {
        return 1;                         // all jobs are the same currently
    }

    public void initWantedByOrder(ResourceClass unused)
    {
        wanted_by_order              = NodePool.makeArray();
        wanted_by_order[share_order] = getJobCap();
        wanted_by_order[0]           = wanted_by_order[share_order];
    }

    public int[] getWantedByOrder()
    {
        return wanted_by_order;
    }

    public int calculateCap(int order, int total)
    {
        return Integer.MAX_VALUE;  // no cap for jobs
    }


    public int countNSharesGiven()
    {
        if ( given_by_order == null) { return 0; }
        return given_by_order[share_order];
    }

    public int countQSharesGiven()
    {
        return countNSharesGiven() * share_order;
    }

    /**
     * Number of N-shares I'm losing.
     */
    public int countNSharesLost()
    {
        return countNShares() - countNSharesGiven();
    }

//     /**
//      * Can I use more N-shares; how many?
//      */
//     public int canUseBonus(int bonus, int[] nSharesByOrder)
//     {
//         int cap = getJobCap();
//         int can_use = Math.max(0, cap - shares_given);         // what can we actually use?

//         if ( can_use > nSharesByOrder[share_order] ) {         // can't use more than physicalliy exist
//             return 0;
//         }

//         for (int i = share_order; i <= Math.min(bonus, (nSharesByOrder.length - 1)); i++ ) {
//             if ( (nSharesByOrder[i] > 0) &&  (i <= can_use) ) {
//                 return i ;
//             }
//         }
//        return 0;
//    }

    /**
     * Can I use more N-shares?

    public int canUseBonus(int bonus, int[] nSharesByOrder)
    {
        int cap = getJobCap();
        int can_use = Math.max(0, cap - countNSharesGiven());         // what can we actually use?
        
        //         if ( can_use > nSharesByOrder[share_order] ) {         // can't use more than physicalliy exist
        //             return 0;
        //         }
        if ( can_use == 0 ) {
            return 0;
        }
        return ( nSharesByOrder[share_order] > 0 ) ? share_order : 0;
    }
    */

    /**
     * Officially allocated shares assigned to this job which are known to be in use.
     */
    public HashMap<Share, Share> getAssignedShares()
    {
        return assignedShares;
    }

    /**
     * Shares recovered from the OR during job recovery.
     */
    public HashMap<Share, Share> getRecoveredShares()
    {
        return recoveredShares;
    }

    /**
     * Newly allocated shares that have not been dispatched.  They're unavailable for scheduling but
     * job manager doesn't know about them yet.  When we tell job manager we'll "promote" them to
     * the assignedShares list.
     */
    public HashMap<Share, Share> getPendingShares()
    {
        return pendingShares;
    }

    /**
     * We're dispatching, move machines to active list, and clear pending list.
     * Tell caller which machines are affected so it can dispatch them.
     */
    public HashMap<Share, Share> promoteShares()
    {
        HashMap<Share, Share> answer = new HashMap<Share, Share>();        
        for ( Share s : pendingShares.values() ) {
            assignedShares.put(s, s);
            Machine m = s.getMachine();
            machineList.put(m, m);
            Map<Share, Share> machine_shares = sharesByMachine.get(m);
            if ( machine_shares == null ) {
                machine_shares = new HashMap<Share, Share>();
                sharesByMachine.put(m, machine_shares);
            }
            machine_shares.put(s, s);

            answer.put(s, s);
        }
        pendingShares.clear();
        return answer;
    }

    /**
     * This share is being donated to someone more needy than I - see defrag code in NodepoolScheduler
     */
     public void cancelPending(Share s)
     {
         pendingShares.remove(s);
     }

//    public void assignReservation(Machine m)
//    {
//        reservations.add(m);
//    }
//
//    public ArrayList<Machine> getReservations(Machine m)
//    {
//        return reservations;
//    }

//    public int countReservations()
//    {
//        return reservations.size();
//    }
//
//    public boolean reservationComplete()
//    {
//        return reservations.size() == machines;
//    }

    /**
     * Scheduler found us a new toy, make it pending until it's given to job manager.
     */
    public void assignShare(Share s)
    {
        pendingShares.put(s, s);
    }

    /**
     * Job recovery: OR reports this share as one it already knew about.
     */
    public void recoverShare(Share s)
    {
        if ( (! assignedShares.containsKey(s)) && (!pendingShares.containsKey(s)) ) {
            recoveredShares.put(s, s);
        }
    }

    public boolean isPendingShare(Share s )
    {
        return pendingShares.containsKey(s);
    }

    /**
     * Do I have un-dispatched shares?
     */
    public boolean isExpanded()
    {
        return pendingShares.size() > 0;
    }

    public boolean isShrunken()
    {
        return pendingRemoves.size() > 0;
    }

    public boolean isStable()
    {
        return (
                ( assignedShares.size() > 0 ) &&
                ( pendingShares.size() == 0 ) &&
                ( pendingRemoves.size() == 0 )
                );
    }

    public boolean isDormant()
    {
        return (
                ( assignedShares.size() == 0 ) &&
                ( pendingShares.size() == 0 ) &&
                ( pendingRemoves.size() == 0 )
                );
    }

    public void removeAllShares()
    {
    	String methodName = "removeAllShares";
        if ( logger.isDebug() ) {
            for ( Map<Share, Share> m : sharesByMachine.values() ) {
                for ( Share s : m.values() ) {
                    logger.debug(methodName, getId(), "Clear share", s);
                }
            }
        }
    	assignedShares.clear();
    	pendingShares.clear();
    	pendingRemoves.clear();
        machineList.clear();
        sharesByMachine.clear();
    }

    /**
     * I've shrunk or this share has nothing left to do.  Remove this specific share.
     */
    public void removeShare(Share share)
    {
        String methodName = "removeShare";

        if ( assignedShares.containsKey(share) ) {
            int prev = assignedShares.size();
            assignedShares.remove(share);
            pendingRemoves.remove(share);

            Machine m = share.getMachine();
            Map<Share, Share> machineShares = sharesByMachine.get(m);
            machineShares.remove(share);
            if ( machineShares.size() == 0 ) {
                sharesByMachine.remove(m);
                machineList.remove(m);
            }

            logger.debug(methodName, getId(), "Job removes ", share.toString(), " reduces from ", prev, " to ", assignedShares.size() + ".");
        } else {
            logger.warn(methodName, getId(), "****** Job cannot find share " + share.toString() + " to remove. ******");
        }
    }

    /**
     * Used by Fixed Share and by Reservations
     */
    public void shrinkByOne(Share share)
    {
        String methodName = "shrinkByOne";
        if ( assignedShares.containsKey(share) ) {
            logger.debug(methodName, getId(), "Job schedules " + share.toString() + " for removal.");
            pendingRemoves.put(share, share);
            share.evict();
        } else {
            logger.warn(methodName, getId(), "****** Job cannot find share " + share.toString() + " to schedule for removal.******");
        }

    }

    /**
     * Shrink by 'shares' from machines of largest order starting from 'order' and decreasing.
     * Investment is not used, this is to shrink-from-largest-machine to MINIMIZE FRAGMENTATIOPN.
     *
     * This implementation is simplest, we just vacate.  This is called in the sequence of highest
     * order wanted so we're leaving the largest holes in the largest machines first.
     *
     * @param shares - number of N-shares that are wanted
     * @param order  - N - try to free up space for shares of this size.
     * @param force - When evicting for non-preemptables, we may need to free the requested
     *                shares even if it puts us over our "fair" count.  If this happens
     *                we'll end up "sliding" onto other machines (eventually).
     * @param nodepool - only interested in shares from this nodepool.
     *
     * So when this is called, somebody needs (shares*order) shares, given out in chunks of
     * (order).
     * 
     * @returns number of Q-shares actually given up
     */
    public int shrinkByOrderByMachine(int shares, int order, boolean force, NodePool nodepool)
    {
    	String methodName = "shrinkByOrderByMachine";

        if ( shares <= 0 ) {
            throw new SchedulingException(getId(), "Trying to shrink by " + shares + " shares.");
        }

        // These are the machines where I have stuff running.  
        ArrayList<Machine> machinesSorted = new ArrayList<Machine>();
        for ( Machine m : machineList.values() ) {
            if ( (m.getNodepool() == nodepool) && ( m.getShareOrder() >= order) ) {
                machinesSorted.add(m);
            }
        }
        Collections.sort(machinesSorted, new MachineByOrderSorter());

        int given = 0;        
        int shares_to_lose = 0;

        //
        // How much to lose?  If we're not forcing, then only shares that are evicted because of
        // the 'howMuch' counts.   If forcing then everything until we meet the goal or we run
        // out of stuff to give.
        //
        if ( force ) {
            shares_to_lose = countNShares();
        } else {
            shares_to_lose = Math.max(0, countNShares() - countNSharesGiven());
        }
        if ( shares_to_lose == 0 ) {
            return 0;
        }

        for ( Machine m : machinesSorted ) {

            logger.debug(methodName, getId(), "Inspecting machine", m.getId());
            ArrayList<Share> slist = new ArrayList<Share>();

            for ( Share s : sharesByMachine.get(m).values() ) {            // get the still-eligible shares 
                if ( ! s.isEvicted() ) {
                    slist.add(s);
                }
            }
            if ( slist.size() == 0 ) {
                continue;
            }

            int to_give = m.countFreedUpShares();
            logger.debug(methodName, getId(), "A given:", given, "to_give:", to_give, "order", order, "shares", shares, "shares_to_lose", shares_to_lose);

            Iterator<Share> iter = slist.iterator();
            while ( iter.hasNext() &&  ( (given + (to_give/order)) < shares ) && (shares_to_lose > 0) ) {
                Share s = iter.next();
                logger.info(methodName, getId(), "Removing share", s.toString());
                pendingRemoves.put(s, s);
                s.evict();
                to_give += share_order;
                shares_to_lose--;
            } 


            given += (to_give / order);
            if ( given >= shares ) {
                break;
            }
        }        
        return given;
    }

    /**
     * Shrink by 'shares' from machines of largest order starting from 'order' and decreasing.
     * Investment is not used, this is a shrink-from-largest-machine to MINIMIZE LOST WORK at the
     * possibl eexpense of fragmentation.
     *
     * @param shares - number of N-shares that are wanted
     * @param order  - N - try to free up space for shares of this size.
     * @param force - When evicting for non-preemptables, we may need to free the requested
     *                shares even if it puts us over our "fair" count.  If this happens
     *                we'll end up "sliding" onto other machines (eventually).
     * @param nodepool - only interested in shares from this nodepool.
     *
     * So when this is called, somebody needs (shares*order) shares, given out in chunks of
     * (order).
     * 
     * @returns number of Q-shares actually given up
     */
    public int shrinkByInvestment(int shares, int order, boolean force, NodePool nodepool)
    {
    	String methodName = "shrinkByInvestment";

        if ( shares <= 0 ) {
            throw new SchedulingException(getId(), "Trying to shrink by " + shares + " shares.");
        }

        logger.debug(methodName, getId(), "Enter: shares", shares, "order", order, "force", force, "nodepool", nodepool.getId(),
                    "nAssignedShares", assignedShares.size(), "nPendingShares", pendingShares.size());

        ArrayList<Share> sharesSorted = new ArrayList<Share>();

        // must pick up only shares in the given nodepool
        for ( Share s : assignedShares.values() ) {
            if ( s.getNodepoolId().equals(nodepool.getId()) && ( !s.isEvicted() ) ) {
                sharesSorted.add(s);
            } else {
                if ( logger.isTrace () ) {
                    logger.trace(methodName, getId(), "Skipping", s.getId(), "s.nodepool", s.getNodepoolId(), "incoming.nodepool", nodepool.getId(), "evicted", s.isEvicted());
                }
            }
        }

        if ( sharesSorted.size() == 0 ) {
            return 0;
        }


        if ( logger.isTrace() ) {
            logger.trace(methodName, getId(), "Shares Before Sort - id, isInitialized, investment:");
            for ( Share s : sharesSorted ) {
                logger.trace(methodName, getId(), s.getId(), s.isInitialized(), s.getInvestment());
            }
        }

        Collections.sort(sharesSorted, new ShareByInvestmentSorter());

        if ( logger.isTrace() ) {
            logger.trace(methodName, getId(), "Shares After Sort - id, isInitialized, investment:");
            for ( Share s : sharesSorted ) {
                logger.trace(methodName, getId(), s.getId(), s.isInitialized(), s.getInvestment());
            }
        }


        //
        // How much to lose?  If we're not forcing, then only shares that are evicted because of
        // the 'howMuch' counts.   If forcing then everything until we meet the goal or we run
        // out of stuff to give.
        //
        int shares_given = 0;                  // number of shares of requested order given - NOT necessarily number of my own processes
        int processes_to_lose = 0;             // number of processes I'm able to lose
        int processes_given = 0;

        if ( force ) {
            processes_to_lose = countNShares();
        } else {
            processes_to_lose = Math.max(0, countNShares() - countNSharesGiven());
        }
        processes_to_lose = Math.min(processes_to_lose, sharesSorted.size());

        if ( processes_to_lose == 0 ) {
            return 0;
        }

        while ( (shares_given < shares) && (processes_given < processes_to_lose) ) {

            int currently_given = 0;

            if ( logger.isTrace() ) {
                logger.trace(methodName, getId(), "In loop: Shares given", shares_given, "shares wanted", shares, 
                             "processes_to_lose", processes_to_lose, "processes_given", processes_given);
            }

            Share s = sharesSorted.get(0);
            Machine m = s.getMachine();
            int to_give = m.countFreedUpShares();
            logger.debug(methodName, getId(), "Inspecting share", s.getId());
            ArrayList<Share> slist = new ArrayList<Share>();

            Iterator<Share> iter = sharesSorted.iterator();
            while ( (to_give < order) && iter.hasNext() ) {               // if we need more shares from this machine to be useful ...
                // Here we search the share list for enough more shares on the machine to make up enough shares
                // to satisy exactly one of the requested sizes.
                Share ss = iter.next();
                if ( ss.getMachine() == s.getMachine() ) {
                    slist.add(ss);
                    to_give += ss.getShareOrder();
                }
            }
            
            if ( to_give >= order ) {                 // did we find enough on the machine to make it useful to evict?
                //slist.add(s);                         // didn't put on the list earlier, in case we couldn't use it
                for ( Share ss : slist ) {
                    logger.info(methodName, getId(), "Removing share", ss.toString());
                    pendingRemoves.put(ss, ss);
                    ss.evict();

                    sharesSorted.remove(ss);
                    processes_given++;
                    currently_given++;
                    if ( processes_given >= processes_to_lose ) break; // if this is too few to be useful, defrag will fix it (mostly)
                }
                shares_given += (to_give / order);
            }
            
            //
            // If we gave nothing away we didn't change any of the structures and we'll
            // never exit.  So exit stage left asap right now.
            // We rarely if ever will enter this but it prevents an infinite loop in
            // varioius corner cases.
            //
            if ( currently_given == 0 ) {
                logger.debug(methodName, getId(), "Gave no shares, breaking loop");
                break;
            }
        }
        
    	return shares_given;
    }

    /**
     * I'm shrinking by this many.  Put into pendingRemoves but don't take them off the assigned list
     * until we know job manager has been told.
     *
     * TODO: this will likely change when the Scheduler starts deciding - either we'll put decision logic
     * into Job proper, or defer it to the IScheduler implementation (probably the latter).
     *
     * @Deprecated.  Keeping until I can nuke or update the simple fair share code.
     */
    public void shrinkTo(int k)
    {	
//         int count = assignedShares.size() - k;
//         for ( int i = 0; i < count; i++ ) {
//             pendingRemoves.add(assignedShares.get(i));
//         }
    }

    /**
     * Waiting for somebody to deal with my shrinkage?
     */
    public boolean isShrunk()
    {
        return pendingRemoves.size() > 0;
    }

    /**
     * Return the reclaimed shares.
     */
    public HashMap<Share, Share> getPendingRemoves()
    {
    	return pendingRemoves;
    }
    
    /**
     * And finally, dump the pending shrinkage.
     */
    public void clearPendingRemoves()
    {
        pendingRemoves.clear();
    }

    /**
     * Recovery complete, clear the share map
     */
    public void clearRecoveredShares()
    {
        recoveredShares.clear();
    }

//     public Machine removeLastMachine()
//     {
//         return assignedMachines.remove(assignedMachines.size() - 1);
//     }

    /**
     * Find number of nShares (virtual shares) this machine has assigned already.
     *
     * If things are slow in the cluster the pending removes might be
     * non-zero.  This is an extreme corner case it's best to be safe.
     */
    public int countNShares()
    {
        return assignedShares.size() + pendingShares.size() - pendingRemoves.size();
    }

    public void refuse(String refusal)
    {
        String methodName = "refusal";
        logger.warn(methodName, id, refusal);
        this.refusalReason = refusal;
    }

    public boolean isRefused()
    {
    	return (refusalReason != null);
    }
    
    public String getRefusalReason()
    {
    	return refusalReason;
    }
    
    public void setShareOrder(int s)
    {
        this.share_order = s;
    }

    public int getShareOrder()
    {
        return share_order;
    }

    /**
     * During the scheduling algorithm we want to track some things by userid.  The "share cap" stuff is used
     * to keep track of max shares that I can actually use or want during scheduling but is generally just
     * nonsense.
     */
    public void setShareCap(int cap)
    {
        this.share_cap = cap;
    }

    public int getShareCap()
    {
        return share_cap;
    }

    /**
     * We try to project the maximum number of shares that this job can use, based on the current rate
     * of completion of work items, and the known initialization time.  
     *
     * Many jobs have very long initialization times, and will complete in their current allocation before
     * new processes can get started and initialized.  We want to avoid growing (evictions) in that case.
     *
     * How to use this ... 
     */
    int getProjectedCap()
    {
    	String methodName = "getPrjCap";        // want this to line up with getJobCap in logs
        if ( init_wait ) {                      // no cap if not initialized, because we don't know.  other caps will dominate.
            return Integer.MAX_VALUE;
        }

        if ( time_per_item == Double.NaN ) {
            return Integer.MAX_VALUE;
        }

        // Get average init time
        int count = 0;
        long total_init = 0;
        for ( Share s : assignedShares.values() ) {
            long t = s.getInitializationTime();
            if ( t > 0 ) {
                count++;
                total_init += t;
            }
        }
        long avg_init = 0;
        if ( total_init > 0 ) {
            avg_init = total_init / count;  // (to seconds)
        } else {
            logger.warn(methodName, getId(), username, "Initialization time is 0, project cap and investment will be inaccurate.");
        }

        // When in the future we want to estimate the amount of remaining work.
        long target = avg_init + ducc_epoch + resource_class.getPredictionFudge();               

        int nprocesses = countNShares();
        double rate = ((double) (nprocesses * threads)) / time_per_item;                   // number of work items per second
                                                                                           // with currently assigned resources
        long projected = Math.round(target * rate);                                        // expected WI we can do after a new
                                                                                           // process gets started

        long future = Math.max(nquestions_remaining - projected, 0);                        // work still to do after doubling

        logger.info(methodName, getId(), username, "O", getShareOrder(),"T", target, "NTh", (nprocesses * threads), "TI", avg_init, "TR", time_per_item,
                    "R", rate, "QR", nquestions_remaining, "P", projected, "F", future, 
                     "return", (future / threads));

        int answer = (int) future / threads;
        if ( (future % threads ) > 0 ) answer++;

        return answer;                                                                     // number of processes we expect to need
                                                                                           // in the future
        
    }

    /**
     * This returns the largest number that can actually be used, which will be either the
     * share cap itself, or nProcess / nThreads, in N shares.
     */
    public int getJobCap()
    {    	
		String methodName = "getJobCap";

        if ( isRefused() ) {
            return 0;
        }

        int c = nquestions_remaining / threads;

        if ( ( nquestions_remaining % threads ) > 0 ) {
            c++;
        }

        int currentResources = assignedShares.size() - pendingRemoves.size();
        c = Math.max(c, currentResources);  // if job is ending we could be fragmented and have to be
                                            // careful not to underestimate, or we end up possibly
                                            // evicting something that should be left alone.

        // 
        // If waiting for initialization, we have to cap as well on the maximum number of shares
        // we give out, in case the job can't start, to avoid unnecessary preemption.
        //
        // Must convert to N-shares, because that is the number of actual processes, which is the
        // unit that the initialization cap is specified in.
        //
        
        int base_cap = Math.min(getMaxShares(), c);
        if ( base_cap < 0 ) base_cap = 0;          // capped by OR

        int projected_cap = getProjectedCap();       

        int potential_cap = base_cap;
        int actual_cap = 0;

        
        if ( resource_class.isUsePrediction() ) {
            if (projected_cap < base_cap ) {                     // If we project less need, revise the estimate down
                potential_cap = Math.max(projected_cap, currentResources);
            } 
        }
            
        if ( init_wait && ( resource_class.getInitializationCap() > 0) ) {
            actual_cap = Math.min(potential_cap, (resource_class.getInitializationCap()));
        } else {

            if ( init_wait ) {                                 // ugly, but true, if not using initialization caps
                actual_cap =  potential_cap;
            } else  if ( resource_class.isExpandByDoubling() ) {
                if ( currentResources == 0 ) {
                    actual_cap = Math.max(1, resource_class.getInitializationCap());   // if we shrink to 0, need to restart from the init cap
                    actual_cap = Math.min(base_cap, actual_cap);                       // must re-min this in case we have a base cap < class init cap
                } else {
                    actual_cap = Math.min(potential_cap, currentResources * 2);
                }
            } else {
                actual_cap = potential_cap;
            }
        }

        logger.debug(methodName, getId(), username, "O", getShareOrder(), "Base cap:", base_cap, "Expected future cap:", projected_cap, "potential cap", potential_cap, "actual cap", actual_cap);
        return actual_cap;
    }

    public int getMaxShares()
    {
        // if set to -1, our max is the number already assigned
        if ( max_shares < 0 ) {
            return countNShares();
        } else {
            return max_shares;
        }
    }

    public int getMinShares()
    {
        return min_shares;
    }

    public void setMaxShares(int s)
    {
        this.max_shares = s;
    }

    public void setMinShares(int s)
    {
        this.min_shares = s;
    }

    public boolean isRunning()
    {
        return countNShares() > 0 ? true : false;
    }

    public String getUserName()
    {
        return username;
    }

    public void setUserName(String n)
    {
        this.username = n;
    }

    public User getUser()
    {
        return user;
    }

    public void setUser(User u)
    {
        this.user = u;
    }

    public long getTimestamp()
    {
    	return submit_time;
    }
    
    public void setTimestamp(long t)
    {
        this.submit_time = t;
    }

    public int getUserPriority() {
        return user_priority;
    }

    public void setUserPriority(int p) {
        this.user_priority = p;
    }

    public String getClassName() {
        return resource_class_name;
    }

    public void setClassName(String class_name) {
        this.resource_class_name = class_name;
    }

    public int getSchedulingPriority() {
        return resource_class.getPriority();
    }

    public Policy getSchedulingPolicy() {
        return resource_class.getPolicy();
    }

    public ResourceClass getResourceClass() {
    	return resource_class;
    }
    
    public int countInstances() {
        return n_machines;
    }

    public void setNInstances(int m)
    {
        this.n_machines = m;
    }

    public int getMaxMachines() {
        return min_shares;
    }

    public int nThreads() {
        return threads;
    }
    public void setThreads(int th)
    {
    	this.threads = th;
    }

    public int getMemory() {
        return memory;
    }

    public void setMemory(int memory) {
        this.memory = memory;
    }

    public void setDuccType(DuccType type)
    {
        this.ducc_type = type;
    }

    public DuccType getDuccType()
    {
        return this.ducc_type;
    }

    /**
     * Is at least one of my current shares initialized?
     */
    public boolean isInitialized()
    {
        for (Share s : assignedShares.values()) {
            if ( s.isInitialized() ) return true;
        }
        return false;
    }

    /**
     * Logging and debugging - nice to know what my assigned shares are.
     */
    public String printShares()
    {
        StringBuffer buf = new StringBuffer("AssignedShares: ");
        if ( assignedShares.size() == 0 ) {
            buf.append("<none>");
        } else {
            for ( Share s : assignedShares.values()) {
                buf.append(s.getId());
                buf.append(" ");
            }
        }

        buf.append("\nPendingShares: ");
        if ( pendingShares.size() == 0 ) {
            buf.append("<none>");
        } else {
            for ( Share s : pendingShares.values()) {
                buf.append(s.getId());
                buf.append(" ");
            }
        }

        buf.append("\nPendingRemoves: ");
        if ( pendingRemoves.size() == 0 ) {
            buf.append("<none>");
        } else {
            for ( Share s : pendingRemoves.values()) {
                buf.append(s.getId());
                buf.append(" ");
            }
        }

        return buf.toString();
    }

    String getShortType()
    {
        String st = "?";
        switch ( ducc_type ) {
        case Reservation:
            st = "R";
            break;
        case Job:
            st = "J";
            break;
        case Service:
            st = "S";
            break;
        }
        return st;
    }

    public static String getHeader()
    {
        return String.format("%6s %30s %10s %10s %6s %5s %13s %8s %6s %9s %11s %8s", 
                             "ID", "JobName", "User", "Class", 
                             "Shares", "Order", "QuantumShares", 
                             "NThreads", "Memory",
                             "Questions", "Q Remaining", "InitWait");
    }

    public String toString()
    {
        int shares = assignedShares.size() + pendingShares.size();        

        if ( isReservation() ) {
            return String.format("%1s%5s %30.30s %10s %10s %6d %5d %13d %8s %6d",
                                 getShortType(),
                                 id.toString(), name, username, getClassName(), 
                                 shares, share_order, (shares * share_order),
                                 "", memory);                                 
        } else {
            return String.format("%1s%5s %30.30s %10s %10s %6d %5d %13d %8d %6d %9d %11d %8s", 
                                 getShortType(),
                                 id.toString(), name, username, getClassName(), 
                                 shares, share_order, (shares * share_order),
                                 threads, memory,
                                 nQuestions(), nQuestionsRemaining(),
                                 init_wait);
        }
    }

    public String toStringWithHeader()
    {
        StringBuilder buf = new StringBuilder(getHeader());
        buf.append("\n");
        buf.append(toString());
        return buf.toString();
    }

    //
    // Order machines by DECREASING order
    //
    class MachineByOrderSorter
    	implements Comparator<Machine>
    {	
    	public int compare(Machine m1, Machine m2)
        {
            if (m1.equals(m2)) return 0;
            return (int) (m2.getShareOrder() - m1.getShareOrder());
        }
    }

    //
    // Order shares by INCREASING investment
    //
    class ShareByInvestmentSorter
    	implements Comparator<Share>
    {	
    	public int compare(Share s1, Share s2)
        {
            if ( s1.equals(s2) ) return 0;
            // First divide them into two pools: 
            // not-initialized shares always sort LESS than initialized shares
            if ( ! s1.isInitialized() ) {
                if ( s2.isInitialized() ) return -1;
                // both not initialized. sort on less time spent initializing so far (fall through)
            } else {
                if ( ! s2.isInitialized() ) return 1;                
                // bot initialized.  Again sort on less time spent ever in init. (fall through)
            }

            return ( (int) (s1.getInvestment() - s2.getInvestment()) );
        }
    }

    @Override
    public int hashCode()
    {
        return id.hashCode();
    }

    @Override
    public boolean equals(Object obj) 
    {

        if (this == obj)                  return true;

        if (obj == null)                  return false;


        if (getClass() != obj.getClass()) return false;

        IRmJob other = (IRmJob) obj;

    
        // can't get null id
        if ( !id.equals(other.getId()) )  return false;

        //can't get null shares.. normal compare should finish it off.
                                          return assignedShares.equals(other.getAssignedShares());
    }

    public Comparator<IEntity> getApportionmentSorter()
    {
        return apportionmentSorter;
    }

    static private class ApportionmentSorterCl
        implements Comparator<IEntity>
    {
        public int compare(IEntity e1, IEntity e2)
        {
            if ( e1.equals(e2) ) return 0;
            return (int) (e1.getTimestamp() - e2.getTimestamp());
        }
    }

}
