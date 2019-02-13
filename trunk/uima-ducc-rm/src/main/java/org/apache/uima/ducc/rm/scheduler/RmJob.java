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
import java.util.List;
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
    protected String state = "New";                   // UIMA-4577 info only, for the db
    protected boolean  arbitrary_process = false;     // Is this an AP?
    protected String name;                            // user's name for job
    protected String resource_class_name;             // Name of the res class, from incoming job parms
    protected ResourceClass resource_class;           // The actual class, assigned as job is received in scheduler.
    protected int    user_priority;                   // user "priority", really apportionment 

    // @deprecated
    // protected int n_machines;                         // RESERVE:     minimum machines to allocate

    protected int max_shares;                         // FAIR_SHARE:  maximum N shares to allocate
    protected boolean is_reservation = false;

    protected int threads;                            // threads per process

    protected int memory;                             // estimated memory usage
    protected int nquestions;                         // number of work-items in total
    protected int nquestions_remaining;               // number of uncompleted work items
    protected double time_per_item = Double.NaN;      // from OR - mean time per work item

    protected int share_order = 0;                    // How many shares per process this job requires (calculated on submission)
    protected boolean share_order_upgraded = false;   // Set true when a Reserve request has been upgraded
    
    protected int share_cap = Integer.MAX_VALUE;      // initially; scheduler policy will reset as the job ages
    protected int job_cap = 0;                        // current, cached cap on the job, reset at the start of every cycle
    protected int pure_fair_share = 0;                // pure uncapped un-bonused share for this job

    protected long submit_time;                       // when job is submitted ( sched or job-manager sets this )

    protected String username;
    protected User user;                              // user id, enforced by submit and job manager. we just believe it in sched.

    protected long serviceId = 0;                     // services only, the SM-assigned service id. UIMA-4712 ref UIMA-4209
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

    protected int total_assigned = 0;                    // non-preemptable only, total shares ever assigned

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

    protected String reason = null;                      // if refused, deferred, etc, this is why, for the message
    boolean   refused = false;
    boolean   deferred = false;

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

    public void setServiceId(long id)
    {
        this.serviceId = id;
    }

    public long getServiceId()
    {
        return serviceId;
    }

    public void setJobName(String name)
    {
        this.name = name;
    }

    public void setState(String state) { this.state = state; }
    public String getState()           { return this.state; }

    public void setReservation()
    {
        this.is_reservation = true;
    }

    public boolean isReservation()
    {
        return is_reservation;
    }

    /**
     * For preemptable, must remember the job completed so we don't accidentally reexpand it.  Can
     * happen in defrag and maybe various races with OR state.
     */
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
     *    We return resched so caller knows to tickle the scheduler
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

    public int queryDemand()
    {
        if ( getSchedulingPolicy() == Policy.FAIR_SHARE ) return getJobCap();
        return max_shares;
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
        wanted_by_order              = unused.makeArray();
        wanted_by_order[share_order] = getJobCap();
        wanted_by_order[0]           = wanted_by_order[share_order];
    }

    public int[] getWantedByOrder()
    {
        return wanted_by_order;
    }

    // UIMA-4275 simplifies this
    public int calculateCap()
    {
        return Integer.MAX_VALUE;  // no cap for jobs
    }


    // UIMA-4275
    public int countOccupancy()
    {

        return (assignedShares.size() + pendingShares.size()) * share_order;

        // if ( (given_by_order == null) || (given_by_order[share_order] == 0) ) {
        //     // must use current allocation because we haven't been counted yet
        //     return countNShares() * share_order;
        // } else {
        //     // new allocation may not match current, so we use that one
        //     return given_by_order[share_order] * share_order;
        //}
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

    /**
     * Can I use more 1 more share of this size?
     * UIMA-4065
     *
     * @param order The size of the available share.  Must be an exact match because the
     *              offerer has already done all reasonable splitting and will have a better
     *              use for it if I can't take it.
     *
     *              The decision is based on the wbo/gbo arrays that the offer has been building up
     *              just before asking this question.
     *
     * @return      True if I can use the share, false otherwise.
     */
    public boolean canUseBonus(int order)              // UIMA-4065
    {
        if ( order != share_order) return false;

        if ( getGivenByOrder()[0] >= getResourceClass().calculateCap() ) return false;  // don't exceed cap UIMA-4275

        return (getWantedByOrder()[order] > 0);        // yep, still want
   }

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
        total_assigned++;
    }

    /**
     * Non-preemptable, need to know total every assigned, in case one of them dies, must be careful
     * not to reassign it.
     */
    public int countTotalAssignments()
    {
        return total_assigned;
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
        if ( logger.isTrace() ) {
            for ( Map<Share, Share> m : sharesByMachine.values() ) {
                for ( Share s : m.values() ) {
                    logger.trace(methodName, getId(), "Clear share", s);
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
     * Remove a process from this job
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
     * This is an investment shrink.  We don't have to walk nodepools because we use nodepool depth of each share
     * to select deeper pools first.
     *
     * The selection strategy is this:
     *    1. Lowest investment always wins.
     *    2. Deepest nodepool is secondary sort.
     *    3. Largest machine is tertiary.
     *
     * The assumption is that the job must give up the indicated shares unconditionally.  We let the
     * defragmentation routine to any additional cleanup if this isn't sufficient to satisfy pending expansions.
     * UIMA-4275
     */
    public void shrinkBy(int howmany)
    {
        String methodName = "shrinkBy";
        List<Share> sharesSorted = new ArrayList<Share>(assignedShares.values());
        Collections.sort(sharesSorted, new ShareByInvestmentSorter());

        int tolose = Math.min(howmany, sharesSorted.size());

        for ( int i = 0; i < tolose; i++ ) {
            Share ss = sharesSorted.get(i);
            logger.info(methodName, getId(), "Removing share", ss.toString());
            pendingRemoves.put(ss, ss);
            ss.evict();
        }
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
        String methodName = "refuse";
        logger.warn(methodName, id, refusal);
        this.reason = refusal;
        deferred = true;
    }

    public void defer(String reason)
    {
        String methodName = "defer";
        logger.info(methodName, id, reason);
        this.reason = reason;
        deferred = true;
    }

    public String getReason()
    {
        return this.reason;
    }

    public void setReason(String reason)
    {
        this.reason = reason;
    }

    public void undefer()
    {
        deferred = false;
        reason = null;
    }

    public boolean isRefused()
    {
    	return refused;
    }
    
    public boolean isDeferred()
    {
    	return deferred;
    }
    
    public String getRefusalReason()
    {
    	return reason;
    }
    
    public void setShareOrder(int s)
    {
        this.share_order = s;
    }

    public int getShareOrder()
    {
        return share_order;
    }

    public void upgradeShareOrder(int s) {
    	share_order = s;
    	share_order_upgraded = true;
    }
    
    public boolean shareOrderUpgraded() {
    	return share_order_upgraded;
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
    private int getProjectedCap()
    {
    	String methodName = "getPrjCap";                      // want this to line up with getJobCap in logs

        // UIMA-4882 jrc
        // Must enhance semantics of init_wait to mean "nothing initialized, and have never seen any
        // execution time for the job."  This accounts for the moment after a job initializes, and before it
        // gets anything running and helps to throttle expansion until a job starts to run.
        //
        // After initialization, the time_per_item will be quite small but non-zero, so we'll tend to predict
        // a future cap as the moral equicalent of "not too many more needed".  For installations without
        // doubling, or where doubling is too fast, this leads to better controlled expansion if the job
        // actually is going to compete soon.  
        //
        // The other part of this update includes the OR updating its "time_per_item" to account for
        // work items in progress as well as work items completed, so we're guarantteed to get a
        // time_per_item != 0 shortly after first initialization.
        //
        // (We update init_wait here because it's used later and needs to be used with the same 
        //  semantics as is used here.)
        init_wait = init_wait || Double.isNaN(time_per_item) || (time_per_item == 0.0);

        if ( init_wait ) {   // no cap if not initialized, or no per-itme time yet
            logger.info(methodName, getId(), username, "Cannot predict cap: init_wait", init_wait, "|| time_per_item", time_per_item);
            return Integer.MAX_VALUE;
        }

        // Get average init time
        int count = 0;
        long initialization_time = 0;           // from OR - 
        for ( Share s : assignedShares.values() ) {
            long t = s.getInitializationTime();
            if ( s.isInitialized() && ( t > 0) ) {
                count++;
                initialization_time += t;
            }
        }
        if ( initialization_time > 0 ) {
            initialization_time = initialization_time / count;  // (to seconds)
        } else {
            logger.warn(methodName, getId(), username, "Initialization time is 0, project cap and investment will be inaccurate.");
        }

        // When in the future we want to estimate the amount of remaining work.
        long target = initialization_time + ducc_epoch + resource_class.getPredictionFudge();               

        int nprocesses = countNShares();
        double rate = ((double) (nprocesses * threads)) / time_per_item;                   // number of work items per second
                                                                                           // with currently assigned resources
        long projected = Math.round(target * rate);                                        // expected WI we can do after a new
                                                                                           // process gets started

        long future = Math.max(nquestions_remaining - projected, 0);                        // work still to do after doubling

        int answer = 0;

        answer = (int) future / threads;
        if ( (future % threads ) > 0 ) answer++;

        // jrc
        // Second problem 
        // if future cap is 0, then the future cap is the current number of processes
        if ( answer == 0 ) {
            answer = countNShares();
        }
        logger.info(methodName, getId(), username, "O", getShareOrder(),"T", target, "NTh", (nprocesses * threads), "TI", initialization_time, 
                    "TR", time_per_item,
                    "R", String.format("%.4e", rate),
                    "QR", nquestions_remaining, "P", projected, "F", future, 
                    "ST", submit_time,
                    "return", answer);

        return answer;                                                                     // number of processes we expect to need
                                                                                           // in the future
        
    }

    /**
     * This returns the largest number that can actually be used, which will be either the
     * share cap itself, or nProcess / nThreads, in N shares.
     */
    public void initJobCap()
    {    	
		String methodName = "initJobCap";

        if ( isRefused() ) {
            job_cap = 0;
            return;
        }

        if ( isCompleted() ) {
            // job is finishing up and will relinquish all shares soon, let's avoid complicating the
            // world and just wait for it to happen.
            job_cap = countNShares();
            return;
        }

        // if ( getSchedulingPolicy() != Policy.FAIR_SHARE ) return;

        int c = nquestions_remaining / threads;

        if ( ( nquestions_remaining % threads ) > 0 ) {
            c++;
        }

        int currentResources = countNShares();
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
        if ( base_cap < 0 ) base_cap = 0;             // getMaxShares comes from OR - protect in case
                                                      // it's messed up

        int projected_cap = getProjectedCap();      
        if ( projected_cap == 0 ) {                   // we know nothing, this is best guess
        	projected_cap = base_cap;
        }

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

            if ( init_wait ) {                                                         // ugly, but true, if not using initialization caps
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

        logger.info(methodName, getId(), username, "O", getShareOrder(), "Base cap:", base_cap, "Expected future cap:", projected_cap, "potential cap", potential_cap, "actual cap", actual_cap);
        job_cap =  actual_cap;
    }

    public int getJobCap()
    {
        return job_cap;
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

    public void setMaxShares(int s)
    {
        this.max_shares = s;
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
    
    // UIMA-4275
    public boolean exceedsFairShareCap()
    {
        return getResourceClass().fairShareCapExceeded(this);
    }

    // @deprecated
    //public int countInstances() {
    //    return n_machines;
    //}

    // @deprecated
    // public void setNInstances(int m)
    // {
    //    this.n_machines = m;
    // }

    public int nThreads() {
        return threads;
    }
    public void setThreads(int th)
    {
    	this.threads = th;
    }

    public int getShareQuantum()
    {
        return resource_class.getShareQuantum();
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

    // UIMA-4142
    public void setArbitraryProcess()
    {
        this.arbitrary_process = true;
    }

    // UIMA-4142
    public boolean isArbitraryProcess()
    {
        return (ducc_type == DuccType.Service) && this.arbitrary_process;
    }

    // UIMA-4142
    public boolean isService()
    {
        return (ducc_type == DuccType.Service) && !this.arbitrary_process;
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

    public String getShortType()
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
            st = ( isArbitraryProcess() ? "M" : "S" );          // UIMA-4142
            break;
        // These last 2 may not be necessary
		case Pop:
            st = "A";
			break;
		default:
			break;
        }
        return st;
    }

    public static String getHeader()
    {
        //                      1    2    3    4   5   6   7   8   9  10  11  12  13
        return String.format("%11s %30s %10s %10s %6s %5s %7s %3s %6s %6s %8s %8s %9s", 
                             "ID", "JobName", "User", "Class",         // 1 2 3 4
                             "Shares", "Order", "QShares",             // 5 6 7
                             "NTh", "Memory",                          // 8 9
                             "nQuest", "Ques Rem", "InitWait",         // 10 11 12
                             "Max P/Nst");                             // 13
    }

    public String toString()
    {
        int shares = assignedShares.size() + pendingShares.size();        
        //if ( getSchedulingPolicy() != Policy.FAIR_SHARE ) {
        //    shares = countInstances();
        //}

        //                    1       2    3    4   5   6   7   8   9  10  11  12 13
        String format = "%11s %30.30s %10s %10s %6d %5d %7d %3d %6d %6d %8d %8s %9d";
        String jid = String.format("%1s%10s", getShortType(), id.toString()).replace(' ', '_');
        if ( isReservation() ) {//     1       2    3    4   5   6    7   8   9  10   11  12 13
            return String.format(format,
                                 jid,                                               // 1
                                 name.replace(' ', '_'), username, getClassName(), // 2 3 4
                                 shares, share_order, (shares * share_order),      // 5 6 7
                                 0, memory,                                        // 8 9
                                 0, 0, 0,                                          // 10 11 12
                                 max_shares);                                      // 13
            
                                 
        } else {   
            return String.format(format,
                                 jid,                                               // 1
                                 name.replace(' ', '_'), username, getClassName(),  // 2 3 4
                                 shares, share_order, (shares * share_order),      // 5 6 7
                                 threads, memory,                                  // 8 9
                                 nQuestions(), nQuestionsRemaining(), init_wait,   // 10 11 12
                                 max_shares);                                      // 13
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

    // pull this out of the ShareByInvest sorter so other sorters can use it - see the
    // ShareByWealth sorter in NodePoolScheduler
    // UIMA-4275
    public static int compareInvestment(Share s1, Share s2)
    {
        if ( s1.equals(s2) ) return 0;
        
        int mask = 0;
        if ( s1.isInitialized() ) mask |= 0x02;
        if ( s2.isInitialized() ) mask |= 0x01;
        
        switch ( mask ) {
            case 0:        // neither is initialized
                return ( (int) (s1.getInitializationTime() - s2.getInitializationTime()) );
            case 1:        // s1 not initialized, s2 is
                return -1;
            case 2:        // s2 initialized, s1 not
                return  1; 
            default:       // both initialized, compare investments
        }
        
        long i1 = s1.getInvestment();
        long i2 = s2.getInvestment();
        if (i1 == i2 ) {
            // same invesstment, go to depper nodepool first
            int d1 = s1.getNodepoolDepth();
            int d2 = s1.getNodepoolDepth();
            
            if ( d1 == d2 ) {
                // same nodepool depth, go for largest machine
                long m1 = s1.getHostMemory();
                long m2 = s2.getHostMemory();
                
                if ( m1 == m2 ) {
                    // last resort, sort on youngest first 
                    return (int) (s2.getId().getFriendly() - s1.getId().getFriendly());
                }
                return (int) (m2 - m1);   // largest machine
            }
            
            return (d2 - d1);     // greatest depth
            
        } else {
            return (int) (i1 - i2);  // least investment
        }
    }

    //
    // Order shares by INCREASING investment
    //
    public static class ShareByInvestmentSorter
    	implements Comparator<Share>
    {	
    	public int compare(Share s1, Share s2)
        {
            return compareInvestment(s1, s2);        // UIMA-4275
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
            // Order by smallest first.  The counter will round up for really
            // small jobs so they don't get buried in the round-off errors.
            //
            // Note that getJobCap() is (must be) pre-computed before this sorter is called.
            if ( e1.equals(e2) ) return 0;
            return (int) (e1.getTimestamp() - e2.getTimestamp());

            // return (int) (((RmJob)e1).getJobCap() - ((RmJob)e2).getJobCap());
        }
    }

}
