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

import org.apache.uima.ducc.common.persistence.rm.IDbJob;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.common.IDuccTypes.DuccType;


/**
 * This class represents a job inside the scheduler.
 */

public interface IRmJob
	extends SchedConstants,
            IEntity,
            IDbJob
{
    
    /**
     * read the props file and set inital values for internal fields
     */
    public void init();

    public DuccId getId();
    
    // public String getShortType();  IDbJob UIMA-4577 // S, R, M, J - service reservation managed-reservation, job

    // public long getFriendlyId();    UIMA 4577 

    public String getName();
    public void setJobName(String name);

    /**
     * Asks it's resource class what the share quantum is that it is being scheduled under.
     */
    public int getShareQuantum();
    
    /**
     * Save ref to the class we are in, and init class-based structures.
     */
    public void setResourceClass(ResourceClass cl);

    /**
     * Number of questions submitted.
     */
    public int nQuestions();
    public void setNQuestions(int allq, int remainingq, double time_per_item);

    /**
     * Number of questions still to be answered.  Used by scheduler to determing current
     * machine requirement.
     */
    public int nQuestionsRemaining();

    public void setReservation();          // set, if this is a reservation
    public boolean isReservation();       // ask ...

    public boolean setInitWait(boolean w);   // When set, job cap is set low, waiting for confirmation that init is ok.
                                             // Returns the prev state

    public void setState(String state);     // UIMA-4577 Information only, for the db. getState() is in IDbJob;

    /**
     * Used during scheduling cycle only, keep track of number of shares given out to this job.
     */
    public void clearShares();
    // public void addQShares(int s);
    public int countNSharesGiven();
    public int countQSharesGiven();
    public int countNSharesLost();

    /**
     * For queries - how many processes do I want in a perfect world?
     */
    // public int queryDemand(); to IDbJob UIMA-4577

    /**
     * Eviction policies, configurable.
     */
    public int shrinkByOrderByMachine(int shares, int order, boolean force, NodePool np); // shrink by largest machine first, minimize fragmentation
    public int shrinkByInvestment(int shares, int order, boolean force, NodePool np);     // shrink by lowest investment, minimize lost work

    /**
     * Get the actual shares that are assigned to this job.
     */
    public HashMap<Share, Share> getAssignedShares();

    /**
     * Newly allocated shares that have not been dispatched.  They're unavailable for scheduling but
     * job manager doesn't know about them yet.  When we tell job manager we'll "promote" them to
     * the assignedShares list.
     */
    public HashMap<Share, Share> getPendingShares();

    /**
     * Get the shares that are scheduled for (job) recovery.
     */
    public HashMap<Share, Share> getRecoveredShares();

    /**
     * We're dispatching, move machines to active list, and clear pending list.
     * Tell caller which machines are affected so it can dispatch them.
     */
    public HashMap<Share, Share> promoteShares();

    /**
     * For defrag, we want to know what machines a job is running on.
     */
    Map<Machine, Map<Share, Share>> getSharesByMachine();
    Map<Machine, Machine> getMachines();

    /**
     * Scheduler found us a new toy, make it pending until it's given to job manager.
     */
    public void assignShare(Share s);

    /**
     * Track total number of shares ever given to this job.
     */
    public int countTotalAssignments();

    /**
     * Set a share for recovery.
     */
    public void recoverShare(Share s);

    /**
     * Cancel a pending assignment, part of defrag
     */
    public void cancelPending(Share s);

    /**
     * What is result of latest scheduling epoch?
     */
    public boolean isExpanded();
    public boolean isStable();
    public boolean isShrunken();
    public boolean isDormant();
    public boolean isPendingShare(Share s);

    /**
     * Clear the job, it has no resources of any form.
     */
    public void removeAllShares();
    
    /**
     * The job is compolete but may linger a bit because of the defrag code.
     * Mark it complete so it doesn't accidentally get used.
     */
    public void markComplete();
    public boolean isCompleted();

    /**
     * I've shrunk or this share has nothing left to do.  Remove this specific share.
     */
    public void removeShare(Share share);

    /**
     * Pick a share, any share, and remove it from active.  Set to pending so jm can kill it.
     */
    public void shrinkByOne(Share share);

    /**
     * Return the reclaimed shares.
     */
    public HashMap<Share, Share> getPendingRemoves();
    
    /**
     * And finally, dump the pending shrinkage.
     */
    public void clearPendingRemoves();

    /**
     * Shares recovered, clear the structures.
     */
    public void clearRecoveredShares();

    /**
     * Find number of nShares this machine has assigned already.
     *
     * If things are slow in the cluster the pending removes might be
     * non-zero.  This is an extreme corner case it's best to be safe.
     */
    public int countNShares();

    /**
     * Can't schedudle this nohow.  Here's why.
     */
    void refuse(String reason);
    String getRefusalReason();
    boolean isRefused();

    /**
     * Can't schedule non-preemptable yet, but maybe later
     */
    void defer(String reason);
    void setReason(String reason);     // maybe want to tell OR/WS/CLI something but nothing really wrong
    void undefer();
    boolean isDeferred();

    public String getReason();    // reason for some scheduling anomoly, if there is one

    /**
     * Scheduler looks at job memory and decides what its share order is.
     */
    public void setShareOrder(int s);
    public int getShareOrder();         // IDbJob UIMA-4577

    /**
     * This returns the largest number that can actually be used, which will be either the
     * share cap itself, or nProcess / nThreads, in quantum shares.
     */
    public int getJobCap();
    public void initJobCap();   // calculate the cap at start of cycle and cache it
                                // because it is frequently used

    // public String getUserName();      // UIMA 4577 IDbJob
    public void   setUserName(String n);
    
    public User getUser();
    public void setUser(User u);

    public long getTimestamp();
    public void setTimestamp(long t);
    
    public int  getUserPriority();
    public void setUserPriority(int p);

    // public String getClassName();         UIMA 4577 IDbJob
    public void   setClassName(String n);

    public int getSchedulingPriority();

    public Policy getSchedulingPolicy();

    public ResourceClass getResourceClass();
    
    //public int countInstances();
    //public void setNInstances(int m);

    public int  nThreads();
    public void setThreads(int threads);

    // public int  getMemory();     UIMA 4577 IDbJob
    public void setMemory(int memory);

    /**
     * Logging and debugging - nice to know what my assigned shares are.
     */
    public String printShares();

    public int getMaxShares();

    public void setMaxShares(int s);

    public void setPureFairShare(int s);
    public int  getPureFairShare();

    public void setDuccType(DuccType dt);
    public DuccType getDuccType();

    public void setArbitraryProcess();            // UIMA-4142
    public boolean isArbitraryProcess();          // UIMA-4142
    public boolean isService();                   // UIMA-4142

    public boolean isInitialized();
    
    // Total number of shares to account to me - either actually assigned, or
    // counted afresh in the current scheduling cycle, for allotments
    // public int countOccupancy();                  // UIMA-4275 moved to IDbJob by UIMA-4577

    // UIMA-4275 Must lose some number of shares unconditionally
    public void shrinkBy(int howmany);

    // UIMA-4275
    public boolean exceedsFairShareCap();
}
