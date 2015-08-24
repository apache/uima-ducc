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
import org.apache.uima.ducc.common.admin.event.RmQueriedMachine;
import org.apache.uima.ducc.common.admin.event.RmQueriedShare;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.id.DuccId;



public class Machine
	implements SchedConstants
{
    private static DuccLogger logger = DuccLogger.getLogger(Machine.class, COMPONENT_NAME);

    private String id;
    private int hbcounter = 0;           // heartbeat counter

    private long memory;            // in Kb
    private int share_order = 1;

    private NodePool nodepool;

    //
    // These are subtly different.
    //    - virtual_share_order is reset to share_order at the start of every scheduling cycle.  It
    //      represents the *potential* shares in this machine.  As a rule, once we give out shares on
    //      this machine we'll try to not move them around but eviction happens, and this helps us
    //      keep track of what we *could* give away on this machine.  It represents the logical capacity
    //      of the machine, that is, true capacity, less shares given to orchestrator, less shares that
    //      we might be giving away this scheduling cycle.
    //
    //    - shares_left tracks exactly the number of shares that are physically available to give away
    //      without preemption. This is updated when a share is assigned, or when it is returned. It
    //      represents the true capacity of the machine at this moment, less the shares that have been
    //      given to the orchestrator.
    //
    // Throughout much of the scheudling cycle these guys will tend to track each other, and at the end
    // of a cycle they should probably bethe same, but they may diverge if shares are given out that we
    // might want to preempt.
    //
    private int virtual_share_order = 0;
    private int shares_left = 0;

    // UIMA-4142
    // count of shares unavailable because of blacklisting
    private int blacklisted_shares = 0;
    private Map<DuccId, Share> blacklistedWork = new HashMap<DuccId, Share>(); // id of process or reservation, share order

    Node node;   

    private HashMap<Share, Share> activeShares = new HashMap<Share, Share>();

    public Machine(Node node)
    {
        this.node = node;
        this.memory =  node.getNodeMetrics().getNodeMemory().getMemTotal();
        this.id = node.getNodeIdentity().getName();
    }

//    public Machine(String id, long memory)
//    {
//        this.id = id;
//        this.memory = memory;
//    }

    /**
     * Return the hashmap key for this thing.
     */
    public Node key()
    {
        return node;
    }

    /**
     * Return the quantum for this machine - the actual amount allocated in a single share on this machine.
     */
    public int getQuantum()
    {
        return (int) memory / share_order;
    }

    // UIMA-4142
    // Black list some number of shres for a specific job and proc.  This reduces the number of
    // schedulable shares until they are whitelisted.
    //public synchronized void blacklist(DuccId jobid, DuccId procid, int nshares)
    public synchronized void blacklist(DuccId jobid, DuccId procid, long jobmem)
    {
        String methodName = "blacklist";
      
        int q = getQuantum();
        int nshares = (int) jobmem / q;
        if ( jobmem % q > 0 ) nshares++;

        if ( ! blacklistedWork.containsKey(procid) ) {                 // already condemned?
            if ( nshares == -1 ) nshares = share_order;                // whole machine - reservations

            Share s = new Share(procid, this, jobid, nshares);
            blacklistedWork.put(procid, s);
            shares_left -= nshares; 
            blacklisted_shares += nshares;

            if ( shares_left < 0 ) {
                try {
                    throw new IllegalStateException("shares_left went negative");
                } catch ( Exception e ) {
                    logger.error(methodName, null , e, "shares went negative on", this.id, "share", procid.toString(), "nshares", nshares, "shares_left", shares_left, "share_order", share_order);
                }
            }
        }
        logger.info(methodName, null, this.id, procid.toString(), procid.getUUID(),  "procs", blacklistedWork.size(), "shares left", shares_left, "/", share_order);
    }


    // UIMA-4142
    // Whitelist shares if they were previously blacklisted, making them available for scheduling.
    public synchronized void whitelist(DuccId procid)
    {
    	String methodName = "whitelist";
        if ( blacklistedWork.containsKey(procid) ) {
            Share s = blacklistedWork.remove(procid);
            int nshares = s.getShareOrder();
            shares_left += nshares;
            blacklisted_shares -= nshares;

            if ( shares_left > share_order ) {
                try {
                    throw new IllegalStateException("shares_left exceeds share_order");
                } catch ( Exception e ) {
                    logger.error(methodName, null , e, "shares left exceeds share_order on", this.id, "share", procid.toString(), "nshares", nshares, "shares_left", shares_left, "share_order", share_order);
                }
            }
        }
        logger.info(methodName, null, this.id, procid.toString(), procid.getUUID(), "procs", blacklistedWork.size(), "shares left", shares_left, "/", share_order);
    }

    // UIMA-4142
    public synchronized boolean isBlacklisted()
    {
        return blacklistedWork.size() > 0;
    }

    public synchronized void heartbeat_down()
    {
        hbcounter = 0;
    }

    public synchronized void heartbeat_up()
    {
        hbcounter++;
    }

    public synchronized int get_heartbeat()
    {
        return hbcounter;
    }

    public NodeIdentity getNodeIdentity()
    {
        return node.getNodeIdentity();
    }

    public void setNodepool(NodePool np)
    {
        this.nodepool = np;
    }

    public NodePool getNodepool()
    {
        return nodepool;
    }

    public boolean isFree()
    {
        //
        // A reservation temporarily might pull this guy out of the pool by setting the virtual share order to 0 but not
        // yet assigning to a job so we have to check both the shares given out, and whether the virtual share order is
        // still pristine.
        //
        // We use this trick so we can use the "normal" allocation mechanisms for bookeeping without special-casing reservations.
        //
        // UIMA-4142, include blacklist considerations
        return ( (activeShares.size()) == 0 && (virtual_share_order == share_order) && ( !isBlacklisted() ) );
    }

    /**
     * Can preemption free this machine?
     */
    public boolean isFreeable()
    {
        boolean answer = true;
        // UIMA-4142, include blacklist considerations
        if ( isBlacklisted() ) return false;

        for ( Share s : activeShares.values() ) {
            if ( s.isFixed() ) {
                return false;
            }
        }
        return answer;
    }

    public int countProcesses()
    {
        return activeShares.size();
    }

    public void assignShare(Share s)
    {
        activeShares.put(s, s);
        shares_left -= s.getShareOrder();
    }

    HashMap<Share, Share> getActiveShares()
    {
        return activeShares;
    }

    public Node getNode()
    {
        return node;
    }

    public String getId() {
        return id;
    }
    
    public String getIp() {
        return node.getNodeIdentity().getIp();
    }

    public void setId(String id) {
        this.id = id;
    }
    
    public long getMemory() {
        return memory;
    }
    
    public void setMemory(long memory) {
        this.memory = memory;
    }

    public int getNodepoolDepth()              // UIMA-4275
    {
        return nodepool.getDepth();
    }

    public int getShareOrder()
    {
        return share_order;
    }

    public int getVirtualShareOrder()
    {
        return virtual_share_order;
    }

    public void setShareOrder(int o)
    {
        this.share_order = o; 
        this.shares_left = share_order;
        resetVirtualShareOrder();             // UIMA-4142 use common code to calculate this
    }

    public void setVirtualShareOrder(int o)
    {
        this.virtual_share_order = o; 
    }

    public void resetVirtualShareOrder()
    {
        // UIMA-4142, include blacklist considerations
        this.virtual_share_order = share_order - blacklisted_shares;
    }

    public void removeShare(Share s)
    {
        activeShares.remove(s);
        nodepool.removeShare(s);
        shares_left += s.getShareOrder();
    }

    /**
     * How many shares of the given order can I support without preemption?
     */
    public int countFreeShares(int order)
    {
        int in_use = 0;

        for ( Share s : activeShares.values() ) {
            in_use += s.getShareOrder();
        }
        // UIMA-4142, include blacklist considerations
        return (share_order - in_use - blacklisted_shares) / order ;
    }

    public int countFreeShares()
    {
        return shares_left;
    }

    /**
     * How much is left unused, plus the shares that are marked for eviction.
     */
    public int countFreedUpShares()
    {
        // String methodName = "countFreedUpShares";
        int cnt = shares_left;

        // logger.debug(methodName, null, "shares_left", shares_left);
        for ( Share s : activeShares.values() ) {
            if ( s.isEvicted() ) {
                // logger.debug(methodName, null, "Adding evicted shares: order", s.getShareOrder(), s);
                cnt += s.getShareOrder();
            } 
        }
        return cnt;
    }

    RmQueriedMachine queryMachine()
    {
        // UIMA-4142, include blacklist considerations
        RmQueriedMachine ret = new RmQueriedMachine(id, nodepool.getId(), memory, share_order, isBlacklisted());
        for ( Share s : activeShares.values() ) {
            RmQueriedShare rqs = new RmQueriedShare(s.getJob().getId().getFriendly(),
                                                    s.getId().getFriendly(),
                                                    s.getShareOrder(),
                                                    s.getInitializationTime(), 
                                                    s.getInvestment());
            rqs.setFixed(s.isFixed());
            rqs.setPurged(s.isPurged());
            rqs.setEvicted(s.isEvicted());
            rqs.setInitialized(s.isInitialized());
            ret.addShare(rqs);
        }
                
        for (Share s : blacklistedWork.values() ) {
            RmQueriedShare rqs = new RmQueriedShare(s.getBlJobId().getFriendly(),
                                                    s.getId().getFriendly(),
                                                    s.getShareOrder(),
                                                    s.getInitializationTime(), 
                                                    s.getInvestment());
            rqs.setBlacklisted();
            ret.addShare(rqs);
        }

        return ret;
    }

    RmQueriedMachine queryOfflineMachine()              // UIMA-4234
    {
        RmQueriedMachine ret = queryMachine();
        ret.setOffline();
        return ret;
    }

    RmQueriedMachine queryUnresponsiveMachine()        // UIMA-4234
    {
        RmQueriedMachine ret = queryMachine();
        ret.setUnresponsive();
        return ret;
    }

    /**
     * A machine's investment is the sum of it's share's investments.
     */
    public int getInvestment()
    {
        int answer = 0;
        for ( Share s : activeShares.values() ) {
            answer += s.getInvestment();
        }
        return answer;
    }
    
    public static String getDashes()
    {
        return String.format("%20s %12s %5s %13s %13s %11s %s", "--------------------", "------------", "-----", "-------------", "-------------", "-----------", "------ ...");
    }

    public static String getHeader()
    {
        // UIMA-4142, include blacklist considerations
        return String.format("%20s %12s %5s %13s %13s %11s %s", "Name", "Blacklisted", "Order", "Active Shares", "Unused Shares", "Memory (MB)", "Jobs");
    }

    /**
     * Does this machine match the thing specified in the input string 's'?
     */
    public boolean matches(String s)
    {
        String name = getNodeIdentity().getName();
        if ( s.equals(name) ) return true;                 // try for a perfect match
        
        // see if s is qualified with the domain name
        int ndx1 = s.indexOf(".");
        int ndx2 = name.indexOf(".");

        if ( (ndx1 > 0) && (ndx2 > 0) ) return false;     // both qualified, so no match

        if ( ndx1 == -1 ) {
            return s.equals(name.substring(0, ndx2));     // s not qualified, so strip name
        } else {
            return s.substring(0, ndx1).equals(name);     // name not qualified, so strip s
        }
    }

    @Override
    public int hashCode()
    {
    	
    	//System.out.println("hashcode for machine" + getId() + " is " + node.hashCode());
    	//return id.hashCode();
        return getNodeIdentity().hashCode();
    }

    public boolean equals(Object o)
    {
        if ( o == null ) return false;
        if ( this == o ) return true;
        if ( this.getClass() != o.getClass() ) return false;

        Machine m = (Machine) o;
        //return (id.equals(m.getId()));
    	return this.getNodeIdentity().equals(m.getNodeIdentity());
    }
    
    public String toString()
    {
        int qshares = 0;
        int unused = share_order;
        String jobs = "<none>";

        if ( activeShares.size() > 0 ) {
            StringBuffer buf = new StringBuffer();

            for ( Share s : activeShares.values() ) {
                qshares += s.getShareOrder();
                buf.append(s.getJob().getId());
                buf.append(" ");
            }
            jobs = buf.toString();

            unused = share_order - qshares;
        } 
        // UIMA-4142, include blacklist considerations
        return String.format("%20s %12s %5d %13d %13d %11d %s", id, isBlacklisted(), share_order, qshares, unused, (memory/1024), jobs);
    }
}
