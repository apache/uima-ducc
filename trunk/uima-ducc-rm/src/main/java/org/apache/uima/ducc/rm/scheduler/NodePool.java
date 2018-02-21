
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.uima.ducc.common.Node;
import org.apache.uima.ducc.common.NodeIdentity;
import org.apache.uima.ducc.common.persistence.rm.IRmPersistence;
import org.apache.uima.ducc.common.persistence.rm.IRmPersistence.RmNodes;
import org.apache.uima.ducc.common.persistence.rm.RmPersistenceFactory;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.SystemPropertyResolver;
import org.apache.uima.ducc.transport.event.common.IDuccTypes.DuccType;


class NodePool
    implements SchedConstants
{
    static DuccLogger logger = DuccLogger.getLogger(NodePool.class, COMPONENT_NAME);
    String id;
    NodePool parent = null;

    int depth;
    int updated = 0;
    int search_order = 100;
    int share_quantum = 0;

    EvictionPolicy evictionPolicy = EvictionPolicy.SHRINK_BY_MACHINE;

    HashMap<String, NodePool> children                       = new HashMap<String, NodePool>();                // key is name of resource class
    Map<String, String>   subpoolNames                       = new HashMap<String, String>();                  // if a subpool, this names the membership

    HashMap<Node, Machine> allMachines                       = new HashMap<Node, Machine>();                   // all active machines in the system
    HashMap<Node, Machine> unresponsiveMachines              = new HashMap<Node, Machine>();                   // machines with excessive missed heartbeats
    HashMap<Node, Machine> offlineMachines                   = new HashMap<Node, Machine>();
    HashMap<Integer, HashMap<Node, Machine>> machinesByOrder = new HashMap<Integer, HashMap<Node, Machine>>(); // All schedulable machines, not necessarily free
    HashMap<String, Machine>                 machinesByName  = new HashMap<String, Machine>();                 // by name, for nodepool support
    HashMap<String, Machine>                 deadByName      = new HashMap<String, Machine>();                 // anything we move to offline or unresponsive,
                                                                                                               // but with the same name we used, because
                                                                                                               // sometimes stupid domain gets in the way
    HashMap<String, Machine>                 machinesByIp    = new HashMap<String, Machine>();                 // by IP, for nodepool support

    HashMap<Share, Share>                    allShares       = new HashMap<Share, Share>();

    HashMap<Node, Machine>                   preemptables    = new HashMap<Node, Machine>();                   // candidates for preemption for reservations
    int total_shares = 0;

    Map<ResourceClass, ResourceClass>        allClasses      = new HashMap<ResourceClass, ResourceClass>();    // all the classes directly serviced by me
    //
    // There are "theoretical" shares based on actual capacities of
    // the machines.  They are used for the "how much" part of the
    // calculations. They aren't correlated with any actual share
    // objects.
    //
    // nMachinesByOrder is initialzed on every scheduling cycle to the number of physical
    // machines of each share order in the system.  During scheduling, we start giving them
    // away, and must reduce the count accordingly.  It is an optimization representing the number
    // of completely free machines - machines that have not had any resources scheduled against them
    // in this cycle.
    //
    //                            assume 2x16G, 0x32G, 1x48G, 4x64G machines ------------------------------+
    //                            the arrays look like this for each init of the scheduler:                v
    int nMachinesByOrder[];       // number of full, free machines of each share order                [ 0  2 0 1  4 ] - physical machines
    int vMachinesByOrder[];       // number of partial machines, indexed by free space
    int nSharesByOrder[];         // shares of each size for each share order                         [ 0 21 9 5  4 ] - collective N Shares for each order
    //int nFreeSharesByOrder[];     // for each order, the theoretical number of shares to give away  [ 0  1 0 3 16 ] - free Q shares per order

    int nPendingByOrder[];        // number of N-shares with pending evictinos

    //int neededByOrder[];         // for each order, how many N-shares do I want to add?

    //     int shareExpansion[];
    Map<Integer, Integer> onlineMachinesByOrder = new HashMap<Integer, Integer>();  // all online machines

    // Indexed by available free shares, the specific machines with the indicated free space
    HashMap<Integer, Map<Node, Machine>> virtualMachinesByOrder = new HashMap<Integer, Map<Node, Machine>>();  // UIMA-4142
    GlobalOrder maxorder = null;

    IRmPersistence persistence = null;
    boolean canReserve = false;       // if we contain a class with policy Reserve, then stuff in this pool is reservable

    int reserve_overage;              // Allowable excess size in GB for unmanaged reservations

    NodePool(NodePool parent, String id, Map<String, String> nodes, EvictionPolicy ep, int depth, int search_order, int share_quantum)
    {
        String methodName = "NodePool.<init>";
        this.parent = parent;
        this.id = id;
        this.subpoolNames = nodes;
        if ( nodes == null ) {            // unlikely, but not illegal
            this.subpoolNames = new HashMap<String, String>();
            logger.warn(methodName, null, "Nodepool", id, ": no nodes in node list");
        }
        this.evictionPolicy = ep;
        this.depth = depth;
        this.search_order = search_order;
        this.share_quantum = share_quantum;              // in KB

        if ( parent == null ) {
            maxorder = new GlobalOrder();
        } else {
            maxorder = parent.getGlobalOrder();
        }

        persistence = RmPersistenceFactory.getInstance(this.getClass().getName(), "RM");

        reserve_overage = SystemPropertyResolver.getIntProperty("ducc.rm.reserve_overage", 0);  // Can't be static as may be reconfigured
    }

    void addResourceClass(ResourceClass cl)
    {  // UIMA-4065
        allClasses.put(cl, cl);
        if ( cl.getPolicy() == Policy.RESERVE) canReserve = true;
    }

    NodePool getParent()
    {
        return this.parent;
    }

    String getId()
    {
        return id;
    }

    int getShareQuantum()
    {
        return share_quantum;
    }

    int getDepth()
    {
        return depth;
    }

    int countShares()
    {
        return allShares.size();
    }

    int countOccupiedShares()
    {
        int count = allShares.size();
        for ( NodePool np : children.values() ) {
            count += np.countOccupiedShares();
        }
        return count;
    }

    //
    // Note, this will only be accurate AFTER reset, but before actuall allocation of
    // shares begins.  After allocation, and before the next reset this will return junk.
    //
    // It is intended to be called from ResourceClass.canUseBonus()
    // UIMA-4065
    int countAssignableShares(int order)
    {
        String methodName = "countAssignableShares";
        // first calculate my contribution
        int ret = nSharesByOrder[order];
        for (ResourceClass rc : allClasses.values() ) {
            int[] gbo = rc.getGivenByOrder();
            if ( gbo != null ) {
                ret -= gbo[order];
            }
        }
        logger.trace(methodName, null, "Shares available for", id, ":", ret);
        // now accumulate the kid's contribution
        for ( NodePool np : children.values() ) {
            ret += np.countAssignableShares(order);
        }
        return ret;
    }

    void removeShare(Share s)
    {
        allShares.remove(s);
    }

    boolean containsPoolNode(Node n)
    {
        // allow the names to be machines or ip addresses
        if ( subpoolNames.containsKey( n.getNodeIdentity().getIp()   )) return true;
        if ( subpoolNames.containsKey( n.getNodeIdentity().getName() )) return true;
        return false;
    }

    /**
     * How many do I have, including recusring down the children?
     */
    int countMachines()
    {
        int count = allMachines.size();
        for ( NodePool np : children.values() ) {
            count += np.countMachines();
        }
        return count;
    }
     /**
     * How many do I have, including recursing down the children?
     */
    int countUnresponsiveMachines()
    {
        int count = unresponsiveMachines.size();
        for ( NodePool np : children.values() ) {
            count += np.countUnresponsiveMachines();
        }
        return count;
    }

     /**
     * How many do I have, just me.
     */
    int countLocalUnresponsiveMachines()
    {
        return unresponsiveMachines.size();
    }

    /**
     * How many do I have, including recusring down the children?
     */
    int countOfflineMachines()
    {
        int count = offlineMachines.size();
        for ( NodePool np : children.values() ) {
            count += np.countOfflineMachines();
        }
        return count;
    }

    /**
     * How many do I have, just me.
     */
    int countLocalOfflineMachines()
    {
        return offlineMachines.size();
    }

    /**
     * Return nodes varied off for me and my kids.
     * UIMA-4142, RM reconfiguration
     */
    Map<Node, Machine> getOfflineMachines()
    {
        @SuppressWarnings("unchecked")
        Map<Node, Machine> ret = (Map<Node, Machine>) offlineMachines.clone();
        for (NodePool np : children.values()) {
            ret.putAll(np.getOfflineMachines());
        }
        return ret;
    }

    /**
     * Return nodes varied off for me and my kids.
     * UIMA-4234, More info in query occupancy
     */
    Map<Node, Machine> getUnresponsiveMachines()
    {
        @SuppressWarnings("unchecked")
        Map<Node, Machine> ret = (Map<Node, Machine>) unresponsiveMachines.clone();
        for (NodePool np : children.values()) {
            ret.putAll(np.unresponsiveMachines);
        }
        return ret;
    }

    /**
     * Non-recursive machine count.
     */
    int countLocalMachines()
    {
        return allMachines.size();
    }

    /**
     * Non recursive share count;
     */
    int countLocalShares()
    {
        return total_shares;
    }

    /**
     * Counts just local, for reservations.
     */
    int countFreeMachines(int order)
    {
        int cnt = 0;

        HashMap<Node, Machine> mlist = null;
        mlist = machinesByOrder.get(order);
        if ( mlist == null ) return 0;

        for ( Machine m : mlist.values() ) {
            if ( isSchedulable(m) && m.isFree() ) {
                cnt++;
            }
        }
        return cnt;
    }

    /**
     * Counts all known machines, just me.
     */
    int[] countLocalFreeMachines()
    {
        return nMachinesByOrder.clone();
    }

    int countTotalShares()
    {
        int answer = total_shares;
        for ( NodePool np : children.values() ) {
            answer += np.countTotalShares();
        }
        return answer;
    }

    /**
     * Total Q shares in the nodepool that are not yet given away in the scheduling cycle.
     */
    int countQShares()
    {
        int count = nSharesByOrder[1];
        for ( NodePool np : children.values() ) {
            count += np.countQShares();
        }
        return count;
    }

    /**
     * Total Q shares in the nodepool still available, just me.
     */
    int countLocalQShares()
    {
        return nSharesByOrder[1];
    }


    int countAllMachinesByOrder(int o)
    {
        int count = 0;
        if ( machinesByOrder.containsKey(o) ) {
            count = machinesByOrder.get(o).size();
        }

        for ( NodePool np : children.values() ) {
            count += np.countAllMachinesByOrder(o);
        }
        return count;
    }

    int[] countAllLocalMachines()
    {
        int[] ret = makeArray();
        for ( int o : machinesByOrder.keySet() ) {
            ret[o] = machinesByOrder.get(o).size();
        }
        return ret;
    }

    /**
     * Returns N-Shares, recursing down
     */
    int countNSharesByOrder(int o)
    {
        int count = nSharesByOrder[o];
        for ( NodePool np : children.values() ) {
            count += np.countNSharesByOrder(o);
        }
        return count;
    }

    /**
     * Returns N-Shares, local
     */
    int countLocalNSharesByOrder(int o)
    {
        return nSharesByOrder[o];
    }

    /**
     * Returns number of N-shares that are still busy but pending eviction.
     */
    int countPendingSharesByOrder(int o)
    {
        int count = nPendingByOrder[o];
        for ( NodePool np : children.values() ) {
            count += np.countPendingSharesByOrder(o);
        }
        return count;
    }

    /**
     * Helper for compatibleNodepool(), recurses down children.
     * UIMA-4142
     */
    private boolean isCompatibleNodepool(Policy p, ResourceClass rc)
    {
        if ( allClasses.containsKey(rc) ) return true;
        for (NodePool np : children.values()) {
            if ( np.isCompatibleNodepool(p, rc) ) return true;
        }
        return false;
    }

    /**
     * Helper for compatibleNodepool(), find the top of the heirarchy.
     * UIMA-4142
     */
    NodePool findTopOfHeirarchy()
    {
        NodePool ret = this;
        while (ret.getParent() != null) {
            ret = ret.getParent();
        }
        return ret;
    }

    /**
     * Interrogate whether work assigned to the indicated rc could end up here.
     *
     * If it's a fair-share allocation, we need to interrogate 'me', my children, and
     * my ancestors.
     *
     * If it's something else, it must reside reight here.
     *
     * This is called during recovery; a change to the class or np config can cause incompatibilities
     * with previously scheduled work after a restart.
     *
     * UIMA-4142
     *
     * @param     p   The scheduling policy; determines whether descent into child pools is allowed.
     * @param    rc  The rc to check
     * @return true If work scheduled to the RC is compatible.
     */
    boolean compatibleNodepool(Policy p, ResourceClass rc)
    {
        switch ( p ) {
            case FAIR_SHARE:
                NodePool top = findTopOfHeirarchy();
                return top.isCompatibleNodepool(p, rc);
            case FIXED_SHARE:
            case RESERVE:
                if ( allClasses.containsKey(rc) ) return true;
        }

        return false;
    }

    int[] cloneNSharesByOrder()
    {
        int[] cln =  nSharesByOrder.clone();
        for ( NodePool np : children.values() ) {
            int[] subcln = np.cloneNSharesByOrder();
            for ( int i = 0; i < cln.length; i++ ) {
                cln[i] += subcln[i];
            }
        }
        return cln;
    }

    int[] cloneVMachinesByOrder()
    {
        int[] cln =  nMachinesByOrder.clone();
        for ( int i = 0; i < cln.length; i++ ) {
            cln[i] += vMachinesByOrder[i];
        }

        for ( NodePool np : children.values() ) {
            int[] subcln = np.cloneVMachinesByOrder();
            for ( int i = 0; i < cln.length; i++ ) {
                cln[i] += subcln[i];
            }
        }
        return cln;
    }

    public GlobalOrder getGlobalOrder()
    {
        return maxorder;
    }

    public void updateMaxOrder(int order)
    {
        maxorder.update(order);
    }

    public int getMaxOrder()
    {
        return maxorder.getOrder();              // must always be the same for parent and all children
    }

    public int getArraySize()
    {
        return getMaxOrder() + 1;                      // a bit bigger, because we're 1-indexed for easier counting
                                                       // same for parent and children
    }

    public int[] makeArray()                           // common static code because getting this right everywhere is painful
    {
        return new int[getArraySize()];
    }


    int getSearchOrder()
    {
        return this.search_order;
    }

    public Machine getMachine(Node n)
    {
        Machine m = allMachines.get(n);

        if ( m == null ) {
            for ( NodePool np : children.values() ) {
                m = np.getMachine(n);
                if ( m != null ) {
                    break;
                }
            }
        }
        return m;
    }

    public Machine getMachine(NodeIdentity ni)
    {
        Machine m = machinesByIp.get(ni.getIp());
        if ( m == null ) {
            for ( NodePool np : children.values() ) {
                m = np.getMachine(ni);
                if ( m != null ) break;
            }
        }
        return m;
    }

    boolean containsMachine(Machine m)
    {
        Map<Node, Machine> allm = getAllMachines();
        return allm.containsKey(m.getNode());
    }

    @SuppressWarnings("unchecked")
    HashMap<Node, Machine> getAllMachinesForPool()
    {
        return (HashMap<Node, Machine>) allMachines.clone();
    }

    HashMap<Node, Machine> getAllMachines()
    {
        @SuppressWarnings("unchecked")
        HashMap<Node, Machine> machs = (HashMap<Node, Machine>) allMachines.clone();
        for ( NodePool np : children.values() ) {
            HashMap<Node, Machine> m = np.getAllMachines();
            if ( m != null ) {
                machs.putAll(m);
            }
        }

        return machs;
    }

    HashMap<String, Machine> getMachinesByName()
    {
        @SuppressWarnings("unchecked")
        HashMap<String, Machine> machs = (HashMap<String, Machine>) machinesByName.clone();
        for ( NodePool np : children.values() ) {
            HashMap<String, Machine> m = np.getMachinesByName();
            if ( m != null ) {
                machs.putAll(m);
            }
        }

        return machs;
    }

    HashMap<String, Machine> getMachinesByIp()
    {
        @SuppressWarnings("unchecked")
        HashMap<String, Machine> machs = (HashMap<String, Machine>) machinesByIp.clone();
        for ( NodePool np : children.values() ) {
            HashMap<String, Machine> m = np.getMachinesByIp();
            if ( m != null ) {
                machs.putAll(m);
            }
        }

        return machs;
    }

    @SuppressWarnings("unchecked")
    HashMap<Node, Machine> getMachinesByOrder(int order)
    {

        HashMap<Node, Machine> machs;

        if( machinesByOrder.containsKey(order) ) {
            machs = (HashMap<Node, Machine>) machinesByOrder.get(order).clone();
        } else {
            machs = new HashMap<Node, Machine>();
        }

        for ( NodePool np : children.values() ) {
            HashMap<Node, Machine> m = np.getMachinesByOrder(order);
            machs.putAll(m);
        }

        return machs;
    }

    @SuppressWarnings("unchecked")
    Map<Node, Machine> getVirtualMachinesByOrder(int order)
    {
        Map<Node, Machine> machs;

        if( virtualMachinesByOrder.containsKey(order) ) {
            HashMap<Node, Machine> tmp = (HashMap<Node, Machine>) virtualMachinesByOrder.get(order);
            machs = (HashMap<Node, Machine>) tmp.clone();
        } else {
            machs = new HashMap<Node, Machine>();
        }

//         for ( NodePool np : children.values() ) {
//             HashMap<Machine, Machine> m = np.getVirtualMachinesByOrder(order);
//             machs.putAll(m);
//         }

        return machs;
    }

    /**
     * Work out the N shares for each share class.
     *
     * Note: This is a helper class, not made public, and does not need to account for child nodepools.
     *       If you need the recursion use countOutNSharesByOrder().
     *
     * Internally, only call this if you mess with the counting arrays.  If you call somebody else who
     * messes with the counting arrays, leave it to them to call this.
     */
    protected void calcNSharesByOrder()
    {
        int len = nMachinesByOrder.length;

        // init nSharesByorder to the sum of 'n and 'v MachinesByOrder
        System.arraycopy(nMachinesByOrder, 0, nSharesByOrder, 0, len);
        for ( int i = 0; i < getMaxOrder() + 1; i++ ) {
            nSharesByOrder[i] += vMachinesByOrder[i];
        }

        for ( int o = 1; o < len; o++ ) {                     // counting by share order
            //nFreeSharesByOrder[o] = nMachinesByOrder[o] * o;
            for ( int p = o+1; p < len; p++ ) {
                if ( nSharesByOrder[p] != 0 ) {
                    nSharesByOrder[o] += (p / o) * nSharesByOrder[p];
                }
            }
        }
    }

    protected int[] countMachinesByOrder()
    {
        int[] ans = nMachinesByOrder.clone();
        for ( NodePool np : children.values() ) {
            int[] tmp = np.countMachinesByOrder();
            for ( int i = 0; i < getArraySize(); i++ ) {
                ans[i] += tmp[i];
            }
        }
        return ans;
    }


    protected int[] countVMachinesByOrder()
    {
        int[] ans = vMachinesByOrder.clone();
        for ( NodePool np : children.values() ) {
            int[] tmp = np.countVMachinesByOrder();
            for ( int i = 0; i < getArraySize(); i++ ) {
                ans[i] += tmp[i];
            }
        }
        return ans;
    }

    protected int[] countLocalVMachinesByOrder()
    {
        return vMachinesByOrder.clone();
    }

    protected int[] countAllNSharesByOrder()
    {
        int[] ans = nSharesByOrder.clone();
        for ( NodePool np : children.values() ) {
            int[] tmp = np.countAllNSharesByOrder();
            for ( int i = 0; i < getArraySize(); i++ ) {
                ans[i] += tmp[i];
            }
        }
        return ans;
    }


    /**
     * Common code to connect a share into the system, used when assigning a new
     * share (from within NodePool), or when reconnecting a share during job recovery
     * (from JobManagerCoverter).
     */
    public synchronized void connectShare(Share s, Machine m, IRmJob j, int order)
    {
        String methodName = "connectShare";
        logger.info(methodName, j.getId(), "share", s,  "order", order, "machine", m);
        j.assignShare(s);
        m.assignShare(s);
        rearrangeVirtual(m, order, j.getSchedulingPolicy());
        allShares.put(s, s);
    }

    void rearrangeVirtual(Machine m, int order, Policy policy)

    {
        String methodName = "rearrangeVirtual";
        if ( allMachines.containsKey(m.key()) ) {
            int v_order = m.getVirtualShareOrder();
            int r_order = m.getShareOrder();

            // UIMA-4913 Avoid index-outta-bounds when a machine's size changes
            // If Share appears bigger than remaining free-space pretend the share is smaller,
            // or if share is smaller but is a whole machine reservation, then pretend the
            // reservation matches the machine, so in both cases the free space = 0
            if (order > v_order) {
                logger.warn(methodName, null, m.getId(), "found a share of size", order, "on a machine with only", v_order, "free slots - set free=0");
                order = v_order;
            } else if (order < v_order && policy == Policy.RESERVE){
                logger.warn(methodName, null, m.getId(), "found a RESERVE share of size", order, "on a machine with", v_order, "free slots - set free=0");
                order = v_order;
            }

            logger.trace(methodName, null, m.getId(), "order", order, "v_order", v_order, "r_order", r_order);

            if ( v_order == r_order ) {
                nMachinesByOrder[r_order]--;
            } else {
                vMachinesByOrder[v_order]--;
            }

            Map<Node, Machine> vlist = virtualMachinesByOrder.get(v_order);
            if ( vlist == null ) {
                // Delivered under UIMA-4275 as that is when I decided to try to avoid NPE here.
                //
                // This is fatal, the internal records are all wrong.  Usually this is because of some
                // external snafu, such as mixing and matching ducc clusters on the same broker.
                // There's really not much we can do though.  There's a good chance that continuing
                // will cause NPE elsewhere.  Maybe we can just ignore it and let it leak?
                logger.error(methodName, null, "ERROR: bad virtual machine list.", m.getId(), "order", order, "v_order", v_order, "r_order", r_order);
                return;
            }
            vlist.remove(m.key());

            v_order -= order;
            m.setVirtualShareOrder(v_order);
            if (v_order != 0 ) {
                vlist = virtualMachinesByOrder.get(v_order);
                if ( vlist == null ) {
                    vlist = new HashMap<Node, Machine>();
                    virtualMachinesByOrder.put(v_order, vlist);
                }
                vlist.put(m.key(), m);
                vMachinesByOrder[v_order]++;
            }
            calcNSharesByOrder();
        } else {
            for ( NodePool np : children.values() ) {
                np.rearrangeVirtual(m, order, policy);
            }
        }
    }


    void accountForShares(HashMap<Share, Share> shares)
    {
        if ( shares == null ) return;

        for ( Share s : shares.values() ) {
            int order = s.getShareOrder();
            Machine m = s.getMachine();
            Policy policy = s.getJob().getSchedulingPolicy();
            rearrangeVirtual(m, order, policy);
        }
    }

    /**
     * Prepare the structures for scheduling.  These get modified in place by the scheduler.
     *
     * @param order is the hightest order of any job that can potentially get scheduled.
     *        We need this to insure the tables have sufficient space and we don't get NPEs.
     */
    void reset(int order)
    {
        String methodName = "reset";
        //
        // TODO: Not all of these are used in every reset cycle.  Maybe we should break up the
        //       reset code so it matches the cycles better.  otoh, this isn't a performance-intensive
        //       scheduler so do we care?
        //
        updateMaxOrder(order);
        logger.info(methodName, null, "Nodepool:", id, "Maxorder set to", getMaxOrder());

        nSharesByOrder     = makeArray();
        nMachinesByOrder   = makeArray();
        vMachinesByOrder   = makeArray();
        //nFreeSharesByOrder = new int[maxorder + 1];
        //neededByOrder      = new int[maxorder + 1];

        nPendingByOrder = makeArray();

        // UIMA-4142 Must set vMachinesByOrder and virtualMachinesByOrder independently of
        //           machinesByOrder because blacklisting can cause v_order != r_order
        //           during reset.
        // UIMA-4910 Ignore unusable machines
        virtualMachinesByOrder.clear();
        for ( Machine m : allMachines.values() ) {
            if ( !isSchedulable(m) ) {
              continue;               // Ignore unusable machines
            }
            m.resetVirtualShareOrder();
            int v_order = m.getVirtualShareOrder();
            int r_order = m.getShareOrder();

            Map<Node, Machine> ml = null;
            if ( v_order == r_order ) {
                nMachinesByOrder[r_order]++;
            } else {
                vMachinesByOrder[v_order]++;
            }

            ml = virtualMachinesByOrder.get(v_order);
            if ( ml == null ) {
                ml = new HashMap<Node, Machine>();
                virtualMachinesByOrder.put(v_order, ml);
            }

            ml.put(m.key(), m);
        }

        // UIMA 4142 this old calc isn't right any more because blacklisting can cause
        //     v_order != r_order during reset
        // virtualMachinesByOrder = new HashMap<Integer, HashMap<Node, Machine>>();
        // for ( Integer i : machinesByOrder.keySet() ) {

        //     @SuppressWarnings("unchecked")
        //     HashMap<Node, Machine> ml = (HashMap<Node, Machine>) machinesByOrder.get(i).clone();

        //     virtualMachinesByOrder.put(i, ml);
        //     nMachinesByOrder[i] = ml.size();
        // }


        calcNSharesByOrder();

        for ( NodePool np : children.values() ) {
            np.reset(order);
        }

        if ( (parent == null) && ( updated > 0 ) ) {
            // top-level nodepool will recurse
            logger.info(methodName, null, "Scheduling Tables:\n", toString());
            updated = 0;
        }
    }

    void resetPreemptables()
    {
        String methodName = "resetPreemptables";
        logger.debug(methodName, null, "Resetting preemptables in nodepool", id);

        // UIMA-4064 Need to do this recursively
        preemptables.clear();
        for ( NodePool np : children.values() ) {
            np.resetPreemptables();
        }

    }


    /**
     * Return the specified subpool, or me!
     */
    NodePool getSubpool(String name)
    {
        if ( name.equals(id) ) {
            return this;
        }

        for ( NodePool np : children.values() ) {
            NodePool ret = np.getSubpool(name);
            if (ret != null) return ret;
        }
        return null;
    }

    /**
     * Do "I" contain the indicated nodepool? More accurately, can "I" access everything in the
     * indicated nodepool?
     */
    boolean containsSubpool(NodePool np)
    {
        if ( np == this ) {
            return true;
        }

        for ( NodePool cnp : children.values() ) {
            if (cnp.containsSubpool(np)) return true;
        }
        return false;
    }

    HashMap<String, NodePool> getChildren()
    {
        return children;
    }

    List<NodePool> getChildrenAscending()
    {
        ArrayList<NodePool> sorted = new ArrayList<NodePool>();
        if ( children.size() > 0 ) {
            sorted.addAll(children.values());
            Collections.sort(sorted, new NodepoolAscendingSorter());
        }
        return sorted;
    }

    List<NodePool> getChildrenDescending()
    {
        ArrayList<NodePool> sorted = new ArrayList<NodePool>();
        if ( children.size() > 0 ) {
            sorted.addAll(children.values());
            Collections.sort(sorted, new NodepoolDescendingSorter());
        }
        return sorted;
    }

    /**
     * Subpools are always associated with a classname.
     *
     * We can assume that all node updates are refused until all subpools are created
     * so we don't have to worry about updating the pools until nodeArrives(), below.
     */
    NodePool createSubpool(String className, Map<String, String> names, int order)
    {
        NodePool np = new NodePool(this, className, names, evictionPolicy, depth + 1, order, share_quantum);
        children.put(className, np);
        return np;
    }

//    private synchronized void incrementOnlineByOrder(int order)
//    {
//        if ( ! onlineMachinesByOrder.containsKey(order) ) {
//            onlineMachinesByOrder.put(order, 1);
//        } else {
//            onlineMachinesByOrder.put(order, onlineMachinesByOrder.get(order) + 1);
//        }
//    }

//    private synchronized void decrementOnlineByOrder(int order)
//    {
//        onlineMachinesByOrder.put(order, onlineMachinesByOrder.get(order) - 1);
//    }

//    synchronized void getLocalOnlineByOrder(int[] ret)         // for queries, just me
//    {
//        for ( int o: onlineMachinesByOrder.keySet() ) {
//            ret[o] += onlineMachinesByOrder.get(o);
//        }
//    }

//    synchronized void getOnlineByOrder(int[] ret)         // for queries
//    {
//        for ( int o: onlineMachinesByOrder.keySet() ) {
//            ret[o] += onlineMachinesByOrder.get(o);
//        }
//        for ( NodePool child : children.values() ) {
//            child.getOnlineByOrder(ret);
//        }
//    }


    void signalDb(Machine m, RmNodes key, Object value)
    {
        String methodName = "signalDb";
        try {
            persistence.setNodeProperty(m.getNode().getNodeIdentity().getName(), key, value);
        } catch (Exception e) {
            logger.warn(methodName, null, "Cannot update DB property", key, "for machine", m);
        }
    }

    Map<RmNodes, Object> initDbProperties(Machine m)
    {
        Node n = m.getNode();
        NodeIdentity nid = n.getNodeIdentity();

        Map<RmNodes, Object> props = new HashMap<RmNodes, Object>();
        props.put(RmNodes.Name, nid.getName());
        props.put(RmNodes.Ip, nid.getIp());
        props.put(RmNodes.Nodepool, id);
        props.put(RmNodes.Quantum, share_quantum / ( 1024*1024));

        props.put(RmNodes.Memory       , m.getMemory() / (1024*1024));
        props.put(RmNodes.ShareOrder  , m.getShareOrder());
        props.put(RmNodes.Blacklisted  , m.isBlacklisted());

        // init these here, but must be maintained by machine
        props.put(RmNodes.Heartbeats   , 0);
        props.put(RmNodes.SharesLeft   , m.countFreeShares());     // qshares remaining
        props.put(RmNodes.Assignments  , m.countProcesses());      // processes
        props.put(RmNodes.NPAssignments, m.countNpShares());

        props.put(RmNodes.Reservable   , canReserve);

        StringBuffer buf = new StringBuffer();
        for ( ResourceClass cl : allClasses.keySet() ) {
            buf.append(cl.getName());
            buf.append(" ");
        }
        props.put(RmNodes.Classes, buf.toString());
        return props;
    }

    void adjustMachinesByOrder(int neworder, Machine m)
    {
        int oldorder = m.getShareOrder();
        if ( oldorder != neworder ) {                  // can change. e.g. if it was taken offline for
            HashMap<Node, Machine> mlist = machinesByOrder.get(oldorder);
            mlist.remove(m.key());
            m.updateShareOrder(neworder);                       // UIMA-5605 Correct shares_left when size changes
            signalDb(m, RmNodes.ShareOrder, neworder);          // Jira 4913 Update DB so ducc-mon can show the current size
            mlist = machinesByOrder.get(neworder);
            if ( mlist == null ) {
                mlist = new HashMap<Node, Machine>();
                machinesByOrder.put(neworder, mlist);
            }
            mlist.put(m.key(), m);
        }
    }

    /**
     * Handle a new node update.
     */
    Machine nodeArrives(Node node, int order)
    {
        String methodName = "nodeArrives";
        // Note: the caller of this method MUST (aka IS REQUIRED) to insure this this is the
        //       right nodepool as we do not recurse.

        updateMaxOrder(order);

        String n = node.getNodeIdentity().getName();

        // if it's offline it can't be restored like this.
        if ( offlineMachines.containsKey(node) ) {
            Machine m = offlineMachines.get(node);
            logger.trace(methodName, null, "Node ", m.getId(), " is offline, not activating.");
            return m;
        }
        // logger.info(methodName, null, "NODEARRIVES", n, "pass offline Machines");

        // if it was dead, then it isn't any more, AND it's mine, so I need to restart it
        if ( unresponsiveMachines.containsKey(node) ) {          // reactive the node

            logger.info(methodName, null, "RECOVER NODE", n);
            Machine m = unresponsiveMachines.remove(node);       // not unresponsive any more

            // Deal with memory on the machine changing
            adjustMachinesByOrder(order, m);

            // Note: The machine must be on all the other lists by definition since it wasn't taken off when it went offline

            signalDb(m, RmNodes.Responsive, true);
            logger.info(methodName, null, "Nodepool:", id, "Host reactivated ", m.getId(), "shares", order, m.toString());
            return m;
        }
        // logger.info(methodName, null, "NODEARRIVES", n, "pass unresponsive Machines");

        // ok, it is my problem?  If so, then it isn't offline or dead, so it's ok, and we're done
        if ( allMachines.containsKey(node) ) {                   // already known, do nothing
            Machine m = allMachines.get(node);

            // Deal with memory on the machine changing
            adjustMachinesByOrder(order, m);

            logger.trace(methodName, null, "Node ", m.getId(), " is already known, not adding.");
            return m;
        }
        // logger.info(methodName, null, "NODEARRIVES", n, "pass allMachines");

        // If we fall through it's a new one.
        Machine machine = new Machine(node);                     // brand new machine, make it active
        Node key = machine.key();
        machine.setShareOrder(order);
        allMachines.put(key, machine);                 // global list
        machinesByName.put(machine.getId(), machine);
        machinesByIp.put(machine.getIp(), machine);
        //incrementOnlineByOrder(order);
        machine.setNodepool(this);

        total_shares += order;

        // index it by its share order to make it easier to find
        HashMap<Node, Machine> mlist = machinesByOrder.get(order);
        if ( mlist == null ) {
            mlist = new HashMap<Node, Machine>();
            machinesByOrder.put(order, mlist);
        }
        mlist.put(key, machine);

        logger.info(methodName, null, "Nodepool:", id, "Host added:", id, ": ", machine.getId(), "Nodefile:", subpoolNames.get(machine.getId()), // UIMA-4142, add file nodefile
                    String.format("shares %2d total %4d:", order, total_shares), machine.toString());
        updated++;

        Map<RmNodes, Object> props = initDbProperties(allMachines.get(key));
        props.put(RmNodes.Responsive, true);
        props.put(RmNodes.Online, true);
        try {
            persistence.createMachine(machine.getId(), props);
        } catch (Exception e) {
            logger.warn(methodName, null, "Cannot write machine to DB:", machine.getId(), e);
        }

        return machine;
    }

    /**
     * Purge all or some of the work on a machine that has died, or been taken offline
     *
     * @param m            node being removed
     * @param removeAll    true if all work is to be purged, otherwise just the preemptable work
     *
     * Ignore unmanaged reservations as they have no ducc-managed work
     * Purge just fair-share/preemptable work if being varyed off, or all work if node has died (UIMA-4752)
     */
    void disable(Machine m, boolean removeAll)
    {
        String methodName = "disable";

        logger.info(methodName, null, "Nodepool:", id, "Host disabled:", m.getId(), "Looking for shares to clear");

        String eventType = removeAll ? "Host dead:" : "Host offline:";

        HashMap<Share, Share> shares = m.getActiveShares();
        for (Share s : shares.values()) {
            IRmJob j = s.getJob();

            if ( j.getDuccType() == DuccType.Reservation ) {
              logger.info(methodName, null, "Nodepool:", id, eventType, m.getId(), "Not purging", j.getDuccType());
              continue;
            }
            if ( removeAll || j.getSchedulingPolicy() == Policy.FAIR_SHARE )  {
                // NOTE: Currently will never get a Pop ... just a Service of type Other !!
                logger.info(methodName, j.getId(), "Nodepool:", id, eventType, j.getDuccType(), "purge:", m.getId());
                if (j.getDuccType() == DuccType.Service || j.getDuccType() == DuccType.Pop) {
                  j.markComplete();      // UIMA-4327 Must avoid reallocation, these guys are toast if they get purged.
                  logger.info(methodName, j.getId(), "Nodepool:", id, eventType, m.getId(), "Mark service/pop completed.");
                }
                j.shrinkByOne(s);   // De-allocate this share
                s.purge();          // This bit tells OR not to wait for confirmation from the agent

                int order = s.getShareOrder();
                nPendingByOrder[order]++;
            } else {
                logger.info(methodName, j.getId(), "Nodepool:", id, eventType, m.getId(), "Not purging NP work - ", j.getDuccType());
            }

        }
    }

    void nodeLeaves(Machine m)
    {
        // note, simpler than varyoff because we really don't care about unusual
        // conditions since there's nobody to tell
        if ( allMachines.containsKey(m.key()) ) {
            disable(m, true);    // Remove all work
            unresponsiveMachines.put(m.key(), m);
            signalDb(m, RmNodes.Responsive, false);
        } else {
            for ( NodePool np : children.values() ) {
                np.nodeLeaves(m);
            }
        }
    }

    // UIMA-4142
    // helper for CLI things that refer to things by name only.  do we know about anything by this
    // name?  see resolve() in Scheduler.java.
    boolean hasNode(String n)
    {
        return machinesByName.containsKey(n);
    }

    NodePool findNodepoolByNodename(String n)
    {
        if ( hasNode(n) ) {
            return this;
        } else {
            for ( NodePool np : children.values() ) {
                NodePool ret = np.findNodepoolByNodename(n);
                if ( ret != null ) {
                    return ret;
                }
            }
        }
        return null;
    }

    private String doVaryOff(String node)
    {
        // caller must insure node is known to "me"
        Machine m = machinesByName.get(node);
        if (offlineMachines.containsKey(m.key()) ) {
            return "VaryOff: Nodepool " + id + " - Already offline: " + node;
        }

        if ( unresponsiveMachines.containsKey(m.key()) ) {
            // lets be friendly and tell caller it's also unresponsive
            offlineMachines.put(m.key(), m);
            signalDb(m, RmNodes.Online, false);
            return "VaryOff: Nodepool " + id + " - Unresponsive machine, marked offline: " + node;
        }

        offlineMachines.put(m.key(), m);
        disable(m, false);                    // Remove just pre-emptable work
        signalDb(m, RmNodes.Online, false);
        return "VaryOff: " + node + " - OK.";
    }

    String varyoff(String node)
    {
        // note, vaguely trickier than 'nodeLeaves' because we need to catch the
        // potential user confusions and reflect them back.
        NodePool np = findNodepoolByNodename(node);
        if ( np == null ) {
            return "VaryOff: Nodepool " + id + " - Cannot find machine: " + node;
        }

        // note we only call this if we know for sure the node can be found and associated with a NP
        return np.doVaryOff(node);   // must direct to the correct context
    }

    private String doVaryOn(String node)
    {

        // caller must insure node is known to "me"
        Machine m = machinesByName.get(node);
        Node key = m.key();

        if ( ! offlineMachines.containsKey(key) ) {
            return "VaryOn: Nodepool " + id + " - Already online: " + m.getId();
        }

        offlineMachines.remove(key);
        signalDb(m, RmNodes.Online, true);

        return "VaryOn: Nodepool " + id + " - Machine marked online: " + node;
    }

    /**
     * We're going to just take it off the offline list and if it happens to come back, fine, it will get picked up
     * in nodeArrives as a new machine.
     */
    String varyon(String node)
    {
        NodePool np = findNodepoolByNodename(node);
        if ( np == null ) {
            return "VaryOff: Nodepool " + id + " - Cannot find machine: " + node;
        }

        return np.doVaryOn(node);         // must pass to the right nodepool, can't do it "here"
    }

    boolean isSchedulable(Machine m)
    {
        if ( m.isBlacklisted() )                         return false;
        if ( unresponsiveMachines.containsKey(m.key()) ) return false;
        if ( offlineMachines.containsKey(m.key()) )      return false;

        return true;
    }

    /**
     * ------------------------------------------------------------------------------------------
     * Routines used during the counting phase
     * ------------------------------------------------------------------------------------------
     */

    /**
     * A quick check to see if there are any machines of the right size. We make a more
     * comprehensive check to see if they're usable in countFreeableMachines later. We do this
     * so we can try to return an accurate reason for deferral.
     * UIMA-5086 Consider a range of sizes for Unmanaged Reservations
     */
    int countReservables(IRmJob j)
    {
        int order = j.getShareOrder();
        int max_order = getMaxShareOrder(j);
        do {
            if (machinesByOrder.containsKey(order) && machinesByOrder.get(order).size() > 0) {
                return machinesByOrder.get(order).size();
            }
        } while (++order <= max_order);
        return 0;
    }

    /**
     * Count total physical machines that could accomodate a 'fixed' request that the job
     * will fit in.
     */
    int countFixable(IRmJob j)
    {
        int order = j.getShareOrder();
        int ret = 0;
        for ( int i = order; i < getMaxOrder(); i++ ) {
            if ( machinesByOrder.containsKey(order) ) {
                ret += machinesByOrder.get(order).size();
            }
        }
        return ret;
    }

    /*
     * Add the allowable overage to the request and convert to shares
     * But only for unmanaged reservations
     * UIMA-5086
     */
    private int getMaxShareOrder(IRmJob j) {
        if (j.getDuccType() != DuccType.Reservation || reserve_overage <= 0) {
            return j.getShareOrder();
        }
        long mem = (j.getMemory() + reserve_overage) << 20;              // GB -> KB
        int share_quantum = j.getShareQuantum();                         // share quantum is in KB!
        return (int) ((mem + share_quantum - 1) / share_quantum);        // round UP
    }

    /**
     * Adjust counts for something that takes full machines, like a reservation.
     * If "enforce" is set the machine order must match, otherwise we just do best effort to match.
     *
     * This is intended for use by reservations only; as such it does NOT recurse into child nodepools.
     *
     * We save some trouble for later by remembering which machines we counted - we wouldn't be
     * counting them if we didn't know FOR SURE at this point that we need them.
     * Sort on least eviction cost to get the cheapest set of preemptables.
     *
     * UIMA-5086 Consider assigning larger machines.  Change request so later code uses just the new size.
     * If no machines assigned yet then search machines up to "reserve_overage" larger than the requested size.
     * If more than one needed they must match the size of the first one found.
     *
     * @returns number of machines given
     *          and updates the table of preemptables
     */
    int countFreeableMachines(IRmJob j, int needed)
    {
        String methodName = "countFreeableMachines";

        logger.info(methodName, j.getId(), "Enter nodepool", id, "preemptables.size() =", preemptables.size());
        int share_order = j.getShareOrder();
        int max_share_order;
        int actual_order;

        // If the reservation size has not yet been upgraded, include larger ones in the search
        // Once it has been upgraded and is perhaps waiting for preemptions, don't let it be raised again
        // as it then might find a free machine more than reserve-overage above the original request.
        if (! j.shareOrderUpgraded() ) {
            actual_order = 0;   // Not yet known
            max_share_order = getMaxShareOrder(j);
        } else {
            actual_order = share_order;      // Additional machines must match this
            max_share_order = share_order;   // Restrict search to just the (possibly adjusted) actual order.
        }

        // Get all machines in this range of sizes and sort by increasing eviction cost, with smallest first if empty
        ArrayList<Machine>  machs = new ArrayList<Machine>();
        for (int order = share_order; order <= max_share_order; ++order) {
            if ( machinesByOrder.containsKey(order) ) {
                machs.addAll(machinesByOrder.get(order).values());            // candidates
            }
        }
        if (machs.size() == 0) {
            return 0;
        }
        Collections.sort(machs, new MachineByAscendingEvictionCostSorter());

        StringBuffer sb = new StringBuffer("Machines to search in order:");
        for ( Machine m : machs ) {
            sb.append(" ");
            sb.append(m.getId());
            sb.append(":");
            sb.append(m.getShareOrder());
        }
        logger.info(methodName, j.getId(), sb.toString());

        int given = 0;           // total to give, free or freeable
        Iterator<Machine> iter = machs.iterator();
        ArrayList<Machine> pables = new ArrayList<Machine>();

        while ( iter.hasNext() && (given < needed) ) {
            Machine m = iter.next();
            logger.info(methodName, j.getId(), "Examining", m.getId());
            if ( !isSchedulable(m) ) {
              logger.info(methodName, j.getId(), "Bypass because machine", m.getId(), "is offline or unresponsive or blacklisted");
              continue;
            }

            if (actual_order > 0 && m.getShareOrder() != actual_order) {
                logger.info(methodName, j.getId(), "Bypass because machine", m.getId(), "is not the same size as the first one found");
                continue;
            }

            if ( preemptables.containsKey(m.key()) ) {         // already counted, don't count twice
                logger.info(methodName, j.getId(), "Bypass because machine", m.getId(), "already counted.");
                continue;
            }

            if ( m.isFree() ) {
                logger.info(methodName, j.getId(), "Giving", m.getId(), "because it is free");
            } else if ( m.isFreeable() ) {
                logger.info(methodName, j.getId(), "Giving", m.getId(), "because it is freeable");
                pables.add(m);
            } else {
                logger.info(methodName, j.getId(), "Bypass because machine", m.getId(), "is not freeable");
                continue;
            }

            // Found a free or freeable machine
            given++;
			if (actual_order == 0) {
				actual_order = m.getShareOrder();
				j.upgradeShareOrder(actual_order);
				if (actual_order > share_order) {    // Need to adjust the user & class jobs-by-order tables
					User user = j.getUser();
					user.upgrade(j, share_order, actual_order);
					ResourceClass rc = j.getResourceClass();
					rc.upgrade(j, share_order, actual_order);
					logger.info(methodName, j.getId(), "Increased order of RESERVE job to", actual_order);
				}
			}
        }

        // Remember how many full machines we need to free up when we get to preemption stage.

        for ( Machine m : pables ) {
            logger.info(methodName, j.getId(), "Setting up", m.getId(), "for eviction");
            preemptables.put(m.key(), m);
            nMachinesByOrder[m.getShareOrder()]--;
        }

        calcNSharesByOrder();  // Fill nSharesByOrder from current free space on each node
        return given;
    }

    /**
     * @param nrequested is number of N shares to remove
     * @param order is the order that is affected
     */
    int countOutNSharesByOrder(int order, int nrequested)
    {
        int given = 0;                                         // track count given, for recursion

        int rem = 0;
        int low = order;

        while ( (given < nrequested ) && ( low <= getMaxOrder() ) ) {

            int avail = vMachinesByOrder[low] + nMachinesByOrder[low];
            if ( avail > 0 ) {
                if (vMachinesByOrder[low] > 0 ) {
                    vMachinesByOrder[low]--;
                } else {
                    nMachinesByOrder[low]--;
                }

                given++;
                rem = low - order;
                if ( rem > 0 ) {
                    vMachinesByOrder[rem]++;
                    low = Math.max(rem, order);
                }

            } else {
                low++;
            }
        }

        // oops, I can't do this myself, make a child do it.
        int k = nrequested - given;            // the number of shares we need
        if ( k > 0 ) {
            Iterator<NodePool> iter = children.values().iterator();
            while ( iter.hasNext() && ( k > 0 ) ) {
                NodePool np  = iter.next();
                given       += np.countOutNSharesByOrder(order, k);
                k            = nrequested - given;
            }
        }

        calcNSharesByOrder();

        return given;
    }

    /********************************************************************************************
     *
     * Routines used in the 'what-of' phase.
     *
     * All the counting is done - we have to reset all the counts before starting to call any of these.
     *
     *******************************************************************************************/

    /**
     * We need to make enough space for 'cnt' full machines.
     *
     * Returns number of machines that are freeable, up to 'needed', or 0, if we can't get enough.
     * If we return 0, we must defer the reservation.
     */
    protected int setupPreemptions(int needed, int order)
    {
        String methodName = "setupPreemptions";
        int given = 0;

        Iterator<Machine> iter = preemptables.values().iterator();

        while ( iter.hasNext() && (given < needed) ) {
            Machine m = iter.next();
            int o = m.getShareOrder();
            if ( order != o ) {
                continue;
            }
            logger.info(methodName, null, "Clearing", m.getId(), "from preemptable list for reservations.");
            HashMap<Share, Share> shares = m.getActiveShares();
            for ( Share s : shares.values() ) {
                if ( s.isPreemptable() ) {
                    IRmJob j = s.getJob();
                    j.shrinkByOne(s);
                    order = s.getShareOrder();       // Must adjust the pending array for each share
                    nPendingByOrder[order]++;
                } else {
                    // if the share was evicted or purged we don't care.  otherwise, it SHOULD be evictable so we
                    // log its state to try to figure out why it didn't evict
                    if ( ! (s.isEvicted() || s.isPurged() ) ) {
                        IRmJob j = s.getJob();
                        logger.warn(methodName, j.getId(), "Found non-preemptable share", s.getId(), "fixed:", s.isFixed(),
                                    "j.NShares", j.countNShares(), "j.NSharesGiven", j.countNSharesGiven());
                    }
                }
            }
            given++;
            iter.remove();
        }

        return given;
    }

    /**
     * Here we have to dig around and find either fully free machines, or machines that we
     * can preempt to fully free it.
     */
    void  findMachines(IRmJob job, ResourceClass rc)
    {
        String methodName = "findMachines";

        int order = job.getShareOrder();

        int counted = job.countNSharesGiven();      // allotment from the counter
        int current = job.countNShares();           // currently allocated, plus pending, less those removed by earlier preemption
        int needed = (counted - current);

        logger.info(methodName, job.getId(), "counted", counted, "current", current, "needed", needed, "order", order);
        if ( needed <= 0 ) return;

        int cnt = countFreeMachines(order);
        if ( cnt < needed ) {
            // Get the preemptions started
            logger.info(methodName, job.getId(), "Setup preemptions.  Have", cnt, "free machines, needed", needed);
            setupPreemptions(needed-cnt, order);
        }

        // something awful happened if we throw here.
        if ( ! machinesByOrder.containsKey(order) ) {       // hosed if this happens
            throw new SchedInternalError(job.getId(), "Scheduling counts are wrong - machinesByOrder does not match nMachinesByOrder");
        }

        // Since all are the same size and only empty ones are considered, no need to sort
        //machs = sortedForReservation(machinesByOrder.get(order));

        for ( Machine mm : machinesByOrder.get(order).values() ) {
            if ( isSchedulable(mm) && mm.isFree() ) {
                Share s = new Share(mm, job, mm.getShareOrder());
                s.setFixed();
                connectShare(s, mm, job, mm.getShareOrder());
                if ( --needed == 0 ) break;
            }
        }

    }

    /**
     * All the jobs passed in here are assigned to this nodepool.  In the case of the global nodepool
     * they're assigned implicitly; all others are assigned explicitly.
     *
     * The only tricky bit here is that there may be reservations waiting for us to clear out a full
     * machine, in which case we *might* have to over-preempt.  The doAdditions code is expected to
     * notice this and compensate by adding new allocations on other nodes.  Counting is expected
     * to guarantee that other nodes will exist, but it may take a preemption cycle or to go clear them.
     *
     * For the most part this should be pretty stable though.
     */

    /**
     * Shares come in sorted by largest first.  We iterate looking for a combination of shares that
     * leaves space of size 'order' free on the machine, using the fewest number of shares evacuated.
     */
    ArrayList<Share> evacuateLargest(int order, ArrayList<Share> shares)
    {
        int found_order = 0;

        // special case to avoid running off the end of the list
        if ( shares.size() == 1 ) {                              // terminate recursion at last share
            // Need to recheck if it's preemptable - if the job has had other preemptions then
            // we need to avoid over-preemptiong.
            Share s = shares.get(0);
            if ( s.isPreemptable() && (s.getShareOrder() == order) ) {
                return shares;                                   // with success
            }
            return null;                                         // or with failure
        }

        ArrayList<Share> slist = new ArrayList<Share>();
        for ( Share s : shares ) {
            found_order = s.getShareOrder();
            if ( s.isPreemptable() && (found_order == order) ) {  // exact match, end recursion
                slist.add(s);
                return slist;
            }

            int new_order = order - found_order;                 // now looking for next order after removing size of what we just fond
            @SuppressWarnings("unchecked")
            ArrayList<Share> new_shares = (ArrayList<Share>) shares.clone();        //  ... and after removing the share we just found without destroying
            new_shares.remove(0);                                //      the incoming list

            ArrayList<Share> found_shares =  evacuateLargest(new_order, new_shares);
            if ( s.isPreemptable() && (found_shares != null) ) {                        // ... else we just advance to the next and try the search again
                slist.add(s);                                    // making progress, end recursion
                slist.addAll(found_shares);
                return slist;
            }
        }
        return null;                                            // found nothing, heck
    }

    private void doEvictions(int[] neededByOrder, HashMap<Integer, HashMap<IRmJob, IRmJob>> candidates, boolean force)
    {

        for ( int nbo = getMaxOrder(); nbo > 0; nbo-- ) {

            if ( neededByOrder[nbo] == 0 ) {                                  // these are N-shares
                continue;
            }
            for ( int oo = getMaxOrder(); oo > 0; oo-- ) {
                HashMap<IRmJob, IRmJob> jobs = candidates.get(oo);
                if ( jobs == null ) {
                    continue;
                }

                Iterator<IRmJob> iter = jobs.values().iterator();             // he has something to give.  is it enough?
                while ( iter.hasNext() && (neededByOrder[nbo] > 0) ) {
                    IRmJob j = iter.next();
                    int loss = 0;

                    switch ( evictionPolicy ) {
                        case SHRINK_BY_MACHINE:
                            // minimize fragmentation
                            loss = j.shrinkByOrderByMachine(neededByOrder[nbo], nbo, force, this); // pass in number of N-shares of given order that we want
                                                                                                       // returns number of quantum shares it had to relinquish
                            break;
                        case SHRINK_BY_INVESTMENT:
                            // minimize lost work
                            loss = j.shrinkByInvestment(neededByOrder[nbo], nbo, force, this);    // pass in number of N-shares of given order that we want
                                                                                                       // returns number of quantum shares it had to relinquish
                            break;
                    }

                    neededByOrder[nbo]   -= loss;
                    neededByOrder[0]     -= loss;
                    nPendingByOrder[oo]  += loss;

                    if ( j.countNShares() == 0 ) {                            // nothing left? don't look here any more
                        iter.remove();
                    }
                }

            }
        }
    }

    /**
     * Here we tell the NP how much we need cleared up.  It will look around and try to do that.
     * @deprecated No longer used, the doEvictions code in NodepoolScheduler handles evictions by itself.
     *             Keeping this for a while for reference.  UIMA-4275
     */
    void doEvictionsByMachine(int [] neededByOrder, boolean force)
    {
        String methodName = "doEvictions";
        //
        // Collect losers that are also squatters, by order, and try them first
        //
        String type;
        type = force ? "forced" : "natural";

        logger.debug(methodName, null, getId(),  "NeededByOrder", type, "on entrance eviction", Arrays.toString(neededByOrder));

        for ( NodePool np : getChildrenDescending() ) {
            logger.info(methodName, null, "Recurse to", np.getId(), "from", getId(), "force:", force);
            np.doEvictionsByMachine(neededByOrder, force);
            logger.info(methodName, null, "Recurse from", np.getId(), "proceed with logic for", getId(), "force", force);
        }

        //
        // Adjust neededByOrder to reflect the number of shares that need to be preempted by subtracting the
        // number of shares that already are free
        //
        for ( int nbo = getMaxOrder(); nbo > 0; nbo-- ) {
            // UIMA-4065 - I think that subtracting countPendingSharesByOrder() amounts to double counting because it
            //             will reflect any evictions from the depth-first recursion.  Instead, we would subtract only
            //             our own shares.
            //
            // int needed = Math.max(0, neededByOrder[nbo] - countNSharesByOrder(nbo) - countPendingSharesByOrder(nbo));
            int needed = Math.max(0, neededByOrder[nbo] - countNSharesByOrder(nbo) - nPendingByOrder[nbo]);
            neededByOrder[nbo] = needed;
            neededByOrder[0] += needed;
        }

        logger.debug(methodName, null, getId(),  "NeededByOrder", type, "after adjustments for pending eviction:", Arrays.toString(neededByOrder));

        HashMap<Integer, HashMap<IRmJob, IRmJob>> squatters = new HashMap<Integer, HashMap<IRmJob, IRmJob>>();
        HashMap<Integer, HashMap<IRmJob, IRmJob>> residents = new HashMap<Integer, HashMap<IRmJob, IRmJob>>();

        for ( Share s : allShares.values() ) {
            HashMap<Integer, HashMap<IRmJob, IRmJob>> map = null;
            boolean is_candidate = force ? s.isForceable() : s.isPreemptable();
            if ( is_candidate ) {
                IRmJob j = s.getJob();
                ResourceClass rc = j.getResourceClass();
                if ( rc.getNodepoolName().equals(id) ) {
                    map = residents;
                } else {
                    map = squatters;
                }

                int order = j.getShareOrder();
                HashMap<IRmJob, IRmJob> jmap = null;
                if ( map.containsKey(order) ) {
                    jmap = map.get(order);
                } else {
                    jmap = new HashMap<IRmJob, IRmJob>();
                    map.put(order, jmap);
                }
                jmap.put(j, j);
            }
        }

        doEvictions(neededByOrder, squatters, force);
        logger.debug(methodName, null, getId(), "NeededByOrder", type, "after eviction of squatters:", Arrays.toString(neededByOrder));
        if ( neededByOrder[0] <= 0 )  {
            return;
        }

        doEvictions(neededByOrder, residents, force);
        logger.debug(methodName, null, getId(), "NeededByOrder", type, "after eviction of residents:", Arrays.toString(neededByOrder));
    }


    // For FIXED: find shares ith caps, disallow vertical stacking.  UIMA-4712
    int findSharesHorizontal( IRmJob j )
    {
        return findShares(j, true, false);
    }

    // For FIXED: find shares ith caps, do allow vertical stacking.  UIMA-4712
    int findSharesVertical( IRmJob j )
    {
        return findShares(j, true, true);
    }

    // For FAIR_SHRE: find shares, caller controls caps, allow vertical stacking.  UIMA-4712
    int findShares( IRmJob j, boolean honorCaps )
    {
        return findShares(j, honorCaps, true);
    }

    int findShares( IRmJob j, boolean honorCaps, boolean allowVertical )  // UIMA-4712, allowVertical
    {
        String methodName = "findShares";

        int counted = j.countNSharesGiven();      // allotment from the counter
        int current = j.countNShares();           // currently allocated, plus pending, less those removed by earlier preemption
        int needed = (counted - current);
        int order = j.getShareOrder();
        int given = 0;
        boolean expansionStopped = false;         // UIMA-4275

        logger.debug(methodName, j.getId(), "counted", counted, "current", current, "needed", needed, "order", order, "given", given);

        if ( needed > 0 ) {
            whatof: {
                for ( int i = order; i < getArraySize(); i++ ) {
                    if ( nSharesByOrder[i] == 0 ) {
                        continue;                                            // nothing here to give
                    }

                    Map<Node, Machine> machs = getVirtualMachinesByOrder(i);
                    ArrayList<Machine> ml = new ArrayList<Machine>();
                    ml.addAll(machs.values());

                    for ( Machine m : ml ) {                                // look for space
                        if ( !isSchedulable(m) ) continue;                  // nope
                        if ( (!allowVertical) && (m.hasVerticalConflict(j)) ) continue;  // UIMA-4712
                        int g = Math.min(needed, m.countFreeShares(order)); // adjust by the order supported on the machine
                        for ( int ndx= 0;  ndx < g; ndx++ ) {
                            if ( honorCaps && j.exceedsFairShareCap() ) {                // UIMA-4275
                                // can't take any more shares, probably because of caps
                                expansionStopped = true;
                                break whatof;
                            } else {
                                Share s = new Share(m, j, order);
                                connectShare(s, m, j, order);
                                logger.info(methodName, j.getId(), "Connecting new share", s.toString());
                                //j.assignShare(s);
                                //m.assignShare(s);
                                //rearrangeVirtual(m, order);
                                //allShares.put(s, s);
                            }
                        }

                        given += g;
                        needed -= g;
                        if ( needed == 0 ) {
                            break whatof;
                        }
                    }
                }
            }

            //calcNSharesByOrder();
        }

        if ( (needed > 0) && ( !expansionStopped ) && ( j.getSchedulingPolicy() == Policy.FAIR_SHARE) ) {            // UIMA-4275
            for ( NodePool np : getChildrenAscending() ) {

                StringBuffer sb = new StringBuffer();
                for ( NodePool sp : getChildrenAscending() ) {
                    sb.append(sp.getId());
                    sb.append(" ");
                }
                logger.info(methodName, null, np.getId(), "Doing expansions in this order:", sb.toString());

                int g = np.findShares(j, honorCaps, allowVertical);
                given += g;
                needed -= g;

                if ( needed == 0 ) {
                    break;
                }
            }
        }

        return given;
    }

    /**
     * Bop through the jobs, and if their current counts exceed their current assignment, find
     * something to give them.
     *
     * It's possible that a job had evictions as a result of clearing space for a reservation -
     * so we need to check the count of allocated shares, the count of pending removals, and the
     * current share count assignment.
     */
    HashMap<IRmJob, IRmJob>  doExpansion(List<IRmJob> jobs)
    {
        String methodName = "doExpansion";
        HashMap<IRmJob, IRmJob> expansions = new HashMap<IRmJob, IRmJob>();

        StringBuffer sb = new StringBuffer();
        sb.append("NP: ");
        sb.append(getId());
        sb.append(" Expansions in this order: ");
        for ( IRmJob j : jobs ) {
            if ( j.isCompleted() ) continue;  // deal with races while job is completing

            j.undefer();
            sb.append(j.getId());
            sb.append(":");
            if ( findShares(j, false) > 0 ) {   // always fair-share, so don't do caps yet. UIMA-4712
                sb.append("found ");
                expansions.put(j, j);
            } else {
                sb.append("notfound ");
            }
            if ( j.countNShares() == 0 ) j.setReason("Waiting for preemptions.");
        }
        logger.info(methodName, null, sb.toString());
        return expansions;
    }

    /**
     * This prints garbage unless you call reset() first.
     */
    public String toString()
    {
        StringBuffer sb = new StringBuffer();
        sb.append("--------------------------------------------------------------------------------\n");
        sb.append("Nodepool ");
        sb.append(id);
        sb.append(" depth ");
        sb.append(depth);
        sb.append(": ");
        sb.append("\n");

        //
        // Print the key tables.  First the header ...
        //
        int len = nMachinesByOrder.length;
        StringBuffer sbsb = new StringBuffer("%18s ");
        for ( int i = 0; i < len; i++ ) {
            sbsb.append("%4s ");
        }
        sbsb.append("\n");
        String fmt = sbsb.toString();
        Object[] vals = new Object[len + 2];
        vals[0] = "Order";
        for ( int i = 0; i < len; i++ ) {
            vals[i+1] = Integer.toString(i);
        }

        sb.append(String.format(fmt, vals));

        // Now nMachinesByorder

        sbsb = new StringBuffer("%18s ");
        for ( int i = 0; i < len; i++ ) {
            sbsb.append("%4d ");
        }
        sbsb.append("\n");
        fmt = sbsb.toString();

        vals[0] = "nMachinesByOrder";
        int[] counts = countMachinesByOrder();
        for ( int i = 0; i < len; i++ ) {
            vals[i+1] = counts[i];
        }
        sb.append(String.format(fmt, vals));

        vals[0] = "vMachinesByOrder";
        counts = countVMachinesByOrder();
        for ( int i = 0; i < len; i++ ) {
            vals[i+1] = counts[i];
        }
        sb.append(String.format(fmt, vals));

        // Now nSharesByorder

        vals[0] = "nSharesByOrder";
        counts = countAllNSharesByOrder();
        for ( int i = 0; i < len; i++ ) {
            vals[i+1] = counts[i];
        }
        sb.append(String.format(fmt, vals));

        // Now nFreeByorder

//        vals[0] = "nFreeSharesByOrder";
//        counts = countFreeSharesByOrder();
//        for ( int i = 0; i < len; i++ ) {
//            vals[i+1] = counts[i];
//        }
//        sb.append(String.format(fmt, vals));

        sb.append("--------------------------------------------------------------------------------\n");

        for ( NodePool np: children.values () ) {
            sb.append(np.toString());
        }

        return sb.toString();
    }

    public void queryMachines()
    {
        String methodName = "queryMachines";
        ArrayList<Machine> machines = new ArrayList<Machine>();
        machines.addAll(getAllMachines().values());
        logger.info(methodName, null, "================================== Query Machines Nodepool:", id, "=========================");
        StringBuffer buf = new StringBuffer();
        buf.append(Machine.getHeader());
        buf.append("\n");
        buf.append(Machine.getDashes());
        buf.append("\n");
        Collections.sort(machines, new MachineByOrderSorter());

        for ( Machine m : machines) {
            buf.append(m.toString());
            int remaining = m.countFreeShares();
            if ( remaining > 0 ) {
                buf.append("[" + m.countFreeShares() + "]");
            }
            buf.append("\n");
        }
        logger.info(methodName, null, "\n", buf.toString());
        logger.info(methodName, null, "================================== End Query Machines Nodepool:", id, "======================");
    }

    //
    // Order shares by INCREASING preemption cost (all free followed by those with least eviction cost)
    // Order the free ones and those with the same cost by ascending size (UIMA-5086)
    // Don't need to check for unschedulable or un-freeable as they will be ignored later.
    //
    class MachineByAscendingEvictionCostSorter implements Comparator<Machine> {
        public int compare(Machine m1, Machine m2) {
            if (m1.equals(m2))
                return 0;

            if (m1.isFree()) {
                if (m2.isFree())
                    return m1.getShareOrder() - m2.getShareOrder();     // Smallest first
                else
                    return -1; // m2 not free, m1 to the front of the list
            } else if (m2.isFree())
                return 1;      // m1 not free, m2 to the front of the list

            // Sort the lowest eviction cost first
            // Either least shares in use or lowest investment
            int diff;
            switch (evictionPolicy) {
                case SHRINK_BY_MACHINE :
                    diff = (m1.getShareOrder()-m1.countFreeShares()) - (m2.getShareOrder()-m2.countFreeShares());
                case SHRINK_BY_INVESTMENT :
                    diff = m1.getInvestment() - m2.getInvestment();
                default:
                    diff = 0;
            }
            // If eviction costs the same, sort smallest machine first
            if (diff == 0) {
                diff = m1.getShareOrder() - m2.getShareOrder();
            }
            return diff;
        }
    }

    static private class NodepoolAscendingSorter
        implements Comparator<NodePool>
    {
        public int compare(NodePool n1, NodePool n2)
        {
            return (n1.getSearchOrder() - n2.getSearchOrder());
        }
    }

    static private class NodepoolDescendingSorter
        implements Comparator<NodePool>
    {
        public int compare(NodePool n1, NodePool n2)
        {
            return (n2.getSearchOrder() - n1.getSearchOrder());
        }
    }

    class InvestmentSorter
        implements Comparator<Share>
    {
        public int compare(Share s1, Share s2)
        {
            return (int) (s1.getInvestment() - s2.getInvestment());           // lowest investment
                                                                      // if we're not tracking investment we
                                                                      // don't care anyway so this works fine
        }
    }

    class DescendingShareOrderSorter
        implements Comparator<Share>
    {
        public int compare(Share s1, Share s2)
        {
            return (int) (s2.getShareOrder() - s1.getShareOrder());
        }
    }


    class MachineByOrderSorter
        implements Comparator<Machine>
    {
        public int compare(Machine m1, Machine m2)
        {
            return m2.getShareOrder() - m1.getShareOrder();
        }
    }

    class MachineByAscendingOrderSorter
        implements Comparator<Machine>
    {
        public int compare(Machine m1, Machine m2)
        {
            return m1.getShareOrder() - m2.getShareOrder();
        }
    }

    class GlobalOrder
    {
        int maxorder = 0;

        GlobalOrder()
        {
            this.maxorder = 0;
        }

        synchronized void reset()
        {
            this.maxorder = 0;
        }

        synchronized void update(int order)
        {
            this.maxorder = Math.max(maxorder, order);
        }

        synchronized int getOrder()
        {
            return maxorder;
        }
    }
}
