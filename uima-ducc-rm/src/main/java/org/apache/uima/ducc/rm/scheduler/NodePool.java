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
import org.apache.uima.ducc.common.utils.DuccLogger;
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

    Map<ResourceClass, ResourceClass>       allClasses       = new HashMap<ResourceClass, ResourceClass>();    // all the classes directly serviced by me
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
    static int maxorder = 0;

//     NodePool(NodePool parent, String id, EvictionPolicy ep, int order)
//     {
//         this.parent = parent;
//         this.id = id;
//         this.evictionPolicy = ep;
//         this.depth = 0;
//         this.order = order;
//     }

    NodePool(NodePool parent, String id, Map<String, String> nodes, EvictionPolicy ep, int depth, int search_order)
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
    }

    void addResourceClass(ResourceClass cl)
    {  // UIMA-4065
        allClasses.put(cl, cl);
    }

    NodePool getParent()
    {
        return this.parent;
    }

    String getId()
    {
        return id;
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
     * How many do I have, including recusring down the children?
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
            if ( m.isFree() ) {
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

    public static int getMaxOrder()
    {
        return maxorder;              // must always be the same for parent and all children
    }

    public static int[] makeArray()        // common static code because getting this right everywhere is painful
    {
        return new int[getArraySize()];
    }

    public static int getArraySize()
    {
        return getMaxOrder() + 1;          // a bit bigger, because we're 1-indexed for easier counting
                                           // same for parent and children
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
        for ( int i = 0; i < maxorder + 1; i++ ) {
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
        rearrangeVirtual(m, order);
        allShares.put(s, s);        
    }

    void rearrangeVirtual(Machine m, int order)
         
    {
    	String methodName = "rearrangeVirtual";
        if ( allMachines.containsKey(m.key()) ) {
            int v_order = m.getVirtualShareOrder();
            int r_order = m.getShareOrder();

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
                np.rearrangeVirtual(m, order);
            }
        }
    }


    void accountForShares(HashMap<Share, Share> shares)
    {
        if ( shares == null ) return;

        for ( Share s : shares.values() ) {
            int order = s.getShareOrder();
            Machine m = s.getMachine();
            rearrangeVirtual(m, order);
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
        maxorder = Math.max(order, maxorder);
        nSharesByOrder     = new int[maxorder + 1];
        nMachinesByOrder   = new int[maxorder + 1];
        vMachinesByOrder   = new int[maxorder + 1];
        //nFreeSharesByOrder = new int[maxorder + 1];
        //neededByOrder      = new int[maxorder + 1];

        nPendingByOrder = new int[maxorder + 1];

        // UIMA-4142 Must set vMachinesByOrder and virtualMachinesByOrder independently of
        //           machinesByOrder because blacklisting can cause v_order != r_order
        //           during reset.
        virtualMachinesByOrder.clear();
        for ( Machine m : allMachines.values() ) {
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
        logger.info(methodName, null, "Resetting preemptables in nodepool", id);

        // UIMA-4064 Need to do this recrsively
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
        NodePool np = new NodePool(this, className, names, evictionPolicy, depth + 1, order);
        children.put(className, np);
        return np;
    }

    private synchronized void incrementOnlineByOrder(int order)
    {
        if ( ! onlineMachinesByOrder.containsKey(order) ) {
            onlineMachinesByOrder.put(order, 1);
        } else {
            onlineMachinesByOrder.put(order, onlineMachinesByOrder.get(order) + 1);
        }
    }

    private synchronized void decrementOnlineByOrder(int order)
    {
        onlineMachinesByOrder.put(order, onlineMachinesByOrder.get(order) - 1);
    }

    synchronized void getLocalOnlineByOrder(int[] ret)         // for queries, just me
    {
        for ( int o: onlineMachinesByOrder.keySet() ) {
            ret[o] += onlineMachinesByOrder.get(o);
        }
    }

    synchronized void getOnlineByOrder(int[] ret)         // for queries
    {
        for ( int o: onlineMachinesByOrder.keySet() ) {
            ret[o] += onlineMachinesByOrder.get(o);
        }
        for ( NodePool child : children.values() ) {
            child.getOnlineByOrder(ret);
        }
    }

    /**
     * Handle a new node update.
     */
    Machine nodeArrives(Node node, int order)
    {
        String methodName = "nodeArrives";

        maxorder = Math.max(order, maxorder);
                
        
        for ( NodePool np : children.values() ) {
            if ( np.containsPoolNode(node) ) {
                Machine m = np.nodeArrives(node, order);
                return m;
            }
        }

        if ( allMachines.containsKey(node) ) {                   // already known, do nothing
            Machine m = allMachines.get(node);
            logger.trace(methodName, null, "Node ", m.getId(), " is already known, not adding.");
            return m;
        }

        if ( offlineMachines.containsKey(node) ) {               // if it's offline it can't be restored like this.
            Machine m = offlineMachines.get(node);
            logger.trace(methodName, null, "Node ", m.getId(), " is offline, not activating.");
            return m;
        }

        if ( unresponsiveMachines.containsKey(node) ) {          // reactive the node
            Machine m = unresponsiveMachines.remove(node);

            if ( m.getShareOrder() != order ) {                  // can change. e.g. if it was taken offline for
                m.setShareOrder(order);                          //    hardware changes.
            }

            // TODO soon ... can I just combine this with the code directly below:
            allMachines.put(node, m);
            machinesByName.put(m.getId(), m);
            machinesByIp.put(m.getIp(), m);
            HashMap<Node, Machine> mlist = machinesByOrder.get(order);
            incrementOnlineByOrder(order);
            if ( mlist == null ) {
                mlist = new HashMap<Node, Machine>();
                machinesByOrder.put(order, mlist);
            }
            mlist.put(m.key(), m);     
   
            total_shares += order;     //      UIMA-3939

            logger.info(methodName, null, "Nodepool:", id, "Host reactivated ", m.getId(), String.format("shares %2d total %4d:", order, total_shares), m.toString());
            return m;
        }

        Machine machine = new Machine(node);                     // brand new machine, make it active
        machine.setShareOrder(order);
        allMachines.put(machine.key(), machine);                 // global list
        machinesByName.put(machine.getId(), machine);
        machinesByIp.put(machine.getIp(), machine);
        incrementOnlineByOrder(order);
        machine.setNodepool(this);

        total_shares += order;     

        // index it by its share order to make it easier to find        
        HashMap<Node, Machine> mlist = machinesByOrder.get(order);
        if ( mlist == null ) {
            mlist = new HashMap<Node, Machine>();
            machinesByOrder.put(order, mlist);
        }
        mlist.put(machine.key(), machine);        

        logger.info(methodName, null, "Nodepool:", id, "Host added:", id, ": ", machine.getId(), "Nodefile:", subpoolNames.get(machine.getId()), // UIMA-4142, add file nodefile
                    String.format("shares %2d total %4d:", order, total_shares), machine.toString()); 
        updated++;
        
        return machine;
    }

    void disable(Machine m, HashMap<Node, Machine> disableMap)
    {
        String methodName = "nodeLeaves";

        if ( allMachines.containsKey(m.key()) ) {
            logger.info(methodName, null, "Nodepool:", id, "Host disabled:", m.getId(), "Looking for shares to clear");

            int order = m.getShareOrder();
            String name = m.getId();
            String ip   = m .getIp();

            HashMap<Share, Share> shares = m.getActiveShares();
            for (Share s : shares.values()) {
                IRmJob j = s.getJob();

                if ( j.getDuccType() == DuccType.Reservation ) {
                    // UIMA-3614.  Only actual reservation is left intact
                    logger.info(methodName, null, "Nodepool:", id, "Host dead/offline:", m.getId(), "Not purging", j.getDuccType());
                    break;
                }

                switch ( j.getDuccType() ) {
                    case Reservation:
                    // UIMA-3614.  Only actual reservation is left intact
                    logger.info(methodName, null, "Nodepool:", id, "Host dead/offline:", m.getId(), "Not purging", j.getDuccType());
                    break;

                    case Service:                        
                    case Pop:
                        j.markComplete();      // UIMA-4327 Must avoid reallocation, these guys are toast if they get purged.
                        logger.info(methodName, null, "Nodepool:", id, "Host dead/offline:", m.getId(), "Mark service/pop completed.");
                        // NO BREAK, must fall through
                    case Job:
                    default:
                        break;
                }

                logger.info(methodName, j.getId(), "Nodepool:", id, "Purge", j.getDuccType(), "on dead/offline:", m.getId());
                j.shrinkByOne(s);
                nPendingByOrder[order]++;

                s.purge();          // This bet tells OR not to wait for confirmation from the agent
            }

            allMachines.remove(m.key());
            decrementOnlineByOrder(order);
            total_shares -= order; 
            disableMap.put(m.key(), m);

            HashMap<Node, Machine> machs = machinesByOrder.get(order);
            machs.remove(m.key());
            if ( machs.size() == 0 ) {
                machinesByOrder.remove(order);
            }
            machinesByName.remove(name);
            machinesByIp.remove(ip);
            logger.info(methodName, null, "Nodepool:", id, "Node leaves:", m.getId(), "total shares:", total_shares);
        } else {
            for ( NodePool np : children.values() ) {
                np.nodeLeaves(m);
            }
        }
    }

    void nodeLeaves(Machine m)
    {
        disable(m, unresponsiveMachines);
    }

    // UIMA-4142
    // helper for CLI things that refer to things by name only.  do we know about anything by this
    // name?  see resolve() in Scheduler.java.
    boolean hasNode(String n)
    {
        if ( machinesByName.containsKey(n) ) return true;

        // If not we have to search the offline machines and the unresponsive machines which are
        // keyed differently.  This is really ugly but hard to fix at this point, so cope.
        for ( Node node : offlineMachines.keySet() ) {
            if ( node.getNodeIdentity().getName().equals(n) ) return true;
        }
        for ( Node node : unresponsiveMachines.keySet() ) {
            if ( node.getNodeIdentity().getName().equals(n) ) return true;
        }
        return false;
    }

    String varyoff(String node)
    {
        Machine m = machinesByName.get(node);
        if ( m == null ) {
            // ok, maybe it's already offline or maybe dead
            // relatively rare, cleaner to search than to make yet another index

            for ( Machine mm : offlineMachines.values() ) {
                if ( mm.getId().equals(node) ) {
                    return "VaryOff: Nodepool " + id + " - Already offline: " + node;
                }
            }

            Iterator<Machine> iter = unresponsiveMachines.values().iterator();
            while ( iter.hasNext() ) {
                Machine mm = iter.next();                
                if ( mm.getId().equals(node) ) {
                    Node key = mm.key();
                    iter.remove();
                    offlineMachines.put(key, mm);
                    return "VaryOff: Nodepool " + id + " - Unresponsive machine, marked offline: " + node;
                }
            }

            return "VaryOff: Nodepool " + id + " - Cannot find machine: " + node;
        }

        disable(m, offlineMachines);
        return "VaryOff: " + node + " - OK.";
    }

    /**
     * We're going to just take it off the offline list and if it happens to come back, fine, it will get picked up
     * in nodeArrives as a new machine.
     */
    String varyon(String node)
    {        
        if ( machinesByName.containsKey(node) ) {
            return "VaryOn: Nodepool " + id + " - Already online: " + node;
        }

        Iterator<Machine> iter = offlineMachines.values().iterator();
        while ( iter.hasNext() ) {
            Machine mm = iter.next();
            if ( mm.getId().equals(node) ) {
                iter.remove();
                return "VaryOn: Nodepool " + id + " - Machine marked online: " + node;
            }
        }

        iter = unresponsiveMachines.values().iterator();
        while ( iter.hasNext() ) {
            Machine mm = iter.next();
            if ( mm.getId().equals(node) ) {
                return "VaryOn: Nodepool " + id + " - Machine is online but not responsive: " + node;
            }
        }
        
        return "VaryOn: Nodepool " + id + " - Cannot find machine: " + node;
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
     */
    int countReservables(IRmJob j)
    {
        int order = j.getShareOrder();
        if ( ! machinesByOrder.containsKey(order) ) return 0;
        return machinesByOrder.get(order).size();
    }

    /**
     * Count total physical machines that could accomodate a 'fixed' request that the job
     * will fit in.
     */
    int countFixable(IRmJob j)
    {
        int order = j.getShareOrder();
        int ret = 0;
        for ( int i = order; i < maxorder; i++ ) {
            if ( machinesByOrder.containsKey(order) ) {
                ret += machinesByOrder.get(order).size();
            }
        }
        return ret;
    }

    
    /**
     * Adjust counts for something that takes full machines, like a reservation.
     * If "enforce" is set the machine order must match, otherwise we just do best effort to match.
     *
     * This is intended for use by reservations only; as such it does NOT recurse into child nodepools.
     *
     * We save some trouble for later by remembering which machines we counted - we wouldn't be 
     * counting them if we didn't know FOR SURE at this point that we need them.
     *
     * @returns number of machines given
     */
    int countFreeableMachines(IRmJob j, int needed)
    {
        String methodName = "countFreeableMachines";

        logger.info(methodName, j.getId(), "Enter nodepool", id, "preemptables.size() =", preemptables.size());
        int order = j.getShareOrder();

        ArrayList<Machine>  machs = new ArrayList<Machine>();
        if ( machinesByOrder.containsKey(order) ) {
            machs.addAll(machinesByOrder.get(order).values());            // candidates
        } else {
            return 0;                                                     // no candidates
        }

        StringBuffer sb = new StringBuffer("Machines to search:");
        for ( Machine m : machs ) {
            sb.append(" ");
            sb.append(m.getId());
        }
        logger.info(methodName, j.getId(), sb.toString());

        Collections.sort(machs, new MachineByAscendingOrderSorter());

        int given = 0;           // total to give, free or freeable
        Iterator<Machine> iter = machs.iterator();
        ArrayList<Machine> pables = new ArrayList<Machine>();
        
        while ( iter.hasNext() && (given < needed) ) {
            Machine m = iter.next();
            logger.info(methodName, j.getId(), "Examining", m.getId());
            if ( preemptables.containsKey(m.key()) ) {         // already counted, don't count twice
                logger.info(methodName, j.getId(), "Bypass because machine", m.getId(), "already counted.");
                continue;
            }

            if ( m.isFree() ) {
                logger.info(methodName, j.getId(), "Giving", m.getId(), "because it is free");
                given++;
                continue;
            }

            if ( m.isFreeable() ) {
                logger.info(methodName, j.getId(), "Giving", m.getId(), "because it is freeable");
                given++;
                pables.add(m);
            } else {
                logger.info(methodName, j.getId(), "Bypass because machine", m.getId(), "is not freeable");
            }
        }

        // Remember how many full machines we need to free up when we get to preemption stage.

        for ( Machine m : pables ) {
            logger.info(methodName, j.getId(), "Setting up", m.getId(), "to clear for reservation");
            preemptables.put(m.key(), m);
            nMachinesByOrder[m.getShareOrder()]--;
        }

        calcNSharesByOrder();
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

        while ( (given < nrequested ) && ( low <= maxorder ) ) {

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

    protected ArrayList<Machine> sortedForReservation(HashMap<Node, Machine> machs)
    {
        ArrayList<Machine> answer = new ArrayList<Machine>();
        answer.addAll(machs.values());

        Collections.sort(answer, new ReservationSorter());
        return answer;
    }

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
        ArrayList<Machine> machs;

        int order = job.getShareOrder();

        int counted = job.countNSharesGiven();      // allotment from the counter
        int current = job.countNShares();           // currently allocated, plus pending, less those removed by earlier preemption
        int needed = (counted - current);

        logger.info(methodName, job.getId(), "counted", counted, "current", current, "needed", needed, "order", order);
        if ( needed <= 0 ) return;

        //
        // Build up 'machs' array, containing all candidate machines, sorted by 
        //    a) primarily, least investment, if SHRINK_BY_INVESTMENT is active
        //    b) secondarily, least number of assigned shares
        //
        // Free machines always sort to the front of the list of course.
        //

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
        machs = sortedForReservation(machinesByOrder.get(order));

        // Machs is all candidate machines, ordered by empty, then most preferable, according to the eviction policy.
        for ( Machine mm : machs ) {
            if ( mm.isFree() ) {
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

        for ( int nbo = maxorder; nbo > 0; nbo-- ) {

            if ( neededByOrder[nbo] == 0 ) {                                  // these are N-shares
                continue;
            }
            for ( int oo = maxorder; oo > 0; oo-- ) {
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
        for ( int nbo = maxorder; nbo > 0; nbo-- ) {
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


    int findShares( IRmJob j )
    {
        return findShares(j, true);
    }

    int findShares( IRmJob j, boolean honorCaps ) 
    {
        String methodName = "findShares";

        int counted = j.countNSharesGiven();      // allotment from the counter
        int current = j.countNShares();           // currently allocated, plus pending, less those removed by earlier preemption
        int needed = (counted - current);
        int order = j.getShareOrder();
        int given = 0;        
        boolean expansionStopped = false;         // UIMA-4275

        logger.info(methodName, j.getId(), "counted", counted, "current", current, "needed", needed, "order", order, "given", given);

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
                        if ( m.isBlacklisted() ) continue;                  // nope
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

        if ( (needed > 0) && ( !expansionStopped ) ) {            // UIMA-4275
            for ( NodePool np : getChildrenAscending() ) {

                StringBuffer sb = new StringBuffer();
                for ( NodePool sp : getChildrenAscending() ) {
                    sb.append(sp.getId());
                    sb.append(" ");
                }
                logger.info(methodName, null, np.getId(), "Doing expansions in this order:", sb.toString());

                int g = np.findShares(j);
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
            if ( findShares(j) > 0 ) {
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
    // Order shares by INCRESING investment
    //
    class ReservationSorter
    	implements Comparator<Machine>
    {	
    	public int compare(Machine m1, Machine m2)
        {
            if ( m1.equals(m2) )   return 0;

            if ( m1.isFree() ) {             // to the front of the list, ordered by smallest memory
                if ( m2.isFree() ) return (m1.getShareOrder() - m2.getShareOrder());
                return -1;                   // m2 not free, m1 to the front of the list
            }

            switch ( evictionPolicy ) {
                case SHRINK_BY_MACHINE:
                    return m2.countFreeShares() - m1.countFreeShares();       // most free shares first ==> smallest eviction

                case SHRINK_BY_INVESTMENT: 
                    return m1.getInvestment() - m2.getInvestment();           // lowest investment
            }

            return 0;                                                         // cannot get here
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

}
