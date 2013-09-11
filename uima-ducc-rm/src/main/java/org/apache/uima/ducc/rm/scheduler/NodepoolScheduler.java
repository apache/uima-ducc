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
import java.util.TreeMap;

import org.apache.uima.ducc.common.Node;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.SystemPropertyResolver;


/**
 * This implementation of IScheduler is the initial implementation of a scheduler using classes.
 */
public class NodepoolScheduler
    implements IScheduler,
               SchedConstants
{
    DuccLogger logger = DuccLogger.getLogger(NodepoolScheduler.class, COMPONENT_NAME);

    Map<ResourceClass, ResourceClass> resourceClasses;
    Map<IRmJob, IRmJob> needyJobs = new TreeMap<IRmJob, IRmJob>(new JobByTimeSorter());
    NodePool globalNodepool;

    Object[] classes;

    SchedulingUpdate schedulingUpdate;

    EvictionPolicy evictionPolicy = EvictionPolicy.SHRINK_BY_MACHINE;

    int fragmentationThreshold = 2;
    boolean do_defragmentation = true;

    NodepoolScheduler()   
    {
        fragmentationThreshold = SystemPropertyResolver.getIntProperty("ducc.rm.fragmentation.threshold", 
                                                                       fragmentationThreshold);
        do_defragmentation = SystemPropertyResolver.getBooleanProperty("ducc.rm.defragmentation", do_defragmentation);
    }

    public void setClasses(Map<ResourceClass, ResourceClass> prclasses)
    {
        this.resourceClasses = prclasses;

        // organize all the priority classes into lists by priority because we process all classes of the
        // same priority in a group
        //
        // This version of the scheduler requires that classes at the same priority MUST be of the same type.
        // i.e. we don't support mixed-share at this point.
        HashMap<Integer, ArrayList<ResourceClass>> sorter = new HashMap<Integer, ArrayList<ResourceClass>>();
        for ( ResourceClass pcl : prclasses.values() ) {
 
            ArrayList<ResourceClass> cl = sorter.get(pcl.getPriority());
            if ( cl == null ) {
                cl = new ArrayList<ResourceClass>();
                sorter.put(pcl.getPriority(), cl);
            }

            // make sure policies match
            if (cl.size() > 0 ) {
                ResourceClass prev = cl.get(0);
                if ( prev.getPolicy() != pcl.getPolicy() ) {
                    throw new SchedulingException(null, "Scheduling policy must match for same-priority classes: class " +
                                                  prev.getName() + " : " + pcl.getName());
                }
            }

            cl.add(pcl);
        }

        // now sort the keys
        ArrayList<Integer> keys = new ArrayList<Integer>();
        keys.addAll(sorter.keySet());
        Collections.sort(keys);  // "best" (lowest) priority first

        // and finally create the "classes" array, ordered by decreasing priority, each element is the
        // collection of priority classes at the same priority.
        classes = new Object[sorter.size()];
        int ndx = 0;
        for ( Integer k : keys ) {
            classes[ndx++] = sorter.get(k);
        }

    }

    public void setNodePool(NodePool np)
    {
        this.globalNodepool = np;
    }

    public void setEvictionPolicy(EvictionPolicy ep)
    {
        this.evictionPolicy = ep;
    }

    /**
     * Convert the absoute / percent cap into an absolute cap over some basis.
     * This is a helper for computing the share or machine caps.
     */
    private int calcCaps(int absolute, double percent, int basis)
    {
        int perccap = Integer.MAX_VALUE;    // the cap, calculated from percent
        if ( percent < 1.0 ) {
            double b = basis;
            b = b * percent;
            perccap = (int) Math.round(b);
        } else {
        	perccap = basis;
        }

        return Math.min(absolute, perccap);
    }

    private void reworknShares(int[] vshares, int[] nshares)
    {
        // now redo nshares
        int len = vshares.length;
        System.arraycopy(vshares, 0, nshares, 0, len);
        for ( int o = 1; o < len; o++ ) {                     // counting by share order
            for ( int p = o+1; p < len; p++ ) {
                if ( nshares[p] != 0 ) {
                    nshares[o] += (p / o) * nshares[p];
                }
            }
        }
        //System.out.println("vshares : " + fmtArray(vmachines));
        //System.out.println("nshares : " + fmtArray(nshares));
    }

    /**
     * @param nshares is a table showing the number of virtual shares for each order.
     * @param count is number of N shares to remove
     * @param order is the order that is affected
     */
    void doShareSplits(int[] vmach, long count, int order)
    {

        rsbo : {
            for ( int o = order; o < vmach.length; o++ ) {
                while ( vmach[o] > 0 ) {
                    
                    int given    = o / order;
                    int residual = o % order;
                    
                    if ( count >= given ) {       // we give it all away
                        count -= given;
                        if ( residual > 0 ) {
                            vmach[residual]++;   // and maybe a leftover
                        }                                        
                    } else {                     // can't give it all away
                        int leftover = o - ((int)count * order); 
                        vmach[leftover]++;
                            count = 0;	
                    }
                    
                    vmach[o] --;
                    
                    if ( count == 0 ) {
                        break rsbo;
                    }
                }
            }
        }

    }


    /**
     * @param nshares is a table showing the number of virtual shares for each order.
     * @param count is number of N shares to remove
     * @param order is the order that is affected
     */
    void removeSharesByOrder(int[] vmach, int[] nshares, long count, int order)
    {
        if ( count == 0 ) return;                 // shortcut so we don't have to keep checking in caller

        //
        // First do the ones that may be able to fill with at most one machine split.
        //
        for ( int f = 1; (f * order) < vmach.length; f++ ) {
            int fo = f * order;
            if ( vmach[fo] > 0 ) {
                long available   = vmach[fo] * f;
                long given       = Math.min(count, available);
                int remaining   = (int) (available - given);
                vmach[fo]       = (remaining * order) / fo;
                int residual    = (remaining * order) % fo;
                if ( residual > 0 ) {
                    vmach[residual] ++;
                }
                count -= given;
            }
            if ( count == 0 ) {
            	break;
            }
        }

        //
        // Now we must do splits if we still need some.
        //
        if ( count > 0 ) {
            doShareSplits(vmach, count, order);
        }

        reworknShares(vmach, nshares);
    }

    /**
     * Create a string showing virtual and quantum shares, given virtual shares and the order.
     * For use in debugging messages.
     */
    String nSharesToString(int shares, int order)
    {
        return String.format("%dQ%d", shares, (shares * order));
    }

    /**
     * Create a string showing virtual and quantum shares, given quantum shares and the order.
     * For use in debugging messages.
     */
    String qSharesToString(int shares, int order)
    {
    	String o = null;
    	if ( order == 0 ) {
    		o = "-";
    	} else {
    		o = (Integer.toString(shares/order));
    	}

    	return String.format("%sQ%d", o, shares);
    }

    /**
     * Return the nodepool for a class, or the global nodepool if none is explicitly associated with the class.
     */
    NodePool getNodepool(ResourceClass rc)
    {
        String id = rc.getNodepoolName();
        if ( id == null ) {
            return globalNodepool;
        }
        return globalNodepool.getSubpool(id);
    }

    // ==========================================================================================
    // ==========================================================================================
    // =========================== REWORKED CODE FOR FAIR SHARE ==================================
    // ==========================================================================================
    // ==========================================================================================

    /**
     * @deprecated - see rc.geMaxOrder();
     */
    int calculateMaxJobOrder(ArrayList<ResourceClass> rcs)
    {
        int max = 0;
        for ( ResourceClass rc: rcs ) {
            HashMap<Integer, HashMap<IRmJob, IRmJob>> jobs = rc.getAllJobsByOrder();
            for ( int i : jobs.keySet() ) {
                max= Math.max(max, i);
            }
        }
        return max;
    }

    private String fmtArray(int[] array)
    {
        Object[] vals = new Object[array.length];
        StringBuffer sb = new StringBuffer();
        
        for ( int i = 0; i < array.length; i++ ) {
            sb.append("%3s ");
            vals[i] = Integer.toString(array[i]);
        }
        return String.format(sb.toString(), vals);
    }

    protected void apportion_qshares(List<IEntity> entities, int[] vshares, int total_shares, String descr)
    {
        String methodName = "apportion_qshares";
        boolean shares_given = false;
        int maxorder = NodePool.getMaxOrder();
        int[] nshares = NodePool.makeArray();           // nshares


        if ( entities.size() == 0 ) return;
        Collections.sort(entities, entities.get(0).getApportionmentSorter());

        reworknShares(vshares, nshares);

        ArrayList<IEntity> working = new ArrayList<IEntity>();
        working.addAll(entities);

        HashMap<IEntity, int[]> given_by_order = new HashMap<IEntity, int[]>();
        HashMap<IEntity, Integer>   deserved   = new HashMap<IEntity, Integer>();      // qshares

        
        // This section dealing with RmCounter writes the counting parameters to the log in a form
        // that can be cut/pasted into a java properties file.  This file can then be used in the
        // RmCounter test/deveopment application.  It also turns out to be very useful in the log
        // in general so it is promoted to 'info' level.
        StringBuffer   enames = null;
        StringBuffer eweights = null;
        logger.info(methodName, null, descr, "RmCounter Start");
        logger.info(methodName, null, descr, "maxorder = ", NodePool.getMaxOrder());
        enames = new StringBuffer();            
        eweights = new StringBuffer();  

        for ( IEntity e : working ) {              
            int[] gbo = NodePool.makeArray();
            e.setGivenByOrder(gbo);

            given_by_order.put(e, gbo);
            deserved.put(e, 0);

            // jrc e.initWantedByOrder();

            enames.append(e.getName());
            enames.append(" ");
            eweights.append(Integer.toString(e.getShareWeight()));
            eweights.append(" ");      
        }

        logger.info(methodName, null, descr, "entity_names = ", enames.toString());
        logger.info(methodName, null, descr, "weights      = ", eweights.toString());
        for ( IEntity e : working ) {
            logger.info(methodName, null, descr, "wantedby." + e.getName() + " = ", fmtArray(e.getWantedByOrder()));
        }
        logger.info(methodName, null, descr, "vmachines =", fmtArray(vshares));
        logger.info(methodName, null, descr, "RmCounter End");


        int pass = 0;
        do {
            // Starting at highest order, give full fair share to any entity that wants it, minus the
            // shares already given.  Remove the newly given shares and trickle down the fragments.

            logger.trace(methodName, null, descr, "----------------------- Pass", (pass++), "------------------------------");
            logger.trace(methodName, null, descr, "vshares", fmtArray(vshares));
            logger.trace(methodName, null, descr, "nshares", fmtArray(nshares));
            shares_given = false;
            HashMap<IEntity, Integer> given_per_round = new HashMap<IEntity, Integer>();        // qshares
            int allweights = 0;
            for ( IEntity e : working ) {
                allweights += e.getShareWeight();
                given_per_round.put(e, 0);
            }

            //
            // work out deserved for everybody based on what's still around.
            //
            int all_qshares = nshares[1];
            for ( IEntity e : working ) {
                int base_fs = (int) Math.floor(nshares[1] * ( (double) e.getShareWeight() / allweights ));
                double d_base_fs = nshares[1] * ( (double) e.getShareWeight() / allweights );
                deserved.put(e, base_fs);
                all_qshares -= base_fs;

                logger.trace(methodName, null, descr, e.getName(), "Wanted  :", fmtArray(e.getWantedByOrder()));
                logger.trace(methodName, null, descr, e.getName(), "deserved:", base_fs, d_base_fs);
            }

            logger.trace(methodName, null, descr,  "Leftover after giving deserved:" + all_qshares);
            if ( all_qshares > 0 ) {
                for ( IEntity e: working ) {
                    deserved.put(e, deserved.get(e) + 1);
                    all_qshares--;
                    if ( all_qshares == 0 ) break;
                }
            }
            for ( IEntity e : working ) {
                logger.trace(methodName, null, descr, String.format("Final deserved by %15s: int[%3d] (after bonus)", e.getName(), deserved.get(e)));
            }

            for ( int o = maxorder; o > 0; o--) {  
                int total_taken = 0;                                 // nshares
                if ( nshares[o] == 0 ) {
                    logger.trace(methodName, null, descr, "O " + o + " no shares to give, moving on.");
                    continue;
                }
                for ( IEntity e : working ) {
                    int[] wbo = e.getWantedByOrder();                 // processes - NShares
                    int[] gbo = given_by_order.get(e);                //             NShares

                    if ( wbo[o] == 0 ) {
                        logger.trace(methodName, null, descr, "O", o, "Entity", e.getName(), "nothing wanted at this order, moving on.");
                        continue;
                    }

                    double dgiven = nshares[o] * ((double) e.getShareWeight() / allweights) * o;     // QShares for base calcs
                    int    des = deserved.get(e);                                                    // total deserved this round QShares
                    int    gpr = given_per_round.get(e);                                             // total given this round
                    int    mpr = Math.max(0, des-gpr);                                               // max this round, deserved less what I aleady was given
                    int    given = Math.min(mpr, (int) Math.floor(dgiven));                          // what is calculated, capped by what I alreay have
                    int    cap = e.calculateCap(o, total_shares);                                    // get caps, if any
                    logger.trace(methodName, null, descr, "O", o, ":", e.getName(), "Before caps, given", given, "cap", cap);

                    given = given / o;                                                               // back to NShares rounding down

                    if ( given + gbo[0] > cap ) {                                                    // adjust for caps
                        given = Math.max(0, cap - gbo[0]);
                    }

                    int    taken = Math.min(given, wbo[o]);                                          // NShares
                    taken = Math.min(taken, nshares[o] - total_taken);                               // cappend on physical (in case rounding overcommitted)

                    logger.trace(methodName, null, descr,
                                 "O", o, ":", e.getName(), "After  caps,",
                                 " dgiven Q[", dgiven,
                                 "] given N[", given ,
                                 "] taken N[", taken ,
                                 "]");

                    gbo[o] += taken;
                    gbo[0] += taken;
                    wbo[o] -= taken;
                    wbo[0] -= taken;
                    total_taken += taken;
                    given_per_round.put(e, given_per_round.get(e) + (taken*o));
                }
                if ( total_taken > 0 ) shares_given = true;
                removeSharesByOrder(vshares, nshares, total_taken, o);

                // If you were given all you deserve this round, then pull the weight so you don't
                // dilute the giving for shares you aren't owed.
                Iterator<IEntity> iter = working.iterator();
                while ( iter.hasNext() ) {
                    IEntity e = iter.next();
                    int des = deserved.get(e);
                    int gpr = given_per_round.get(e);
                    int got_all = Math.max(0, des - gpr);
                    if ( got_all == 0 ) {
                        allweights -= e.getShareWeight();
                    }
                }                
                if ( allweights <=0 ) break;   // JRC JRC
            }

            // Remove entities that have everything they want
            Iterator<IEntity> iter = working.iterator();
            while ( iter.hasNext() ) {
                IEntity e = iter.next();
                if ( e.getWantedByOrder()[0] == 0 ) {
                    logger.info(methodName, null, descr, e.getName(), "reaped, nothing more wanted:", fmtArray(e.getWantedByOrder()));
                    iter.remove();
                }
            }

            // Remove entities that can't get anythng more.  This is important to get better convergence - otherwise
            // the "spectator" jobs will pollute the fair-share and convergence will be much harder to achieve.
            
            iter = working.iterator();
            while ( iter.hasNext() ) {
                IEntity e = iter.next();
                int[] wbo = e.getWantedByOrder();
                boolean purge = true;
                for ( int o = maxorder; o > 0; o-- ) {
                    if ( (wbo[o] > 0) && (nshares[o] > 0) ) {   // if wants something, and resources still exist for it ...
                        purge = false;                          // then no purge
                        break;
                    }
                }
                if ( purge ) {
                    logger.info(methodName, null, descr, e.getName(), "reaped, nothing more usablee:", fmtArray(e.getWantedByOrder()), "usable:",
                                fmtArray(nshares));
                    iter.remove();
                }
            }

            if ( logger.isTrace() ) {
                logger.trace(methodName, null, descr, "Survivors at end of pass:");
                for ( IEntity e : working ) {
                    logger.trace(methodName, null, descr, e.toString());
                }
            }
        } while ( shares_given );

        if ( logger.isTrace() ) {
            logger.trace(methodName, null, descr, "Final before bonus:");
            for ( IEntity e : entities ) {
                int[] gbo = e.getGivenByOrder();
                logger.trace(methodName, null, descr, String.format("%12s %s", e.getName(), fmtArray(gbo)));
            }
        }

        //
        // A final pass, in case something was left behind due to rounding.
        // These are all bonus shares.  We'll give preference to the "oldest" 
        // entities.  But only one extra per pass, in order to evenly distribute.
        //
        // Technically, we might want to distribute the according to the
        // entity weights, but we're not doing that (assuming all weights are 1).
        //
        boolean given = true;
        int     bonus = 0;
        while ( (nshares[1] > 0) && (given)) {
            given = false;
            for ( IEntity e : entities ) {
                int[] wbo = e.getWantedByOrder();         // nshares
                int[] gbo = e.getGivenByOrder();          // nshares

                for ( int o = maxorder; o > 0; o-- ) {                
                    int canuse = wbo[o] - gbo[o];
                    while ( (canuse > 0 ) && (vshares[o] > 0) ) {
                        gbo[o]++;
                        bonus++;
                        canuse = wbo[o] - gbo[o];
                        removeSharesByOrder(vshares, nshares, 1, o);
                        given = true;
                        break;
                    }
                }
            }
        } 
        
        if ( bonus > 0 ) {
            logger.debug(methodName, null, descr, "Final after bonus:");
            for ( IEntity e : entities ) {
                int[] gbo = e.getGivenByOrder();          // nshares
                logger.debug(methodName, null, descr, String.format("%12s %s", e.getName(), fmtArray(gbo)));                
            }
            logger.debug(methodName, null, descr, "vshares", fmtArray(vshares));
            logger.debug(methodName, null, descr, "nshares", fmtArray(nshares));
        } else {
            logger.debug(methodName, null, descr, "No bonus to give.");
        }
    }


    /**
     * Count out shares for only the jobs in the ResouceClasses here, and only from the given
     * nodepool.
     */
	protected void countClassShares(NodePool np, ArrayList<ResourceClass> rcs)
    { 		
		String methodName = "countClassShares";

        logger.debug(methodName, null, "Counting for nodepool", np.getId());
        // pull the counts.  these don't get updated by the counting routines per-se.  after doing counting the np's are
        // expected to do the 'what-of' calculations that do acutall allocation and which update the counts
        int[] vshares = np.cloneVMachinesByOrder();
        int total_shares = np.countTotalShares();   // for caps, Qshares

        ArrayList<IEntity> l = new ArrayList<IEntity>();
        l.addAll(rcs); 

        for ( IEntity e : l ) {
            e.initWantedByOrder((ResourceClass) e);
        }

        apportion_qshares((List<IEntity>) l, vshares, total_shares, methodName);

        int sum_of_weights = 0;
        for ( ResourceClass rc : rcs ) {
            rc.markSubpoolCounted();               // Remember that we've attempted to apportion shares to this guy.
                                                   // This is specific to rc only, not IEntity, so we don't
                                                   // generalize this into apportion_shares.

            sum_of_weights += rc.getShareWeight(); // see next loop
        }

        //
        // If we have to do a "final eviction" because of fragmentation (evict stuff even though
        // it's below it's calcualted counts), we want to know the pure, unmodified fair share
        // for each rc.  Here is a great place to do that calculation.
        //
        for ( ResourceClass rc : rcs ) {
            int fair_share = (int) Math.floor(total_shares * ( (double)  rc.getShareWeight() / sum_of_weights ));
            rc.setPureFairShare(fair_share);
        }
    }


    /**
     * Count out shares for only the jobs in the ResouceClasses here, and only from the given
     * nodepool.
     */
	protected boolean countUserShares(ResourceClass rc)
    {
		String methodName = "countUserShares";

        HashMap<IRmJob, IRmJob> allJobs = rc.getAllJobs();
        if ( allJobs.size() == 0 ) {
            return false;                                                          // nothing to do ...
        }

        int[] vshares = rc.getGivenByOrder();                                      // assigned in countClassShares

        HashMap<User, User> users = new HashMap<User, User>();                     // get a map of all users in this class by examining the curent jobs
        for ( IRmJob j : allJobs.values() ) {
        	User u = j.getUser();
            if ( users.containsKey(u) ) continue;
            u.initWantedByOrder(rc);
            users.put(u, u);
        }

        ArrayList<IEntity> l = new ArrayList<IEntity>();
        l.addAll(users.values()); 
        apportion_qshares((List<IEntity>) l, vshares, 0, methodName);

        //
        // For final eviction if needed, calculate the pure uncapped un-bonused count for
        // each user.
        //
        int pure_share = rc.getPureFairShare();
        int fs = (int) Math.floor((double) pure_share / users.size());
        for ( User u : users.values() ) {
            u.setPureFairShare(fs);
        }
               
        return true;
    }

    private void countJobShares(ResourceClass rc)
    {
    	String methodName = "countJobShares";

        HashMap<User, HashMap<IRmJob,IRmJob>> userJobs = rc.getAllJobsByUser();

        for ( User u : userJobs.keySet() ) {
            HashMap<IRmJob, IRmJob> jobs = userJobs.get(u);
            if ( jobs.size() == 0 ) {
                continue;
            }
            
            int[] vshares = u.getGivenByOrder();
            ArrayList<IEntity> l = new ArrayList<IEntity>();
            l.addAll(jobs.values()); 

            for ( IEntity e : l ) {
                e.initWantedByOrder(rc);
            }

            apportion_qshares((List<IEntity>) l, vshares, 0,  methodName);

            //
            // For final eviction if needed, calculate the pure uncapped un-bonused count for
            // each user.
            //
            int pure_share = u.getPureFairShare();
            int fs = (int) Math.floor((double) pure_share / jobs.size());
            for ( IRmJob j : jobs.values() ) {
                j.setPureFairShare(fs);
            }
        }
    }

    /**
     * Depth-first traversal of the nodepool.  Once you get to a leaf, count the shares.  This sets an
     * upper-bound on the number of shares a class can have.  As you wind back up the tree the counts may
     * be reduced because of competition from jobs in the parent node.  By the time we're dont we should have
     * accounted for all jobs and all usable resources.
     *
     * Note how this works:
     * Consider a configuration with two nodepools plus global, A, B, and G.  Suppose nodepools A and B have
     * 30 shares each and G only has 10 shares.  G can apportion over it's 10, plus the 60 from A and B. So
     * after apporioning over A and B we need to do fair-share over G+A+B to insure that jobs submitted
     * to G are not "cheated" - recall that jobs in this set of classes have the same weight and priority,
     * and thus the same "right" to all the shares.  However, allocating a job from class A over the
     * full set of 10+30 shares could over-allocate it.  So the cap calculations must be sure never to
     * increase the already-given shares for subpools.
     *
     * Therefore we traverse the FULL SET of classes on every recursion.  When calculating caps from
     * apportion_shares the resource classes will have to account for multiple traversals and not over-allocate
     * if a class has already been apportioned from a subpool.
     */
    protected void traverseNodepoolsForCounts(NodePool np, ArrayList<ResourceClass> rcs)
    {
        //HashMap<String, NodePool> subpools = np.getChildren();
        List<NodePool> subpools = np.getChildrenAscending();
        for ( NodePool subpool : subpools ) {
            ArrayList<ResourceClass> cls = new ArrayList<ResourceClass>();
            String npn = subpool.getId();
            int njobs = 0;
            for ( ResourceClass rc : rcs ) {
                String rcnpn = rc.getNodepoolName();
                if ( rcnpn == null ) continue;

                if ( rc.getNodepoolName().equals(npn) ) {
                    cls.add(rc);
                    njobs += rc.countJobs();
                }
            }
            if ( njobs > 0 ) {
                traverseNodepoolsForCounts(subpool, cls);
            }
        }

        countClassShares(np, rcs);
    }


    protected void updateNodepools(NodePool np, ArrayList<ResourceClass> rcs)
    {
        //HashMap<String, NodePool> subpools = np.getChildren();
        List<NodePool> subpools = np.getChildrenAscending();
        for ( NodePool subpool : subpools ) {
            ArrayList<ResourceClass> cls = new ArrayList<ResourceClass>();
            String npn = subpool.getId();
            int njobs = 0;
            for ( ResourceClass rc : rcs ) {
                String rcnpn = rc.getNodepoolName();
                if ( rcnpn == null ) continue;

                if ( rc.getNodepoolName().equals(npn) ) {
                    cls.add(rc);
                    njobs += rc.countJobs();
                }
            }
            if ( njobs > 0 ) {
                updateNodepools(subpool, cls);
            }
        }

        // All calls have rcs EXACTLY the same as classes assigned to the np except
        // for the last one, to global.  At the end of the recursion everything except
        // those have been processed.  We grab just the ones in --global-- so we don't
        // end up counting them more than once.
        if ( np == globalNodepool ) {
            ArrayList<ResourceClass> tmp = new ArrayList<ResourceClass>();
            for ( ResourceClass rc : rcs ) {
                if ( rc.getNodepoolName().equals(globalNodepool.getId()) ) {
                    tmp.add(rc);
                }
            }
            rcs = tmp;
        }

        for (ResourceClass rc : rcs ) {
            if ( rc.countJobs() == 0 ) continue;
            rc.updateNodepool(np);
        }
    }

    private void howMuchFairShare(ArrayList<ResourceClass> rcs)
    {
        String methodName = "howMuchFairShare";
        if ( logger.isTrace() ) {
            logger.trace(methodName, null, "Scheduling FAIR SHARE for these classes:");
            logger.trace(methodName, null, "   ", ResourceClass.getHeader());
            logger.trace(methodName, null, "   ", ResourceClass.getDashes());
            for ( ResourceClass pc : rcs ) {
                logger.trace(methodName, null, "   ", pc.toString());
            }
        }

        ArrayList<ResourceClass> eligible = new ArrayList<ResourceClass>();
        Collections.sort(rcs, new ClassByWeightSorter());

        for ( ResourceClass rc : rcs ) {
            HashMap<IRmJob, IRmJob> jobs = rc.getAllJobs();
            logger.info(methodName, null, "Schedule class", rc.getName());
            rc.clearShares();

            if ( jobs.size() == 0 ) {
                logger.info(methodName, null, "No jobs to schedule in class ", rc.getName());
            } else {
                eligible.add(rc);
                for ( IRmJob j : jobs.values() ) {                    
                	logger.info(methodName, j.getId(), "Scheduling job in class ", rc.getName(), ":", j.toString());
                }
            }
        }
        if ( eligible.size() == 0 ) {
            return;
        }

        //
        // First step, figure out how many shares per class.
        //
        traverseNodepoolsForCounts(globalNodepool, eligible);

        //
        // Everything should be stable - now reduce the counts in the nodepools
        //
        /**
        logger.info(methodName, null, "Fair-share class counts:");
        for ( ResourceClass rc : rcs ) {
            logger.info(methodName, null, rc.tablesToString());
        }
        */
        // 
        // Update the class-level counts
        //
        updateNodepools(globalNodepool, eligible);

        //
        // Now the easy part, user and job allocations
        //

        //
        // We now know how many shares per class.  Let's do something similar for all the users in each class.
        // This will leave us with the number of shares per user for each resource class.
        //
        // The fair-share is easier to calculate because all priorities are the same within a class - we
        // dealt with class weights in the loop above.  But we always have to deal with jobs that don't
        // use their full allocation.
        //
        for ( ResourceClass rc : rcs ) {
        
            if ( !rc.hasSharesGiven() ) {
                for (IRmJob j : rc.getAllJobs().values()) {
                    j.clearShares();
                }
            	continue;
            }

            // 
            // Common rc-based share assignment for counting both user and job shares
            //
            /**
            int[] tmpSharesByOrder = rc.getSharesByOrder(globalNodepool.getArraySize());                       // get the shares given by countClassShares            
            int len = tmpSharesByOrder.length;
            for ( int i = 1; i < len; i++ ) {                                // now get by-share totals, same as nSharesByOrder in the node pool,
                // only for the class, not globally
                for ( int j = i+1; j < len; j++ ) {
                    if ( tmpSharesByOrder[j] != 0 ) {
                        tmpSharesByOrder[i] += (j / i) * tmpSharesByOrder[j];
                    }
                }
            }
            */
            
            if ( ! countUserShares(rc) ) {
                continue;                                // nothing to do, on to the next class
            }

            countJobShares(rc);
        }
        
    }


    /**
     * Is the job submitted to one of these classes?
     */
    protected boolean jobInClass(ArrayList<ResourceClass> rcs, IRmJob j)
    {
        for ( ResourceClass rc : rcs ) {
            if ( j.getResourceClass() == rc ) return true;
        }
        return false;
    }

    /**
     * Expand jobs from the needy list ahead of other jobs, because we likely stole the
     * shares from older jobs which would otherwise get priority.
     */
    protected void expandNeedyJobs(NodePool np, ArrayList<ResourceClass> rcs) 
    {
        String methodName = "expandNeedyJobs";

        if ( needyJobs.size() == 0 ) return;

        logger.trace(methodName, null, "Enter: needyJobs.size =", needyJobs.size());

        List<NodePool> subpools = np.getChildrenAscending();
        for ( NodePool subpool : subpools ) {
            expandNeedyJobs(subpool, rcs);
        }

        List<IRmJob> jobs = new ArrayList<IRmJob>();
        List<IRmJob> removeList = new ArrayList<IRmJob>();
        for ( IRmJob j : needyJobs.values() ) {
            if ( j.isCompleted() ) {
                removeList.add(j);
                continue;
            }

            ResourceClass rc = j.getResourceClass();
            if ( rc == null ) {
                removeList.add(j);
                continue;           // job completed or was canceled or some other wierdness
            }

            if ( !jobInClass(rcs, j) ) continue;

            if ( getNodepool(rc) == np ) {
                jobs.add(j);
                removeList.add(j);
            } 
        }
        
        for ( IRmJob j : removeList ) {
            needyJobs.remove(j);
        }

        Collections.sort(jobs, new JobByTimeSorter());
        logger.trace(methodName, null, "NP[", np.getId(), "Expand needy jobs.", listJobSet(jobs));
        np.doExpansion(jobs);
        logger.trace(methodName, null, "Exit : needyJobs.size =", needyJobs.size());
    }

    private static int stop_here_dx = 0;
    protected void traverseNodepoolsForExpansion(NodePool np, ArrayList<ResourceClass> rcs)
    {
    	String methodName = "traverseNodepoolsForExpansion";
        // HashMap<String, NodePool> subpools = np.getChildren();
        List<NodePool> subpools = np.getChildrenAscending();

        for ( NodePool subpool : subpools ) {
            traverseNodepoolsForExpansion(subpool, rcs);
        }

        logger.info(methodName, null, "--- stop_here_dx", stop_here_dx);
        if ( stop_here_dx == 13 ) {
            @SuppressWarnings("unused")
			int stophere;
            stophere=1;
        }
        stop_here_dx++;

        // fall through at leaves
        
        // gather all the classes that are assigned to this pool

        ArrayList<ResourceClass> cls = new ArrayList<ResourceClass>();
        String npn = np.getId();
        int njobs = 0;
        for ( ResourceClass rc : rcs ) {
            String rcnpn = rc.getNodepoolName();
            if ( rcnpn == null ) continue;
            
            if ( rc.getNodepoolName().equals(npn) ) {
                cls.add(rc);
                njobs += rc.countJobs();
            }
        }

        if ( njobs == 0 ) return;
        
        ArrayList<IRmJob> jobs = new ArrayList<IRmJob>();    // collect all the jobs for the current np
        for ( ResourceClass rc : cls ) {
            jobs.addAll(rc.getAllJobs().values());
        }
        Collections.sort(jobs, new JobByTimeSorter());
        
        np.doExpansion(jobs);
    }

    /**
     * Counts are established.  We strictly honor them inside each job and evict where the count is 
     * less than the currently allocated shares, and add where it is greater.
     */
    static int stophere = 0;
    protected void whatOfFairShare(ArrayList<ResourceClass> rcs)
    {
    	String methodName = "whatOfFairShare";

        ArrayList<ResourceClass> eligible = new ArrayList<ResourceClass>();
        Collections.sort(rcs, new ClassByWeightSorter());

        for ( ResourceClass rc : rcs ) {
            HashMap<IRmJob, IRmJob> jobs = rc.getAllJobs();
            logger.info(methodName, null, "Schedule class", rc.getName());

            if ( jobs.size() == 0 ) {
                logger.info(methodName, null, "No jobs to schedule in class ", rc.getName());
            } else { 
                eligible.add(rc);
                for ( IRmJob j : jobs.values() ) {  // just logging for debug for now
                	logger.info(methodName, j.getId(), "Scheduling job in class ", rc.getName(), ":", j.countNSharesGiven(), "shares given, order", j.getShareOrder());
                }
            }
        }

        if ( eligible.size() == 0 ) {
            return;
        }

        //traverseNodepoolsForEviction(globalNodepool, eligible);
        logger.trace(methodName, null, "Machine occupancy before expansion", stophere++);
        if ( stophere == 7 ) {
            @SuppressWarnings("unused")
			int stophere;
            stophere = 1 ;
        }
        traverseNodepoolsForExpansion(globalNodepool, eligible);
    }

    // ==========================================================================================
    // ==========================================================================================
    // =========================== REWORKED CODE FOR FIXED_SHARE =================================
    // ==========================================================================================
    // ==========================================================================================

    /**
     * Make sure there are enough shares to allocate either directly, or through preemption,
     * and count them out.
     */
    protected void howMuchFixed(ArrayList<ResourceClass> rcs)
    {
    	String methodName = "howMuchFixedShare";

        if ( logger.isTrace() ) {
            logger.trace(methodName, null, "Scheduling FIXED SHARE for these classes:");
            logger.trace(methodName, null, "   ", ResourceClass.getHeader());
            logger.trace(methodName, null, "   ", ResourceClass.getDashes());
            for ( ResourceClass pc : rcs ) {
                logger.trace(methodName, null, "   ", pc.toString());
            }
        }

        int total_jobs = 0;
        for ( ResourceClass rc : rcs ) {
            HashMap<IRmJob, IRmJob> jobs = rc.getAllJobs();
            total_jobs += jobs.size();
            if ( jobs.size() == 0 ) {
                logger.info(methodName, null, "No jobs to schedule in class ", rc.getName());
            } else {
                StringBuffer buf = new StringBuffer();
                for ( IRmJob j : jobs.values() ) {
                    buf.append(" ");
                    buf.append(j.getId());
                }
                logger.info(methodName, null, "Scheduling jobs in class:", rc.getName(), buf.toString());
            }
        }
        if ( total_jobs == 0 ) {
            return;
        }

        for ( ResourceClass rc : rcs ) {
            ArrayList<IRmJob> jobs = rc.getAllJobsSorted(new JobByTimeSorter());

            int shares_given_out = 0;                       // n-shares; virtual shares
            for ( IRmJob j : jobs ) {
                shares_given_out += j.countNShares();
                j.clearShares();
            }

            NodePool np = getNodepool(rc);

            int classcap = 0;
            classcap = calcCaps(rc.getAbsoluteCap(), rc.getPercentCap(), np.countTotalShares());       // quantum shares

            for ( IRmJob j : jobs ) {

                if ( j.countNShares() > 0 ) {
                    // already accounted for as well, since it is a non-preemptable share
                    logger.info(methodName, j.getId(), "[stable]", j.countNShares(), "proc, ", 
                                (j.countNShares() * j.getShareOrder()), "QS");
                    // j.addQShares(j.countNShares() * j.getShareOrder());
                    int[] gbo = NodePool.makeArray();
                    gbo[j.getShareOrder()] = j.countNShares();       // must set the allocation so eviction works right
                    j.setGivenByOrder(gbo);
                    continue;
                }

                //
                // For now, only schedule minshares, ignore maxshares.
                //
                int n_instances = Math.max(j.countInstances(), 1);  // n-shrares; virtual shares - API treats this as a reservation
                                                                    // and we overload the n-machines field for the count.
                int order = j.getShareOrder();

                // Don't schedule non-preemptable shares over subpools
                if ( np.countLocalShares() < n_instances ) {
                    schedulingUpdate.refuse(j, "Cannot accept Fixed Share job, nodepool " + np.getId() 
                                            + " has insufficient nodes left. Available[" 
                                            + np.countLocalShares() 
                                            + "] requested[" + n_instances + "]");
                    continue;
                }
             
                //
                // Now see if we have sufficient shares in the system for this allocation. Note that pool nodes are accounted for here as well.
                //
                if ( np.countNSharesByOrder(order) < n_instances ) {     // countSharesByOrder is N shares, as is minshares
                    schedulingUpdate.refuse(j, "Cannot accept Fixed Share job, insufficient shares available. Available[" + np.countNSharesByOrder(order) + "] requested[" + n_instances + "]");
                    continue;
                }

                //
                // Make sure this allocation does not blow the class cap.
                //
                if ( ((n_instances + shares_given_out) * order) > classcap ) {                         // to q-shares before comparing
                    schedulingUpdate.refuse(j, "Cannot accept Fixed Share job, class cap of " + classcap + " is exceeded.");
                    continue;
                }

                if ( rc.getMaxProcesses() < n_instances ) {               // Does it blow the configured limit for this class?
                    schedulingUpdate.refuse(j, "Cannot accept Fixed Share job, requested processes exceeds class max of " + rc.getMaxProcesses() + ".");
                    continue;
                }

                //
                // The job passes.  Assign it a count and get on with life ...
                //
                logger.info(methodName, j.getId(), "+++++ nodepool", np.getId(), "class", rc.getName(), "order", order, "shares", nSharesToString(n_instances, order));
                int[] gbo = NodePool.makeArray();
                gbo[order] = n_instances;
                j.setGivenByOrder(gbo);

                np.countOutNSharesByOrder(order, n_instances);
            }
        }
    }

    /**
     * All-or-nothing makes this easy.  If there are free shares of the right order just assign them.  Otherwise
     * the counts will cause evictions in lower-priority code so we just wait.
     */
    protected void whatOfFixedShare(ArrayList<ResourceClass> rcs)
    {
    	String methodName = "whatOfFixedShare";
        for ( ResourceClass rc : rcs ) {
            ArrayList<IRmJob> jobs = rc.getAllJobsSorted(new JobByTimeSorter());

            NodePool np = getNodepool(rc);
            for ( IRmJob j : jobs ) {

                if ( j.countNShares() > 0 ) {               // all or nothing - if we have any, we're fully satisfied
                    continue;
                }

                int order = j.getShareOrder();
                int count = j.countNSharesGiven();
                int avail = np.countNSharesByOrder(order);

                if ( avail >= count ) {
                    if ( np.findShares(j) != count ) {
                        throw new SchedInternalError(j.getId(), "Can't get enough shares but counts say there should be plenty.");
                    }
                    //
                    // Need to fix the shares here, if any, because the findShares() code is same for fixed and fair share so it
                    // won't have done that yet.
                    //
                    for ( Share s : j.getPendingShares().values() ) {
                        s.setFixed();
                    }
                    logger.info(methodName, j.getId(), "Assign:", nSharesToString(count, j.getShareOrder()));
                } 


                // 
                // If not we're waiting on preemptions which will occur naturally, or by forcible eviction of squatters.
                //
            }
        }
    }

    // ==========================================================================================
    // ==========================================================================================
    // =========================== REWORKED CODE FOR RESERVATIONS ================================
    // ==========================================================================================
    // ==========================================================================================

    /**
     * TODO: what to do if there are machines that are larger than requested, but
     *       not enough of the exact size.  For now, refuse, the request will only
     *       match exactly.
     */
	private void howMuchReserve(ArrayList<ResourceClass> rcs)
    {
        String methodName = "howMuchToreserve";

        if ( logger.isTrace() ) {
            logger.trace(methodName, null, "Calculating counts for RESERVATION for these classes:");
            logger.trace(methodName, null, "   ", ResourceClass.getHeader());
            logger.trace(methodName, null, "   ", ResourceClass.getDashes());
            for ( ResourceClass pc : rcs ) {
                logger.trace(methodName, null, "   ", pc.toString());
            }
        }

        for ( ResourceClass rc : rcs ) {

            // Get jobs into order by submission time - new ones ones may just be out of luck
            ArrayList<IRmJob> jobs = rc.getAllJobsSorted(new JobByTimeSorter());
            int machines_given_out = 0;

            NodePool np = getNodepool(rc);

            // Find out what is given out already, for class caps.  These are already accounted for
            // in the global counts.
            Iterator<IRmJob> jlist = jobs.iterator();
            while ( jlist.hasNext() ) {
                IRmJob j = jlist.next();

                if ( np == null ) {                      // oops - no nodes here yet, must refuse all jobs
                    schedulingUpdate.refuse(j, "Job scheduled to class "
                                            + rc.getName()
                                            + " but associated nodepool has no resources");
                    continue;
                }

                j.setReservation();                     // in case it's a new reservation

                int nshares = j.countNShares();         // for reservation each share is one full machine
                if ( nshares > 0 ) {                    // if it has any, it has all of them, so take off list
                    int[] gbo = NodePool.makeArray();   // needed to for defrag 
                    gbo[j.getShareOrder()] = j.countInstances();
                    j.setGivenByOrder(gbo);
                    
                    machines_given_out += nshares;
                    jlist.remove();
                }
            }

            if ( np == null ) {                         // no np. jobs have been refused, cannot continue.
                return;
            }

            // Now pick up the jobs that can still be scheduled, if any
            jlist = jobs.iterator();

            while ( jlist.hasNext() ) {
                IRmJob j = jlist.next();
                logger.info(methodName, j.getId(), "Scheduling (reserve) job in class ", rc.getName());

                if ( j.countNShares() > 0 ) {
                    logger.info(methodName, j.getId(), "Already scheduled with ", j.countInstances(),  "shares");
                    continue;
                }

                int order      = j.getShareOrder();     // memory, coverted to order, so we can find stuff
                int nrequested = j.countInstances();     // in machines                
                int classcap;
                
                if ( np.countLocalMachines() == 0 ) {
                    schedulingUpdate.refuse(j, "Job asks for " 
                                            + nrequested 
                                            + " reserved machines but reservable resources are exhausted for nodepool "
                                            + np.getId());
                    continue;
                }

                if ( rc.getMaxMachines() < nrequested ) {               // Does it blow the configured limit for this class?
                    schedulingUpdate.refuse(j, "Cannot accept reservation, requested machines exceeds configured class max of " + rc.getMaxMachines() + ".");
                    continue;
                }

                classcap = calcCaps(rc.getAbsoluteCap(), rc.getPercentCap(), np.countLocalMachines());
                
                //
                // Assumption to continue is that this is a new reservation
                //
                if ( (machines_given_out + nrequested) > classcap ) {
                    schedulingUpdate.refuse(j, "Job asks for " 
                                            + nrequested 
                                            + " reserved machines but total machines for class '" + rc.getName()
                                            + "' exceeds class cap of "
                                            + classcap);
                                            
                    continue;
                }               
                
                logger.info(methodName, j.getId(), "Job is granted " + nrequested + " machines for reservation.");
                //j.addQShares(nrequested * order);
                int[] gbo = NodePool.makeArray();
                gbo[order] = nrequested;
                j.setGivenByOrder(gbo);
                machines_given_out += nrequested;

                int given = 0;
                if ( rc.enforceMemory() ) { 
                    given = np.countFreeableMachines(j, true);
                } else {
                    given = np.countFreeableMachines(j, false);
                }           

                if ( given == 0 ) {
                    if ( rc.enforceMemory() ) {
                        schedulingUpdate.refuse(j, "Job asks for " 
                                                + nrequested 
                                                + " reserved machines with exactly "
                                                + j.getShareOrder()  
                                                + " shares but there are insufficient freeable machines.");
                    } else {
                        schedulingUpdate.refuse(j, "Job asks for " 
                                                + nrequested 
                                                + " reserved machines with at least "
                                                + j.getShareOrder()  
                                                + " shares but there are insufficient freeable machines.");
                    }
                    continue;
                }
            }
        }
    }

    /**
     */
	private void whatOfReserve(ArrayList<ResourceClass> rcs)
    {
        String methodName = "whatOfToReserve";
        for ( ResourceClass rc : rcs ) {
            NodePool np = getNodepool(rc);

            ArrayList<IRmJob> jobs = rc.getAllJobsSorted(new JobByTimeSorter());
            for ( IRmJob j: jobs ) {

                if ( j.isRefused() ) {                   // bypass jobs that we know can't be allocated
                    continue;
                }

                if ( j.countNShares() > 0 ) {            // shares already allocated, nothing to do (all-or-nothing policy in effect)
                    continue;
                }

                try {
                    np.allocateForReservation(j, rc);
                } catch (Exception e) {
                    logger.error(methodName, j.getId(), "Reservation issues:", e);
                    continue;
                }

                // 
                // Either shares were assigned or not.  If not we wait for evictions, otherwise it is
                // fully allocated. Nothing more to do here.
                //
            }

        }
    }

    /**
     * Remove non-preemptable resources from the counts.
     */
    protected void accountForNonPreemptable()
    {
        for (ResourceClass rc : resourceClasses.values() ) {
            switch ( rc.getPolicy() ) {
               case FAIR_SHARE:
                   break;

               case FIXED_SHARE: 
                   {
                       NodePool np = getNodepool(rc);
                       HashMap<IRmJob, IRmJob> jobs = rc.getAllJobs();
                       for ( IRmJob j : jobs.values() ) {
                           if ( j.countNShares() > 0 ) {   // all-or-nothing - if there's anything, it's fully scheduled.
                               HashMap<Share, Share> shares = j.getAssignedShares();
                               np.accountForShares(shares);
                           }
                       }
                   }
                   break;                

               case RESERVE:               
                   {
                       NodePool np = getNodepool(rc);
                       HashMap<IRmJob, IRmJob> jobs = rc.getAllJobs();
                       for ( IRmJob j : jobs.values() ) {
                           if ( j.countNShares() > 0 ) {   // all-or-nothing - if there's anything, it's fully scheduled.
                               HashMap<Share, Share> shares = j.getAssignedShares();
                               np.accountForShares(shares);
                           }
                       }
                   }
                   break;                
            }
        }
    }

    /**
     * Remove active shares from the nodepool counts, leaving us with just the free shares in the tables.
     */
    protected void accountForFairShare()
    {
        for (ResourceClass rc : resourceClasses.values() ) {
            if ( rc.getPolicy() == Policy.FAIR_SHARE ) {
                NodePool np = getNodepool(rc);
                HashMap<IRmJob, IRmJob> jobs = rc.getAllJobs();
                for ( IRmJob j : jobs.values() ) {
                    HashMap<Share, Share> shares = j.getAssignedShares();
                    np.accountForShares(shares);
                }
            }
        }
    }
        
    /**
     * The second stage - work up all counts globally
     */
    protected void findHowMuch(ArrayList<ResourceClass> rcs)
    {
        switch ( rcs.get(0).getPolicy() ) {
             case FAIR_SHARE:
                  howMuchFairShare(rcs);
                  break;
             case FIXED_SHARE:
                 howMuchFixed(rcs);
                 break;
             case RESERVE:
                 howMuchReserve(rcs);
                 break;
        }
    }

    /**
     * Collect all the fair-share classes and pass them to the nodepools for eviction
     * This interacts recursively with NodePool.doEvictions*.
     *
     * In NodepoolScheduler we recurse doing depth-first traversal to have every nodepool do evictions for jobs
     * that are submitted to that pool.  The NodePool then recurses again in depth-first mode to evict shares that
     * have spilled into lower-level pools.  We have a double-recursion:
     * 1.  Evict shares for jobs that originate in each node pool
     * 2.  Evict shares that have spilled into lower-level pools.
     */
    private static int stop_here_de = 0;
    protected void doEvictions(NodePool nodepool)
    {
    	String methodName = "doEvictions";
    	
        logger.info(methodName, null, "--- stop_here_de", stop_here_de);
        if ( stop_here_de == 7 ) {
            @SuppressWarnings("unused")
			int stophere;
            stophere=1;
        }
        stop_here_de++;

        for ( NodePool np : nodepool.getChildrenDescending() ) {   // recurse down the tree
            doEvictions(np);                                      // depth-first traversal
        }

        int neededByOrder[] = NodePool.makeArray();         // for each order, how many N-shares do I want to add?
        int total_needed = 0;

        for ( ResourceClass cl : resourceClasses.values() ) {
            if ( cl.getNodepoolName().equals(nodepool.getId()) && (cl.getAllJobs().size() > 0) ) {
                HashMap<IRmJob, IRmJob> jobs = cl.getAllJobs();
                for ( IRmJob j : jobs.values() ) {
                    int counted = j.countNSharesGiven();      // allotment from the counter
                    int current = j.countNShares();           // currently allocated, plus pending, less those removed by earlier preemption
                    int needed = (counted - current);
                    int order = j.getShareOrder();
         
                    needed = Math.abs(needed);
                    //needed = Math.max(0, needed);
                    neededByOrder[order] += needed;
                    total_needed += needed;
                }
            }
    
        }
        logger.debug(methodName, null, nodepool.getId(),  "NeededByOrder before any eviction:", Arrays.toString(neededByOrder));        
        if ( (nodepool.countShares() > 0) && (total_needed > 0) ) {
            nodepool.doEvictionsByMachine(neededByOrder, false);
        }
    }

    /**
     * Determine if a candidate share can or cannot be transferred (eventually) to a needy job based on nodepool constraints.
     */
    boolean compatibleNodepools(Share candidate, IRmJob needy)
    {
        Machine m = candidate.getMachine();
        ResourceClass nrc = needy.getResourceClass();
        NodePool np = getNodepool(nrc);

        return np.containsMachine(m);           // can we get to the candidate share from 'needy's np?
    }

    /**
     * Discover whether the potential job is able or unable to supply shares to a needy job because of nodepool restrictions.
     */
    boolean compatibleNodepools(IRmJob potential, IRmJob needy)
    {
        ResourceClass prc = potential.getResourceClass();
        ResourceClass nrc = needy.getResourceClass();

        NodePool np = getNodepool(nrc);
        NodePool pp = getNodepool(prc);

        return np.containsSubpool(pp) || pp.containsSubpool(np);
    }

    /**
     * For debugging, get job ids onto a single line.
     */
    String listJobSet(Map<IRmJob, IRmJob> jobs)
    {
        if ( jobs.size() == 0 ) return "NONE";
        StringBuffer sb = new StringBuffer("[");
        for ( IRmJob j : jobs.keySet() ) {
            sb.append(j.getId());
            sb.append(" ");
        }
        sb.append("]");
        return sb.toString();
    }

    String listJobSet(List<IRmJob> jobs)
    {
        if ( jobs.size() == 0 ) return "NONE";
        StringBuffer sb = new StringBuffer("[");
        for ( IRmJob j : jobs ) {
            sb.append(j.getId());
            sb.append(" ");
        }
        sb.append("]");
        return sb.toString();
    }

    /**
     * Make a share available to job 'nj'.  If possible, just reassign  If not, cancel it's expansion or evict it, as appropriate.
     */
    boolean clearShare(Share s, IRmJob nj)
    {
    	String methodName = "clearShare";
        IRmJob rich_j = s.getJob();
        if ( s.isPending() ) {
            if (s.getShareOrder() == nj.getShareOrder() ) {                            // same size, reassign it directly
                logger.debug(methodName, nj.getId(), "Reassign expanded share", s.toString(), "from", rich_j.getId());
                s.reassignJob(nj);
                rich_j.cancelPending(s);
                nj.assignShare(s);                
                return false;
            } else {                                                                   // different size, just discard it
                logger.debug(methodName, nj.getId(), "Canceling expansion share", s.toString(), "from", rich_j.getId());
                rich_j.cancelPending(s);
                Machine m = s.getMachine();
                m.removeShare(s);
                return true;
            }
        } else {                                                                       // not pending, must evict it
            logger.debug(methodName, nj.getId(), "Donate", s.toString(), "from", rich_j.getId());
            rich_j.shrinkByOne(s);
            return false;
        }
    }

    /**
     * This routine tries to find enough process that can be coopted from "rich" users for jobs that deserved
     * shares but couldn't get them because of fragmentation.
     *
     * @param j               This is the job we are trying to find shrares for. 
     * @param needed          This is the number of processes we need for job j.
     * @param users_by_wealth This is all the users who can donate, ordered by wealth.
     * @param jobs_by_user    This is all the candidate jobs owned by the user.  Note that this is not necessarily
     *                        ALL the user's jobs, we will have culled everything that makes no sense to
     *                        take from in the caller.
     *
     * @return The number of processes we found space for.  Note this could be different from the number
     *         of processes evicted, if it took more than one eviction to make spece.  Also We may have
     *         evicted a process smaller than is needed, because there was already some free space on
     *         the machine.
     */
    int takeFromTheRich(IRmJob nj, int needed,
                            TreeMap<User, User> users_by_wealth,
                            HashMap<User, TreeMap<IRmJob, IRmJob>> jobs_by_user)
    {
    	String methodName = "takeFromTheRich";
        // 1. Collect all machines that have shares, which if evicted, would make enough space
        //    - in compatible NP
        //    - g + sum(shares belonging to rich users on the machine);
        // 2. Order the machiens by 
        //    a) richest user
        //    b) largest machine
        // 3. Pick next machine,
        //    - clear enough shares
        //    - remove machine from list
        //    - update wealth
        // 4. Repeat at 2 until
        //    a) have given what is needed
        //    b) nothing left to give

        // Map<Share, Share>     exemptShares = new HashMap<Share, Share>(); // not eligible for various reasons
        Map<IRmJob, IRmJob>   candidateJobs = new HashMap<IRmJob, IRmJob>();
        Map<Machine, Machine> eligibleMachines = new TreeMap<Machine, Machine>(new EligibleMachineSorter());

        for ( TreeMap<IRmJob, IRmJob> jobs : jobs_by_user.values() ) {
            candidateJobs.putAll(jobs);
        }

        int given = 0;
        int orderNeeded = nj.getShareOrder();
        
        ResourceClass cl = nj.getResourceClass();
        String npname = cl.getNodepoolName();
        NodePool np = globalNodepool.getSubpool(npname);
        Map<Node, Machine> machines = np.getAllMachines();          // everything here is a candidate, nothing else is

        for ( Machine m : machines.values() ) {
            if ( m.getShareOrder() < orderNeeded ) {
                logger.trace(methodName, nj.getId(), "Bypass ", m.getId(), ": too small for request of order", orderNeeded);
                continue;
            }

            // if the job is a reservation the machine size has to match
            if ( nj.isReservation() && ( m.getShareOrder() != orderNeeded )) {
                logger.trace(methodName, nj.getId(), "Bypass ", m.getId(), ": reservation requires exact match for order", orderNeeded);
                continue;
            }

            Map<Share, Share> as = m.getActiveShares();
            int g = m.getVirtualShareOrder();
            for ( Share s : as.values() ) {
                IRmJob j = s.getJob();
                if ( s.isForceable() && candidateJobs.containsKey(j) ) {
                    g += j.getShareOrder();
                }
            }
            if ( g >= orderNeeded ) {
                logger.trace(methodName, nj.getId(), "Candidate machine:", m.getId());
                eligibleMachines.put(m, m);
            } else {
                // (a) the share is not forceable (non-preemptbable, or already being removed), or
                // (b) the share is not owned by a rich job
                logger.trace(methodName, nj.getId(), "Not a candidate, insufficient rich jobs:", m.getId());
            }
        }

        if ( nj.isReservation() && ( eligibleMachines.size() < needed ) ) {
            // if we can't clear enough for the reservation we have to wait.  Very unlikely, but not impossible.
            logger.info(methodName, nj.getId(), "Found insufficient machines (", eligibleMachines.size(), "for reservation. Not clearing.");
            return 0;
        }

        logger.info(methodName, nj.getId(), "Found", eligibleMachines.size(), "machines to be searched in this order:");
        StringBuffer buf = new StringBuffer();
        for ( Machine m : eligibleMachines.keySet() ) {
            buf.append(m.getId());
            buf.append(" ");
            }
        logger.info(methodName, nj.getId(), "Eligible machines:", buf.toString());

        // first part done

        // Now just bop through the machines until either we can't find anything, or we find everything.
        int given_per_round = 0;
        do {
            int g = 0;
            for ( Machine m : eligibleMachines.keySet() ) {

                //
                // How best to order candidate shares?  You can choose the "wealthiest" first, but if it's not a good
                // match by size, end up evicting too many shares which could include a not-so-wealthy share, or
                // increase frag by breaking it up and leaving a useless bit.
                //
                // So we're going to try ordering shares by "wealthiest", but then if we find an exact match by size,
                // order that to the front of the candidates.  We may not end up evicting the "wealthiest", but we
                // should end up evicting tne least disruptive share.
                //
                List<Share> sh = new ArrayList<Share>();
                sh.addAll(m.getActiveShares().values());
                Collections.sort(sh, new ShareByWealthSorter());

                g = m.getVirtualShareOrder();
                List<Share> potentialShares     = new ArrayList<Share>();
                for ( Share s : sh ) {
                    IRmJob j = s.getJob();
                    User u = j.getUser();
                    
                    if ( s.isForceable() ) {
                        TreeMap<IRmJob, IRmJob> potentialJobs = jobs_by_user.get(u);
                        if ( (potentialJobs != null) && ( potentialJobs.containsKey(j) ) ) {
                            g += s.getShareOrder();
                            if ( s.getShareOrder() == orderNeeded ) {
                                potentialShares.add(0, s);    // exact matches first
                            } else {
                                potentialShares.add(s);
                            }
                        }
                    }
                    if ( g >= orderNeeded ) break;
                }
                
                // potentialShares should be properly ordered as discussed above at this point
                if ( g >= orderNeeded ) {
                    // found enough on this machine for 1 share!
                    logger.debug(methodName, nj.getId(), "Clearing shares: g[", g, "], orderNeeded[", orderNeeded, "]");
                    g = m.getVirtualShareOrder();             // reset
                    for ( Share s : potentialShares ) {
                        IRmJob j = s.getJob();
                        User u = j.getUser();

                        g += s.getShareOrder();
                        given_per_round++;
                        clearShare(s, nj);
                        u.subtractWealth(s.getShareOrder());
                        logger.debug(methodName, nj.getId(), "Clearing share", s, "order[", s.getShareOrder(),
                                     "]: g[", g, "], orderNeeded[", orderNeeded, "]");
                        if ( g >= orderNeeded) break; // inner loop, could break on exact match without giving everything away
                    }
                    break;                            // outer loop, if anything was found
                }       
            }

            if ( given_per_round > 0 ) {
                // Must reorder the eligible list to get the "next" best candidate.  We could try to remove 
                // machines that were exhausted above ...
                Map<Machine, Machine> tmp = new HashMap<Machine, Machine>();
                tmp.putAll(eligibleMachines);
                eligibleMachines.clear();
                for ( Machine m : tmp.keySet() ) {
                    eligibleMachines.put(m, m);
                }

                // and also must track how many processes we ma made space for
                given = given + (g / orderNeeded);    // at least one,or else we have a bug 
                logger.debug(methodName, nj.getId(), "LOOPEND: given[", given, "] g[", g, "] orderNeeded[", orderNeeded, "]");
            }
            logger.debug(methodName, nj.getId(), "Given_per_round", given_per_round, "given", given, "needed", needed);
        } while ( (given_per_round > 0) && ( given < needed ));
        
        return given;
    }

    void doFinalEvictions(HashMap<IRmJob, Integer> needy)
    {
    	String methodName = "doFinalEvictions";

        for ( IRmJob j : needy.keySet() ) {
            logger.debug(methodName, j.getId(), "Will attempt to have space made for", needy.get(j), "processes");
        }

        //
        // Put candidate donors into a map, ordered by "most able to be generous".
        // Candidates must not be needy, must be initialized already, be in compatible nodepools, and have sufficient shares to give.
        //

        for ( IRmJob nj : needy.keySet() ) {
            TreeMap<IRmJob, IRmJob> candidates = new TreeMap<IRmJob, IRmJob>(new FragmentationSorter());
            for ( ResourceClass rc : resourceClasses.values() ) {
                
                if ( rc.getPolicy() == Policy.RESERVE )     continue;          // exempt from preemption
                if ( rc.getPolicy() == Policy.FIXED_SHARE ) continue;          // exempt from preemption

                HashMap<IRmJob, IRmJob> jobs = rc.getAllJobs();
                for ( IRmJob j : jobs.values() ) {
                    int nshares = j.countNShares();
                    int qshares = nshares * j.getShareOrder();

                    if ( nj.isReservation() && (nj.getSchedulingPriority() <= j.getSchedulingPriority()) ) {
                        if ( nshares == 0 ) {
                            logger.debug(methodName, nj.getId(), "Job", j.getId(), "is not a candidate because it has no share.");
                            continue;
                        } 
                        // We could end up evictin really needy stuff - hopefully not, but these guys are Top Men so there.
                        logger.debug(methodName, nj.getId(), "Reservation priority override on candidate selection.");
                    } else {
                        if ( needy.containsKey(j) ) {                            // if needy it's not a candidate
                            logger.debug(methodName, nj.getId(), "Job", j.getId(), "is not a candidate because it's needy.");
                            continue;
                        }
                        
                        if ( ! j.isInitialized() ) {
                            logger.debug(methodName, nj.getId(), "Job", j.getId(), "is not a candidate because it's not initialized yet.");
                            continue;                                            // if not initialized its not a candidate
                        }
                        
                        //
                        // Need at least one potential candidate of worse or equal priority
                        //
                        
                        if ( j.getSchedulingPriority() < nj.getSchedulingPriority() ) {
                            logger.debug(methodName, nj.getId(), "Job", j.getId(), "is not a candidate because it has better priority.");
                            continue;
                        }
                        
                        if ( ! compatibleNodepools(j, nj) ) {
                            logger.debug(methodName, nj.getId(), "Job", j.getId(), "is not a candidate because of incompatible nodepools.");
                            continue;
                        }
                        
                        if ( nshares < fragmentationThreshold ) {
                            // If you're already below the threshold then you're safe, unless we're clearing for a reservation.
                            logger.debug(methodName, nj.getId(), "Job", j.getId(), "is not a candidate because not enough processes[", nshares, "] qshares[", qshares, "]");
                            continue;
                        }
                    }
                    
                    logger.debug(methodName, nj.getId(), "Job", j.getId(), "is a candidate with processes[", nshares, "] qshares[", qshares, "]");
                    candidates.put(j, j);
                }
            }

            //
            // Collect total wealth and order the wealthy by spondulix
            //
            HashMap<User, Integer> shares_by_user = new HashMap<User, Integer>();                                // use this to track user's wealth
            HashMap<User, TreeMap<IRmJob, IRmJob>> jobs_by_user = new HashMap<User, TreeMap<IRmJob, IRmJob>>();  // use this to track where the wealth originates
            for ( IRmJob j : candidates.values() ) {
                User u = j.getUser();
                
                if ( shares_by_user.get(u) == null ) {
                    shares_by_user.put(u, 0);
                }
                shares_by_user.put(u, shares_by_user.get(u) + (j.countNShares() * j.getShareOrder()));
                
                TreeMap<IRmJob, IRmJob> ujobs = jobs_by_user.get(u);
                if ( ujobs == null ) {
                    ujobs = new TreeMap<IRmJob, IRmJob>(new JobByShareSorter()); // orders by largest number of assigned shares
                    jobs_by_user.put(u, ujobs);
                }
                ujobs.put(j, j);
            }
            TreeMap<User, User> users_by_wealth = new TreeMap<User, User>(new UserByWealthSorter()); // orders users by wealth
                                                                                                     // and tracks their fat jobs
            for ( User u : shares_by_user.keySet() ) {
                u.setShareWealth(shares_by_user.get(u));       // qshares
                users_by_wealth.put(u, u);
            }

            //
            // Try stealing shares from 'users_by_wealth' until the needy
            // job has met its fragmentation threshold, or until we decide its impossible to do so.
            //

            int needed = needy.get(nj);      // this was adjusted to a reasonable level in the caller
            logger.debug(methodName, nj.getId(), "Needy job looking for", needed, "more processes of O[", nj.getShareOrder(), "]");

            // while ( ( needed > 0 ) && takeFromTheRichX(nj, users_by_wealth, jobs_by_user) ) {
            needed -= takeFromTheRich(nj, needed, users_by_wealth, jobs_by_user);
            if ( needed <= 0 ) {
                // This can go <0 if total space freed + unused space on a node adds up to >1 share.
                // It's slimplest to just not sweat it and call it satisfied.
                logger.info(methodName, nj.getId(), "Satisfied needs of job by taking from the rich.");
            } else {
                logger.info(methodName, nj.getId(), "Could not get enough from the rich. Asked for", needy.get(nj), "still needing", needed);
            }
        }
    }

    void getNodepools(NodePool top, List<NodePool> nodepools)
    {        
        for ( NodePool np : top.getChildren().values()) {
            getNodepools(np, nodepools);
        }
        nodepools.add(top);
    }

    /**
     * Check to see if there are jobs whose counts indicate they should be getting shares, but which
     * can't be satisfied either from free shares or from pending evictions because of
     * fragmentation.
     *
     * If there's frag, we return a map that shows how much each under-allocated job needs, and
     * a map of the pending evictions.
     */
    void detectFragmentation(HashMap<IRmJob, Integer> needed_by_job)
    {
    	String methodName = "detectFragmentation";

        if ( logger.isDebug() ) {
            logger.debug(methodName, null, "vMachines:", fmtArray(globalNodepool.cloneVMachinesByOrder()));
        }

        List<NodePool> poollist = new ArrayList<NodePool>();
        getNodepools(globalNodepool, poollist);
        NodePool[] allPools = poollist.toArray(new NodePool[poollist.size()]);

        if ( logger.isDebug() ) {
            // make sure the node pool list is built correctly
            StringBuffer sb = new StringBuffer("Nodepools:");
            for ( NodePool np : allPools ) {
                sb.append(np.getId());
                sb.append(" ");
            }
            logger.debug(methodName, null, sb.toString());
        }

        // These next two maps are built lazily, once per call to this routine
        Map<String, int[]> vshares = new HashMap<String, int[]>();     // Virtual shares of each order in each nodepool.
                                               // This gets enhanced with pending evictions so we
                                               // can tell whether enough space is accounted for
                                               // for each job.
        Map<String, int[]> nshares = new HashMap<String, int[]>();     // For each order, the number of shares available,
                                               // either directly, or through splits from higher-order
                                               // space, enhanced with pending evictions and purges.
        Map<String, Map<IRmJob, Integer>> jobs = new HashMap<String, Map<IRmJob, Integer>>();

        for ( int npi = 0; npi < allPools.length; npi++ ) {                       // Turns out to be depth-first traversal !
            // First pass, init the structures, including any free space that may have been unusable
            NodePool np = allPools[npi];
            String id = np.getId();

            int[] vmach = NodePool.makeArray();
            int[] nmach = NodePool.makeArray();
            Map<IRmJob, Integer> jobmap = new HashMap<IRmJob, Integer>();
            vshares.put(id, vmach);
            nshares.put(id, nmach);
            jobs.put(id, jobmap);
        }

        boolean must_defrag = false;
        String headerfmt = "%12s %10s %6s %4s %7s %6s %2s";
        String datafmt   = "%12s %10s %6d %4d %7d %6d %2d";

        for ( ResourceClass rc : resourceClasses.values() ) {
            // Next: Look at every job and work out its "need".  Collect jobs by nodepool into the jobmaps.
            Map<IRmJob, IRmJob> allJobs = rc.getAllJobs();
            String npn = rc.getNodepoolName();
            Map<IRmJob, Integer> jobmap = jobs.get(npn);

            if ( allJobs.size() == 0 ) continue;

            logger.info(methodName, null, String.format(headerfmt, "Nodepool", "User", "PureFS", "NSh", "Counted", "Needed", "O"), "Class:", rc.getName());
            for ( IRmJob j : allJobs.values() ) {

                int counted = 0;
                switch ( rc.getPolicy() ) {
                    case FAIR_SHARE:
                        counted = j.countNSharesGiven();       // fair share allocation
                        break;
                    default:
                        counted = j.countInstances();          // fixed, all, or nothing
                        break;
                }

                int current = j.countNShares();                // currently allocated, plus pending, less those removed by earlier preemption
                int needed = counted - current;                // could go negative if its evicting
                int order = j.getShareOrder();
                
                if ( j.getSchedulingPolicy() == Policy.FAIR_SHARE ) {   // cap on frag threshold
                    if ( current >= fragmentationThreshold ) { 
                        needed = 0;
                    } else if ( needed < 0 ) {
                        needed = 0;
                    } else if ( needed > 0) {
                        needed = Math.min(needed, fragmentationThreshold);
                        jobmap.put(j, needed);                 // we'll log this in a minute, not here
                        must_defrag = true;
                    }                    
                } else {                                       // if not fair-share, must always try to defrag if needed
                    if ( needed > 0 ) {
                        jobmap.put(j, needed);   
                        must_defrag = true;
                    }
                }


                logger.info(methodName, j.getId(), 
                            String.format(datafmt, 
                                          npn,
                                          j.getUser().getName(),
                                          j.getPureFairShare(),
                                          current,
                                          counted,
                                          needed,
                                          order),
                            (needed > 0) ? "POTENTIALLY NEEDY" : ""
                            );

//                             String.format("NP: %10s User %8s Pure fs[%3d] Nshares[%3d] AsgnNshares[%3d] needed[%3d] O[%d] %s",
//                                           npn,
//                                           j.getUser().getName(),
//                                           j.getPureFairShare(),
//                                           current,
//                                           counted,
//                                           needed,
//                                           order,
//                                           (needed > 0) ? "POTENTIALLY NEEDY" : ""
//                                           ));

                
            }
        }
        if ( ! must_defrag ) return;                          // the rest of this is expensive, let's bypass if we can

        for ( int npi = 0; npi < allPools.length; npi++ ) {                       // Turns out to be depth-first traversal !
            // Next pass, find all the open and potentially open spots in each nodepool.  We need to coalesce
            // evicted shares with each other and with open space on each machine in order to know whether the
            // space is potentially usable by jobs.
            NodePool np = allPools[npi];

            Map<Node, Machine> machs = np.getAllMachinesForPool();
            for ( Machine m : machs.values() ) {

                int free = m.countFreedUpShares();                           // free space plus evicted shares - eventual space
                if ( free != 0 ) {
                    logger.trace(methodName, null, "Freed shares", free, "on machine", m.getId());
                    for ( NodePool npj = np; npj != null; npj = npj.getParent() ) {        // must propogate up because of how these tables work
                        String id_j = npj.getId();
                        int[] vmach_j = vshares.get(id_j);
                        logger.trace(methodName, null, "Update v before: NP[", id_j, "] v:", fmtArray(vmach_j));
                        vmach_j[free]++;                                         // This is the largest potential share that can be made on this machine,
                        // after evictions starting 'here' and propogating up
                        logger.trace(methodName, null, "Update v after : NP[", id_j, "] v:", fmtArray(vmach_j));
                    }
                }                
            }
        }
        
        for ( int npi = 0; npi < allPools.length; npi++ ) {
            // Next pass, create the cumulative "n" style table from the "v" table of holes
            String id = allPools[npi].getId();

            int[] vmach = vshares.get(id);
            int[] nmach = nshares.get(id);
            reworknShares(vmach, nmach);                                 // Populate nmach from vmach for this np, with free or potentially free shares

            if ( logger.isInfo() ) {
                logger.info(methodName, null, "NP", id, "After check: virtual    free Space", fmtArray(vmach));
                logger.info(methodName, null, "NP", id, "After check: cumulative free Space", fmtArray(nmach));
            }
        }

        for ( int npi = 0; npi < allPools.length; npi++ ) {
            // Last step, traverse jobs by nodepool and determine their need.
            String id = allPools[npi].getId();

            Map<IRmJob, Integer> jobmap = jobs.get(id);
            if ( jobmap.size() == 0 ) continue;                          // only looking at potentially needy jobs

            int[] nmach = nshares.get(id);

            for ( IRmJob j : jobmap.keySet() ) {

                int needed = jobmap.get(j);
                int order = j.getShareOrder();
                int available = nmach[order];
                int to_remove = 0;

                //if ( j.getSchedulingPolicy() == Policy.FAIR_SHARE ) {
                   // Preference is given during expinsion in next cycle because usually, if
                   // we Took From The Rich, we took from older jobs, which would normally
                   // have priority for available resources.
                   //
                   // We don't need to include the non-preemptable jobs here, they're handled
                   // well enough in their normal what-of code.
                   needyJobs.put(j, j);
                //}
                
                if ( available >= needed ) {
                    needed = 0;
                    to_remove = needed;
                } else {
                    to_remove = available;
                    needed -= to_remove;
                }

                // TODO TODO TODO Is this loop useful, and if so, for what? Is it old code I forgot to remove?
                if ( to_remove > 0 ) {
                    NodePool np = allPools[npi];
                    for ( NodePool npj = np; npj != null; npj = npj.getParent() ) {        // must propogate up because of how these tables work
                        String id_j = npj.getId();
                        int[] vmach_j = vshares.get(id_j);
                        int[] nmach_j = nshares.get(id_j);
                        removeSharesByOrder(vmach_j, nmach_j, to_remove, order);
                    }
                }
                
                if ( needed > 0 ) {
                    needed_by_job.put(j, needed);
                    logger.info(methodName, j.getId(), 
                                String.format("NP: %10s User: %10s Pure fs: %3d needed %3d O[%d] %s",
                                              id,
                                              j.getUser().getName(),
                                              j.getPureFairShare(),
                                              needed,
                                              order,
                                              "ACTUALLY NEEDY"
                                              ));
                }
            }
        }            
    }

    void insureFullEviction()
    {
    	String methodName = "insureFullEviction";

        int jobcount = 0;
        for ( ResourceClass rc : resourceClasses.values() ) {
            if ( rc.getPolicy() != Policy.RESERVE ) {
                jobcount += rc.countJobs();
            }
        }
        if ( jobcount == 0 ) return;

        HashMap<IRmJob, Integer> needy = new HashMap<IRmJob, Integer>();
        detectFragmentation(needy);
        if ( needy.size() == 0 ) {
            logger.info(methodName, null, "No needy jobs, defragmentation bypassed.");
            return;
        }

        logger.info(methodName, null, "NEEDY JOBS DETECTED");
        doFinalEvictions(needy);
    }

    /**
     * The third stage - work up all counts globally
     */
    protected void findWhatOf(ArrayList<ResourceClass> rcs)
    {
        switch ( rcs.get(0).getPolicy() ) {
            case FAIR_SHARE:
                whatOfFairShare(rcs);
                break;
            case FIXED_SHARE:
                whatOfFixedShare(rcs);
                break;
            case RESERVE:
                whatOfReserve(rcs);
                break;
        }
    }

    void setSchedulingUpdate(ArrayList<ResourceClass> rcs)
    {
        for ( ResourceClass rc : rcs ) {
            HashMap<IRmJob, IRmJob> jobs = rc.getAllJobs();
            for ( IRmJob j : jobs.values() ) {
                if ( j.isRefused() ) {
                    continue;
                }
                if ( j.isExpanded() ) {
                    schedulingUpdate.addExpandedJob(j);
                }
                if ( j.isShrunken() ) {
                    schedulingUpdate.addShrunkenJob(j);
                }
                if ( j.isStable() && ( ! j.isReservation() ) ) {
                    schedulingUpdate.addStableJob(j);
                }
                if ( j.isDormant() ) {
                    schedulingUpdate.addDormantJob(j);
                }
                if ( j.isReservation() ) {
                    schedulingUpdate.addReservation(j);
                }
            }
        }
    }

    /**
     * Figure out what we have to give away.  Called on entry, and after any kind of
     * action that takes machines out of the schedulable pool ( e.g. reservation ).
     */
	private void resetNodepools()
    {
    	//String methodName = "countResources";
        int maxorder = 0;
        for ( ResourceClass rc: resourceClasses.values() ) {
            maxorder = Math.max(maxorder, rc.getMaxJobOrder());
        }
        globalNodepool.reset(maxorder);
        // logger.info(methodName, null, "Scheduling Tables at Start of Epoch:\n", globalNodepool.toString());
    }

    /**
     * IScheduler entry point for the fairShare calculation.
     *
     * This implements the easy three step process described at the top of the file.
     */
    public void schedule(SchedulingUpdate upd)
    {
        String methodName = "schedule";
        
        int jobcount = 0;
        for ( ResourceClass rc : resourceClasses.values() ) {

            HashMap<IRmJob, IRmJob> allJobs = rc.getAllJobs();
            jobcount += allJobs.size();
            for ( IRmJob j : allJobs.values() ) {
                j.initJobCap();
            }
        }

        if ( jobcount == 0 ) {
            logger.info(methodName, null, "No jobs to schedule under nodepool", globalNodepool.getId());
            return;
        }

        logger.info(methodName, null, "Machine occupancy before schedule");
        globalNodepool.queryMachines();

        this.schedulingUpdate = upd;

        //
        // Reset all counts, and adjust for the non-preemptable work.  Nodepool counts end up reflecting all
        // *potential* shares in the system.
        //
        resetNodepools();
        accountForNonPreemptable();

        //
        // Walk through the classes starting with highest priority and just pass out the resources.
        // On each iteration we pass the list of all classes at the same priority, to get scheduled
        // together.  We assume that we do NOT mix policy among priorities, enforced by the caller.
        //        
        for ( int i = 0; i < classes.length; i++ ) {
            @SuppressWarnings("unchecked")
                ArrayList<ResourceClass> rcs = (ArrayList<ResourceClass>) classes[i];
            findHowMuch(rcs);
        }
        
        //
        // Counts are now placed into all the class, user, and job structures.  We need to account for
        // all *actual* resources, so we can find free things more gracefully.
        //
        resetNodepools();        
        accountForNonPreemptable();
        accountForFairShare();
        
        // ////////////////////////////////////////////////////////////////////////////////////////////////////
        // And now, find real, physical resources.
        //
        // First pre-expansion of needy jobs, prioritized
        // Second, normal expandion of the scheduled work.
        //
        if ( logger.isTrace() ) {
            globalNodepool.queryMachines();
        }

        for ( int i = 0; i < classes.length; i++ ) {
            @SuppressWarnings("unchecked")
                ArrayList<ResourceClass> rcs = (ArrayList<ResourceClass>) classes[i];
            expandNeedyJobs(globalNodepool, rcs);  
        }

        for ( int i = 0; i < classes.length; i++ ) {
            @SuppressWarnings("unchecked")
                ArrayList<ResourceClass> rcs = (ArrayList<ResourceClass>) classes[i];
            findWhatOf(rcs);
        }
        //
        //
        // ////////////////////////////////////////////////////////////////////////////////////////////////////

//         //
//         // Ethnic cleansing of shares. We look at each subpool and force eviction of non-member
//         // shares, strictly according to counts, to prevent squatters from locking out legitimate
//         // members of the pool.
//         //
//         cleanNodepools(globalNodepool);
        doEvictions(globalNodepool);
        // doForcedEvictions(globalNodepool);

        //
        // If we've fragmented we may not have been able to make enough space to get everybody's fair
        // share startd.  Here we check to make sure everybody who is capped got at least what
        // the deserved, and if not, we go in more agressively against jobs that have more than
        // their "absolute" fair share.
        //
        if ( do_defragmentation ) {
            insureFullEviction();
        }

        //
        // At last, walk through all the jobs and place them into the scheduling update appropripriately
        //
        for ( int i = 0; i < classes.length; i++ ) {
            @SuppressWarnings("unchecked")
                ArrayList<ResourceClass> rcs = (ArrayList<ResourceClass>) classes[i];
            setSchedulingUpdate(rcs);
        }

        globalNodepool.resetPreemptables();                            // Reservations: preemptables are machines that are going to get cleared
                                                                       // for pendingreservations. preemptable machines do not get reset 
                                                                       // normally so we can save state across the scheduling phases.
    }

//     //
//     // Order entities by age,  oldest first
//     //
//     static private class EntityByAgeSorter
//         implements Comparator<IEntity>
//     {
//         public int compare(IEntity e1, IEntity e2)
//         {
//             if ( e1 == e2 ) return 0;

//             // remember older has lower time
//             return (int) (e1.getTimestamp() - e2.getTimestamp());
//         }
//     }


    static private class JobByTimeSorter
        implements Comparator<IRmJob>
    {
        public int compare(IRmJob j1, IRmJob j2)
        {
            if ( j1.equals(j2) ) return 0;

            if ( j1.getTimestamp() == j2.getTimestamp() ) {           // if tied on time (unlikely)
                return (j2.getShareOrder() - j1.getShareOrder());       // break tie on share order, decreasing
            }
            return (int) (j1.getTimestamp() - j2.getTimestamp());     // increasing time order
        }
    }


    //
    // Order classes by share weight, descending
    //
    static private class ClassByWeightSorter
        implements Comparator<ResourceClass>
    {
        public int compare(ResourceClass r1, ResourceClass r2)
        {
            if ( r1 == r2 ) return 0;

            return (r2.getShareWeight() - r1.getShareWeight());
        }
    }

    //
    // Treemap sorter, must never return 0
    //
    // Order classes by Q share wealth, descending
    //
    static private class UserByWealthSorter
        implements Comparator<User>
    {
        public int compare(User u1, User u2)
        {
            if ( u1.equals(u2) ) return 0;

            int w1 = u1.getShareWealth();
            int w2 = u2.getShareWealth();

            if ( w1 == w2 ) return -1;       // we don't ever want these guys to compare equal
                                             // unless the instances are the same, checked above
            return w2 - w1;
        }
    }

    //
    // Treemap sorter, must never return 0
    //
    // Order jobs by largest assigned q shares first
    //
    static private class JobByShareSorter
        implements Comparator<IRmJob>
    {
        public int compare(IRmJob j1, IRmJob j2)
        {
            if ( j1.equals(j2) ) return 0;       // same instances MUST return equal, 

            int s1 = j1.countNShares() * j1.getShareOrder();
            int s2 = j2.countNShares() * j2.getShareOrder();

            if ( s1 == s2 ) return -1;           // similar share count but never equal
            return ( s2 - s1 );
        }
    }

    //
    // Order jobs by "most able to be generous".
    //
    static private class FragmentationSorter
        implements Comparator<IRmJob>
    {
        //
        // Order by
        // a) most over pure fair share
        // b) most currently allocated
        //
        public int compare(IRmJob j1, IRmJob j2)
        {

            if ( j1.equals(j2) ) return 0;

            // pure fair-share
            int p1 = j1.getPureFairShare();    // qshares
            int p2 = j2.getPureFairShare();

            // actual current allocation
            int c1 = j1.countNShares() * j1.getShareOrder();  // to qshares
            int c2 = j2.countNShares() * j2.getShareOrder();

            // overage ...
            int o1 = Math.max(0, c1 - p1);
            int o2 = Math.max(0, c2 - p2);

            if ( o1 < o2 ) return  1;         // largest overage first
            if ( o1 > o2 ) return -1;

            if ( c1 < c2 ) return  1;         // largest allocation first
            if ( c1 > c2 ) return -1;

            // and the tie breaker, these guys are never allowed to compare equal unless the
            // instances are the same, and we don't care about the order if we get this far
            return -1;
        }
    }

    //
    // Order shares by
    // - pending (can be reassigned or dropped)
    // - investment
    //
    // Pending always sorts before not-pending.
    //
    // This is a sorter for a tree map so we have to be sure not to return equality unless the objects
    // are the same objects.
    //
//    static private class FinalEvictionSorter
//        implements Comparator<Share>
//    {
//        
//        public int compare(Share s1, Share s2)
//        {
//            if ( s1 == s2 ) return 0;
//
//            // pending shares first, no point expanding them if we don't have to
//            if ( s1.isPending() && s2.isPending() ) return -1;            
//            if ( s1.isPending() ) return -1;
//            if  (s2.isPending() ) return 1;
//
//            // Shares on machines with more space first, deal with defrag, which is why we're here
//            int vso1 = s1.getMachine().countFreedUpShares();
//            int vso2 = s2.getMachine().countFreedUpShares();
//
//            if ( vso1 != vso2 ) {
//                return vso2 - vso1;      // (more space first)
//            }
//
//            // All else being equal, use investment
//            int inv =  (int) (s1.getInvestment() - s2.getInvestment());  
//            if ( inv == 0 ) return -1;                  // careful not to return 0
//            return inv;
//        }
//    }

    //
    // Sort machines for defrag.
    // a) machines with richest users first
    // b) largest machine second
    //
    static private class EligibleMachineSorter
        implements Comparator<Machine>
    {
        
        public int compare(Machine m1, Machine m2)
        {
            if ( m1.equals(m2) ) return 0;       

            int m1wealth = 0;
            int m2wealth = 0;
            Map<Share, Share> sh1 = m1.getActiveShares();
            for ( Share s : sh1.values() ) {
                IRmJob j = s.getJob();
                User u = j.getUser();
                m1wealth = Math.max(m1wealth, u.getShareWealth());
            }
 
            Map<Share, Share> sh2 = m2.getActiveShares();
            for ( Share s : sh2.values() ) {
                IRmJob j = s.getJob();
                User u = j.getUser();
                m2wealth = Math.max(m2wealth, u.getShareWealth());
            }

            if ( m1wealth != m2wealth ) return m2wealth - m1wealth;       // richest uesr first

            long m1mem = m1.getMemory();
            long m2mem = m2.getMemory();

            if ( m1mem == m2mem ) return -1;       // for tree map, must not return 0 unless same object
            return (int) (m2mem - m1mem);          // largest machine first.
        }
    }

    //
    // Order shares by most wealthy owner - defrag
    // Orders by wealthiest owner first.
    //
    static private class ShareByWealthSorter
        implements Comparator<Share>
    {
        
        public int compare(Share s1, Share s2)
        {
            if ( s1.equals(s2) ) return 0;       

            int s1wealth = 0;
            int s2wealth = 0;

            IRmJob j1 = s1.getJob();
            User u1 = j1.getUser();
            s1wealth = u1.getShareWealth();

            IRmJob j2 = s2.getJob();
            User u2 = j2.getUser();
            s2wealth = u2.getShareWealth();

            return s2wealth - s1wealth;
        }
    }

}
