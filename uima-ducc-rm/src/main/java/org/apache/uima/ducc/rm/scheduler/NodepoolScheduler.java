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
import java.util.Collection;
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
    boolean use_global_allotment = true;
    int global_allotment = Integer.MAX_VALUE;
    int scheduling_quantum;

    NodepoolScheduler()   
    {
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

        fragmentationThreshold = SystemPropertyResolver.getIntProperty("ducc.rm.fragmentation.threshold", fragmentationThreshold);
        scheduling_quantum = SystemPropertyResolver.getIntProperty("ducc.rm.share.quantum", scheduling_quantum);
        do_defragmentation = SystemPropertyResolver.getBooleanProperty("ducc.rm.defragmentation", do_defragmentation);
        use_global_allotment = SystemPropertyResolver.getBooleanProperty("ducc.rm.use_global_allotment",  use_global_allotment);
        global_allotment = SystemPropertyResolver.getIntProperty("ducc.rm.global_allotment", global_allotment);
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
     * Check the allotment for the user, given that we want to allocate
     *    - nprocs new processes for
     *    - job j
     */
    boolean validSingleAllotment(IRmJob j)
    {
        String methodName = "validAllotment";
        //
        // Original design and implementation for UIMA-4275: class based.  Subsequent discussion resulted in
        // changing to a single global allotment.  I'll leave the class-based allotment in for now because
        // I suspect it will re-raise its ugly head. (jrc)
        //

        if ( use_global_allotment ) {
            User u = j.getUser();
            int lim = u.getOverrideLimit();
            if ( lim < 0 ) {
                lim = global_allotment;
            }

            int shares = u.countNPShares();
            long sharesInGB = ((shares + j.getShareOrder()) * scheduling_quantum);
            if ( sharesInGB > lim ) {
                schedulingUpdate.defer(j, "Deferred because allotment of " + lim + "GB is exceeded by user " + j.getUserName());
                logger.info(methodName, j.getId(), "Deferred because allotment of " + lim + "GB is exceeded by user " + j.getUserName());
                return false;
            }
        } else {
            ResourceClass rc = j.getResourceClass();
            if ( rc.allotmentExceeded(j) ) {
                schedulingUpdate.defer(j, "Deferred because allotment of " + rc.getAllotment(j) + "GB is exceeded by user " + j.getUserName());
                logger.info(methodName, j.getId(), "Deferred because allotment of " + rc.getAllotment(j) + "GB is exceeded by user " + j.getUserName());
                return false;
            }
        } 
        return true; 
    }

    /**
     * This returns the total number of PROCESSES we get to allocate for this job, including those
     * already given.
     */
    int getAllotmentForJob(IRmJob j)
    {
    	String methodName = "getAllotmentForJob";
        User u = j.getUser();
        
        // Let the job ask for the world.  This accounts for init cap, prediction, number usable, etc
        int order = j.getShareOrder();
        int wanted = j.getJobCap();                // in nshares
        logger.info(methodName, j.getId(), "Job cap", nSharesToString(wanted, order));
        
        // Find out how many qshares we're allowed
        int allotment_in_gb = u.getOverrideLimit();
        if ( allotment_in_gb < 0 ) {
            allotment_in_gb = global_allotment;
        }
        int user_allotment = allotment_in_gb / scheduling_quantum;     // qshares

        // How many qshares we, the user, have used
        int allocated = u.countNPShares();
        logger.info(methodName, j.getId(), "Current NP allocation for user", allocated, "qshares", (allocated * scheduling_quantum), "GB",
                    "user_allotment", user_allotment, "user_allotment in GB", allotment_in_gb );

        // This is how many QShares we get to allocate for the job
        int additional = Math.max(0, user_allotment - allocated);
        int additional_processes = additional / order;
        logger.info(methodName, j.getId(), "Additional Nshares allowed for request:", nSharesToString(additional_processes, order));

        // No shares, so we show deferred
        if ( (additional_processes == 0) ) {
            if (j.countNShares() == 0)  {
                // over allotment, never had anything, can get anything so is deferred
                schedulingUpdate.defer(j, "Deferred because allotment of " + allotment_in_gb + "GB is exceeded.");
                logger.info(methodName, j.getId(), "Deferred because allotment of",  allotment_in_gb, "GB is exceeded.");
            } else {
                logger.info(methodName, j.getId(), "Allotment of", allotment_in_gb, "GB caps request. Return with", allocated, "qshares allocated.");
            }
            return j.countNShares();
        }

        int allowed = j.countNShares() + additional_processes;

        if ( allowed < wanted ) {
            logger.info(methodName, j.getId(), "Capping job on allotment: ", allotment_in_gb + " GB. Remaining allowed nshares [",
                        allowed, "] wanted [", wanted, "]");
        }

        logger.info(methodName, j.getId(), "Allowed", nSharesToString(allowed, order), "wanted", nSharesToString(wanted, order));
        return Math.min(wanted, allowed);
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

    // /**
    //  * Return the nodepool for a class, or the global nodepool if none is explicitly associated with the class.
    //  * @deprecated Remove as soon as it is verified corrcct.
    //  */
    // NodePool getNodepool(ResourceClass rc)
    // {
    //     String id = rc.getNodepoolName();
    //     if ( id == null ) {
    //         return globalNodepool;
    //     }
    //     return globalNodepool.getSubpool(id);
    // }

    // ==========================================================================================
    // ==========================================================================================
    // =========================== REWORKED CODE FOR FAIR SHARE ==================================
    // ==========================================================================================
    // ==========================================================================================

    // /**
    //  * @deprecated - see rc.geMaxOrder();
    //  */
    // int calculateMaxJobOrder(ArrayList<ResourceClass> rcs)
    // {
    //     int max = 0;
    //     for ( ResourceClass rc: rcs ) {
    //         HashMap<Integer, HashMap<IRmJob, IRmJob>> jobs = rc.getAllJobsByOrder();
    //         for ( int i : jobs.keySet() ) {
    //             max= Math.max(max, i);
    //         }
    //     }
    //     return max;
    // }

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

    // UIMA-4275 Don't pass in total shares any more, we used only for class caps, which work differently now.
    protected void apportion_qshares(List<IEntity> entities, int[] vshares, String descr)
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
                    //int    tgiven = Math.min(mpr, (int) Math.floor(dgiven));                         // what is calculated, capped by what I alreay have
                    // UIMA-4275, floor to ciel.  Below with tgiven and rgiven we deal with ther emainder also.  The floor plus the residual below
                    //            seemed really agressive and some small allocation were working out to 0 when they shouldn't
                    int    tgiven = Math.min(mpr, (int) Math.ceil(dgiven));                          // what is calculated, capped by what I alreay have
                    int    cap = e.calculateCap();                                                   // get caps, if any, in qshares (simplified in UIMA-4275)
                    logger.trace(methodName, null, descr, "O", o, ":", e.getName(), "Before caps, given", tgiven, "cap", cap);

                    if ( gbo[0] >= cap ) {           // UIMA-4275
                        logger.trace(methodName, null, descr, "O", o, "Entity", e.getName(), "cap prevents further allocation.");
                        continue;
                    }

                    int    given = tgiven / o;                                                       // tentatively given, back to NShares
                    int    rgiven = tgiven % o;                                                      // residual - remainder
                    //int    twanted = wbo[0] + gbo[0];                                              // actual wanted: still wanted plus alredy given
                    // if ( twanted <= fragmentationThreshold ) {                                    // if under the defrag limit, round up
                    if ( (rgiven > 0) && ( given == 0) ) {
                        given = Math.min( ++given, nshares[o] );                                     // UIMA-3664
                    }
                    // }                                                                                // if not under the defrag limit, round down

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

            // Remove entities that have everything they want or could otherwise get
            Iterator<IEntity> iter = working.iterator();
            while ( iter.hasNext() ) {
                IEntity e = iter.next();
                if ( (e.getWantedByOrder()[0] == 0) || (e.getGivenByOrder()[0] >= e.calculateCap()) ) {      // UIMA-4275, checking fair-share cap
                    // logger.info(methodName, null, descr, e.getName(), "reaped, nothing more wanted:", fmtArray(e.getWantedByOrder()));
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
                    //logger.info(methodName, null, descr, e.getName(), "reaped, nothing more usablee:", fmtArray(e.getWantedByOrder()), "usable:",
                    //            fmtArray(nshares));
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
            logger.info(methodName, null, descr, "Final before bonus:");
            for ( IEntity e : entities ) {
                int[] gbo = e.getGivenByOrder();
                logger.info(methodName, null, descr, String.format("%12s %s", e.getName(), fmtArray(gbo)));
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
        //int     bonus = 0;
        while ( (nshares[1] > 0) && (given)) {
            given = false;
            for ( IEntity e : entities ) {
                //int[] wbo = e.getWantedByOrder();         // nshares
                int[] gbo = e.getGivenByOrder();          // nshares

                for ( int o = maxorder; o > 0; o-- ) {                
                    // the entity access its wbo, gbo, and entity-specific knowledge to decide whether
                    // the bonus is usable.  if so, we give out exactly one in an attempt to spread the wealth.
                    //
                    // An example of where you can't use, is a class over a nodepool whose resources
                    // are exhausted, in which case we'd loop and see if anybody else was game.
                    // UIMA-4065
                    while ( (e.canUseBonus(o) ) && (vshares[o] > 0) ) {
                        gbo[o]++;
                        gbo[0]++;
                        removeSharesByOrder(vshares, nshares, 1, o);
                        given = true;
                        break;
                    }
                }

                // UIMA-4605 - old 'bonus' code -- keep for a while as quick reference
                // for ( int o = maxorder; o > 0; o-- ) {                
                //     int canuse = wbo[o] - gbo[o];
                //     while ( (canuse > 0 ) && (vshares[o] > 0) ) {
                //         gbo[o]++;
                //         //bonus++;
                //         canuse = wbo[o] - gbo[o];
                //         removeSharesByOrder(vshares, nshares, 1, o);
                //         given = true;
                //         break;
                //     }
                // }
            }
        } 

        logger.debug(methodName, null, descr, "Final apportionment:");
        for ( IEntity e : entities ) {
            int[] gbo = e.getGivenByOrder();          // nshares
            logger.debug(methodName, null, descr, String.format("%12s gbo %s", e.getName(), fmtArray(gbo)));                
        }
        logger.debug(methodName, null, descr, "vshares", fmtArray(vshares));
        logger.debug(methodName, null, descr, "nshares", fmtArray(nshares));
          
        // if ( bonus > 0 ) {
        //     logger.debug(methodName, null, descr, "Final after bonus:");
        //     for ( IEntity e : entities ) {
        //         int[] gbo = e.getGivenByOrder();          // nshares
        //         logger.debug(methodName, null, descr, String.format("%12s %s", e.getName(), fmtArray(gbo)));                
        //     }
        //     logger.debug(methodName, null, descr, "vshares", fmtArray(vshares));
        //     logger.debug(methodName, null, descr, "nshares", fmtArray(nshares));
        // } else {
        //     logger.debug(methodName, null, descr, "No bonus to give.");
        // }
    }


    /**
     * Count out shares for only the jobs in the ResouceClasses here, and only from the given
     * nodepool.
     */
	protected void countClassShares(NodePool np, List<ResourceClass> rcs)
    { 		
		String methodName = "countClassShares";

        if ( logger.isDebug() ) {
            StringBuffer sb = new StringBuffer("Counting for nodepool ");
            sb.append(np.getId());
            sb.append(" - classes -");
            for ( ResourceClass rc : rcs ) {
                sb.append(" ");
                sb.append(rc.getName());
            }

            logger.debug(methodName, null, sb.toString());
        }
        // if ( true ) return;

        // pull the counts.  these don't get updated by the counting routines per-se.  after doing counting the np's are
        // expected to do the 'what-of' calculations that do acutall allocation and which update the counts
        int[] vshares = np.cloneVMachinesByOrder();

        ArrayList<IEntity> l = new ArrayList<IEntity>();
        l.addAll(rcs); 

        for ( IEntity e : l ) {
            e.initWantedByOrder((ResourceClass) e);
        }

        apportion_qshares((List<IEntity>) l, vshares, methodName);               // UIMA-4275, remove 'total'

        int sum_of_weights = 0;
        for ( ResourceClass rc : rcs ) {
            sum_of_weights += rc.getShareWeight(); // see next loop
        }

        //
        // If we have to do a "final eviction" because of fragmentation (evict stuff even though
        // it's below it's calcualted counts), we want to know the pure, unmodified fair share
        // for each rc.  Here is a great place to do that calculation.
        //
        for ( ResourceClass rc : rcs ) {
            int fair_share = (int) Math.floor(np.countTotalShares() * ( (double)  rc.getShareWeight() / sum_of_weights ));
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

        // if ( rc.getName().equals("urgent") ) {
        //     int stop_here = 1;
        //     stop_here++;
        // }

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
        apportion_qshares((List<IEntity>) l, vshares, methodName);                // UIMA-4275 remove 'total'

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

            apportion_qshares((List<IEntity>) l, vshares, methodName);     // UIMA-4275, remove "total"

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
     * Find the set of classes from the presented set of elibible classes that have jobs in
     * the given nodepool.  UIMA-4065
     *
     * @param np        Relevent nodepool
     * @param eligible  (Possibly restricted) set of classes that **might** have jobs in the nodepool
     * @return List of classes with jobs in the nodepool
     */
    private List<ResourceClass> gatherRcs(NodePool np, List<ResourceClass> eligible)
    {
        ArrayList<ResourceClass> ret = new ArrayList<ResourceClass>();
        String npn = np.getId();
        for ( ResourceClass rc : eligible ) {
            String rcnpn = rc.getNodepoolName();
            if ( rcnpn == null       ) continue;
            if ( rc.countJobs() == 0 ) continue;
            if ( rcnpn.equals(npn)   ) ret.add(rc);
        }
        return ret;        
    }

    /**
     * Do a depth-first traversal of the nodepool calculating counts for all the jobs in the nodepool and its children.
     *
     * Starting at each leaf NP, gather all the classes that have jobs in the NP, and if there are any, get class counts
     * for the classes.  Pass the classes up to the caller who can do a more global recount if necessary.  If there are
     * no jobs over the nodepool then bypass the count for it, of course.  This is a rewrite of the original which did
     * not properly handle the recursion past 1 level (root + 1).  It uses gatherRcs() as a helper to find relevent classes.
     * jrc 2014-11-05. UIMA-4605
     *
     * Note that this is tricky - please make sure you understand all the logic in countClassShares before changing
     * anything.
     * 
     * @param np
     * @param eligible
     * @return List of classes with potential counts.
     */
    protected List<ResourceClass> traverseNodepoolsForCounts(NodePool np, List<ResourceClass> eligible)
    {
        //String methodName = "traverseNodepoolsForCounts";

        List<ResourceClass> myRcs = gatherRcs(np, eligible);       // the resource classes for NodePool np
        boolean hasJobs = (myRcs.size() > 0);                      // do I have jobs for this np?

        List<NodePool> subpools = np.getChildrenAscending();       // now recurse down to leaves from here
        for ( NodePool subpool : subpools ) {
            List<ResourceClass> subrc = traverseNodepoolsForCounts(subpool, eligible);
            myRcs.addAll(subrc);
        }        

        // now do our fs, if there are jobs resident in this np
        if ( hasJobs ) countClassShares(np, myRcs);
        return myRcs;                                             // return aggregated classes to caller
    }

//     /**
//      * Depth-first traversal of the nodepool.  Once you get to a leaf, count the shares.  This sets an
//      * upper-bound on the number of shares a class can have.  As you wind back up the tree the counts may
//      * be reduced because of competition from jobs in the parent node.  By the time we're done we should have
//      * accounted for all jobs and all usable resources.
//      *
//      * Note how this works:
//      * Consider a configuration with two nodepools plus global, A, B, and G.  Suppose nodepools A and B have
//      * 30 shares each and G only has 10 shares.  G can apportion over it's 10, plus the 60 from A and B. So
//      * after apporioning over A and B we need to do fair-share over G+A+B to insure that jobs submitted
//      * to G are not "cheated" - recall that jobs in this set of classes have the same weight and priority,
//      * and thus the same "right" to all the shares.  However, allocating a job from class A over the
//      * full set of 10+30 shares could over-allocate it.  So the cap calculations must be sure never to
//      * increase the already-given shares for subpools.
//      *
//      * Therefore we traverse the FULL SET of classes on every recursion.  When calculating caps from
//      * apportion_shares the resource classes will have to account for multiple traversals and not over-allocate
//      * if a class has already been apportioned from a subpool.
//      *
//      * Keep for a while for reference.  It is wrong but if there are still bugs in the rewrite we
//      * want easy reference to the original. jrc 2014-11-05 UIMA-4065
//      */
//     protected void traverseNodepoolsForCounts(NodePool np, List<ResourceClass> rcs)
//     {
//         //HashMap<String, NodePool> subpools = np.getChildren();
//         List<NodePool> subpools = np.getChildrenAscending();
//         for ( NodePool subpool : subpools ) {
//             ArrayList<ResourceClass> cls = new ArrayList<ResourceClass>();
//             String npn = subpool.getId();
//             int njobs = 0;
//             for ( ResourceClass rc : rcs ) {
//                 String rcnpn = rc.getNodepoolName();
//                 if ( rcnpn == null ) continue;

//                 if ( rc.getNodepoolName().equals(npn) ) {
//                     cls.add(rc);
//                     njobs += rc.countJobs();
//                 }
//             }
//             if ( njobs > 0 ) {
//                 traverseNodepoolsForCounts(subpool, cls);
//             }
//         }

//         countClassShares(np, rcs);
//     }


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
            logger.info(methodName, null, "Scheduling FAIR SHARE for these classes:");
            logger.info(methodName, null, "   ", ResourceClass.getHeader());
            logger.info(methodName, null, "   ", ResourceClass.getDashes());
            for ( ResourceClass pc : rcs ) {
                logger.info(methodName, null, "   ", pc.toString());
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
        traverseNodepoolsForCounts(globalNodepool, eligible);   // (only these classes, not the global set)

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
                    j.undefer();         // can get message set during defrag
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

        List<IRmJob> fair_share_jobs = new ArrayList<IRmJob>();
        List<IRmJob> fixed_share_jobs = new ArrayList<IRmJob>();
        List<IRmJob> reservations = new ArrayList<IRmJob>();

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

            if ( rc.getNodepool() == np ) {
                switch ( rc.getPolicy()) {
                    case FAIR_SHARE:
                        fair_share_jobs.add(j);
                        break;
                    case FIXED_SHARE:
                        fixed_share_jobs.add(j);
                        break;
                    case RESERVE:
                        reservations.add(j);
                        break;
                }
                removeList.add(j);
            } 
        }
        
        for ( IRmJob j : removeList ) {
            needyJobs.remove(j);
        }


        // try to connect shares now
        Collections.sort(reservations, new JobByTimeSorter());
        logger.debug(methodName, null, "NP[", np.getId(), "Expand needy reservations.", listJobSet(reservations));
        for ( IRmJob j : reservations ) {
            ResourceClass rc = j.getResourceClass();
            np.findMachines(j, rc);
        }

        Collections.sort(fixed_share_jobs, new JobByTimeSorter());
        logger.debug(methodName, null, "NP[", np.getId(), "Expand needy fixed.", listJobSet(fixed_share_jobs));
        for ( IRmJob j : fixed_share_jobs ) {
            if ( np.findShares(j, false) > 0 ) {
                //
                // Need to fix the shares here, if any, because the findShares() code is same for fixed and fair share so it
                // won't have done that yet.
                //
                for ( Share s : j.getPendingShares().values() ) {
                    s.setFixed();
                    logger.info(methodName, j.getId(), "Assign:", s);
                }
            }
        }
 
        Collections.sort(fair_share_jobs, new JobByTimeSorter());
        logger.debug(methodName, null, "NP[", np.getId(), "Expand needy jobs.", listJobSet(fair_share_jobs));
        np.doExpansion(fair_share_jobs);

        logger.trace(methodName, null, "Exit : needyJobs.size =", needyJobs.size());
    }

    // private static int stop_here_dx = 0;
    protected void traverseNodepoolsForExpansion(NodePool np, ArrayList<ResourceClass> rcs)
    {
		String methodName = "traverseNodepoolsForExpansion";
        // HashMap<String, NodePool> subpools = np.getChildren();
        List<NodePool> subpools = np.getChildrenAscending();

        StringBuffer sb = new StringBuffer();
        for ( NodePool sp : subpools ) {
            sb.append(sp.getId());
            sb.append(" ");
        }
        logger.info(methodName, null, np.getId(), "Doing expansions in this order:", sb.toString());

        for ( NodePool subpool : subpools ) {
            traverseNodepoolsForExpansion(subpool, rcs);
        }

        // logger.info(methodName, null, "--- stop_here_dx", stop_here_dx);
        // if ( stop_here_dx == 13 ) {
        //     @SuppressWarnings("unused")
		// 	int stophere;
        //     stophere=1;
        // }
        // stop_here_dx++;

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
    //static int stophere = 0;
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
        // logger.trace(methodName, null, "Machine occupancy before expansion", stophere++);
        // if ( stophere == 7 ) {
        //     @SuppressWarnings("unused")
		// 	int stophere;
        //     stophere = 1 ;
        // }
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
    void howMuchFixed(ArrayList<ResourceClass> rcs)
    {
    	String methodName = "howMuchFixed";

        if ( logger.isTrace() ) {
            logger.info(methodName, null, "Scheduling FIXED SHARE for these classes:");
            logger.info(methodName, null, "   ", ResourceClass.getHeader());
            logger.info(methodName, null, "   ", ResourceClass.getDashes());
            for ( ResourceClass pc : rcs ) {
                logger.info(methodName, null, "   ", pc.toString());
            }
        }

        int total_jobs = 0;
        for ( ResourceClass rc : rcs ) {
            HashMap<IRmJob, IRmJob> jobs = rc.getAllJobs();
            total_jobs += jobs.size();
        }
        if ( total_jobs == 0 ) {
            return;
        }

        for ( ResourceClass rc : rcs ) {
            ArrayList<IRmJob> jobs = rc.getAllJobsSorted(new JobByTimeSorter());            

            for ( IRmJob j : jobs ) {
                logger.info(methodName, j.getId(), "Scheduling job to class:", rc.getName());

                j.clearShares();                               // reset virtual shares at start of each schedling cycle
                j.undefer();                                   // in case it works this time!

                switch ( j.getDuccType() ) {
                    case Job:
                        countFixedForJob(j, rc);
                        break;
                    case Service:
                    case Pop:
                    case Reservation:
                    default:
                        countSingleFixedProcess(j, rc);
                        break;
                }
            }            
        }
    }

    
    void countFixedForJob(IRmJob j, ResourceClass rc)
    {
        String methodName = "countFixedForJob";

        logger.info(methodName, j.getId(), "Counting shares for", j.getShortType() + "." + j.getId());

        // Allowed something if we get here.  Must see if we have something to give.
        NodePool np = rc.getNodepool();

        int order = j.getShareOrder();
        int available = np.countLocalNSharesByOrder(order);
        logger.info(methodName, j.getId(), "available shares of order", order, "in np:", available);

        if ( available == 0 ) {
            if (j.countNShares() == 0)  {
                if ( np.countFixable(j) > 0 ) {
                    schedulingUpdate.defer(j, "Deferred because insufficient resources are availble.");
                    logger.info(methodName, j.getId(), "Deferring, insufficient shares available. NP", np.getId(), 
                                "available[", np.countNSharesByOrder(order), "]");
                } else {
                    schedulingUpdate.defer(j, "Deferred because no hosts in class " + rc.getName() + " have sufficient memory to accomodate the request.");
                    logger.info(methodName, j.getId(), "Deferring, no machines big enough for the request. NP", np.getId(), 
                                "available[", np.countNSharesByOrder(order), "]");
                }
                return;
            } else {
                logger.info(methodName, j.getId(), "Nodepool is out of shares: NP", np.getId(), 
                            "available[", np.countNSharesByOrder(order), "]");
            }
        }

        int granted = getAllotmentForJob(j); // in nshares, processes

        //
        // The job passes; give the job a count
        //
        logger.info(methodName, j.getId(), "+++++ nodepool", np.getId(), "class", rc.getName(), "order", order, "shares", nSharesToString(granted, order));
        int[] gbo = NodePool.makeArray();
        gbo[order] = granted;                      // what we get
        j.setGivenByOrder(gbo);
        
        // The difference between what we pass to 'what of', and what we already have.  The shares we already have are accounted
        // for in a special step at the start of the scheduling round.
        np.countOutNSharesByOrder(order, granted - j.countNShares());
    }

    void countSingleFixedProcess(IRmJob j, ResourceClass rc)
    {
        String methodName = "countSingleFixedProcess";


        logger.info(methodName, j.getId(), "Counting shares for", j.getShortType() + "." + j.getId(), "in class", rc.getName());
        NodePool np = rc.getNodepool();

        if ( j.isCompleted() ) {
            return;
        }

        if ( j.countNShares() > 0 ) {                  // only 1 allowed, UIMA-4275
            // already accounted for as well, since it is a non-preemptable share
            logger.info(methodName, j.getId(), "[stable]", "assigned", j.countNShares(), "processes, ", 
                        (j.countNShares() * j.getShareOrder()), "QS");
            int[] gbo = NodePool.makeArray();
            
            gbo[j.getShareOrder()] = 1;                // must set the allocation so eviction works right
            j.setGivenByOrder(gbo);
            return;
        }
        
        int order = j.getShareOrder();
        
        //
        // Now see if we have sufficient shares in the nodepool for this allocation.
        //
        if ( np.countLocalNSharesByOrder(order) == 0 ) {            
            if (np.countFixable(j) > 0 ) {
                schedulingUpdate.defer(j, "Deferred  because insufficient resources are availble.");
                logger.info(methodName, j.getId(), "Deferring, insufficient shares available. NP", np.getId(), "available[", np.countNSharesByOrder(order), "]");
                
            } else {
                schedulingUpdate.defer(j, "Deferred because no hosts in class " + rc.getName() + " have sufficient memory to accomodate the request.");
                logger.info(methodName, j.getId(), "Deferring, no machines big enough for the request. NP", np.getId(), 
                            "available[", np.countNSharesByOrder(order), "]");
            }
            return;
        }
        
        //
        // Make sure this allocation does not blow the allotment cap.
        //
        if ( ! validSingleAllotment(j) ) return;        // this method will defer the job and log it
        
        //
        // The job passes.  Assign it a count and get on with life ...
        //
        logger.info(methodName, j.getId(), "+++++ nodepool", np.getId(), "class", rc.getName(), "order", order, "shares", nSharesToString(1, order));
        int[] gbo = NodePool.makeArray();
        gbo[order] = 1;
        j.setGivenByOrder(gbo);
        
        np.countOutNSharesByOrder(order, 1);
    }

    /**
     * If there are free shares of the right order just assign them.  Otherwise
     * the counts will cause evictions in lower-priority code so we just wait.
     */
    protected void whatOfFixedShare(ArrayList<ResourceClass> rcs)
    {
    	String methodName = "whatOfFixedShare";
        for ( ResourceClass rc : rcs ) {
            ArrayList<IRmJob> jobs = rc.getAllJobsSorted(new JobByTimeSorter());

            NodePool np = rc.getNodepool();
            for ( IRmJob j : jobs ) {
                if ( j.countNShares() == j.countNSharesGiven() ) {  // got what we need, we're done
                    continue;
                }

                if ( j.isRefused() ) {                      // bypass jobs that we know can't be allocated. unlikely after UIMA-4275.
                    continue;
                }

                if ( j.isDeferred() ) {                     // UIMA-4275 - still waiting for an allocation
                    continue;
                }

                if ( j.isCompleted() ) {                    // UIMA-4327 - reinstated, if this gets set we aren't allowed to expand any more
                    continue;
                }

                int order = j.getShareOrder();
                int count = j.countNSharesGiven();

                if ( np.findShares(j) > 0 ) {               // UIMA-4275, no longer require full allocation, we'll take what we can
                    //
                    // Need to fix the shares here, if any, because the findShares() code is same for fixed and fair share so it
                    // won't have done that yet.
                    //
                    for ( Share s : j.getPendingShares().values() ) {
                        s.setFixed();
                    }
                    logger.info(methodName, j.getId(), "Assign:", nSharesToString(count, order));
                }

                // 
                // If nothing assigned we're waiting on preemptions which will occur naturally, or by forcible eviction of squatters,
                // or defrag.
                //
                if ( j.countNShares() == 0 ) {
                    j.setReason("Waiting for preemptions.");
                }                
            }
        }
    }

    // ==========================================================================================
    // ==========================================================================================
    // =========================== REWORKED CODE FOR RESERVATIONS ================================
    // ==========================================================================================
    // ==========================================================================================
	private void howMuchReserve(ArrayList<ResourceClass> rcs)
    {
        String methodName = "howMuchreserve";

        if ( logger.isTrace() ) {
            logger.info(methodName, null, "Calculating counts for RESERVATION for these classes:");
            logger.info(methodName, null, "   ", ResourceClass.getHeader());
            logger.info(methodName, null, "   ", ResourceClass.getDashes());
            for ( ResourceClass pc : rcs ) {
                logger.info(methodName, null, "   ", pc.toString());
            }
        }

        int total_jobs = 0;
        for ( ResourceClass rc : rcs ) {
            HashMap<IRmJob, IRmJob> jobs = rc.getAllJobs();
            total_jobs += jobs.size();
        }
        if ( total_jobs == 0 ) {
            return;
        }

        for ( ResourceClass rc : rcs ) {
            ArrayList<IRmJob> jobs = rc.getAllJobsSorted(new JobByTimeSorter());

            // Now pick up the work that can  be scheduled, if any
            for ( IRmJob j : jobs) {
                j.clearShares();                               // reset shares assigned at start of each schedling cycle
                j.undefer();                                   // in case it works this time!

                switch ( j.getDuccType() ) {
                    case Job:
                        countReservationForJob(j, rc);
                        break;
                    case Service:
                    case Pop:
                    case Reservation:
                    default:
                        countSingleReservation(j, rc);
                        break;
                }
            }            
        }
    }

    void countReservationForJob(IRmJob j, ResourceClass rc)
    {
        String methodName = "countReservationForJob";

        logger.info(methodName, j.getId(), "Counting full machines for", j.getShortType() + "." + j.getId());

        // Allowed something if we get here.  Must see if we have something to give.
        NodePool np = rc.getNodepool();

        // Are there any machines of the right size in the NP that can be reserved for this job?
        int available = np.countReservables(j);
        if ( available == 0 ) {
            if (j.countNShares() == 0)  {
                schedulingUpdate.defer(j, "Deferred because there are no hosts of the correct size in class " + rc.getName());
                logger.info(methodName, j.getId(), "Deferred because no hosts of correct size. NP", np.getId());
                           
            } else {
                logger.info(methodName, j.getId(), "Nodepool is out of shares: NP", np.getId());
            }
            return;
        }

        int granted = getAllotmentForJob(j);           // total allowed, including those already scheduled
   
        int needed = granted - j.countNShares();                     // additional shares
        int freeable = 0;
        if ( needed > 0 ) {
            freeable = np.countFreeableMachines(j, needed);         // might schedule evictions
            if ( (freeable + j.countNShares()) == 0 ) {
                schedulingUpdate.defer(j, "Deferred because resources are exhausted."); 
                logger.warn(methodName, j.getId(), "Deferred because resources are exhausted in nodepool " + np.getId());
                return;
            }
        }

        //
        // The job passes; give the job a count
        //
        logger.info(methodName, j.getId(), "Request is granted a machine for reservation.");
        int[] gbo = NodePool.makeArray();
        int order = j.getShareOrder();     // memory, coverted to order, so we can find stuff        
        gbo[order] = freeable + j.countNShares(); // account for new stuff plus what it already has
        j.setGivenByOrder(gbo);

    }

    void countSingleReservation(IRmJob j, ResourceClass rc)
    {
        String methodName = "countSingleReservation";

        logger.info(methodName, j.getId(), "Counting shares for", j.getShortType() + "." + j.getId(), "in class", rc.getName());
        NodePool np = rc.getNodepool();

        if ( j.countNShares() > 0 ) {
            logger.info(methodName, j.getId(), "[stable]", "assigned", j.countNShares(), "processes, ", 
                        (j.countNShares() * j.getShareOrder()), "QS");

            int[] gbo = NodePool.makeArray();

            gbo[j.getShareOrder()] = 1;         // UIMA4275 - only one
            j.setGivenByOrder(gbo);            
            return;
        }

        //
        // Make sure this allocation does not blow the allotment cap.
        //
        if ( ! validSingleAllotment(j) ) return;   // defers and logs 

        if ( np.countReservables(j) == 0 ) {
            schedulingUpdate.defer(j, "Deferred because there are no hosts of the correct size in class " + rc.getName());
            logger.warn(methodName, j.getId(), "Deferred because requested memory " + j.getMemory() + " does not match any machine.");
            return;
        }

        if ( np.countFreeableMachines(j, 1) == 0 ) {         // might also schedule preemptions
            schedulingUpdate.defer(j, "Deferred because resources are exhausted."); 
            logger.warn(methodName, j.getId(), "Deferred because resources are exhausted in nodepool " + np.getId());
            return;
        }
                
        logger.info(methodName, j.getId(), "Request is granted a machine for reservation.");
        int[] gbo = NodePool.makeArray();
        int order = j.getShareOrder();     // memory, coverted to order, so we can find stuff        
        gbo[order] = 1;
        j.setGivenByOrder(gbo);
        
    }

    /**
     */
	private void whatOfReserve(ArrayList<ResourceClass> rcs)
    {
        String methodName = "whatOfToReserve";
        for ( ResourceClass rc : rcs ) {
            NodePool np = rc.getNodepool();

            ArrayList<IRmJob> jobs = rc.getAllJobsSorted(new JobByTimeSorter());
            for ( IRmJob j: jobs ) {

                if ( j.isRefused() ) {                   // bypass jobs that we know can't be allocated
                    continue;
                }

                if ( j.isDeferred() ) {                  // counts don't work, we can't do this yet
                    continue;
                }

                if ( j.isCompleted() ) {                 // UIMA-4327 - reinstated, if this gets set we aren't allowed to expand any more
                    continue;
                }

                try {
                    np.findMachines(j, rc);
                } catch (Exception e) {
                    logger.error(methodName, j.getId(), "Reservation issues:", e);
                    continue;
                }

                // 
                // Either shares were assigned or not.  If not we wait for evictions, otherwise it is
                // fully allocated. Nothing more to do here.
                //
                if ( j.countNShares() == 0 ) {
                    j.setReason("Waiting for preemptions.");
                }
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
               case RESERVE:               
                   {
                       NodePool np = rc.getNodepool();
                       HashMap<IRmJob, IRmJob> jobs = rc.getAllJobs();
                       for ( IRmJob j : jobs.values() ) {
                           HashMap<Share, Share> shares = j.getAssignedShares();
                           np.accountForShares(shares);
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
                NodePool np = rc.getNodepool();
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
    // private static int stop_here_de = 0;
    protected void doEvictions(NodePool nodepool)
    {
    	String methodName = "doEvictions";
    	
        // logger.info(methodName, null, "--- stop_here_de", stop_here_de);
        // if ( stop_here_de == 7 ) {
        //     @SuppressWarnings("unused")
		// 	int stophere;
        //     stophere=1;
        // }
        // stop_here_de++;

        for ( NodePool np : nodepool.getChildrenDescending() ) {   // recurse down the tree
            logger.info(methodName, null, "Recurse to", np.getId(), "from", nodepool.getId());
            doEvictions(np);                                       // depth-first traversal
            logger.info(methodName, null, "Return from", np.getId(), "proceeding with logic for", nodepool.getId());
        }

        int neededByOrder[] = NodePool.makeArray();         // for each order, how many N-shares do I want to add?
        // int total_needed = 0;

        Map<IRmJob, Integer> overages = new HashMap<IRmJob, Integer>();        // UIMA-4275
        for ( ResourceClass cl : resourceClasses.values() ) {
            if ( cl.getNodepoolName().equals(nodepool.getId()) && (cl.getAllJobs().size() > 0) ) {
                HashMap<IRmJob, IRmJob> jobs = cl.getAllJobs();
                String npn = cl.getNodepoolName();
                logger.info(methodName, null, String.format("%12s %7s %7s %6s %5s", npn, "Counted", "Current", "Needed", "Order"));

                for ( IRmJob j : jobs.values() ) {
                    int counted = j.countNSharesGiven();      // allotment from the counter
                    int current = j.countNShares();           // currently allocated, plus pending, less those removed by earlier preemption
                    int needed = (counted - current);
                    int order = j.getShareOrder();
         
                    // Why abs and not max?  Because if needed > 0, that's shares we need to make space for.
                    //                               if needed < 0, that's shares we need to dump because the
                    //                                              counts say so.
                    //                               if needed == 0 then clearly nothing

                    if ( needed < 0 ) {
                        // UIMA-4275 these guys must be forced to shrink
                        overages.put(j, -needed);
                    } else {
                        // needed = Math.abs(needed); 
                        // needed = Math.max(0, needed);
                        
                        logger.info(methodName, j.getId(), String.format("%12s %7d %7d %6d %5d", npn, counted, current, needed, order));
                        neededByOrder[order] += needed;
                        // total_needed += needed;
                    }
                }
            }
    
        }

        // Every job in overages is required to lose the indicated number of share.  If this is done optimally it
        // will leave suficcient space for the counted shares of all the expansions.  Therein lies the rub.
        //
        // The older code below does its best to make space for the 'needed' array but it fails to fully evict
        // an over-deployed job in a number of situations.  The loop here is going to rely on defragmentation, which
        // we did not have originally to do final cleanup.  The job will be asked to dump its extra processes according
        // to the mechanism in its shrinkBy() method.  See that method for details.
        // UIMA-4275
        for (IRmJob j : overages.keySet()) {
            j.shrinkBy(overages.get(j));
        }

        // First we try to make enough space in the right places for under-allocation jobs
        // logger.debug(methodName, null, nodepool.getId(),  "NeededByOrder before any eviction:", Arrays.toString(neededByOrder));        
        // if ( (nodepool.countOccupiedShares() > 0) && (total_needed > 0) ) {
        //     nodepool.doEvictionsByMachine(neededByOrder, false);


    }

    /**
     * Determine if a candidate share can or cannot be transferred (eventually) to a needy job based on nodepool constraints.
     */
    boolean compatibleNodepools(Share candidate, IRmJob needy)
    {
        Machine m = candidate.getMachine();
        ResourceClass nrc = needy.getResourceClass();
        NodePool np = nrc.getNodepool();

        return np.containsMachine(m);           // can we get to the candidate share from 'needy's np?
    }

    // /**
    //  * Discover whether the potential job is able or unable to supply shares to a needy job because of nodepool restrictions.
    //  */
    // boolean compatibleNodepools(IRmJob potential, IRmJob needy)
    // {
    //     ResourceClass prc = potential.getResourceClass();
    //     ResourceClass nrc = needy.getResourceClass();

    //     NodePool pp = prc.getNodepool();
    //     NodePool np = nrc.getNodepool();

    //     return np.containsSubpool(pp) || pp.containsSubpool(np);
    // }

    /**
     * Discover whether the potential resource class is able or unable to supply shares to a jobs in a needy class because of nodepool restrictions.
     */
    boolean compatibleNodepools(ResourceClass potential, IRmJob needy)
    {
        ResourceClass nrc = needy.getResourceClass();

        NodePool pp = potential.getNodepool();
        NodePool np = nrc.getNodepool();

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
    int takeFromTheRich(IRmJob nj, 
                        int needed,
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

        Map<IRmJob,  IRmJob>  candidateJobs    = new HashMap<IRmJob,  IRmJob>();
        Map<Machine, Machine> eligibleMachines = new TreeMap<Machine, Machine>(new EligibleMachineSorter());

        for ( TreeMap<IRmJob, IRmJob> jobs : jobs_by_user.values() ) {
            candidateJobs.putAll(jobs);
        }

        int given = 0;
        int orderNeeded = nj.getShareOrder();
        
        ResourceClass cl     = nj.getResourceClass();               // needy job's resource class
        String        npname = cl.getNodepoolName();                // name of the class
        NodePool      np     = globalNodepool.getSubpool(npname);   // job's nodepool
        Map<Node, Machine> machines = np.getAllMachines();          // everything here is a candidate, nothing else is
                                                                    //   this is the machines in the pool, and all the
                                                                    //   subpools

        // Here we filter all the machines looking for machines that *might* be able to satisfy the defrag.  At the 
        // end this set of machines is eligbleMachines.
        machine_loop : 
            for ( Machine m : machines.values() ) {

                if ( m.getShareOrder() < orderNeeded ) {                // nope, too small
                    logger.debug(methodName, nj.getId(), "Bypass ", m.getId(), ": too small for request of order", orderNeeded); 
                    continue;
                }

                // if the job is a reservation the machine size has to matchm and machine must be clearable
                if ( nj.getSchedulingPolicy() == Policy.RESERVE ) {
                    if ( m.getShareOrder() != orderNeeded ) {
                        logger.debug(methodName, nj.getId(), "Bypass ", m.getId(), ": RESERVE policy requires exact match for order", orderNeeded);
                        continue;
                    }
                    // machine must be clearable as well
                    Collection<Share> shares = m.getActiveShares().values();
                    for ( Share s : shares ) {
                        if ( ! candidateJobs.containsKey(s.getJob()) ) {
                            logger.debug(methodName, nj.getId(), "Bypass ", m.getId(), ": for reservation, machine contains non-candidate job", s.getJob().getId());
                            continue machine_loop;
                        }
                    }                
                }

                Map<Share, Share> as = m.getActiveShares();            // everything alloacated here
                int g = m.getVirtualShareOrder();                      // g is space that we might be able to make after defrag:
                //    free space + freeable-from-candidates
                for ( Share s : as.values() ) {
                    IRmJob j = s.getJob();
                    if ( s.isForceable() && candidateJobs.containsKey(j) ) {  // evictable, and a candidate for reclamation by defrag
                        g += j.getShareOrder();
                    }
                }

                if ( g >= orderNeeded ) {                              // if it's usable by the job, it's a candidate
                    logger.info(methodName, nj.getId(), "Candidate machine:", m.getId());
                    eligibleMachines.put(m, m);
                } else {
                    logger.info(methodName, nj.getId(), "Not a candidate, insufficient free space + candidate shares:", m.getId());
                }
            }
        
        // Now eligibleMachines is the set of candidate machines for defrag

        logger.info(methodName, nj.getId(), "Found", eligibleMachines.size(), "machines to be searched in this order:");
        StringBuffer buf = new StringBuffer();
        for ( Machine m : eligibleMachines.keySet() ) {
            buf.append(m.getId());
            buf.append(" ");
            }
        logger.info(methodName, nj.getId(), "Eligible machines:", buf.toString());

        // first part done, we know where to look.

        // Now just bop through the machines to see if we can get anything for this specific job (nj)
        int given_per_round = 0;
        do {
            int g = 0;
            given_per_round = 0;
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

                g = m.getVirtualShareOrder();         // ( free space at this point )
                List<Share> potentialShares     = new ArrayList<Share>();
                for ( Share s : sh ) {
                    IRmJob j = s.getJob();
                    // User u = j.getUser();
                    
                    if ( s.isForceable() ) {
                        if ( candidateJobs.containsKey(j) ) {
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

                // and also must track how many processes we made space for
                given = given + (g / orderNeeded);    // at least one,or else we have a bug 
                logger.debug(methodName, nj.getId(), "LOOPEND: given[", given, "] g[", g, "] orderNeeded[", orderNeeded, "]");
            }
            logger.debug(methodName, nj.getId(), "Given_per_round", given_per_round, "given", given, "needed", needed);
        } while ( (given_per_round > 0) && ( given < needed ));

        // Sometimes we can directly reassign a share, in which case the job isn't waiting any more.
        // We only care about setting a message if the poor thing is still totally starved of resources.
        if ( nj.countNShares() == 0 ) {
            nj.setReason("Waiting for defragmentation.");
        }

        return given;
    }

    void doFinalEvictions(HashMap<IRmJob, Integer> needy)
    {
    	String methodName = "doFinalEvictions";

        for ( IRmJob j : needy.keySet() ) {
            logger.debug(methodName, j.getId(), "Will attempt to have space made for", needy.get(j), "processes");
        }

        //
        // Search for candidate donors and order by "most able to be generous".  Nodepools must be compatible.
        //
        // If prioritiy of needy is same or better, the cCandidates must not be needy, must be initialized already, 
        //     and have sufficient shares to give.
        // 
        // If priority of needy is better we keep track of the rich vs the poor jobs and possibly perform a second
        //     pass that includes poor jobs, if we can't get enougg from the rich.
        //
        for ( IRmJob nj : needy.keySet() ) {
            int priority_needy = nj.getSchedulingPriority();
            TreeMap<IRmJob, IRmJob> rich_candidates = new TreeMap<IRmJob, IRmJob>(new FragmentationSorter());  // first class candidates, they're rich and available
            TreeMap<IRmJob, IRmJob> poor_candidates = new TreeMap<IRmJob, IRmJob>(new FragmentationSorter());  // clearing for better priority job, we only use this if it's
                                                                                                               // impossible to clear from the rich candidates

            for ( ResourceClass rc : resourceClasses.values() ) {
                
                if ( rc.getPolicy() == Policy.RESERVE )     continue;          // exempt from preemption
                if ( rc.getPolicy() == Policy.FIXED_SHARE ) continue;          // exempt from preemption

                if ( ! compatibleNodepools(rc, nj) ) {
                    logger.debug(methodName, nj.getId(), "Skipping class", rc.getName(), "vs job class", nj.getResourceClass().getName(), "because of incompatible nodepools.");
                    continue;
                }

                int priority_candidate = rc.getPriority();
                boolean use_expanded_pool = false;      // better priority job is allowed to look at poor jobs if can't be satisfied from the rich

                if ( priority_needy > priority_candidate ) {  // Greater means worse 
                    logger.debug(methodName, nj.getId(), "Jobs in class", rc.getName(), "are not candidates because better priority: [", 
                                 priority_candidate, "vs", priority_needy, "]");
                    continue;
                }

                if ( priority_needy < priority_candidate ) {   // less means better
                    logger.debug(methodName, nj.getId(), "Needy job has better priority than jobs in class", rc.getName(), "[", 
                                 priority_candidate, "vs", priority_needy, "]. Using expanded pool.");
                    use_expanded_pool = true;
                }

                HashMap<IRmJob, IRmJob> jobs = rc.getAllJobs();
                for ( IRmJob j : jobs.values() ) {
                    int nshares = j.countNShares();
                    int qshares = nshares * j.getShareOrder();

                    if ( nshares == 0 ) {
                        logger.debug(methodName, nj.getId(), "Job", j.getId(), "is not a candidate because it has no share.");
                        continue;
                    } 

                    if ( needy.containsKey(j) ) {
                        if ( use_expanded_pool ) {
                            logger.debug(methodName, nj.getId(), "Job", j.getId(), "is a backup candidate because it's needy.");
                            poor_candidates.put(j, j);
                        } else {
                            logger.debug(methodName, nj.getId(), "Job", j.getId(), "is a not a candidate because it's needy.");
                        }
                        continue;
                    }
                    
                    if ( ! j.isInitialized() ) {
                        if ( use_expanded_pool ) {
                            logger.debug(methodName, nj.getId(), "Job", j.getId(), "is a backup candidate because it's not initialized yet.");
                            poor_candidates.put(j, j);
                        } else {
                            logger.debug(methodName, nj.getId(), "Job", j.getId(), "is not a candidate because it's not initialized yet.");
                        }
                        continue;
                    }
                    
                    if ( nshares < fragmentationThreshold ) {
                        if ( use_expanded_pool ) {
                            logger.debug(methodName, nj.getId(), "Job", j.getId(), "is a backup candidate because below frag threshold. nshares[", nshares, "] qshares[", qshares, "] threshold[", fragmentationThreshold, "]");
                            poor_candidates.put(j, j);
                        } else {
                            logger.debug(methodName, nj.getId(), "Job", j.getId(), "is not a candidate because below frag threshold. nshares[", nshares, "] qshares[", qshares, "] threshold[", fragmentationThreshold, "]");
                        }
                        continue;
                    }

                    logger.debug(methodName, nj.getId(), "Job", j.getId(), "is a candidate with processes[", nshares, "] qshares[", qshares, "]");
                    rich_candidates.put(j, j);
                }
            } // End search for candidate donors

            //
            // Here start looking at 'needy' and trying to match them agains the candidates
            //
            HashMap<User, TreeMap<IRmJob, IRmJob>> jobs_by_user = new HashMap<User, TreeMap<IRmJob, IRmJob>>();  // use this to track where the wealth originatses
            TreeMap<User, User> users_by_wealth = new TreeMap<User, User>(new UserByWealthSorter());             // orders users by wealth

            collectWealth(rich_candidates, users_by_wealth, jobs_by_user);

            int needed = needy.get(nj);      // this was adjusted to a reasonable level in the caller
            logger.debug(methodName, nj.getId(), "Needy job looking for", needed, "more processes of O[", nj.getShareOrder(), "]");

            //
            // Try stealing shares from the "rich" candidates first.
            //
            needed -= takeFromTheRich(nj, needed, users_by_wealth, jobs_by_user);
            if ( needed <= 0 ) {
                // This can go <0 if total space freed + unused space on a node adds up to >1 share.
                // It's slimplest to just not sweat it and call it satisfied.
                logger.info(methodName, nj.getId(), "Satisfied needs of job by taking from the rich.");
                continue;
            }

            //
            // The needy job had sufficient priority that be built up a list of emergency-backup jobs to evict.
            //
            if ( poor_candidates.size() > 0) {
                logger.info(methodName, nj.getId(), "Could not clear sufficient space from rich candidates.  Retrying with all candidates.");
                jobs_by_user.clear();
                users_by_wealth.clear();
                rich_candidates.putAll(poor_candidates);
                collectWealth(rich_candidates, users_by_wealth, jobs_by_user);

                needed -= takeFromTheRich(nj, needed, users_by_wealth, jobs_by_user);
                if ( needed <= 0 ) {
                    // This can go <0 if total space freed + unused space on a node adds up to >1 share.
                    // It's slimplest to just not sweat it and call it satisfied.
                    logger.info(methodName, nj.getId(), "Satisfied needs of job by taking from all candidates.");
                    continue;
                }
            }
            logger.info(methodName, nj.getId(), "Could not get enough from the rich. Asked for", needy.get(nj), "still needing", needed);
            nj.setReason("Waiting for defragmentation.");
        }
    }

    
    void collectWealth(TreeMap<IRmJob, IRmJob> candidates, TreeMap<User, User> users_by_wealth, HashMap<User, TreeMap<IRmJob, IRmJob>> jobs_by_user)
    {
        // Candidates are ordered by the FragmentationSorter
        //   - most over pure fair share
        //   - hten most currently allocated

        // user_by_wealth is ordered by the UserByWealthSorter
        //   - ordered by most wealth - actual qshares over all jobs

        //
        // Collect total wealth and order the wealthy by spondulix
        //
        HashMap<User, Integer> shares_by_user = new HashMap<User, Integer>();                                // use this to track user's wealth
        
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
        
        // and tracks their fat jobs
        for ( User u : shares_by_user.keySet() ) {
            u.setShareWealth(shares_by_user.get(u));       // qshares
            users_by_wealth.put(u, u);
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
        String headerfmt = "%14s %20s %6s %4s %7s %6s %2s";
        String datafmt   = "%14s %20s %6d %4d %7d %6d %2d";

        for ( ResourceClass rc : resourceClasses.values() ) {
            // Next: Look at every job and work out its "need".  Collect jobs by nodepool into the jobmaps.
            Map<IRmJob, IRmJob> allJobs = rc.getAllJobs();
            String npn = rc.getNodepoolName();
            Map<IRmJob, Integer> jobmap = jobs.get(npn);

            if ( allJobs.size() == 0 ) continue;

            logger.info(methodName, null, String.format(headerfmt, "Nodepool", "User", "PureFS", "NSh", "Counted", "Needed", "O"), "Class:", rc.getName());
            for ( IRmJob j : allJobs.values() ) {

                if ( j.isRefused() ) {
                    continue;
                }

                if ( j.isDeferred() ) {
                    continue;
                }

                int counted = j.countNSharesGiven();          // accounting for ramp-up, various caps, etc. 

                int current = j.countNShares();                // currently allocated, plus pending, less those removed by earlier preemption
                int needed = counted - current;                // could go negative if its evicting
                int order = j.getShareOrder();
                
                if ( j.getSchedulingPolicy() == Policy.FAIR_SHARE ) {   // cap on frag threshold
                    if ( current >= fragmentationThreshold ) { 
                        needed = 0;
                    } else if ( current >= j.getPureFairShare() ) {     // more than our pure share, we're not needy
                        needed = 0;
                    } else if ( needed < 0 ) {                          // more than out count, likely are evicting
                        needed = 0;
                    } else if ( needed > 0) {
                        needed = Math.min(needed, fragmentationThreshold);
                        jobmap.put(j, needed);                 // we'll log this in a minute, not here
                        must_defrag = true;
                    }                    
                } else {                                       // if not fair-share, must always try to defrag if needed
                                                               // its full allocation, and therefore cannot be needy
                    // UIMA-4275 We rely on quotas to keep 'needed' under control
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

                needyJobs.put(j, j);
                
                if ( available >= needed ) {
                    needed = 0;
                    to_remove = needed;
                } else {
                    to_remove = available;
                    needed -= to_remove;
                }
                
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

    // 1 any machine with free space F, and a candidate job j of order O(j) such
    //    that F + O(j) == O(nj)
    //    Tiebraker on j is wealth W: W(j1) > W(j2)

    // 2 choice, any machine with a candidate job of the same order as the needy job
    //    Secondary sort, candidate job A is richer than candidate job B
    //
    // Tiebreak 1 ~ 2: W(j1) > W(j2) - choose host whose job is richest
    //
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

            if ( s2wealth == s1wealth ) {                  
                return RmJob.compareInvestment(s1, s2);    // UIMA-4275
            }

            return s2wealth - s1wealth;
        }
    }

}
