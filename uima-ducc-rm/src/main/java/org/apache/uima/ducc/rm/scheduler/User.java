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

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import org.apache.uima.ducc.rm.scheduler.SchedConstants.Policy;

public class User
    implements IEntity
{
    private String id;
    private Map<IRmJob, IRmJob> jobs = new HashMap<IRmJob, IRmJob>();    // my jobs
    private Map<ResourceClass, Map<IRmJob, IRmJob>> jobsByClass = new HashMap<ResourceClass, Map<IRmJob, IRmJob>>();

    private Map<Integer, Map<IRmJob, IRmJob>> jobsByOrder = new HashMap<Integer, Map<IRmJob, IRmJob>>();

    private Map<ResourceClass, Integer> classLimits = new HashMap<ResourceClass, Integer>(); // UIMA-4275
    private int globalLimit = -1;  // use global limit by default;

    //private int user_shares;       // number of shares to apportion to jobs in this user in current epoch
    private int pure_fair_share;   // uncapped un-bonused counts
    private int share_wealth;      // defrag, how many relevent Q shares do i really have?
    private int[] given_by_order =  null;
    private int[] wanted_by_order = null; // transient and not immutable, can't use for queries
    private int totalWantedByOrder = 0;   // transient, calculated for each schedule

    private static Comparator<IEntity> apportionmentSorter = new ApportionmentSorterCl();
    public User(String name)
    {
        this.id = name;
    }

    public long getTimestamp()
    {
        return 0;
    }

    // UIMA-4275, class-based limit
    void overrideLimit(ResourceClass rc, int lim)
    {
        classLimits.put(rc, lim);
    }

    // UIMA-4275 Global NPshare limit override from registry
    void overrideGlobalLimit(int lim)
    {
        globalLimit = lim;
    }
    
    // UIMA-4275 Get the override on the global limit
    int getOverrideLimit()
    {
        return globalLimit;
    }


    // UIMA-4275, count all Non-Preemptable shares for this user, quantum shares
    int countNPShares()
    {
        int occupancy = 0;
        for ( IRmJob j : jobs.values() ) {
            if ( j.getSchedulingPolicy() != Policy.FAIR_SHARE ) {
                // nshares_given is shares counted out for the job but maybe not assigned
                // nshares       is shares given
                // share_order   is used to convert nshares to qshares so
                // so ( nshares_give + nshares ) * share_order is the current potential occupancy of the job
                occupancy += ( j.countOccupancy() );
            }
        }
        return occupancy;
    }

    int getClassLimit(ResourceClass rc)
    {
        if ( classLimits.containsKey(rc) ) return classLimits.get(rc);
        else                               return Integer.MAX_VALUE;
    }

    void addJob(IRmJob j)
    {
        jobs.put(j, j);
        int order = j.getShareOrder();
        
        Map<IRmJob, IRmJob> ojobs = jobsByOrder.get(order);
        if ( ! jobsByOrder.containsKey(order) ) {
            ojobs = new HashMap<IRmJob, IRmJob>();
            jobsByOrder.put(order, ojobs);
        }
        ojobs.put(j, j);

        ResourceClass cl = j.getResourceClass();
        ojobs = jobsByClass.get(cl);
        if ( ojobs == null ) {
            ojobs = new HashMap<IRmJob, IRmJob>();
            jobsByClass.put(cl, ojobs);
        }
        ojobs.put(j, j);
    }

    /**
     * Remove a job from the list and return how many jobs remain.
     */
    int remove(IRmJob j)
    {
        if ( jobs.containsKey(j) ) {
            jobs.remove(j);

            int order = j.getShareOrder();
            Map<IRmJob, IRmJob> ojobs = jobsByOrder.get(order);
            ojobs.remove(j);
            
            ResourceClass cl = j.getResourceClass();
            if ( jobsByClass.containsKey(cl) ) {                // if not it's likely an early refusal
                ojobs = jobsByClass.get(cl);
                ojobs.remove(j);
            }       
        } else {
            throw new SchedulingException(j.getId(), "User " + id + " is asked to remove job " + j.getId() + " but the job is not assigned.");
        }
        return jobs.size();
    }

    /**
     * Currently, all users are equal.
     */
    public int getShareWeight()
    {
        return 1;
    }

    /**
     * Returns total N-shares wanted by order for a given class. Processes of size order.
     */
    private int countNSharesWanted(int order, ResourceClass rc)
    {
        int K = 0;
        
        // First sum the max shares all my jobs can actually use
        Map<IRmJob, IRmJob> jobs = jobsByOrder.get(order);
        if ( jobs == null ) {
            return 0;
        }

        String rcname = rc.getName();
        for ( IRmJob j : jobs.values() ) {
            if ( j.getResourceClass().getName().equals(rcname) ) {
                K += j.getJobCap();
            }
        }

        return K;
    }

    public void initWantedByOrder(ResourceClass rc)
    {
    	wanted_by_order = NodePool.makeArray();
        for ( int o = NodePool.getMaxOrder(); o > 0; o-- ) {
            wanted_by_order[o] = countNSharesWanted(o, rc);
            wanted_by_order[0] +=  wanted_by_order[o];
        }
        totalWantedByOrder = wanted_by_order[0]; // needed for sorting later as the counter changes the array
    }

    public void setPureFairShare(int pfs)
    {
        this.pure_fair_share = pfs;
    }

    public int getPureFairShare()
    {
        return pure_fair_share;
    }

    public int[] getWantedByOrder()
    {
        return wanted_by_order;
    }

    public void setGivenByOrder(int[] gbo)
    {
        this.given_by_order = gbo;
    }

    public int[] getGivenByOrder()
    {
        return given_by_order;
    }

    public void setShareWealth(int w)
    {
        this.share_wealth = w; // qshares
    }

    public int getShareWealth()
    {
        return share_wealth;  // qshares
    }

    public void subtractWealth(int w)
    {
        share_wealth -= w;
    }

    public int calculateCap(int order, int total)
    {
        return Integer.MAX_VALUE;  // no cap for users
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
        return (getWantedByOrder()[order] > 0);        // yep, still want
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

        User u = (User) o;
    	return this.id.equals(u.getName());
    }

    /**
    public String getId()
    {
        return id;
    }
    */

    public String getName()
    {
        return id;
    }

    public String toString()
    {
        return id;
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
            // Order the users by smallest wanted first.  The counter will 
            // round up for really small jobs so they don't get buried in
            // round-off errors.
            //
            // Note that wanted_by_order must be precomputed before this is called, and
            // that botn wanted_ and given_ by_order are modified by the counter, which
            // is ok. 
            if ( e1 == e2 ) return 0;
            //return e1.getName().compareTo(e2.getName());

            User u1 = (User) e1;
            User u2 = (User) e2;
            return u1.totalWantedByOrder = u2.totalWantedByOrder;
        }
    }
}
