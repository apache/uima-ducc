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

public class User
    implements IEntity
{
    private String id;
    private HashMap<IRmJob, IRmJob> jobs = new HashMap<IRmJob, IRmJob>();    // my jobs
    private HashMap<ResourceClass, HashMap<IRmJob, IRmJob>> jobsByClass = new HashMap<ResourceClass, HashMap<IRmJob, IRmJob>>();

    private HashMap<Integer, HashMap<IRmJob, IRmJob>> jobsByOrder = new HashMap<Integer, HashMap<IRmJob, IRmJob>>();
    private int user_shares;       // number of shares to apportion to jobs in this user in current epoch
    private int pure_fair_share;   // uncapped un-bonused counts
    private int share_wealth;      // defrag, how many relevent Q shares do i really have?
    private int[] given_by_order =  null;
    private int[] wanted_by_order = null;

    private static Comparator<IEntity> apportionmentSorter = new ApportionmentSorterCl();
    public User(String name)
    {
        this.id = name;
    }

    public long getTimestamp()
    {
        return 0;
    }

    void addJob(IRmJob j)
    {
        jobs.put(j, j);
        int order = j.getShareOrder();
        
        HashMap<IRmJob, IRmJob> ojobs = jobsByOrder.get(order);
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
            HashMap<IRmJob, IRmJob> ojobs = jobsByOrder.get(order);
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
        HashMap<IRmJob, IRmJob> jobs = jobsByOrder.get(order);
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

    HashMap<IRmJob, IRmJob> getJobs()
    {
        return jobs;
    }

    HashMap<IRmJob, IRmJob> getJobsOfOrder(int order)
    {
        return jobsByOrder.get(order);
    }

    HashMap<Integer, HashMap<IRmJob, IRmJob>> getJobsByOrder()
    {
        return jobsByOrder;
    }

    HashMap<String, Machine> getMachines()
    {
        // TODO: fill this in - walk the jobs and return the hash
        System.out.println("Warning: getMachines() is not implemented and is returning null");
        return null;
    }

    public int countJobs()
    {
        return jobs.size();
    }

    public int countJobs(int o)
    {
        if ( jobsByOrder.containsKey(o) ) {
            return jobsByOrder.get(o).size();
        }
        return 0;
    }

    public void clearShares()
    {
        user_shares = 0;
        //System.out.println("**** user " + getId() + "/" + uniqueId + " clearing shares");
        //sharesByOrder.clear();
    }

    public void addQShares(int s)
    {
        user_shares += s;
        //System.out.println("***** user " + getId() + "/" + uniqueId + " shares are " + s);
    }

    /**
     * Try to find the smallest bonus shares we can use.
     */
    public int canUseBonus(int bonus, int[] tmpSharesByOrder)
    {
        for ( int i = 1; i <= Math.min(bonus, tmpSharesByOrder.length); i++ ) {
            
            if ( jobsByOrder.containsKey(i) && (tmpSharesByOrder[i] > 0) ) {
                return i;
            }
        }
        return 0;
    }

    public int countQShares(String x)
    {
        //System.out.println(x + " **** user " + getId() + "/" + uniqueId + " returning " + user_shares + " shares");
        return this.user_shares;
    }


    int countCappedQShares(int physicalCap, int order)
    {
        int K = 0;
        physicalCap = physicalCap * order;                         // to quantum shares
        HashMap<IRmJob, IRmJob> jobs = jobsByOrder.get(order);

        if ( jobs == null ) {
        	return 0;
        }
        
        for ( IRmJob j : jobs.values() ) {
            K += (Math.min(j.getJobCap(), physicalCap));
        }

        return Math.min(K, physicalCap) * order;
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
            if ( e1 == e2 ) return 0;
            return e1.getName().compareTo(e2.getName());
        }
    }
}
