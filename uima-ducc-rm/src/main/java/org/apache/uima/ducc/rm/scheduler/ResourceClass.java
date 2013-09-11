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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;

import org.apache.uima.ducc.common.utils.DuccProperties;
import org.apache.uima.ducc.common.utils.SystemPropertyResolver;


/**
 * This represents a priority class.
 */
public class ResourceClass
    implements SchedConstants,
               IEntity
{
    //private DuccLogger logger = DuccLogger.getLogger(this.getClass(), COMPONENT_NAME);

    private String id;
    private Policy policy;
    private int priority;           // orders evaluation of the class


    private int share_weight;       // for fair-share, the share weight to use
    private int min_shares;         // fixed-shre: min shares to hand out
    private int max_processes = 0;      // fixed-share: max shares to hand out regardless of
                                    // what is requested or what fair-share turns out to be

    private int max_machines = 0;   // reservation: max machines that can be reserved by a single user - global across
                                    // all this user's requests.

    // for reservation, this caps machines. 
    // for shares, this caps shares
    private int absolute_cap;       // max shares or machines this class can hand out
    private double percent_cap;     // max shares or machines this class can hand out as a percentage of all shares
    private int true_cap;           // set during scheduling, based on actual current resource availability
    private int pure_fair_share;    // the unmodified fair share, not counting caps, and not adding in bonuses

    private HashMap<IRmJob, IRmJob> allJobs = new HashMap<IRmJob, IRmJob>();
    private HashMap<Integer, HashMap<IRmJob, IRmJob>> jobsByOrder = new HashMap<Integer, HashMap<IRmJob, IRmJob>>();
    private HashMap<User, HashMap<IRmJob, IRmJob>> jobsByUser = new HashMap<User, HashMap<IRmJob, IRmJob>>();
    private int max_job_order = 0;  // largest order of any job still alive in this rc (not necessarily globally though)

    // private HashMap<Integer, Integer> nSharesByOrder = new HashMap<Integer, Integer>();         // order, N shares of that order
    private boolean subpool_counted = false;

    // the physical presence of nodes in the pool is somewhat dynamic - we'll store names only, and generate
    // a map of machines on demand by the schedler from currently present machnes
    private String nodepoolName = null;

//     ArrayList<String> nodepool = new ArrayList<String>();                               // nodepool names only
//     HashMap<String, Machine> machinesByName = new HashMap<String, Machine>();
//     HashMap<String, Machine> machinesByIp = new HashMap<String, Machine>();

    // Whether to enforce memory constraints for matching reservations
    private boolean enforce_memory = true;

    // int class_shares;       // number of shares to apportion to jobs in this class in current epoch

    private boolean expand_by_doubling = true;
    private int initialization_cap = 2;
    private long prediction_fudge = 60000;
    private boolean use_prediction = true;

    private int[] given_by_order  = null;
    private int[] wanted_by_order = null;               // volatile - changes during countClassesByOrder

    private static Comparator<IEntity> apportionmentSorter = new ApportionmentSorterCl();

    public ResourceClass(DuccProperties props)
    {
        //
        // We can assume everything useful is here because the parser insured it
        //
        this.id = props.getStringProperty("name");
        this.policy = Policy.valueOf(props.getStringProperty("policy"));
        this.priority = props.getIntProperty("priority");
        this.min_shares = 0;

        if ( policy == Policy.RESERVE ) {
            this.max_machines = props.getIntProperty("max-machines");
            this.enforce_memory = props.getBooleanProperty("enforce-memory", true);
        }

        if ( policy != Policy.RESERVE ) {
            this.max_processes = props.getIntProperty("max-processes");
        }

        if ( max_processes <= 0 ) max_processes = Integer.MAX_VALUE;
        if ( max_machines <= 0 )  max_machines  = Integer.MAX_VALUE;

        this.absolute_cap = Integer.MAX_VALUE;
        this.percent_cap  = 1.0;

        String cap  = props.getStringProperty("cap");
        if ( cap.endsWith("%") ) {
            int t = Integer.parseInt(cap.substring(0, cap.length()-1));
            this.percent_cap = (t * 1.0 ) / 100.0;
        } else {
            absolute_cap = Integer.parseInt(cap);
            if (absolute_cap == 0) absolute_cap = Integer.MAX_VALUE;
        }

        if ( this.policy == Policy.FAIR_SHARE ) {
            this.share_weight = props.getIntProperty("weight");
            if ( props.containsKey("expand-by-doubling") ) {
                this.expand_by_doubling = props.getBooleanProperty("expand-by-doubling", true);
            } else {
                this.expand_by_doubling  = SystemPropertyResolver.getBooleanProperty("ducc.rm.expand.by.doubling", true);
            }
            
            if ( props.containsKey("initialization-cap") ) {
                this.initialization_cap = props.getIntProperty("initialization-cap");
            } else {
                this.initialization_cap  = SystemPropertyResolver.getIntProperty("ducc.rm.initialization.cap", 2);
            }
            
            if ( props.containsKey("use-prediction") ) {
                this.use_prediction = props.getBooleanProperty("use-prediction", true);
            } else {
                this.use_prediction = SystemPropertyResolver.getBooleanProperty("ducc.rm.prediction", true);
            }
            
            if ( props.containsKey("prediction-fudge") ) {
                this.prediction_fudge = props.getLongProperty("prediction-fudge");
            } else {
                this.prediction_fudge  = SystemPropertyResolver.getLongProperty("ducc.rm.prediction.fudge", 60000);
            }
        }

        this.nodepoolName = props.getStringProperty("nodepool");

                                                                        
    }

//     // TODO: sanity check 
//     //   - emit warnings if shares are specified in reservations
//     //                   if machines are sprcified for fair or fixed-share
//     //                   etc.
//     void init(DuccProperties props)
//     {
//     	//String methodName = "init";
//     	String k = "scheduling.class." + id + ".";
//         String s;
//         s = props.getProperty(k + "policy");
        
//         if ( s == null ) {
//         	throw new SchedulingException(null, "Configuration problem: no policy for class " + id + ".");
//         }
//         policy = Policy.valueOf(s);

//         share_weight     = props.getIntProperty(k + "share_weight"   , DEFAULT_SHARE_WEIGHT);
//         priority  = props.getIntProperty(k + "priority", DEFAULT_PRIORITY);
//         min_shares = props.getIntProperty(k + "min_shares", 0);       // default no min

//         switch ( policy ) {
//             case FAIR_SHARE:
//                 max_processes = props.getIntProperty(k + "max_processes", DEFAULT_MAX_PROCESSES);       // default no max
//                 max_machines = 0;
//                 break;

//             case FIXED_SHARE:
//                 max_processes = props.getIntProperty(k + "max_processes", DEFAULT_MAX_PROCESSES);       // default no max
//                 max_machines = 0;
//                 break;

//             case RESERVE:
//                 max_processes = 0;
//                 max_machines = props.getIntProperty(k + "max_machines", DEFAULT_MAX_INSTANCES);       // default max 1
//                 break;

//         }
//         if ( max_processes <= 0 ) max_processes = Integer.MAX_VALUE;
//         if ( max_machines <= 0 )  max_machines  = Integer.MAX_VALUE;

//         enforce_memory = props.getBooleanProperty(k + "enforce.memory", true);
        
//         initialization_cap = props.getIntProperty(k + "initialization.cap", initialization_cap);
//         expand_by_doubling = props.getBooleanProperty(k + "expand.by.doubling", expand_by_doubling);
//         use_prediction     = props.getBooleanProperty(k + "prediction", use_prediction);
//         prediction_fudge   = props.getIntProperty(k + "prediction.fudge", prediction_fudge);

//         s = props.getStringProperty(k + "cap", ""+Integer.MAX_VALUE);                // default no cap
//         if ( s.endsWith("%") ) {
//             int t = Integer.parseInt(s.substring(0, s.length()-1));
//             percent_cap = (t * 1.0 ) / 100.0;
//         } else {
//             absolute_cap = Integer.parseInt(s);
//             if (absolute_cap == 0) absolute_cap = Integer.MAX_VALUE;
//         }

//         nodepoolName = props.getStringProperty(k + "nodepool", null);                               // optional nodepool
//         if (nodepoolName == null) {
//             nodepoolName = NodePool.globalName;
//         } 
//     }

    public long getTimestamp()
    {
        return 0;
    }

    String getNodepoolName()
    {
        return nodepoolName;
    }

    public void setPureFairShare(int pfs)
    {
        this.pure_fair_share = pfs;
    }

    public int getPureFairShare()
    {
        return pure_fair_share;
    }

    public boolean isExpandByDoubling()
    {
        return expand_by_doubling;
    }

    public void setExpandByDoubling(boolean ebd)
    {
        this.expand_by_doubling = ebd;
    }

    public int getInitializationCap()
    {
        return initialization_cap;
    }

    public void setInitializationCap(int c)
    {
        this.initialization_cap = c;
    }

    public boolean isUsePrediction()
    {
        return use_prediction;
    }

    public long getPredictionFudge()
    {
        return prediction_fudge;
    }

    public boolean enforceMemory()
    {
        return enforce_memory;
    }

    public Policy getPolicy()
    {
        return policy;
    }

    public void setTrueCap(int cap)
    {
        this.true_cap = cap;
    }

    public int getTrueCap()
    {
        return true_cap;
    }

    public double getPercentCap() {
        return percent_cap;
    }

    public int getAbsoluteCap() {
        return absolute_cap;
    }
        
    public int getMaxProcesses() {
        return max_processes;
    }

    public int getMinShares() {
        return min_shares;
    }

    public int getMaxMachines() {
        return max_machines;
    }
    
    void setPolicy(Policy p)
    {
        this.policy = p;
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

    public int getShareWeight()
    {
        return share_weight;
    }

    /**
     * Return my share weight, if I have any jobs of the given order or less.  If not,
     * return 0;
     */
    public int getEffectiveWeight(int order)
    {
        for ( int o = order; o > 0; o-- ) {
            if ( jobsByOrder.containsKey(o) && ( jobsByOrder.get(o).size() > 0) ) {
                return share_weight;
            }
        }
        return 0;
    }

//     /** @deprecated */
//     public void setClassShares(int s)
//     {
//         this.class_shares = Math.min(s, class_shares);
//     }

    /**
     * Add 's' ** quantum ** shares of the indicated order.
     * Return the actual number of shares, which might have been capped.

    public int setClassSharesByOrder(int order, int s)
    {
        int val = 0;
        s /= order;                                              // convert to nShares
        if ( nSharesByOrder.containsKey(order ) ) {
            val = Math.min(s, nSharesByOrder.get(order));        // not first time, must use min
        } else {
            val = s;                                             // first time, just accept it
        }
        nSharesByOrder.put(order, val);
        return val * order;
    }
     */

    /**
     * Add one ** virtual ** share of the given order.

    public void addClassShare(int order)
    {
        nSharesByOrder.put(order, nSharesByOrder.get(order) + 1);
    }
     */
    /**
    public int canUseShares(NodePool np, int[] tmpSharesByOrder)
    {
        if ( !np.getId().equals(nodepoolName) ) return 0;             // can't use any from somebody else's nodepool

        for ( int o = max_job_order; o > 0; o-- ) {

            if ( !nSharesByOrder.containsKey(o) ) continue;

            int given = nSharesByOrder.get(o);
            int can_use = 0;

            if ( tmpSharesByOrder[o] > 0 ) {                          // do we have any this size?
                HashMap<IRmJob, IRmJob> jbo = jobsByOrder.get(o);     // yeah, see if any job wants it
                if ( jbo != null ) {
                    for (IRmJob j : jbo.values()) {                        
                        can_use += j.getJobCap();
                    }
                }
            }
            if ( (can_use - given) > 0 ) {
                return o;
            }
        }
        return 0;
    }
    */

    void updateNodepool(NodePool np)
    {
        //String methodName = "updateNodepool";

//         for ( int k : nSharesByOrder.keySet() ) {
//             np.countOutNSharesByOrder(k, nSharesByOrder.get(k));
//         }

        if ( given_by_order == null ) return;       // nothing given, nothing to adjust

        for ( int o = NodePool.getMaxOrder(); o > 0; o-- ) {
            np.countOutNSharesByOrder(o, given_by_order[o]);
        }
    }
    
    public int getPriority()
    {
    	return priority;
    }
    
    public void clearShares()
    {
        //class_shares = 0;
        given_by_order = null;
        subpool_counted = false;
    }
    
    public void markSubpoolCounted()
    {
        subpool_counted = true;
    }

    void addJob(IRmJob j)
    {
        allJobs.put(j, j);

        int order = j.getShareOrder();
        HashMap<IRmJob, IRmJob> jbo = jobsByOrder.get(order);
        if ( jbo == null ) {
            jbo = new HashMap<IRmJob, IRmJob>();
            jobsByOrder.put(order, jbo);
            max_job_order = Math.max(max_job_order, order);
        }
        jbo.put(j, j);

        User u = j.getUser();
        jbo = jobsByUser.get(u);
        if ( jbo == null ) {
            jbo = new HashMap<IRmJob, IRmJob>();
            jobsByUser.put(u, jbo);
        }
        jbo.put(j, j);

    }

    void removeJob(IRmJob j)
    {
        if ( ! allJobs.containsKey(j) ) {
            if ( j.isRefused() ) return;

            throw new SchedulingException(j.getId(), "Priority class " + getName() + " cannot find job to remove.");
        }

        allJobs.remove(j);

        int order = j.getShareOrder();
        HashMap<IRmJob, IRmJob> jbo = jobsByOrder.get(order);
        jbo.remove(j);
        if ( jbo.size() == 0 ) {
            jobsByOrder.remove(order);

            for ( int o = order - 1; o > 0; o-- ) {
                if ( jobsByOrder.containsKey(o) ) {
                    max_job_order = o;
                    break;
                }
            }
        }

//         if ( jbo.size() == 0 ) {
//             jobsByOrder.remove(order);
//         }

        User u = j.getUser();
        jbo = jobsByUser.get(u);
        jbo.remove(j);
        if ( jbo.size() == 0 ) {
            jobsByUser.remove(u);
        }
    }

    int countJobs()
    {
        return allJobs.size();
    }

    /**
    int countJobs(int order)
    {
        if ( jobsByOrder.containsKey(order) ) {
            return jobsByOrder.get(order).size();
        } else {
            return 0;
        }
    }
*/
    /**
    HashMap<IRmJob, IRmJob> getJobsOfOrder(int order)
    {
        return jobsByOrder.get(order);
    }
*/

  /**
    int sumAllWeights()
    {
        return allJobs.size() * share_weight;
    }
*/
    /**
     * The total weights of all jobs of order 'order' or less.
     *
    int sumAllWeights(int order)
    {
        int sum = 0;
        for ( int o = order; o > 0; o-- ) {
            if ( jobsByOrder.containsKey(o) ){ 
                sum++;
            }
        }
        return sum * share_weight;
    }
*/
    /**
     * Returns total N-shares wanted by order. Processes of size order.
     */
    private int countNSharesWanted(int order)
    {
        int K = 0;
        
        // First sum the max shares all my jobs can actually use
        HashMap<IRmJob, IRmJob> jobs = jobsByOrder.get(order);
        if ( jobs == null ) {
            return 0;
        }

        for ( IRmJob j : jobs.values() ) {
            K += j.getJobCap();
        }

        return K;
    }

    public void initWantedByOrder(ResourceClass unused)
    {
        int ord = NodePool.getMaxOrder();
        wanted_by_order = NodePool.makeArray();
        for ( int o = ord; o > 0; o-- ) {
            wanted_by_order[o] = countNSharesWanted(o);
            wanted_by_order[0] += wanted_by_order[o];
        }
    }

    public int[] getWantedByOrder()
    {
        return wanted_by_order;
    }

    public int[] getGivenByOrder()
    {
    	return given_by_order;
    }

    public void setGivenByOrder(int[] gbo)
    {
        if ( given_by_order == null ) {      // Can have multiple passes, don't reset on subsequent ones.
            this.given_by_order = gbo;       // Look carefuly at calculateCap() below for details.
        }
    }

    public int calculateCap(int order, int basis)
    {
        int perccap = Integer.MAX_VALUE;    // the cap, calculated from percent
        int absolute = getAbsoluteCap();
        double percent = getPercentCap();

        if ( percent < 1.0 ) {
            double b = basis;
            b = b * percent;
            perccap = (int) Math.round(b);
        } else {
            perccap = basis;
        }

        int cap =  Math.min(absolute, perccap) / order;   // cap on total shares available

        //
        // If this RC is defined over a nodepool that isn't the global nodepool then its share
        // gets calculated multiple times.  The first time when it is encountered during the
        // depth-first traversal, to work out the fair-share for all classes defined over the
        // nodepool.  Subpool resources are also available to parent pools however, and must
        // be reevaluated to insure the resources are fairly allocated over the larger pool
        // of work.
        //
        // So at this point there might already be shares assigned.  If so, we need to
        // recap on whatever is already given to avoid over-allocating "outside" of the
        // assigned shares.
        //
        if ( (given_by_order != null) && subpool_counted )  {
            cap = Math.min(cap, given_by_order[order]);
        } // else - never been counted or assigned at this order, no subpool cap
        
        return cap;
    }



    /**
     * Get capped number of quantum shares this resource class can support.
     *
    int countCappedQShares(int[] tmpSharesByOrder)
    {
        int K = 0;
        for ( IRmJob j : allJobs.values() ) {
            int order = j.getShareOrder();
            K += (Math.min(j.getJobCap(), tmpSharesByOrder[order] * order));            
        }
        return K;
    }
    */
    /**
     * Sum all the capped shares of the jobs.  Then cap on the physical cap and
     * again on the (possible) nodepool cap.
     *
    int countCappedNShares(int physicalCap, int order)
    {
        int K = 0;
        
        // First sum the max shares all my jobs can actually use
        HashMap<IRmJob, IRmJob> jobs = jobsByOrder.get(order);
        for ( IRmJob j : jobs.values() ) {
            K += j.getJobCap();
        }

        // cap by what is physically there
        K = Math.min(K, physicalCap);
        return K;
    }
*/

    /**
     * Sum all the capped shares of the jobs.  Then cap on the physical cap and
     * again on the (possible) nodepool cap.

     TODO: this seems all wrong.
    int getSubpoolCap(int order)
    {
        if ( subpool_counted ) {                   // share are dirty, cap is valid
            if( given_by_order[order] == 0 ) {
                //if ( !nSharesByOrder.containsKey(order) ) {
                return Integer.MAX_VALUE;
            } 
            return given_by_order[order];
        } else {
            return Integer.MAX_VALUE;              // shares are clean, no cap
        }
    }
    */

//    int countCappedShares()
//    {
//        int count = 0;
//        for ( IRmJob j : allJobs.values() ) {
//            count += j.getJobCap();
//        }
//        return count;
//    }

    /**
    int countSharesByOrder(int order)
    {
        if ( nSharesByOrder.containsKey(order) ) {
            return nSharesByOrder.get(order);
        }
        return 0;
    }
*/
    /**
    int[] getSharesByOrder(int asize)
    {
        int[] answer = new int[asize];
        for ( int i = 0; i < asize; i++ ) {
            if ( nSharesByOrder.containsKey(i) ) {
                answer[i] = nSharesByOrder.get(i);
            } else {
                answer[i] = 0;
            }
        }
        return answer;
    }
*/
    public boolean hasSharesGiven() 
    {
        return ( (given_by_order != null) && (given_by_order[0] > 0) );
    }

    private int countActiveShares()
    {
        int sum = 0;
        for ( IRmJob j : allJobs.values() ) {
            sum += (j.countNShares() * j.getShareOrder());          // in quantum shares
        }
        return sum;
    }

    /**
     * Get the highest order of any job in this class.
     
    protected int getMaxOrder()
    {
        int max = 0;
        for ( IRmJob j : allJobs.values() ) {
            max = Math.max(max, j.getShareOrder());
        }
        return max;
    }
    */

    HashMap<IRmJob, IRmJob> getAllJobs()
    {
        return allJobs;
    }

    HashMap<Integer, HashMap<IRmJob, IRmJob>> getAllJobsByOrder()
    {
        return jobsByOrder;
    }

    HashMap<User, HashMap<IRmJob, IRmJob>> getAllJobsByUser()
    {
        return jobsByUser;
    }

    ArrayList<IRmJob> getAllJobsSorted(Comparator<IRmJob> sorter)
    {
        ArrayList<IRmJob> answer = new ArrayList<IRmJob>();
        answer.addAll(allJobs.values());
        Collections.sort(answer, sorter);
        return answer;
    }

    int getMaxJobOrder()
    {
        return max_job_order;
    }

    int makeReadable(int i)
    {
        return (i == Integer.MAX_VALUE ? -1 : i);
    }

    /**
     * Stringify the share tables for the logs
     */
    
    /**
    public String tablesToString()
    {
        ArrayList<Integer> keys = new ArrayList<Integer>();
        keys.addAll(nSharesByOrder.keySet());
        Collections.sort(keys);
        int max = 0;
        for ( int k : keys ) {
            max = Math.max(max, k);
        }
        String[] values = new String[max+1];
        values[0] = id;
        for ( int i = 1; i < max+1; i++) {
            if ( !nSharesByOrder.containsKey(i) ) {
                values[i] = "0";
            } else {
                values[i] = Integer.toString(nSharesByOrder.get(i));
            }
        }
        StringBuffer sb = new StringBuffer();
        sb.append("%23s:");
        for ( int i = 0; i < max; i++ ) {
            sb.append("%s ");
        }
        
        return String.format(sb.toString(), (Object[]) values);
    }
    */

    // note we assume Nodepool is the last token so we don't set a len for it!
    private static String formatString = "%12s %11s %4s %5s %5s %5s %6s %6s %7s %6s %6s %7s %5s %7s %s";
    public static String getDashes()
    {
        return String.format(formatString, "------------", "-----------",  "----", "-----", "-----", "-----", "------", "------", "-------", "------", "------", "-------", "-----", "-------", "--------");
    }

    public static String getHeader()
    {
        return String.format(formatString, "Class Name", "Policy", "Prio", "Wgt", "MinSh", "MaxSh", "AbsCap", "PctCap", "InitCap", "Dbling", "Prdct", "PFudge", "Shr", "Enforce", "Nodepool");
    }

    @Override
    public int hashCode()
    { 
        return id.hashCode();
    }

    public String toString() {
        return String.format("%12s %11s %4d %5d %5d %5d %6d %6d %7d %6s %6s %7d %5d %7s %s", 
                             id,
                             policy.toString(),
                             priority, 
                             share_weight, 
                             makeReadable(min_shares), 
                             makeReadable(max_processes), 
                             makeReadable(absolute_cap), 
                             (int) (percent_cap *100), 
                             initialization_cap,
                             expand_by_doubling,
                             use_prediction,
                             prediction_fudge,
                             countActiveShares(),
                             enforce_memory,
                             nodepoolName
            );
    }

    public String toStringWithHeader()
    {
        StringBuffer buf = new StringBuffer();
        

        buf.append(getHeader());
        buf.append("\n");
        buf.append(toString());
        return buf.toString();
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
        	// we want a consistent sort, that favors higher share weights
            if ( e1 == e2 ) return 0;
            int w1 = e1.getShareWeight();
            int w2 = e2.getShareWeight();
            if ( w1 == w2 ) {
                return e1.getName().compareTo(e2.getName());
            }
            return (int) (w2 - w1);
        }
    }

}
            
