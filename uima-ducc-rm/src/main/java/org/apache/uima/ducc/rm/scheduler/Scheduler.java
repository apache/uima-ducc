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

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.uima.ducc.common.IIdentity;
import org.apache.uima.ducc.common.Node;
import org.apache.uima.ducc.common.NodeIdentity;
import org.apache.uima.ducc.common.Pair;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccProperties;
import org.apache.uima.ducc.common.utils.SystemPropertyResolver;
import org.apache.uima.ducc.common.utils.Version;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.common.utils.id.DuccIdFactory;


/**
 * This process orchestrates scheduling.
 * - Receives requests from clients ( job manager, service manager, etc ) for resources
 * - Forwards requests and current state to pluggable scheduling implementation
 * - Receives a schedule, updates state, sends responses to requestors
 * - Maintains state as needed (work item life cycle etc)
 */
public class Scheduler
//    extends Thread
    implements ISchedulerMain,
    	SchedConstants
{
    IJobManager jobManager;
    static DuccLogger     logger = DuccLogger.getLogger(Scheduler.class, COMPONENT_NAME);

    boolean done = false;
    // Boolean force_epoch = false;
    String ducc_home;
    // Integer epoch = 5;                                                 // scheduling epoch, seconds

    NodePool nodepool;

    //
    // Fair-share and fixed-share use shares only, not machines
    //
    HashMap<DuccId, Share> busyShares        = new HashMap<DuccId, Share>(); // Running "fair" share jobs

    // incoming reports of machines that are now free
    HashMap<DuccId, Pair<IRmJob, Share>> vacatedShares= new HashMap<DuccId, Pair<IRmJob, Share>>();
    // boolean growthOccurred = false;                                           // don't care which grew, just that something grew

    ArrayList<IRmJob>        incomingJobs    = new ArrayList<IRmJob>();       // coming in from external world but not added our queues yet
    ArrayList<IRmJob>        recoveredJobs   = new ArrayList<IRmJob>();       // coming in from external world but we don't now about them, (hopefully
                                                                              //    because we crashed and not for more nefarious reasons)
    ArrayList<IRmJob>        completedJobs   = new ArrayList<IRmJob>();       // signaled complete from outside but not yet dealt with
    ArrayList<IRmJob>        initializedJobs = new ArrayList<IRmJob>();       // Init is complete so we can begin full (un)fair share allocation

    //HashMap<Node, Node> incomingNodes  = new HashMap<Node, Node>();         // node updates
    HashMap<Node, Node> deadNodes      = new HashMap<Node, Node>();           // missed too many heartbeats
    HashMap<Node, Node> allNodes       = new HashMap<Node, Node>();           // the guys we know

    HashMap<String, User>    users     = new HashMap<String, User>();         // Active users - has a job in the system
    //HashMap<DuccId, IRmJob>    runningJobs = new HashMap<DuccId, IRmJob>();

    HashMap<DuccId, IRmJob>  allJobs = new HashMap<DuccId, IRmJob>();

    HashMap<ResourceClass, ResourceClass> resourceClasses = new HashMap<ResourceClass, ResourceClass>();
    HashMap<String, ResourceClass> resourceClassesByName = new HashMap<String, ResourceClass>();

    String defaultClassName = null;
    int defaultNThreads = 1;
    int defaultNTasks = 10;
    int defaultMemory = 16;

    // these two are initialized in constructor
    String schedImplName;
    IScheduler scheduler;

    long share_quantum    = 16;             // 16 GB in KB - smallest share size
    long share_free_dram  = 0;              // 0  GB in KB  - minim memory after shares are allocated
    long dramOverride     = 0;              // if > 0, use this instead of amount reported by agents (modeling and testing)

    EvictionPolicy evictionPolicy = EvictionPolicy.SHRINK_BY_MACHINE;

//     int nodeMetricsUpdateRate = 30000;
//     int startupCountdown = 0;       // update each epoch.  only schedule when it's > nodeStability
    int nodeStability = 3;
    boolean stability = false;

    private static DuccIdFactory idFactory;

    // static boolean expandByDoubling = true;
    // static int initializationCap = 2;      // Max allocation until we know initialization works in
                                           // units of *processes*, not shares (i.e.N-shares).

    //
    // Version
    //    0 - major version
    //    6 - minor version
    //    3 - ptf - forced eviction under fragmentation.
    //    4 - defrag code complete
    //  beta - not yet "real"!
    //
    // Bring up to speed with rest of ducc version. 2013-03-06 jrc
    //
    final static int rmversion_major = 0;
    final static int rmversion_minor = 8;
    final static int rmversion_ptf   = 0;  
    final static String rmversion_string = "beta";

    boolean initialized = false;           // we refuse nodeupdates until this is true
    public Scheduler()
    {
    }

    public synchronized void init()
        throws Exception
    {
        String methodName = "init";
        //setName("Scheduler");

        DuccLogger.setUnthreaded();

        String ep         = SystemPropertyResolver.getStringProperty("ducc.rm.eviction.policy", "SHRINK_BY_MACHINE");
        evictionPolicy    = EvictionPolicy.valueOf(ep);        

        nodepool          = new NodePool(null, evictionPolicy, 0);   // global nodepool
        share_quantum     = SystemPropertyResolver.getLongProperty("ducc.rm.share.quantum", share_quantum) * 1024 * 1024;        // GB -> KB
        share_free_dram   = SystemPropertyResolver.getLongProperty("ducc.rm.reserved.dram", share_free_dram) * 1024 * 1024;   // GB -> KB
        ducc_home         = SystemPropertyResolver.getStringProperty("DUCC_HOME");

        // some defaults, for jobs that don't specify them
        defaultNTasks     = SystemPropertyResolver.getIntProperty("ducc.rm.default.tasks", 10); 
        defaultNThreads   = SystemPropertyResolver.getIntProperty("ducc.rm.default.threads", 1);
        defaultMemory     = SystemPropertyResolver.getIntProperty("ducc.rm.default.memory", 16);      // in GB
        // expandByDoubling  = RmUtil.getBooleanProperty("ducc.rm.expand.by.doubling", true);

        nodeStability     = SystemPropertyResolver.getIntProperty("ducc.rm.node.stability", 3);        // number of node metrics updates to wait for before scheduling
                                                                                  // 0 means, just jump right in and don't wait

        dramOverride = SystemPropertyResolver.getLongProperty("ducc.rm.override.dram", 0);
        if ( dramOverride > 0 ) {
            dramOverride = dramOverride * (1024 * 1024);         // convert to KB
        }

        idFactory = new DuccIdFactory(1);

        try {
            schedImplName = SystemPropertyResolver.getStringProperty("ducc.rm.scheduler", "org.apache.uima.ducc.rm.rm.ClassBasedScheduler");
            @SuppressWarnings("unchecked")
			Class<IScheduler> cl = (Class<IScheduler>) Class.forName(schedImplName);
            scheduler = (IScheduler) cl.newInstance();
        } catch (ClassNotFoundException e) {
            throw new SchedulingException(null, "Cannot find class " + schedImplName);
        } catch (InstantiationException e) {
            throw new SchedulingException(null, "Cannot instantiate class " + schedImplName);            
        } catch (IllegalAccessException e) {
            throw new SchedulingException(null, "Cannot instantiate class " + schedImplName + ": can't access constructor.");            
        }

        String class_definitions = SystemPropertyResolver.getStringProperty("ducc.rm.class.definitions", "scheduler.classes");
        try {
            initClasses(class_definitions);
        } catch ( Exception e ) {
            logger.error(methodName, null, e);
            throw e;
        }

        // we share most of the state with the actual scheduling code - no need to keep passing this around
        // TODO: Make sure these are all Sialized correctly
        scheduler.setEvictionPolicy(evictionPolicy);
        scheduler.setClasses(resourceClasses);
        scheduler.setNodePool(nodepool);

        logger.info(methodName, null, "Scheduler running with share quantum           : ", (share_quantum / (1024*1024)), " GB");
        logger.info(methodName, null, "                       reserved DRAM           : ", (share_free_dram / (1024*1024)), " GB");
        logger.info(methodName, null, "                       DRAM override           : ", (dramOverride / (1024*1024)), " GB");
        logger.info(methodName, null, "                       scheduler               : ", schedImplName);
        logger.info(methodName, null, "                       default threads         : ", defaultNThreads);
        logger.info(methodName, null, "                       default tasks           : ", defaultNTasks);
        logger.info(methodName, null, "                       default memory          : ", defaultMemory);
        logger.info(methodName, null, "                       class definition file   : ", class_definitions);
        logger.info(methodName, null, "                       eviction policy         : ", evictionPolicy);
        logger.info(methodName, null, "                       use prediction          : ", SystemPropertyResolver.getBooleanProperty("ducc.rm.prediction", true));
        logger.info(methodName, null, "                       prediction fudge factor : ", SystemPropertyResolver.getIntProperty("ducc.rm.prediction.fudge", 10000));
        logger.info(methodName, null, "                       node stability          : ", nodeStability);
        logger.info(methodName, null, "                       init stability          : ", SystemPropertyResolver.getIntProperty("ducc.rm.init.stability"));
        logger.info(methodName, null, "                       fast recovery           : ", SystemPropertyResolver.getBooleanProperty("ducc.rm.fast.recovery", true));
        logger.info(methodName, null, "                       RM publish rate         : ", SystemPropertyResolver.getIntProperty("ducc.rm.state.publish.rate", 60));
        logger.info(methodName, null, "                       metrics update rate     : ", SystemPropertyResolver.getIntProperty("ducc.agent.node.metrics.publish.rate", 
                                                                                                                                 DEFAULT_NODE_METRICS_RATE));
        logger.info(methodName, null, "                       initialization cap      : ", SystemPropertyResolver.getIntProperty("ducc.rm.initialization.cap"));
        logger.info(methodName, null, "                       expand by doubling      : ", SystemPropertyResolver.getBooleanProperty("ducc.rm.expand.by.doubling", true));
        logger.info(methodName, null, "                       fragmentation threshold : ", SystemPropertyResolver.getIntProperty("ducc.rm.fragmentation.threshold", 2));
        logger.info(methodName, null, "                       do defragmentation      : ", SystemPropertyResolver.getBooleanProperty("ducc.rm.defragmentation", true));
        logger.info(methodName, null, "                       DUCC home               : ", System.getProperty("DUCC_HOME"));
        logger.info(methodName, null, "                       ActiveMQ URL            : ", SystemPropertyResolver.getStringProperty("ducc.broker.url"));
        logger.info(methodName, null, "                       JVM                     : ", System.getProperty("java.vendor") +
                                                                                      " "+ System.getProperty("java.version"));
        logger.info(methodName, null, "                       JAVA_HOME               : ", System.getProperty("java.home"));
        logger.info(methodName, null, "                       JVM Path                : ", System.getProperty("ducc.jvm"));
        logger.info(methodName, null, "                       JMX URL                 : ", System.getProperty("ducc.jmx.url"));
        logger.info(methodName, null, "                       OS Architecture         : ", System.getProperty("os.arch"));
        logger.info(methodName, null, "                       DUCC Version            : ", Version.version());
        logger.info(methodName, null, "                       RM Version              : ", ""+ rmversion_major   + "." 
                                                                                             + rmversion_minor   + "." 
                                                                                             + rmversion_ptf     + "-" 
                                                                                             + rmversion_string);
        initialized = true;
    }

    public synchronized boolean isInitialized()
    {
        return initialized;
    }

    public Machine getMachine(NodeIdentity ni)
    {
    	return nodepool.getMachine(ni);        
    }

    public void setJobManager(IJobManager jobmanager)
    {
        this.jobManager = jobmanager;
    }

    public String getDefaultClassName()
    {
    	return defaultClassName;
    }

    public int getDefaultNThreads()
    {
    	return defaultNThreads;
    }

    public int getDefaultNTasks()
    {
    	return defaultNTasks;
    }

    public int getDefaultMemory()
    {
    	return defaultMemory;
    }

    public ResourceClass getResourceClass(String name)
    {
        return resourceClassesByName.get(name);
    }

    public IRmJob getJob(DuccId id)
    {
        return allJobs.get(id);
    }

    public Share getShare(DuccId id)
    {
        return busyShares.get(id);
    }

//    public static int getInitializationCap()
//    {
//        return initializationCap;
//    }
//
//    public static boolean isExpandByDoubling()
//    {
//        return expandByDoubling;
//    }

    /**
     * Calculate share order, given some memory size in GB (as in from a job spec)
     */
    int calcShareOrder(long mem)
    {
        // Calculate its share order
        mem = mem * 1024 * 1024;                 // to GB
        
        int share_order = (int) (mem / share_quantum);               // liberal calc, round UP
        if ( (mem % share_quantum) > 0 ) {
            share_order++;
        }
        return share_order;
    }

    /**
     * Use the NodeIdentity to infer my the domain name.
     *
     * Itertate through the possible names - if one of them has a '.'
     * the we have to assume the following stuff is the domain name.
     * We only get one such name, so we give up the search if we find
     * it.
     */
    static String cached_domain = null;
    private String getDomainName()
    {
    	String methodName = "getDomainName";

        if ( cached_domain != null ) return cached_domain;
        try {
			NodeIdentity ni   = new NodeIdentity();
			for ( IIdentity id : ni.getNodeIdentities()) {
			    String n = id.getName();
			    int ndx = n.indexOf(".");
			    if ( ndx > 0 ) {
			        cached_domain =  n.substring(ndx + 1);
                    return cached_domain;
			    }
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			logger.warn(methodName, null, "Cannot create my own node identity:", e);
		}
        return null;  // crappy config if this happens, some stuff may not match nodepools and
                      // nothing to do about it.
    }

    Map<String, String> readNodepoolFile(String npfile)
    {
        String my_domain = getDomainName();
        String ducc_home = System.getProperty("DUCC_HOME");
        npfile = ducc_home + "/resources/" + npfile;

        Map<String, String> response = new HashMap<String, String>();

        try {
            BufferedReader br = new BufferedReader(new FileReader(npfile));
            String node = "";
            while ( (node = br.readLine()) != null ) {
                int ndx = node.indexOf("#");
                if ( ndx >= 0 ) {
                    node = node.substring(0, ndx);
                }
                node = node.trim();
                if (node.equals("") ) {
                    continue;
                }

                if ( node.startsWith("import") ) {
                    String[] tmp = node.split("\\s");
                    response.putAll(readNodepoolFile(tmp[1]));
                    continue;
                }
                response.put(node, node);

                // include fully and non-fully qualified names to allow sloppiness of config
                ndx = node.indexOf(".");
                String dnode;
                if ( ndx >= 0 ) {
                    dnode = node.substring(0, ndx);
                    response.put(dnode, dnode);
                } else if ( my_domain != null ) {
                    dnode = node + "." + my_domain;
                    response.put(dnode, dnode);
                }
            }
            br.close();                        
            
        } catch (FileNotFoundException e) {
            throw new SchedulingException(null, "Cannot open NodePool file \"" + npfile + "\": file not found.");
        } catch (IOException e) {
            throw new SchedulingException(null, "Cannot read NodePool file \"" + npfile + "\": I/O Error.");
        }
                
        return response;
    }

    void initClasses(String filename)
        throws Exception
    {
        String methodName = "initClasses";
        DuccProperties props = new DuccProperties();
        props.load(ducc_home + "/resources/" + filename);

        defaultClassName = props.getProperty("scheduling.default.name");

        // read in nodepools
        String npn = props.getProperty("scheduling.nodepool");
        if ( npn != null ) {
            String[] npnames = npn.split(" ");
            for ( String nodepoolName : npnames ) {
                int nporder = props.getIntProperty("scheduling.nodepool." + nodepoolName + ".order", 100);                
                String npfile = props.getProperty("scheduling.nodepool." + nodepoolName).trim();
                Map<String,String> npnodes = readNodepoolFile(npfile);                
                nodepool.createSubpool(nodepoolName, npnodes, nporder);                    
//                 } catch (FileNotFoundException e) {
//                     throw new SchedulingException(null, "Cannot open NodePool file \"" + npfile + "\": file not found.");
//                 } catch (IOException e) {
//                     throw new SchedulingException(null, "Cannot read NodePool file \"" + npfile + "\": I/O Error.");
//                 }
            }
        }
        
        // read in the class definitions
        String cn = props.getProperty("scheduling.class_set");
        if ( cn == null ) {
            throw new SchedulingException(null, "No class definitions found, scheduler cannot start.");
        }
        
        String[] classNames = cn.split("\\s+");
        logger.info(methodName, null, "Classes:");
        logger.info(methodName, null, ResourceClass.getHeader());
        logger.info(methodName, null, ResourceClass.getDashes());
        for ( String n : classNames ) {
        	n = n.trim();
            ResourceClass rc = new ResourceClass(n); //, nodepool.getMachinesByName(), nodepool.getMachinesByIp());
            rc.init(props);
            resourceClasses.put(rc, rc);
            resourceClassesByName.put(n, rc);
            logger.info(methodName, null, rc.toString());
        }
    }

    void initClassesX(String filename)
        throws Exception
    {
        String methodName = "initClasses";
        DuccProperties props = new DuccProperties();
        props.load(ducc_home + "/resources/" + filename);

        defaultClassName = props.getProperty("scheduling.default.name");
        String my_domain = getDomainName();

        // read in nodepools
        String npn = props.getProperty("scheduling.nodepool");
        if ( npn != null ) {
            String[] npnames = npn.split(" ");
            for ( String nodepoolName : npnames ) {
                int nporder = props.getIntProperty("scheduling.nodepool." + nodepoolName + ".order", 100);                
                String npfile = props.getProperty("scheduling.nodepool." + nodepoolName).trim();
                try {
                    String ducc_home = System.getProperty("DUCC_HOME");
                    npfile = ducc_home + "/resources/" + npfile;
                    BufferedReader br = new BufferedReader(new FileReader(npfile));
                    String node = "";
                    HashMap<String, String> npnodes = new HashMap<String, String>();
                    while ( (node = br.readLine()) != null ) {
                        int ndx = node.indexOf("#");
                        if ( ndx >0 ) {
                            node = node.substring(0, ndx);
                        }
                        node = node.trim();
                        if (node.equals("") ) {
                            continue;
                        }

                        npnodes.put(node, node);

                        // include fully and non-fully qualified names to allow sloppiness of config
                        ndx = node.indexOf(".");
                        String dnode;
                        if ( ndx >= 0 ) {
                            dnode = node.substring(0, ndx);
                            npnodes.put(dnode, dnode);
                        } else if ( my_domain != null ) {
                            dnode = node + "." + my_domain;
                            npnodes.put(dnode, dnode);
                        }
                    }
                    br.close();                        
                    nodepool.createSubpool(nodepoolName, npnodes, nporder);
                    
                } catch (FileNotFoundException e) {
                    throw new SchedulingException(null, "Cannot open NodePool file \"" + npfile + "\": file not found.");
                } catch (IOException e) {
                    throw new SchedulingException(null, "Cannot read NodePool file \"" + npfile + "\": I/O Error.");
                }
            }
        }
        
        // read in the class definitions
        String cn = props.getProperty("scheduling.class_set");
        if ( cn == null ) {
            throw new SchedulingException(null, "No class definitions found, scheduler cannot start.");
        }
        
        String[] classNames = cn.split("\\s+");
        logger.info(methodName, null, "Classes:");
        logger.info(methodName, null, ResourceClass.getHeader());
        logger.info(methodName, null, ResourceClass.getDashes());
        for ( String n : classNames ) {
        	n = n.trim();
            ResourceClass rc = new ResourceClass(n); //, nodepool.getMachinesByName(), nodepool.getMachinesByIp());
            rc.init(props);
            resourceClasses.put(rc, rc);
            resourceClassesByName.put(n, rc);
            logger.info(methodName, null, rc.toString());
        }
    }



    /**
     * Called only from schedule, under the 'this' monitor.
     *
     * We then take the SchedulingUpdate from the IScheduler and dispatches orders to
     * the world to make it happen.
     *
     * For jobs that lose resources, job manager is asked to stop execution in specific shares.
     * For jobs that gain resources, job manager is asked to start execution in specific shares.
     * Jobs that don't change are leftovers.  If they're not running at all, they're in the pending
     *  list; they might also be in the running list but had no allocation changes in the current epoch.
     */
    private JobManagerUpdate dispatch(SchedulingUpdate upd, JobManagerUpdate jmu)
    {
        String methodName = "dispatch";
        HashMap<IRmJob, IRmJob> jobs;

        // Go through shrunken jobs - if they are shrunken to 0, move to dormant
        jobs = upd.getShrunkenJobs();
        for (IRmJob j : jobs.values()) {
            
            logger.info(methodName, j.getId(), ">>>>>>>>>> SHRINK");

            HashMap<Share, Share> sharesE = j.getAssignedShares();
            HashMap<Share, Share> sharesR = j.getPendingRemoves();
            logger.info(methodName, j.getId(), "removing", sharesR.size(), "of existing", sharesE.size(), "shares.");

            for ( Share s : sharesE.values() ) {
                logger.debug(methodName, j.getId(), "    current", s.toString());
            }

            for ( Share s : sharesR.values() ) {
                logger.debug(methodName, j.getId(), "    remove ", s.toString());
            }
            logger.info(methodName, j.getId(), ">>>>>>>>>>");

            jmu.removeShares(j, sharesR);
            // jobManager.stopJob(j, shares);                 // stops job on everything on the pendingRemoves list
            // j.clearPendingRemoves();
        }

        // Go through expanded jobs - if they are dormant, remove from dormant
        //                            then add to running.
        // Tell the server it needs to start some machines for the job
        jobs = upd.getExpandedJobs();
        for (IRmJob j : jobs.values() ) {
            HashMap<Share, Share> sharesE = j.getAssignedShares();
            HashMap<Share, Share> sharesN = j.getPendingShares();        	

            logger.info(methodName, j.getId(), "<<<<<<<<<<  EXPAND");
            logger.info(methodName, j.getId(), "adding", sharesN.size(), "new shares to existing", sharesE.size(), "shares.");

            for ( Share s : sharesE.values()) {
                logger.debug(methodName, j.getId(), "    existing ", s.toString());
            }

            for ( Share s : sharesN.values()) {
                logger.debug(methodName, j.getId(), "    expanding", s.toString());
            }
            logger.info(methodName, j.getId(), "<<<<<<<<<<");

            sharesN = j.promoteShares();
            if ( sharesN.size() == 0 ) {
                // internal error - should not be marked expanded if no machines
                throw new SchedulingException(j.getId(), "Trying to execute expanded job but no pending machines.");
            }

            for ( Share s : sharesN.values()) {                           // update machine books                
                // Sanity checks on the bookkeeping
                busyShares.put(s.getId(), s);                
            }

//            DuccId id = j.getId();                                  // pull from dormant, maybe
//            if ( dormantJobs .containsKey(id) ) {
//                dormantJobs .remove(id);
//            }

            //runningJobs.put(id, j);
            jmu.addShares(j, sharesN);
            // jobManager.executeJob(j, shares);                      // will update job's pending lists

        }

        jobs = upd.getStableJobs();                             // squirrel these away to try next time
        for (IRmJob j: jobs.values()) {
            if ( j.countNShares() < 0 ) {
                throw new SchedulingException(j.getId(), "Share count went negative " + j.countNShares());
            }
            logger.info(methodName, j.getId(), ".......... STABLE with ", j.countNShares(), " shares.");
        }

        jobs = upd.getDormantJobs();                             // squirrel these away to try next time
        for (IRmJob j: jobs.values()) {
            logger.info(methodName, j.getId(), ".......... DORMANT");
//            dormantJobs .put(j.getId(), j);
        }

        jobs = upd.getReservedJobs();
        for (IRmJob j: jobs.values()) {
            logger.info(methodName, j.getId(), "<<<<<<<<<<  RESERVE");

            HashMap<Share, Share> sharesE = j.getAssignedShares();
            HashMap<Share, Share> sharesN = j.getPendingShares();        	

            if ( sharesE.size() == j.countInstances() ) {
                logger.info(methodName, j.getId(), "reserve_stable", sharesE.size(), "machines");
            } else  if ( sharesN.size() == j.countInstances() ) {           // reservation is complete but not yet confirmed?
                logger.info(methodName, j.getId(), "reserve_adding", sharesN.size(), "machines");
                for ( Share s : sharesN.values()) {
                    logger.debug(methodName, j.getId(), "    reserve_expanding ", s.toString());
                }
                jmu.addShares(j, sharesN);                
                j.promoteShares();
            } else {
                logger.info(methodName, j.getId(), "reserve_pending", j.countInstances(), "machines");
            }
            logger.info(methodName, j.getId(), "<<<<<<<<<<");
        }

        jmu.setAllJobs(allJobs);

        jobs = upd.getRefusedJobs();
        Iterator<IRmJob> iter = jobs.values().iterator();
        while ( iter.hasNext() ) {
            IRmJob j = iter.next();
            logger.info(methodName, j.getId(), ".......... REFUSED");
        }

        return jmu;
    }

    /**
     * We don't accept new work or even Orchestrator state updates until "ready". We do
     * want machines, but be sure the internal structures are protected.
     */
    public synchronized boolean ready()
    {
    	return stability;
    }

    public synchronized void start()
    {
        stability = true;
    }

    protected void handleDeadNodes()
    {
    	String methodName = "handleDeadNodes";
    	
        if ( ! isInitialized() ) {
            return;
        }

        HashMap<Node, Node> nodeUpdates = new HashMap<Node, Node>();
        synchronized(deadNodes) {
            nodeUpdates.putAll(deadNodes);
            deadNodes.clear();
        }

        synchronized(this) {

            for ( Node n : nodeUpdates.values() ) {
                Machine m = nodepool.getMachine(n);

                if ( m == null ) {
                    // must have been removed because of earlier missed hb
                    continue;
                }

                logger.warn(methodName, null, "***Purging machine***", m.getId(), "due to missed heartbeats. THreshold:",  nodeStability);
                NodePool np = m.getNodepool();
                np.nodeLeaves(m);
            }
        }        
    }

    /**
     * We first accept any changes and requests from the outside world and place them where they
     * can be acted on in this epoch.
     *
     * We then pass all relevent requests and resources to the IScheduler.  This returns a
     * SchedulingUpdate which is passed to the dispatcher to be acted upon.
     */
    public JobManagerUpdate schedule()
    {
    	String methodName = "schedule";


//         if ( startupCountdown++ < nodeStability ) {
//             logger.info(methodName, null, "Startup countdown:", startupCountdown, "of", nodeStability);
//             return null;
//         }

        if ( ! ready() ) {
            return null;
        }

        if ( ! isInitialized() ) {
            return null;
        }

        // tracking the OR hang problem - are topics being delivered?
        logger.debug("nodeArrives", null, "Total arrivals:", total_arrivals);

        handleDeadNodes();
        nodepool.reset(NodePool.getMaxOrder());

        // TODO: Can we combine these two into one?
        SchedulingUpdate upd = new SchedulingUpdate();              // state from internal scheduler
        JobManagerUpdate jmu = new JobManagerUpdate();              // state we forward to job manager

        // int nchanges = 0;
    	

        ArrayList<IRmJob> jobsToRecover = new ArrayList<IRmJob>();
        synchronized(recoveredJobs) {
            jobsToRecover.addAll(recoveredJobs);
            recoveredJobs.clear();
            // nchanges += jobsToRecover.size();
        }

        ArrayList<IRmJob> newJobs = new ArrayList<IRmJob>();
        // 
        // If there are new jobs we need to init some things and start a scheduling cycle.
        //
        synchronized(incomingJobs) {            
            newJobs.addAll(incomingJobs);
            incomingJobs.clear();
            // nchanges += newJobs.size();
        }

        //
        // If some jobs pased initializion we need to signal a scheduling cycle to get
        // them their fair share
        //
//        synchronized(initializedJobs) {            
//            if ( initializedJobs.size() > 0 ) {
//                nchanges++;
//            }
//            initializedJobs.clear();
//        }

        //
        // If some jobs completed we need to process clearning them out and signal a
        // scheduling cycle to try to reuse their resources.
        //
        ArrayList<IRmJob> doneJobs = new ArrayList<IRmJob>();
        synchronized(completedJobs) {
            doneJobs.addAll(completedJobs);
            completedJobs.clear();
            //nchanges += doneJobs.size();
        }

        //
        // If some shares were vacated we need to clear them out and run a scheduling cycle.
        //
        ArrayList<Pair<IRmJob, Share>> doneShares= new ArrayList<Pair<IRmJob, Share>>();
        synchronized(vacatedShares) {
            doneShares.addAll(vacatedShares.values());
            vacatedShares.clear();
            //nchanges += doneShares.size();

            // we use the vacatedShares object to control share growth as well
            //if ( growthOccurred ) nchanges++;
            //growthOccurred = false;
        }

//         boolean must_run = false;
//         synchronized(force_epoch) {
//             must_run = force_epoch;
//             force_epoch = false;
//         }

//         if ( (nchanges == 0) && !must_run ) { 
//             jmu.setAllJobs(allJobs);
//             return jmu;
//         }
// TODO if we remove this code above be sure to clear out all the force_epoch nonsense
// TODO does this even use growthOccurred?

        synchronized(this) {

            // before looking at jobs, insure we're updated after a crash
            for ( IRmJob j : jobsToRecover ) {
                processRecovery(j);
            }

            // process these next to free up resources for the scheduling cycle
            for (Pair<IRmJob, Share> p : doneShares) {
                processCompletion(p.first(), p.second());
            }

            for (IRmJob j : doneJobs) {
                processCompletion(j);
            }

            // update user records, "check in" new jobs
            if ( newJobs.size() > 0 ) {
                logger.info(methodName, null, "Jobs arrive:");
                logger.info(methodName, null, "submit", RmJob.getHeader());
            }

            Iterator<IRmJob> iter = newJobs.iterator();
            while ( iter.hasNext() ) {
                IRmJob j = iter.next();


                if ( j.isRefused() ) {          // the JobManagerConverter has already refused it
                    upd.refuse(j, j.getRefusalReason());
                }

                String user = j.getUserName();
                User u = users.get(user);
                if ( u == null ) {
                    u = new User(user);
                    users.put(user, u);
                }
                j.setUser(u);

                // Calculate its share order
                int share_order = calcShareOrder(j.getMemory());
                j.setShareOrder(share_order);

                // Assign it to its priority class
                String clid = j.getClassName();
                ResourceClass prclass = resourceClassesByName.get(clid);

                u.addJob(j);
                allJobs.put(j.getId(), j);
                if ( prclass == null ) {                    
                    upd.refuse(j, "Cannot find priority class " + clid + " for job");
                    continue;
                }

                /**
                 * We want to allow this - a normal job, submitted to a reservation class.
                   if ( (prclass.getPolicy() == Policy.RESERVE ) && ( ! j.isReservation() ) ) {
                   upd.refuse(j, "Reservaction class " + 
                   prclass.getId() + " specified but work is not a reservation.");
                   continue;
                   }
                */

                if ( ((prclass.getPolicy() != Policy.RESERVE ) && (prclass.getPolicy() != Policy.FIXED_SHARE)) && ( j.isReservation() ) ) {
                    upd.refuse(j, "Class " + prclass.getName() + " is policy " + 
                               prclass.getPolicy() + " but the work is submitted as a reservation.");
                    continue;
                }

                prclass.addJob(j);
                j.setResourceClass(prclass);
                logger.info(methodName, j.getId(), "submit", j.toString());
            }

            logger.info(methodName, null, "Scheduling " + newJobs.size(), " new jobs.  Existing jobs: " + allJobs.size());
            scheduler.schedule(upd);

            logger.info(methodName, null, "--------------- Scheduler returns ---------------");
            logger.info(methodName, null, "\n", upd.toString());
            logger.info(methodName, null, "------------------------------------------------");                
            dispatch(upd, jmu);                 // my own job lists get updated by this

            return jmu;
        }
    }

    synchronized public void shutdown()
    {
        done = true;
    }

//     public void run()
//     {
//     	String methodName = "run";
//         while ( ! done ) {
//             try { sleep(epoch); } catch (InterruptedException e) { }

//             logger.info(methodName, null, "========================== Epoch starts ===========================");
//             try {
//                 schedule();
//             } catch ( SchedulingException e ) {
//                 logger.info(methodName, e.jobid, e);
//             }

//             logger.info(methodName, null, "========================== Epoch ends   ===========================");
//         }
//     }

    private int total_arrivals = 0;
    public void nodeArrives(Node node)
    {        
    	// String methodName = "nodeArrives";
        // The first block insures the node is in the scheduler's records as soon as possible

        total_arrivals++;       // report these in the main schedule loop
        synchronized(this) {
            // the amount of memory available for shares, adjusted with configured overhead

            Machine m = nodepool.getMachine(node);
            int share_order = 0;

            if ( m == null ) {
                allNodes.put(node, node);
                long allocatable_mem =  node.getNodeMetrics().getNodeMemory().getMemTotal() - share_free_dram;
                if ( dramOverride > 0 ) {
                    allocatable_mem = dramOverride;
                }
                share_order = (int) (allocatable_mem / share_quantum);           // conservative - rounds down (this will always cast ok)                
            } else {
                share_order = m.getShareOrder();
            }
            
            m = nodepool.nodeArrives(node, share_order);                         // announce to the nodepools
            // m.heartbeat_down();
            // logger.info(methodName, null, "Node arrives:", m.getId());                                                              // make SURE it's reset ok
        }

        // The second block registers it in the heartbeat map
//        synchronized(incomingNodes) {
//            incomingNodes.put(node, node);
//        }
    }

    public void nodeDeath(Map<Node, Node> nodes)
    {
        synchronized(deadNodes) {
            deadNodes.putAll(nodes);
        }
    }

    /**
     * Callback from job manager, need shares for a new fair-share job.
     */
    public void signalNewWork(IRmJob job)
    {
        // We'll synchronize only on the incoming job list 
        synchronized(incomingJobs) {            
            incomingJobs.add(job);
        }
    }

//     public void signalForceEpoch()
//     {
//         synchronized( force_epoch ) {
//             force_epoch = true;
//         }
//     }

    public void signalInitialized(IRmJob job)
    {
        // We'll synchronize only on the incoming job list 
        synchronized(initializedJobs) {            
            initializedJobs.add(job);
        }
    }

    public void signalRecovery(IRmJob job)
    {
        synchronized(recoveredJobs) {
            recoveredJobs.add(job);
        }
    }

    public void jobCancelled(DuccId id)
    {
        // TODO Fill this in.
    }

    /**
     * Callback from job manager when a job completes. We just believe him, no sanity checks or other such stuff.
     */
    public void signalCompletion(DuccId id)
    {
        String methodName = "signalCompletion";
        synchronized(completedJobs) {
            try {
                IRmJob job = allJobs.get(id);
                logger.info(methodName, id, "Job completion signal.");
                completedJobs.add(job);
            } catch (Exception e) {
                logger.warn(methodName, id, e);
            }
        }
    }

    /**
     * Callback from job manager when a specific share exits but the job is still alive.
     */
    public void signalCompletion(IRmJob job, Share share)
    {
        String methodName = "signalCompletion";
        synchronized(vacatedShares) {
            logger.info(methodName, job.getId(), "Job vacate signal share: ", share.toString());
            vacatedShares.put(share.getId(), new Pair<IRmJob, Share>(job, share));
        }            
    }

    /**
     * Callback from job manager when a specific share gets a process associated.
     */
//    public void signalGrowth(DuccId jobid, Share share)
//    {
//        String methodName = "signalGrowth";
//        synchronized(vacatedShares) {
//            logger.info(methodName, jobid, "Job growth signal share: ", share.toString());
//            growthOccurred = true;
//        }            
//    }

    /**
     * Called in scheduling cycle, to actually complete the job - avoids deadlock
     */
    private synchronized void processCompletion(IRmJob job)
    {
        String methodName = "processCompletion";
        logger.info(methodName, job.getId(), "Job completes.");

        // -- clean up the running jobs list
        IRmJob j = allJobs.remove(job.getId());
        if ( j == null ) {
            logger.info(methodName, job.getId(), "Job is not in run list!");  // can happen if job is refused very early
            return;
        }

        j.markComplete();

        // -- clean up user list
        User user = users.get(j.getUserName());
        if ( user.remove(job) == 0 ) {
            users.remove(user.getName());
        }

        ResourceClass rc = job.getResourceClass();
        if ( rc != null ) {
            rc.removeJob(j);            // also clears it if it's a reservation
        } else if ( !j.isRefused() ) {
            throw new SchedInternalError(j.getId(), "Job exits from class " + job.getClassName() + " but we cannot find the priority class definition.");
        }


        // -- clean up machine lists
        HashMap<Share, Share> shares= job.getAssignedShares();        
        for (Share s: shares.values()) {
            purgeShare(s, job);
        }
        job.removeAllShares();
    }

    /**
     * Called from scheduling cycle - a specific share has run out of work for the give job (but the
     * job is not done yet).
     */
    private synchronized void processCompletion(IRmJob job, Share share)
    {
        String methodName = "processCompletion";
        
        logger.debug(methodName, job.getId(), "Job vacates share ", share.toString());
        //share.removeJob();
        job.removeShare(share);
        purgeShare(share, job);
    }

    private synchronized void processRecovery(IRmJob j)
    {
    	String methodName = "processRecovery";

        int share_order = calcShareOrder(j.getMemory());
        ResourceClass rc = resourceClassesByName.get(j.getClassName());
        j.setShareOrder(share_order);
        j.setResourceClass(rc);
        HashMap<Share, Share> shares = j.getRecoveredShares();
        StringBuffer sharenames = new StringBuffer();
        for ( Share s : shares.values() ) {
            sharenames.append(s.toString());
            sharenames.append(" ");

            if ( rc.getPolicy() != Policy.RESERVE ) {          // if it's RESERVE, the share order is already set from
                                                               // the machine when the job arrives. 
                s.setShareOrder(share_order);
            }
            Machine m = s.getMachine();
            NodePool np = m.getNodepool();
            np.connectShare(s, m, j, s.getShareOrder());

            busyShares.put(s.getId(), s);
        }
        String username = j.getUserName();
        User user = users.get(username);
        if ( user == null ) {
            user = new User(username);
            users.put(username, user);
            logger.info(methodName, j.getId(), "&&&&&&&&&&&&&&&& new user", user.toString(), "-------------------");
        }
        j.setUser(user);
        user.addJob(j);

    	j.promoteShares();                       // NOT expanded, just recovered, promote them right away
        j.clearRecoveredShares();

        String clid = j.getClassName();
        ResourceClass prclass = resourceClassesByName.get(clid);
        
        allJobs.put(j.getId(), j);
        prclass.addJob(j);
        j.setResourceClass(prclass);
        logger.info(methodName, j.getId(), "Recovered job:", j.toString());
        logger.info(methodName, j.getId(), "Recovered shares:", sharenames.toString());
    }

    /**
     * The share is gone, purge from our structures.
     */
    private void purgeShare(Share s, IRmJob j)
    {
        busyShares.remove(s.getId());         // so long, and thanks for all the fish
        Machine m = s.getMachine();
        m.removeShare(s);
    }

    public synchronized static DuccId newId()
    {
        DuccId id = idFactory.next();
        return id;
    }

    public void queryMachines()
    {
        nodepool.queryMachines();
    }

    class MachineByOrderSorter
    	implements Comparator<Machine>
    {	
    	public int compare(Machine m1, Machine m2)
        {
            if (m1.getShareOrder() == m2.getShareOrder()) {
                return (m1.getId().compareTo(m2.getId()));
            }
            return (int) (m1.getShareOrder() - m2.getShareOrder());
        }
    }


}
