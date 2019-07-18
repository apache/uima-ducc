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
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.uima.ducc.common.IDuccEnv;
import org.apache.uima.ducc.common.Node;
import org.apache.uima.ducc.common.NodeConfiguration;
import org.apache.uima.ducc.common.NodeIdentity;
import org.apache.uima.ducc.common.Pair;
import org.apache.uima.ducc.common.admin.event.RmAdminQLoadReply;
import org.apache.uima.ducc.common.admin.event.RmAdminQOccupancyReply;
import org.apache.uima.ducc.common.admin.event.RmAdminReply;
import org.apache.uima.ducc.common.admin.event.RmAdminVaryReply;
import org.apache.uima.ducc.common.admin.event.RmQueriedClass;
import org.apache.uima.ducc.common.admin.event.RmQueriedMachine;
import org.apache.uima.ducc.common.admin.event.RmQueriedNodepool;
import org.apache.uima.ducc.common.component.AbstractDuccComponent;
import org.apache.uima.ducc.common.db.DbHelper;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccProperties;
import org.apache.uima.ducc.common.utils.DuccPropertiesResolver;
import org.apache.uima.ducc.common.utils.IllegalConfigurationException;
import org.apache.uima.ducc.common.utils.SystemPropertyResolver;
import org.apache.uima.ducc.common.utils.Version;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.common.utils.id.DuccIdFactory;
import org.apache.uima.ducc.rm.persistence.access.IPersistenceAccess;
import org.apache.uima.ducc.rm.persistence.access.PersistenceAccess;


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
    static DuccId jobid = null;
    String dot_regex = ".regex";
    boolean done = false;
    // Boolean force_epoch = false;
    String ducc_home;
    // Integer epoch = 5;                                                 // scheduling epoch, seconds

    NodeConfiguration configuration = null;                               // UIMA-4142 make it global
    
    String defaultDomain = null;                                          // UIMA-4142
    boolean needRecovery = false;                                         // UIMA-4142 tell outer layer that recovery is required
    AbstractDuccComponent baseComponent;                                  // UIMA-4142, pass in the base for reconfig - reread ducc.properties
    NodePool[] nodepools;                                                 // top-level nodepools
    List<String> listRules = new ArrayList<String>();  // ordered list of rules 
    Map<String,NodePool> mapRules = new HashMap<String,NodePool>();
    int max_order = 0;

    //
    // Fair-share and fixed-share use shares only, not machines
    //
    Map<DuccId, Share> busyShares        = new HashMap<DuccId, Share>(); // Running "fair" share jobs

    // incoming reports of machines that are now free
    Map<DuccId, Pair<IRmJob, Share>> vacatedShares= new HashMap<DuccId, Pair<IRmJob, Share>>();
    // boolean growthOccurred = false;                                           // don't care which grew, just that something grew

    List<IRmJob>        incomingJobs    = new ArrayList<IRmJob>();       // coming in from external world but not added our queues yet
    List<IRmJob>        recoveredJobs   = new ArrayList<IRmJob>();       // coming in from external world but we don't now about them, (hopefully
                                                                         //    because we crashed and not for more nefarious reasons)
    List<IRmJob>        completedJobs   = new ArrayList<IRmJob>();       // signaled complete from outside but not yet dealt with
    List<IRmJob>        initializedJobs = new ArrayList<IRmJob>();       // Init is complete so we can begin full (un)fair share allocation

    //HashMap<Node, Node> incomingNodes  = new HashMap<Node, Node>();         // node updates
    Map<Node, Node> deadNodes      = new HashMap<Node, Node>();           // missed too many heartbeats
    Map<Node, Integer> illNodes    = new HashMap<Node, Integer>();        // starting to miss, keep track of how many for the db
    // HashMap<Node, Node> allNodes       = new HashMap<Node, Node>();           // the guys we know
    Map<String, NodePool>    nodepoolsByNode = new HashMap<String, NodePool>(); // all nodes, and their associated pool
    Map<String, String>      shortToLongNode = new HashMap<String, String>();   // 

    Map<String, User>    users     = new HashMap<String, User>();         // Active users - has a job in the system
    //HashMap<DuccId, IRmJob>    runningJobs = new HashMap<DuccId, IRmJob>();

    Map<DuccId, IRmJob>  allJobs = new HashMap<DuccId, IRmJob>();

    Map<ResourceClass, ResourceClass> resourceClasses = new HashMap<ResourceClass, ResourceClass>();
    Map<String, ResourceClass> resourceClassesByName = new HashMap<String, ResourceClass>();

    String defaultFairShareName = null;
    String defaultReserveName = null;

    int defaultNThreads = 1;
    int defaultNTasks = 10;
    int defaultMemory = 15;

    // these two are initialized in constructor
    String schedImplName;
    IScheduler[] schedulers;

    long share_free_dram  = 0;               // 0  GB in KB  - minim memory after shares are allocated
    long dramOverride     = 0;               // if > 0, use this instead of amount reported by agents (modeling and testing)

    int pending_evictions = 0;                    // for queries
    int pending_expansions = 0;                  // for queries

    EvictionPolicy evictionPolicy = EvictionPolicy.SHRINK_BY_MACHINE;

//     int nodeMetricsUpdateRate = 30000;
//     int startupCountdown = 0;       // update each epoch.  only schedule when it's > nodeStability
    int nodeStability = 3;
    boolean stability = false;

    private static DuccIdFactory idFactory;
    private IPersistenceAccess persistenceAccess = PersistenceAccess.getInstance();

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
    // 1.0.1 - RM can purge non-preemptables except for Unmanaged Reservations.  UIMA-3614
    // 1.0.2 - vary-on, vary-off
    // 1.0.3 - fix bad check in recursion in NodepoolScheduler.doEvictions
    // 1.1.0 - Syncnronize with release
    final static int rmversion_major = 2;
    final static int rmversion_minor = 0;
    final static int rmversion_ptf   = 0;  
    final static String rmversion_string = null;

    boolean initialized = false;           // we refuse nodeupdates until this is true
    public Scheduler(AbstractDuccComponent baseComponent)
    {
        this.baseComponent = baseComponent;                // UIMA-4142, pass in the base for reconfig
    }

    public synchronized void init()
        throws Exception
    {
        String methodName = "init";
        //setName("Scheduler");

        DuccLogger.setUnthreaded();

        String ep         = SystemPropertyResolver.getStringProperty("ducc.rm.eviction.policy", "SHRINK_BY_MACHINE");
        evictionPolicy    = EvictionPolicy.valueOf(ep);        

        // nodepool          = new NodePool(null, evictionPolicy, 0);   // global nodepool
        share_free_dram   = SystemPropertyResolver.getLongProperty("ducc.rm.reserved.dram", share_free_dram) * 1024 * 1024;   // GB -> KB
        ducc_home         = SystemPropertyResolver.getStringProperty("DUCC_HOME");

        // some defaults, for jobs that don't specify them
        defaultNTasks     = SystemPropertyResolver.getIntProperty("ducc.rm.default.tasks", 10); 
        defaultNThreads   = SystemPropertyResolver.getIntProperty("ducc.rm.default.threads", 1);
        defaultMemory     = SystemPropertyResolver.getIntProperty("ducc.rm.default.memory", 15);      // in GB
        // expandByDoubling  = RmUtil.getBooleanProperty("ducc.rm.expand.by.doubling", true);

        nodeStability     = SystemPropertyResolver.getIntProperty("ducc.rm.node.stability", 3);        // number of node metrics updates to wait for before scheduling
                                                                                  // 0 means, just jump right in and don't wait

        dramOverride = SystemPropertyResolver.getLongProperty("ducc.rm.override.dram", 0);
        if ( dramOverride > 0 ) {
            dramOverride = dramOverride * (1024 * 1024);         // convert to KB
        }

        if ( idFactory == null ) {                               // UIMA-4142 only remake it on first boot
            idFactory = new DuccIdFactory(1);
        }

//        try {
//            schedImplName = SystemPropertyResolver.getStringProperty("ducc.rm.scheduler", "org.apache.uima.ducc.rm.ClassBasedScheduler");
//            @SuppressWarnings("unchecked")
//			Class<IScheduler> cl = (Class<IScheduler>) Class.forName(schedImplName);
//            scheduler = (IScheduler) cl.newInstance();
//        } catch (ClassNotFoundException e) {
//            throw new SchedulingException(null, "Cannot find class " + schedImplName);
//        } catch (InstantiationException e) {
//            throw new SchedulingException(null, "Cannot instantiate class " + schedImplName);            
//        } catch (IllegalAccessException e) {
//            throw new SchedulingException(null, "Cannot instantiate class " + schedImplName + ": can't access constructor.");            
//        }

        String class_definitions = SystemPropertyResolver
            .getStringProperty(DuccPropertiesResolver
                               .ducc_rm_class_definitions, "scheduler.classes");
        class_definitions = System.getProperty("DUCC_HOME") + "/resources/" + class_definitions;

        try {
            initClasses();
        } catch ( Exception e ) {
            logger.error(methodName, null, e);
            throw e;
        }

        // we share most of the state with the actual scheduling code - no need to keep passing this around
        // TODO: Make sure these are all Sialized correctly
//         scheduler.setEvictionPolicy(evictionPolicy);
//         scheduler.setClasses(resourceClasses);
//         scheduler.setNodePool(nodepools[0]);

        logger.info(methodName, null, "                       reserved DRAM           : ", (share_free_dram / (1024*1024)), " GB");
        logger.info(methodName, null, "                       DRAM override           : ", (dramOverride / (1024*1024)), " GB");
        logger.info(methodName, null, "                       scheduler               : ", schedImplName);
        logger.info(methodName, null, "                       default threads         : ", defaultNThreads);
        logger.info(methodName, null, "                       default tasks           : ", defaultNTasks);
        logger.info(methodName, null, "                       default memory          : ", defaultMemory);
        logger.info(methodName, null, "                       default fairshare class : ", defaultFairShareName);
        logger.info(methodName, null, "                       default reserve         : ", defaultReserveName);
        logger.info(methodName, null, "                       reserve overage         : ", SystemPropertyResolver.getIntProperty("ducc.rm.reserve_overage", 0), " GB");
        logger.info(methodName, null, "                       class definition file   : ", class_definitions);
        logger.info(methodName, null, "                       default domain          : ", defaultDomain);      // UIMA-4142
        logger.info(methodName, null, "                       eviction policy         : ", evictionPolicy);
        logger.info(methodName, null, "                       database enabled        : ", DbHelper.isDbEnabled());
        logger.info(methodName, null, "                       database implementation : ", System.getProperty("ducc.rm.persistence.impl"));
        logger.info(methodName, null, "                       use prediction          : ", SystemPropertyResolver.getBooleanProperty("ducc.rm.prediction", true));
        logger.info(methodName, null, "                       prediction fudge factor : ", SystemPropertyResolver.getIntProperty("ducc.rm.prediction.fudge", 10000));
        logger.info(methodName, null, "                       node stability          : ", nodeStability);
        logger.info(methodName, null, "                       init stability          : ", SystemPropertyResolver.getIntProperty("ducc.rm.init.stability"));
        logger.info(methodName, null, "                       fast recovery           : ", SystemPropertyResolver.getBooleanProperty("ducc.rm.fast.recovery", true));
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
        logger.info(methodName, null, "                       OS Name                 : ", System.getProperty("os.name"));
        logger.info(methodName, null, "                       DUCC Version            : ", Version.version());
        logger.info(methodName, null, "                       RM Version              : ", ""+ rmversion_major   + "." 
                                                                                             + rmversion_minor   + "." 
                                                                                             + rmversion_ptf);
        persistenceAccess.clear();
        initialized = true;
    }

    public RmAdminReply reconfigure()          // UIMA-4142
    {
        String methodName = "reconfigure";

        RmAdminReply ret = new RmAdminReply();
        logger.info(methodName, null, "Reconfiguration starts.");

        setInitialized(false);           // stop receipt of OR and Agent publications

        // First we run the logic that reads the configuration, and if it fails, we abort the reconfig without crashing
        // We'll throw it away because we call init() in a minute, which will do the actual configuration, as if booting
		try {
			readConfiguration();                                    // UIMA-4142
		} catch (Throwable e) {
            setInitialized(true);
            logger.warn(methodName, null, "Reconfiguration aborted:", e.toString());
            ret.setRc(false);
            ret.setMessage("Reconfiguration failed: " + e.toString());
            return ret;
		}

        HashMap<Node, Machine> offlineMachines                   = new HashMap<Node, Machine>();
        for (NodePool np : nodepools) {
            offlineMachines.putAll(np.getOfflineMachines());            
        }

        // (be careful, don't use the value, that must be discarded as it points to the OLD np)
        List<String> offlineHostnames = new ArrayList<String>();
        for ( Machine m : offlineMachines.values()) {
        	logger.info(methodName, null, "Saving offline status of", m.getId());
            offlineHostnames.add(m.getId());
        }
        offlineMachines = null;

        this.configuration = null;
        this.defaultDomain = null;
        this.nodepools = null;
        this.mapRules.clear();
        this.listRules.clear();
        this.max_order = 0;
        this.busyShares.clear();
        this.vacatedShares.clear();
        this.incomingJobs.clear();
        this.recoveredJobs.clear();
        this.initializedJobs.clear();
        this.deadNodes.clear();
        this.nodepoolsByNode.clear();
        this.shortToLongNode.clear();
        this.users.clear();
        this.allJobs.clear();
        this.resourceClasses.clear();
        this.resourceClassesByName.clear();

        try {
            baseComponent.reloadProperties("ducc.deploy.configuration");
            init();

            if ( offlineHostnames.size() > 0 ) {
                String[] offline = offlineHostnames.toArray(new String[offlineHostnames.size()]);
                varyoff(offline);
            }
        } catch ( Throwable t ) {
        	// TODO do something?  What?  If this fails its pretty awful.
        }
        
        setRecovery(true);               // signal to outer layer that full recovery is needed
        setInitialized(true);            // resume receipt of publications
        logger.info(methodName, null, "Reconfiguration complete.");

        ret.setMessage("Reconfiguration complete.");
        return ret;
    }

    public synchronized void setRecovery(boolean v)
    {
        this.needRecovery = v;
    }

    public synchronized boolean mustRecover()
    {
        return this.needRecovery;
    }

    public synchronized boolean isInitialized()
    {
        return initialized;
    }

    public synchronized void setInitialized(boolean v)
    {
        this.initialized = v;
    }

    public Machine getMachine(Node n)
    {
        return getMachine(n.getNodeIdentity());
    }

    public Machine getMachine(NodeIdentity ni)
    {
        NodePool nodepool = getNodepoolByName(ni);
    	return nodepool.getMachine(ni);        
    }

    public void setJobManager(IJobManager jobmanager)
    {
        this.jobManager = jobmanager;
    }

    public String getDefaultFairShareName()
    {
    	return defaultFairShareName;
    }

    public String getDefaultReserveName()
    {
    	return defaultReserveName;
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
    public int calcShareOrder(IRmJob j)
    {
        // Calculate its share order
        long mem = j.getMemory() << 20 ;                             // to KB from GB
        int share_quantum = j.getShareQuantum();
        
        int share_order = (int) (mem / share_quantum);               // liberal calc, round UP
        if ( (mem % share_quantum) > 0 ) {
            share_order++;
        }
        return share_order;
    }


    /**
     * Collect all the classes served by the indicated nodepool (property set).  This fills
     * in the 'ret' map from the parameter 'dp' and recursive calls to the children in dp.

     * @param dp This is the properties object from the configurator for a top-level
     *            nodepool.
     * @param ret This is the map to be filled in by this routine.
     */
    void getClassesForNodepool(DuccProperties dp, Map<ResourceClass, ResourceClass> ret)
    {
        @SuppressWarnings("unchecked")
		List<DuccProperties> class_set = (List<DuccProperties>) dp.get("classes");
        if ( class_set != null ) {
            for ( DuccProperties cl : class_set ) {
                ResourceClass rc = resourceClassesByName.get(cl.getStringProperty("name"));
                ret.put(rc, rc);
            }
        }

        @SuppressWarnings("unchecked")
		List<DuccProperties> children = (List<DuccProperties>) dp.get("children");
        if ( children != null ) {
            for (DuccProperties child : children ) {
                getClassesForNodepool(child, ret);
            }
        }        
    }

    /**
     * Map each node by name into the nodepool it belongs to
     */
    void mapNodesToNodepool(Map<String, String> nodes, NodePool pool)
    {
        if ( nodes == null ) return;

        for ( String s : nodes.keySet() ) {
            updateNodepoolsByNode(s, pool);        // maps from both the fully-qualified name and th shortnmae
        }
    }
    
    String readNodepoolRegex(String nodefile)
    		throws IllegalConfigurationException
    {    	
    	String location = "readNodepoolRegex";
    	String regex = null;
    	BufferedReader br = null;
    	try {
    		if(nodefile == null) {
    			throw new IllegalConfigurationException("Missing parameter \"nodefile\".");
    		}
    		String fn = IDuccEnv.DUCC_HOME+"/resources/"+nodefile;
    		br = new BufferedReader(new FileReader(fn));
    		String line = null;
    		StringBuffer sb = new StringBuffer();
    		while ( (line = br.readLine()) != null ) {
    			sb.append(line.trim());
    		}
    		regex = sb.toString().toString();
    		if(regex.isEmpty()) {
    			throw new IllegalConfigurationException("Missing regex in "+nodefile);
    		}
    		try {
    			Pattern.compile(regex);
    			String text = nodefile+":"+regex;
    			logger.info(location, jobid, text);
    		}
    		catch(Exception e) {
    			throw new IllegalConfigurationException("Illegal regex in "+nodefile);
    		}
    	}
    	catch (FileNotFoundException e) {
            throw new IllegalConfigurationException("File not found: "+nodefile);
        } 
    	catch (IOException e) {
            throw new IllegalConfigurationException("File I/O error: "+nodefile);
        } 
    	catch ( Exception e ) {
    		e.printStackTrace();
    		throw new IllegalConfigurationException(e);
        } 
    	finally {
            if ( br != null ) {
                try { br.close(); } catch (IOException e) { }
            }
        }     
    	return regex;
    }
    
    void mapNodeRule(DuccProperties dp, NodePool nodepool) {
    	String location = "mapNodeRule";
    	try {
        	String nodefile = dp.getProperty("nodefile");
        	if(nodefile != null) {
        		if(nodefile.endsWith(dot_regex)) {
            		String noderule = readNodepoolRegex(nodefile);
            		addRule(noderule, nodepool);
            	}
        	}
        }
        catch(Exception e) {
        	logger.error(location, jobid, e);
        }
    }
        
    /*
     * only add rule if it is unique (first one seen wins)
     */
    private void addRule(String noderule, NodePool np) {
    	String location = "addRule";
    	if(noderule != null) {
    		if(mapRules.containsKey(noderule)) {
        		logger.warn(location, jobid, "duplicate ignored: ", noderule, np.getId());
        	}
        	else {
        		listRules.add(noderule);
        		mapRules.put(noderule, np);
        		logger.info(location, jobid, noderule, np.getId());
        	}
    	}
    }

    /**
     * (Recursively) build up the heirarchy under the parent nodepool.
     */
    void createSubpools(NodePool parent, List<DuccProperties> children)
    {
        if ( children == null ) return;

        for ( DuccProperties dp : children ) {
            String id = dp.getStringProperty("name");
            @SuppressWarnings("unchecked")
			Map<String, String> nodes = (Map<String, String>) dp.get("nodes");
            int search_order = dp.getIntProperty("search-order", 100);
            NodePool child = parent.createSubpool(id, nodes, search_order);
            mapNodesToNodepool(nodes, child);
            mapNodeRule(dp, child);
            @SuppressWarnings("unchecked")
			List<DuccProperties> grandkids = (List<DuccProperties>) dp.get("children");
            createSubpools(child, grandkids);            
        }
    }

    // UIMA-4142 better modularize this code
    NodeConfiguration readConfiguration()
        throws Exception
    {
        String class_definitions = SystemPropertyResolver
            .getStringProperty(DuccPropertiesResolver
                               .ducc_rm_class_definitions, "scheduler.classes");
        String user_registry = SystemPropertyResolver
            .getStringProperty(DuccPropertiesResolver
                               .ducc_rm_user_registry, "ducc.users");
        class_definitions = System.getProperty("DUCC_HOME") + "/resources/" + class_definitions;
        String me = Scheduler.class.getName() + ".Config";
        DuccLogger initLogger = new DuccLogger(me, COMPONENT_NAME);
        NodeConfiguration nc = new NodeConfiguration(class_definitions, null, user_registry, initLogger);        // UIMA-4142 make the config global
        nc.readConfiguration();
        return nc;                                             // UIMA-4142
    }

    // UIMA-4142, don't pass in class def file, instead use common readConfiguration
    void initClasses()
    {
    	String methodName = "initClasses";
        
		try {
            configuration = readConfiguration();
		} catch (Throwable e) {
            // RM boot.  We must abort being has we have no prior working configuration to fall back to.
            logger.error(methodName, null, e);
            logger.error(methodName, null, "Scheduler exits: unable to read configuration.");
            System.out.println("Scheduler exits: unable to read configuration.");
            e.printStackTrace();
            System.exit(1);
		}

        defaultDomain = configuration.getDefaultDomain();                   // UIMA-4142
        configuration.printConfiguration();

        DuccProperties[] nps = configuration.getToplevelNodepools();
        Map<String, DuccProperties> cls = configuration.getClasses();

        nodepools = new NodePool[nps.length];                   // top-level nodepools
        schedulers = new IScheduler[nps.length];                // a schedler for each top-level nodepool

        // Here build up the ResourceClass definitions
        logger.info(methodName, null, "Classes:");
        logger.info(methodName, null, ResourceClass.getHeader());
        logger.info(methodName, null, ResourceClass.getDashes());
        for ( DuccProperties props : cls.values() ) {
            ResourceClass rc = new ResourceClass(props);
            resourceClasses.put(rc, rc);
            resourceClassesByName.put(rc.getName(), rc);
            logger.info(methodName, null, rc.toString());
        }

        DuccProperties dc = configuration.getDefaultFairShareClass();
        if ( dc != null ) {
            defaultFairShareName = dc.getProperty("name");
        }

        dc = configuration.getDefaultReserveClass();
        if ( dc != null ) {
            defaultReserveName = dc.getProperty("name");
        }

        // Instatntiate one scheduler per top-level nodepool
        try {
            schedImplName = SystemPropertyResolver.getStringProperty("ducc.rm.scheduler", "org.apache.uima.ducc.rm.ClassBasedScheduler");
            @SuppressWarnings("unchecked")
			Class<IScheduler> cl = (Class<IScheduler>) Class.forName(schedImplName);
            for ( int i = 0; i < nps.length; i++ ) {
                logger.info(methodName, null, "Rebuilding", schedImplName, "for top level nodepool", nps[i].get("name"));
                schedulers[i] = (IScheduler) cl.newInstance();
                schedulers[i].setEvictionPolicy(evictionPolicy);
            }

        } catch (ClassNotFoundException e) {
            throw new SchedulingException(null, "Cannot find class " + schedImplName);
        } catch (InstantiationException e) {
            throw new SchedulingException(null, "Cannot instantiate class " + schedImplName);            
        } catch (IllegalAccessException e) {
            throw new SchedulingException(null, "Cannot instantiate class " + schedImplName + ": can't access constructor.");            
        }

        // Here create the nodepool configuration
        for ( int i = 0; i < nps.length; i++ ) {
            DuccProperties np = nps[i];
            String id = np.getStringProperty("name");
            @SuppressWarnings("unchecked")
			Map<String, String> nodes = (Map<String, String>) np.get("nodes");
            int search_order = np.getIntProperty("search-order", 100);
            int q = np.getIntProperty("share-quantum", 15) << 20 ;      // to kB which is how the nodes report in
            nodepools[i] = new NodePool(null, id, nodes, evictionPolicy, 0, search_order, q);
            schedulers[i].setNodePool(nodepools[i]);                    // set its top-level nodepool

            mapNodesToNodepool(nodes, nodepools[i]);
            logger.info(methodName, null, "Created top-level nodepool", id);
            mapNodeRule(np, nodepools[i]);
            @SuppressWarnings("unchecked")
			List<DuccProperties> children = (List<DuccProperties>) np.get("children");
            createSubpools(nodepools[i], children);

            Map<ResourceClass, ResourceClass> classesForNp = new HashMap<ResourceClass, ResourceClass>();
            getClassesForNodepool(np, classesForNp);           // all classes served by this heirarchy - fills in classesForNp
            for ( ResourceClass rc: classesForNp.values() ) {               // UIMA-4065 tell each cl which np serves it
                String rcid = rc.getNodepoolName();
                if ( rcid != null ) {
                    // set the two-way pointers between rc and np
                    NodePool subpool = nodepools[i].getSubpool(rcid);
                    rc.setNodepool(subpool);                  // rc -> nodepool
                    logger.info(methodName, null, "Assign rc", rc.getName(), "to np", subpool.getId());
                    subpool.addResourceClass(rc);             // nodepool -> rc
                }
            }

            schedulers[i].setClasses(classesForNp);

        }

        // Here create or update Users with constraints from the registry
        Map<String, DuccProperties> usrs = configuration.getUsers();                // UIMA-4275
        for ( Object o : usrs.keySet() ) {                     // iterate over users
            String n = (String) o;
            DuccProperties dp = usrs.get(n);
            for ( Object l : dp.keySet() ) {                  // iterate over limits for the user
                if ( !((String)l).startsWith("max-allotment")) continue; // only this supported at this time
                String val = ((String) dp.get(l)).trim();

                int lim = Integer.parseInt( val ); // verified parsable int during parsing


                User user = users.get(n);
                if (user == null) {
                    user = new User(n);
                    users.put(n, user);
                }

                if ( val.contains(".") ) {
                    String[] tmp = ((String)l).split("\\.");                // max_allotment.classname
                    ResourceClass rc = resourceClassesByName.get(tmp[1]);
                    user.overrideLimit(rc, lim);   // constrain allotment for this class to value in l
                } else {
                    user.overrideGlobalLimit(lim);
                }
            }
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

        pending_evictions = 0;                    // for queries
        pending_expansions = 0;                  // for queries

        // Go through shrunken jobs - if they are shrunken to 0, move to dormant
        jobs = upd.getShrunkenJobs();
        for (IRmJob j : jobs.values()) {
            
            logger.trace(methodName, j.getId(), ">>>>>>>>>> SHRINK");

            HashMap<Share, Share> sharesE = j.getAssignedShares();
            HashMap<Share, Share> sharesR = j.getPendingRemoves();
            logger.trace(methodName, j.getId(), "removing", sharesR.size(), "of existing", sharesE.size(), "shares.");
            pending_evictions += (sharesR.size() * j.getShareOrder());

            for ( Share s : sharesE.values() ) {
                logger.trace(methodName, j.getId(), "    current", s.toString());
            }

            for ( Share s : sharesR.values() ) {
                logger.trace(methodName, j.getId(), "    remove ", s.toString());
            }
            logger.trace(methodName, j.getId(), ">>>>>>>>>>");

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

            logger.trace(methodName, j.getId(), "<<<<<<<<<<  EXPAND");
            logger.trace(methodName, j.getId(), "adding", sharesN.size(), "new shares to existing", sharesE.size(), "shares.");
            pending_expansions += (sharesN.size() * j.getShareOrder());

            for ( Share s : sharesE.values()) {
                logger.trace(methodName, j.getId(), "    existing ", s.toString());
            }

            for ( Share s : sharesN.values()) {
                logger.trace(methodName, j.getId(), "    expanding", s.toString());
            }
            logger.trace(methodName, j.getId(), "<<<<<<<<<<");

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
            logger.trace(methodName, j.getId(), ".......... STABLE with ", j.countNShares(), " shares.");
        }

        jobs = upd.getDormantJobs();                             // squirrel these away to try next time
        for (IRmJob j: jobs.values()) {
            logger.trace(methodName, j.getId(), ".......... DORMANT");
//            dormantJobs .put(j.getId(), j);
        }

        jobs = upd.getReservedJobs();
        for (IRmJob j: jobs.values()) {
            logger.trace(methodName, j.getId(), "<<<<<<<<<<  RESERVE");

            HashMap<Share, Share> sharesE = j.getAssignedShares();
            HashMap<Share, Share> sharesN = j.getPendingShares();        	

            if ( sharesE.size() == j.getMaxShares() ) {
                logger.trace(methodName, j.getId(), "reserve_stable", sharesE.size(), "machines");
            } else  if ( sharesN.size() == j.getMaxShares() ) {           // reservation is complete but not yet confirmed?
                logger.trace(methodName, j.getId(), "reserve_adding", sharesN.size(), "machines");
                for ( Share s : sharesN.values()) {
                    logger.trace(methodName, j.getId(), "    reserve_expanding ", s.toString());
                }
                jmu.addShares(j, sharesN);                
                j.promoteShares();
            } else {
                logger.trace(methodName, j.getId(), "reserve_pending", j.getMaxShares(), "machines");
            }
            logger.trace(methodName, j.getId(), "<<<<<<<<<<");
        }

        jmu.setAllJobs((HashMap<DuccId, IRmJob>)allJobs);

        jobs = upd.getRefusedJobs();
        Iterator<IRmJob> iter = jobs.values().iterator();
        while ( iter.hasNext() ) {
            IRmJob j = iter.next();
            logger.trace(methodName, j.getId(), ".......... REFUSED");
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

    public void stop()
    {
        persistenceAccess.close();
    }

    protected void handleIllNodes()
    {
    	String methodName = "handleIllNodes";
    	
        if ( ! isInitialized() ) {
            logger.info(methodName, null, "Waiting for (re)initialization.");
            return;
        }

        HashMap<Node, Integer> nodeUpdates = new HashMap<Node, Integer>();
        synchronized(illNodes) {
            nodeUpdates.putAll(illNodes);
            illNodes.clear();
        }
        
        synchronized(this) {
            for ( Node n : nodeUpdates.keySet() ) {
                Machine m = getMachine(n);

                if ( m == null ) {
                    logger.warn(methodName, null, "Cannot find any record of machine", n.getNodeIdentity().getCanonicalName());
                    continue;
                }

                int count = nodeUpdates.get(n);
                if ( count == 0 ) {
                    m.heartbeatArrives();
                } else {
                    m.heartbeatMissed(count);
                }
            }
        }
    }

    protected void handleDeadNodes()
    {
    	String methodName = "handleDeadNodes";
    	
        if ( ! isInitialized() ) {
            logger.info(methodName, null, "Waiting for (re)initialization.");
            return;
        }

        HashMap<Node, Node> nodeUpdates = new HashMap<Node, Node>();
        synchronized(deadNodes) {
            nodeUpdates.putAll(deadNodes);
            deadNodes.clear();
        }

        synchronized(this) {

            for ( Node n : nodeUpdates.values() ) {
                Machine m = getMachine(n);

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
     * We then pass all relevant requests and resources to the IScheduler.  This returns a
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
            logger.info(methodName, null, "Waiting for (re)initialization.");
            return null;
        }

        // tracking the OR hang problem - are topics being delivered?
        logger.info("nodeArrives", null, "Total arrivals:", total_arrivals);

        synchronized(this) {
            handleIllNodes();
            handleDeadNodes();
            resetNodepools();
        }

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
                    logger.info(methodName, j.getId(), "Bypassing previously refused job.");
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
                int share_order = calcShareOrder(j);
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

                // UIMA-4275 never refuse impossible work, just let it hang out
//                 if ( share_order > max_order ) {
//                     upd.refuse(j, "Memory requested " + j.getMemory() + "GB exceeds the capacity of any machine in the cluster.");
//                     continue;
//                 }

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
                try {
					persistenceAccess.addJob(j);
				} catch (Exception e) {
					logger.warn(methodName, j.getId(), "Cannot persist new job in database:", e);					
				}
                logger.info(methodName, j.getId(), "submit", j.toString());
            }

            logger.info(methodName, null, "Scheduling " + newJobs.size(), " new jobs.  Existing jobs: " + allJobs.size());
            for ( int i = 0; i < schedulers.length; i++ ) {
                logger.info(methodName, null, "Run scheduler", i, "with top-level nodepool", nodepools[i].getId());
                schedulers[i].schedule(upd);
            }

            for ( IRmJob j : allJobs.values() ) {       // UIMA-4577 persist 'demand'
                try {
					persistenceAccess.updateDemand(j);
				} catch (Exception e) {
					logger.warn(methodName, j.getId(), "Cannot update demand in database:", e);
				}
            }

            // UIMA-5190 Reduce logging
            logger.debug(methodName, null, "--------------- Scheduler returns ---------------");
            logger.debug(methodName, null, "\n", upd.toString());
            logger.debug(methodName, null, "------------------------------------------------");                
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


    /**
     * maps from both the fully-qualified name and th shortnmae
     */
    void updateNodepoolsByNode(String longname, NodePool np)
    {
    	String methodName = "updateNodepoolsByNode";
        String shortname = longname;
        int ndx = longname.indexOf(".");

        logger.info(methodName, null, "Map", longname, "to", np.getId());
        nodepoolsByNode.put(longname, np);

        if ( ndx >= 0 ) {
            shortname = longname.substring(0, ndx);
            nodepoolsByNode.put(shortname, np);
            shortToLongNode.put(shortname, longname);
            logger.info(methodName, null, "Map", shortname, "to", np.getId());
        }
    }
    
    /*
     * find nodepool by rule, which is a regular expression
     */
    private NodePool findNodepoolByRule(NodeIdentity ni) {
    	String location = "findNodepoolByRule";
    	NodePool np = null;
    	NodePool testnp;
    	try {
    		String name1 = ni.getCanonicalName();
    		String name2 = ni.getShortName();
        	String ip = ni.getIp();
        	logger.info(location, jobid, mapRules.size(), name1, name2, ip);
        	for(String noderule : listRules) {
        		testnp = mapRules.get(noderule);
        		// match name with domain
        		if(name1.matches(noderule)) {
        		  np = testnp;
        			logger.info(location, jobid, "match by name: ", noderule, name1, np.getId());
        			break;
        		}
        		else {
        			logger.debug(location, jobid, "no match by name: ", noderule, name1);
        		}
        		// match name without domain
        		if(name2.matches(noderule)) {
              np = testnp;
        			logger.info(location, jobid, "match by name: ", noderule, name2, np.getId());
        			break;
        		}
        		else {
        			logger.debug(location, jobid, "no match by name: ", noderule, name2);
        		}
        		// match ip
        		if(ip.matches(noderule)) {
              np = testnp;
        			logger.info(location, jobid, "match by ip: ", noderule, ip,  np.getId());
        			break;
        		}
        		else {
        			logger.debug(location, jobid, "no match by ip: ", noderule, ip);
        		}
        	}
    	}
    	catch(Exception e) {
    		logger.error(location, jobid, e);
    	}
    	return np;
    }
    
    //
    // Return a nodepool by Node.  If the node can't be associated with a nodepool, return the
    // default nodepool, which is always the first one defined in the config file.
    //
    NodePool getNodepoolByName(NodeIdentity ni)
    {
    	String location = "getNodepoolByName";
    	NodePool np = findNodepoolByRule(ni);
    	if(np != null) {
    		String text = "node:"+ni.getShortName()+" "+np.getId()+" "+"add by rule.";
    		logger.info(location, jobid, text);
    		updateNodepoolsByNode(ni.getCanonicalName(), np);
    	}
    	if(np == null) {
    		np = nodepoolsByNode.get( ni.getCanonicalName() );
    	}
        if ( np == null ) {
            np = nodepoolsByNode.get( ni.getIp() );
        }
        if ( np == null ) {
            np = nodepools[0];
            updateNodepoolsByNode(ni.getCanonicalName(), np);     // assign this guy to the default np
            // nodepoolsByNode.put( ni.getName(), np);          // assign this guy to the default np
        }
        return np;
    }

    private int total_arrivals = 0;
    public synchronized void nodeArrives(Node node)
    {        
        String methodName = "nodeArrives";
        if ( ! isInitialized() ) {
            logger.info(methodName, null, "Waiting for (re)initialization; node = " + node.getNodeIdentity().getCanonicalName());
            return;
        }

        synchronized(illNodes) {        // stop flagging it as a problem
            illNodes.remove(node);
        }

    	// String methodName = "nodeArrives";
        // The first block insures the node is in the scheduler's records as soon as possible

        total_arrivals++;       // report these in the main schedule loop
        
        NodePool np = getNodepoolByName(node.getNodeIdentity());  // finds np assigned in ducc.nodes; if none, returns the default np
        Machine m = np.getMachine(node);
        int share_order = 0;

        // let's always recalculate this in case it changes for whatever bizarre reason (reboot, or pinned process gone, or whatever)
        long allocatable_mem =  node.getNodeMetrics().getNodeMemory().getMemFree() - share_free_dram;
        if ( dramOverride > 0 ) {
            allocatable_mem = dramOverride;
        }
        share_order = (int) (allocatable_mem / np.getShareQuantum());           // conservative - rounds down (this will always cast ok)                
        // NOTE: we cannot set the order into the machine yet, in case it has changed, because NodePool needs to adjust based
        //       on current and new

        max_order = Math.max(share_order, max_order);
        m = np.nodeArrives(node, share_order);                         // announce to the nodepools
        m.heartbeatArrives();
    }

    public void nodeHb(Node n, int count)
    {
        synchronized(illNodes) {
            illNodes.put(n, count);
        }
    }

    public void nodeDeath(Map<Node, Node> nodes)
    {
        synchronized(deadNodes) {
            deadNodes.putAll(nodes);
        }
    }

    /**
     * User passed us a node by name.  Maybe did and maybe didn't qualify it.  
     * Maybe the node checked in qualified maybe it didn't.  Here we try to find
     * something that kind of matches.
     * UIMA-4142.  Technically a bug on vary-on and vary-off but found and fixed as part of
     *             the indicated Jira.
     */
    synchronized String resolve(String node)
    {
        NodePool np = nodepoolsByNode.get(node);
        if ( np == null ) return null;                  // indexed by long and short so if not found we're stuck

        if ( np.hasNode(node) ) return node;            // he knows it by this name we're done

        int ndx = node.indexOf(".");
        if ( ndx > 0 ) {
            // np MUST know it by either long or short or it wouldn't be in nodepoolsByNode
            // so it must be short
            return node.substring(0, ndx);
        } else {
            // and vice-versa, it must be the long
            return shortToLongNode.get(node);
        }
    }

    public synchronized RmAdminReply varyon(String[] nodes)
    {
        String methodName = "varyon";
        RmAdminVaryReply ret = new RmAdminVaryReply();
        StringBuffer sb = new StringBuffer();
        for (String n : nodes ) {

            String rn = resolve(n);
            if ( rn == null ) {
                ret.setRc(false);
                ret.addFailedHost(n);
                sb.append("VaryOn: " + n + " cannot be found in the RM.\n");
            } else {                
                NodePool np = nodepoolsByNode.get(rn);  // if null, resolve will fail
                if ( np == null ) {
                    ret.setRc(false);
                    ret.addFailedHost(rn);
                    sb.append("VaryOn: " + n + " cannot find associated nodepool.\n");
                } else {
                    String repl = np.varyon(rn);
                    logger.info(methodName, null, repl);
                    sb.append(repl);
                    sb.append("\n");
                }
            }
        }
        ret.setMessage(sb.toString());
        return ret;
    }

    public synchronized RmAdminReply varyoff(String[] nodes)
    {
        String methodName = "varyoff";
        RmAdminVaryReply ret = new RmAdminVaryReply();
        StringBuffer sb = new StringBuffer();
        for (String n : nodes ) {

            String rn = resolve(n);
            if ( rn == null ) {
                ret.setRc(false);
                ret.addFailedHost(n);
                sb.append("VaryOff: " + n + " cannot be found in the RM.\n");
            } else {
                NodePool np = nodepoolsByNode.get(rn);  // if null, resolve will fail
                if ( np == null ) {
                    ret.setRc(false);
                    ret.addFailedHost(rn);
                } else {
                    String repl = np.varyoff(rn);
                    logger.info(methodName, null, repl);
                    sb.append(repl);
                    sb.append("\n");
                }
            }
        }
        ret.setMessage(sb.toString());
    	return ret;
    }

    RmQueriedNodepool  getNpStats(NodePool np)
    {

        RmQueriedNodepool ret = new RmQueriedNodepool();
        
        ret.setName(np.getId());
        ret.setOnline(np.countLocalMachines());
        ret.setDead(np.countLocalUnresponsiveMachines());
        ret.setOffline(np.countLocalOfflineMachines());
        
        ret.setSharesAvailable(np.countLocalShares());
        ret.setSharesFree(np.countLocalQShares());

        ret.setAllMachines(np.countAllLocalMachines());

        int[] onlineMachines = np.makeArray();
        int[] freeMachines = np.makeArray();
        for ( int i = 1; i < freeMachines.length; i++ ) {
            freeMachines[i] += np.countFreeMachines(i);         // (these are local, as we want)
        }

        //np.getLocalOnlineByOrder(onlineMachines);
        ret.setOnlineMachines(onlineMachines);
        ret.setFreeMachines(freeMachines);

        ret.setVirtualMachines(np.countLocalVMachinesByOrder());

        // logger.info(methodName, null, np.getId() + ": online", online, "dead", dead, "offline", offline, "shares_available", shares_available, "shares_free", shares_free);
        // logger.info(methodName, null, np.getId() + ": allMachines    ", Arrays.toString(allMachines));
        // logger.info(methodName, null, np.getId() + ": onlineByOrder  ", Arrays.toString(onlineMachines));

        // logger.info(methodName, null, np.getId() + "------- freeMachines should match free -------");
        // logger.info(methodName, null, np.getId() + ": freeMachines   ", Arrays.toString(freeMachines));
        // logger.info(methodName, null, np.getId() + ": free           ", Arrays.toString(free));
        // logger.info(methodName, null, np.getId() + "----------------------------------------------");
        // logger.info(methodName, null, np.getId() + ": virtualMachines", Arrays.toString(virtualMachines));

        return ret;
    }

    void calculateLoad(RmAdminQLoadReply reply)
    {

        for ( ResourceClass cl : resourceClasses.values() ) {
            RmQueriedClass qcl = new RmQueriedClass();

            switch ( cl.getPolicy() ) {
                case FAIR_SHARE:
                    qcl.setPolicy("FAIR_SHARE");
                    break;
                case FIXED_SHARE:
                    qcl.setPolicy("FIXED_SHARE");
                    break;
                case RESERVE:
                    qcl.setPolicy("RESERVE");
                    break;
            }

            // TODO MUST FIX THIS

            // int[] demanded = NodePool.makeArray();
            // int[] awarded  = NodePool.makeArray();

            // HashMap<IRmJob, IRmJob> jobs = cl.getAllJobs();
            // for ( IRmJob j : jobs.values() ) {
            //     int o = j.getShareOrder();
            //     demanded[o] += j.queryDemand();
            //     awarded[o]  += j.countNShares();
            // }
            
            // qcl.setName(cl.getName());
            // qcl.setDemanded(demanded);
            // qcl.setAwarded(awarded);
            // reply.addClass(qcl);
        }
    }

    void listAllNodepools(NodePool parent, ArrayList<NodePool> list)
    {
        list.add(parent);
        for (NodePool np : parent.getChildren().values() ) {
            listAllNodepools(np, list);
        }
    }

    public synchronized RmAdminQLoadReply queryLoad()
    {        

        RmAdminQLoadReply ret = new RmAdminQLoadReply();
        if ( ! ready() ) {
            ret.notReady();
            return ret;
        }

        calculateLoad(ret);

        ArrayList<NodePool> allpools = new ArrayList<NodePool>();
        for ( NodePool np : nodepools ) {
            listAllNodepools(np, allpools);
        }

        for ( NodePool np : allpools ) {
            ret.addNodepool(getNpStats(np));
        }
        
        return ret;
    }


    public synchronized RmAdminQOccupancyReply queryOccupancy()
    {
        RmAdminQOccupancyReply ret = new RmAdminQOccupancyReply();
        if ( ! ready() ) {
            ret.notReady();
            return ret;
        }

        //
        // iterate top-level nodepools to get all their subpools
        //   iterate the subpools to get all their machines
        //      iterage the machines and request a query object
        //         add query object to ret
        // return ret

        // We want to be dependent on common project, not the other way around, so
        // we keep the query objects in common and put knowledge of how to construc
        // them into rm's Machine class.
        //
        // The alternative, passing RM's Machine to the query object creates a circular
        // dependency with RM depending on common and common depending on RM.
        //
        
        //
        // Not a cheap query, by the way.
        //
        // NOTE: No longer used by the rm_qoccupancy script which now goes directly to the database
        //
        
        
        for ( NodePool np : nodepools ) {

            // NOTE:  The offline & dead nodes are also in the AllMachines list so must be removed
            Map<Node, Machine> allMachs     = np.getAllMachines();
            Map<Node, Machine> offline      = np.getOfflineMachines();          // UIMA-4234
            Map<Node, Machine> unresponsive = np.getUnresponsiveMachines();     // UIMA-4234

            for ( Node n : offline.keySet() ) {
                Machine m = offline.get(n);
                RmQueriedMachine qm = m.queryMachine();
                qm.setOffline();
                if ( unresponsive.containsKey(n) ) {
                    unresponsive.remove(n);
                    qm.setUnresponsive();
                }
                ret.addMachine(qm);
                allMachs.remove(n);
            }

            for ( Node n : unresponsive.keySet() ) {
                Machine m = unresponsive.get(n);
                RmQueriedMachine qm = m.queryMachine();
                qm.setUnresponsive();
                ret.addMachine(qm);
                allMachs.remove(n);
            }
            
            for ( Node n : allMachs.keySet() ) { 
              Machine m = allMachs.get(n);
              ret.addMachine(m.queryMachine());
          }
        }

        return ret;
    }

    public synchronized void signalState(DuccId jobid, String state)
    {
        IRmJob j = allJobs.get(jobid);
        if ( j != null ) {                // might not be here yet, we'll get it later
            j.setState(state);
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
                if ( job == null ) {
                    logger.warn(methodName, id, "Job completion signal: early termination; nothing to complete.");
                    return;  // canceled or terminated very soon.
                }

                logger.info(methodName, id, "Job completion signal.");
                completedJobs.add(job);
            } catch (Throwable t) {
                logger.warn(methodName, id, t);
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

        try {
			persistenceAccess.deleteJob(job);     // UIMA-4577
		} catch (Exception e) {
			logger.warn(methodName, job.getId(), "Cannot delete job from database:", e);
		}

        // -- clean up the running jobs list
        IRmJob j = allJobs.remove(job.getId());
        if ( j == null ) {
            logger.info(methodName, job.getId(), "Job is not in run list!");  // can happen if job is refused very early
            return;
        }

        j.markComplete();

        // -- clean up user list
        User user = users.get(j.getUserName());
        user.remove(job);         // UIMA4275 don't clean up users list because it may have registry things in it

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

    /**
     * Log following / reconstruction, needed to init before recovery.
     */
    public void resetNodepools()
    {
        for ( NodePool np : nodepools ) {
            np.reset(np.getMaxOrder());
        }
    }

    /**
     * Determine if the given share is in a nodepool that this job is allowed to be scheduled over.
     * You can get a mismatch if the classes or nodepools are reconfigured and RM is restarted
     * with jobs still in the system.
     *
     * UIMA-4142
     *
     * @param     s  The share to validate.
     * @param     j  The job to validate against.
     * @return true  if s and j are compatible, false otherwise.
     */
    boolean compatibleNodepool(Share s, IRmJob j)
    {
        // cut to the chase and ask the NP directly if this dude is allowed

        NodePool np = s.getNodepool();
        ResourceClass rc = j.getResourceClass();
        Policy p = rc.getPolicy();

        return np.compatibleNodepool(p, rc);
    }

    /**
     * Make this public for log following.
     */
    public synchronized void processRecovery(IRmJob j)
    {
    	String methodName = "processRecovery";

        ResourceClass rc = resourceClassesByName.get(j.getClassName());
        j.setResourceClass(rc);
        
        int share_order = calcShareOrder(j);
        j.setShareOrder(share_order);

        HashMap<Share, Share> shares = j.getRecoveredShares();
        List<Share> sharesToShrink = new ArrayList<Share>();       // UIMA-4142
        StringBuffer sharenames = new StringBuffer();
        for ( Share s : shares.values() ) {
            sharenames.append(s.toString());
            sharenames.append(" ");
            switch ( rc.getPolicy() ) {
                case FAIR_SHARE:
                    s.setShareOrder(share_order);
                    if ( !compatibleNodepool(s, j) ) {            // UIMA-4142
                        sharesToShrink.add(s);
                        break;
                    } 
                    break;
                case FIXED_SHARE:
                    logger.info(methodName, j.getId(), "Set fixed bit for FIXED job");
                    s.setShareOrder(share_order);
                    s.setFixed();
                    if ( !compatibleNodepool(s, j) ) {            // UIMA-4142
                        if ( j.isService() ) {
                            sharesToShrink.add(s);   // nodepool reconfig snafu, SM will reallocate the process
                        } else {
                            logger.warn(methodName, j.getId(), "Share is in incompatible nodepool but cannot be evicted:", s);
                        }
                    }
                    break;
                case RESERVE:
                    logger.info(methodName, j.getId(), "Set fixed bit for RESERVE job");
                    s.setFixed();
                    // Use machine size for the share-order in case the request has been upgraded
                    // (the share's share_order should be correct but perhaps the machine has become larger!)
                    share_order = s.getMachineOrder();
                    s.setShareOrder(share_order);
                    if (share_order > j.getShareOrder()) {
                    	logger.info(methodName, j.getId(), "Increased order of RESERVE job to", share_order);
                    }
                    j.upgradeShareOrder(share_order);
                    if ( j.isService() && !compatibleNodepool(s, j) ) {       // UIMA-4142
                        sharesToShrink.add(s);   // nodepool reconfig snafu, SM will reallocate the process
                    }
                    break;
            }

            // if ( rc.getPolicy() != Policy.RESERVE ) {          // if it's RESERVE, the share order is already set from
            //                                                    // the machine when the job arrives. 
            //     s.setShareOrder(share_order);
            // }

            Machine m = s.getMachine();
            NodePool np = m.getNodepool();
            np.connectShare(s, m, j, share_order);

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

        try {
			persistenceAccess.addJob(j);
		} catch (Exception e) {
			logger.warn(methodName, j.getId(), "Cannot persist recovered job in database:", j);
		}
        // After a reconfig/restart the share may be in the wrong place, in which case it
        // needs to be removed.  We have to wait until it is fully hooked into the structures
        // before scheduling for removal because it could take a while to go away and
        // we have to be careful not to overcommit.
        // UIMA-4142
        for ( Share s : sharesToShrink ) {
            logger.info(methodName, j.getId(), "Recovery - Removing share from wrong nodepool after reconfiguration:", s);
            j.shrinkByOne(s);
        }

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
        return idFactory.next();
    }

    public synchronized static DuccId newId(long id)
    {
        return idFactory.next(id);
    }

    public void queryMachines()
    {
        for ( NodePool np : nodepools ) {
            np.queryMachines();
        }
    }

    class MachineByOrderSorter
    	implements Comparator<Machine>
    {	
    	public int compare(Machine m1, Machine m2)
        {
            if ( m1.equals(m2) ) return 0;

            if (m1.getShareOrder() == m2.getShareOrder()) {
                return (m1.getId().compareTo(m2.getId()));
            }
            return (int) (m1.getShareOrder() - m2.getShareOrder());
        }
    }


}
