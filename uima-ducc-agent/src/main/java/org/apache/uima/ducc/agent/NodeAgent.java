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
package org.apache.uima.ducc.agent;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.camel.CamelContext;
import org.apache.camel.Processor;
import org.apache.camel.Route;
import org.apache.camel.model.RouteDefinition;
import org.apache.camel.builder.RouteBuilder;
import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.uima.ducc.agent.config.AgentConfiguration;
import org.apache.uima.ducc.agent.event.AgentEventListener;
import org.apache.uima.ducc.agent.event.ProcessLifecycleObserver;
import org.apache.uima.ducc.agent.launcher.CGroupsManager;
import org.apache.uima.ducc.agent.launcher.DefunctProcessDetector;
import org.apache.uima.ducc.agent.launcher.ICommand;
import org.apache.uima.ducc.agent.launcher.Launcher;
import org.apache.uima.ducc.agent.launcher.ManagedProcess;
import org.apache.uima.ducc.agent.launcher.ManagedProcess.StopPriority;
import org.apache.uima.ducc.agent.launcher.SigKillCommand;
import org.apache.uima.ducc.agent.launcher.SigTermCommand;
import org.apache.uima.ducc.agent.metrics.collectors.NodeUsersCollector;
import org.apache.uima.ducc.agent.processors.DefaultNodeInventoryProcessor;
import org.apache.uima.ducc.common.Node;
import org.apache.uima.ducc.common.NodeIdentity;
import org.apache.uima.ducc.common.admin.event.DuccAdminEvent;
import org.apache.uima.ducc.common.admin.event.DuccAdminEventQuiesceAndStop;
import org.apache.uima.ducc.common.admin.event.DuccAdminEventStop;
import org.apache.uima.ducc.common.admin.event.DuccAdminEventStopMetrics;
import org.apache.uima.ducc.common.boot.DuccDaemonRuntimeProperties;
import org.apache.uima.ducc.common.component.AbstractDuccComponent;
import org.apache.uima.ducc.common.main.DuccService;
import org.apache.uima.ducc.common.node.metrics.NodeUsersInfo;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.TimeStamp;
import org.apache.uima.ducc.common.utils.Utils;
import org.apache.uima.ducc.common.utils.IDuccLoggerComponents.Daemon;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.common.utils.id.IDuccId;
import org.apache.uima.ducc.transport.agent.IUimaPipelineAEComponent;
import org.apache.uima.ducc.transport.agent.ProcessStateUpdate;
import org.apache.uima.ducc.transport.cmdline.ICommandLine;
import org.apache.uima.ducc.transport.cmdline.NonJavaCommandLine;
import org.apache.uima.ducc.transport.dispatcher.DuccEventDispatcher;
import org.apache.uima.ducc.transport.event.AgentProcessLifecycleReportDuccEvent;
import org.apache.uima.ducc.transport.event.AgentProcessLifecycleReportDuccEvent.LifecycleEvent;
import org.apache.uima.ducc.transport.event.DuccEvent.EventType;
import org.apache.uima.ducc.transport.event.DaemonDuccEvent;
import org.apache.uima.ducc.transport.event.DuccEvent;
import org.apache.uima.ducc.transport.event.NodeInventoryUpdateDuccEvent;
import org.apache.uima.ducc.transport.event.ProcessStateUpdateDuccEvent;
import org.apache.uima.ducc.transport.event.common.DuccProcess;
import org.apache.uima.ducc.transport.event.common.DuccReservation;
import org.apache.uima.ducc.transport.event.common.DuccReservationMap;
import org.apache.uima.ducc.transport.event.common.DuccUserReservation;
import org.apache.uima.ducc.transport.event.common.IDuccJobDeployment;
import org.apache.uima.ducc.transport.event.common.IDuccProcess;
import org.apache.uima.ducc.transport.event.common.IDuccProcess.ReasonForStoppingProcess;
import org.apache.uima.ducc.transport.event.common.IDuccReservation;
import org.apache.uima.ducc.transport.event.common.IDuccReservationMap;
import org.apache.uima.ducc.transport.event.common.IDuccStandardInfo;
import org.apache.uima.ducc.transport.event.common.IProcessState.ProcessState;
import org.apache.uima.ducc.transport.event.common.IResourceState.ResourceState;
import org.apache.uima.ducc.transport.event.common.ITimeWindow;
import org.apache.uima.ducc.transport.event.common.ProcessMemoryAssignment;
import org.apache.uima.ducc.transport.event.common.TimeWindow;

public class NodeAgent extends AbstractDuccComponent implements Agent, ProcessLifecycleObserver {
  public static DuccLogger logger = DuccLogger.getLogger(NodeAgent.class, COMPONENT_NAME);

  private static DuccId jobid = null;
  // Replaced by duplicate in org.apache.uima.ducc.transport.agent.ProcessStateUpdate
  // public static final String ProcessStateUpdatePort = "ducc.agent.process.state.update.port";

  public static int SIGKILL = 9;

  public static int SIGTERM = 15;

  // for LinuxNodeMetrics logging
  public static AtomicLong logCounter = new AtomicLong();

  public static String cgroupFailureReason;

  // Map of known processes this agent is managing. This map is published
  // at regular intervals as part of agent's inventory update.
  private Map<DuccId, IDuccProcess> inventory = new ConcurrentHashMap<>();

  // Semaphore controlling access to inventory Map
  private Semaphore inventorySemaphore = new Semaphore(1);

  List<ManagedProcess> deployedProcesses = new ArrayList<ManagedProcess>();

  // This agent's identity ( host name and IP address)
  private NodeIdentity nodeIdentity;

  // Component to launch processes
  private Launcher launcher;

  // Reference to the Agent's configuration factory component. This is where
  // Agent
  // dependencies are instantiated and injected via Agent's c'tor.
  public AgentConfiguration configurationFactory;

  private static Semaphore agentLock = new Semaphore(1);

  private DuccEventDispatcher commonProcessDispatcher;

  private DuccEventDispatcher ORDispatcher;

  private Object monitor = new Object();

  boolean duccLingExists = false;

  boolean runWithDuccLing = false;

  private List<DuccUserReservation> reservations = new ArrayList<DuccUserReservation>();

  private Semaphore reservationsSemaphore = new Semaphore(1);

  // private AgentMonitor nodeMonitor;

  private volatile boolean stopping = false;

  private Object stopLock = new Object();

  private RogueProcessReaper rogueProcessReaper = new RogueProcessReaper(logger, 5, 10);

  public volatile boolean useCgroups = false;

  public CGroupsManager cgroupsManager = null;

  public Node node = null;

  public volatile boolean excludeAPs = false;

  public int shareQuantum;

  private boolean agentVirtual = System.getProperty("ducc.agent.virtual") == null ? false : true;

  // This flag, when true, forces the agent to be "real"
  // regardless of the value of ducc.agent.virtual.
  // In the future, this flag and support for ducc.agent.virtual
  // should be removed altogether.
  // This is to support CGroups for all agents,
  // which was previously disabled for virtual ones.
  private boolean agentRealOnly = true;

  public boolean pageSizeFetched = false;

  public int pageSize = 4096; // default

  public int cpuClockRate = 100;

  public int numProcessors = 0;

  ExecutorService defunctDetectorExecutor = Executors.newCachedThreadPool();

  private AgentEventListener eventListener;

  // indicates whether or not this agent received at least one publication
  // from the PM. This flag is used to determine if the agent should use
  // rogue process detector. The detector will be used if this flag is true.
  public volatile boolean receivedDuccState = false;

  private String stateChangeEndpoint;

  long maxTimeToWaitForProcessToStop = 60000; // default 1 minute

  public void setStateChangeEndpoint(String stateChangeEndpoint) {
    this.stateChangeEndpoint = stateChangeEndpoint;
  }

  /**
   * Ctor used exclusively for black-box testing of this class.
   */
  public NodeAgent() {
    super(COMPONENT_NAME, null);
  }

  public NodeAgent(NodeIdentity ni) {
    this();
    this.nodeIdentity = ni;
    Utils.findDuccHome(); // add DUCC_HOME to System.properties

    if (configurationFactory.processStopTimeout != null) {
      maxTimeToWaitForProcessToStop = Long.valueOf(configurationFactory.processStopTimeout);
    }
  }

  public long getLastORSequence() {
    long lastORSequence = 0;
    if (eventListener != null) {
      lastORSequence = eventListener.getLastSequence();
    }
    return lastORSequence;
  }

  public AgentEventListener getEventListener() {
    return eventListener;
  }

  public void setAgentEventListener(AgentEventListener listener) {
    eventListener = listener;
  }

  public boolean isVirtual() {
    boolean retVal = agentVirtual;
    if (agentRealOnly) {
      retVal = false;
    }
    return retVal;
  }

  /**
   * Tell Orchestrator about state change for recording into system-events.log
   */
  private void stateChange(EventType eventType) {
    String methodName = "stateChange";
    try {
      Daemon daemon = Daemon.Agent;
      NodeIdentity nodeIdentity = new NodeIdentity();
      DaemonDuccEvent ev = new DaemonDuccEvent(daemon, eventType, nodeIdentity);
      ORDispatcher.dispatch(stateChangeEndpoint, ev, "");
      logger.info(methodName, null, stateChangeEndpoint, eventType.name(),
              nodeIdentity.getCanonicalName());
    } catch (Exception e) {
      logger.error(methodName, null, e);
    }
  }

  /*
   * Report process lifecycle events on same channel that inventory is reported
   */
  public DuccEventDispatcher getProcessLifecycleReportDispatcher() {
    return ORDispatcher;
  }

  /*
   * Send process lifecycle event to interested listeners
   */
  private void sendProcessLifecycleEventReport(IDuccProcess process,
          LifecycleEvent lifecycleEvent) {
    String location = "sendProcessLifecycleEventReport";
    try {
      NodeIdentity nodeIdentity = getIdentity();
      AgentProcessLifecycleReportDuccEvent duccEvent = new AgentProcessLifecycleReportDuccEvent(
              process, nodeIdentity, lifecycleEvent);
      DuccEventDispatcher dispatcher = getProcessLifecycleReportDispatcher();
      dispatcher.dispatch(duccEvent);
      StringBuffer sb = new StringBuffer();
      sb.append("id:" + process.getDuccId().toString() + " ");
      sb.append("lifecycleEvent:" + lifecycleEvent.name() + " ");
      String args = sb.toString().trim();
      logger.info(location, jobid, args);
    } catch (Exception e) {
      logger.error(location, jobid, e);
    }
  }

  /*
   * Add ManagedProcess to map and send lifecycle event
   */
  private void processDeploy(ManagedProcess mp) {
    String location = "processDeploy";
    if (mp != null) {
      if (deployedProcesses.contains(mp)) {
        String args = "mp:" + mp.getProcessId();
        logger.error(location, jobid, args);
      } else {
        String args = "mp:" + mp.getProcessId();
        logger.debug(location, jobid, args);
        deployedProcesses.add(mp);
        IDuccProcess process = mp.getDuccProcess();
        sendProcessLifecycleEventReport(process, LifecycleEvent.Launch);
      }
    } else {
      String args = "mp:" + mp;
      logger.error(location, jobid, args);
    }
  }

  /*
   * Remove ManagedProcess from map and send lifecycle event
   */
  private void processUndeploy(ManagedProcess mp, Iterator<ManagedProcess> it) {
    String location = "processUndeploy";
    if (mp != null) {
      if (!deployedProcesses.contains(mp)) {
        String args = "mp:" + mp.getProcessId();
        logger.error(location, jobid, args);
      } else {
        String args = "mp:" + mp.getProcessId();
        logger.debug(location, jobid, args);
        // deployedProcesses.remove(mp);
        // it.remove();
        IDuccProcess process = mp.getDuccProcess();
        sendProcessLifecycleEventReport(process, LifecycleEvent.Terminate);
      }
    } else {
      String args = "mp:" + mp;
      logger.error(location, jobid, args);
    }
  }

  /**
   * C'tor for dependecy injection
   *
   * @param nodeIdentity
   *          - this Agent's identity
   * @param launcher
   *          - component to launch processes
   * @param context
   *          - camel context
   */
  public NodeAgent(NodeIdentity nodeIdentity, Launcher launcher, CamelContext context,
          AgentConfiguration factory) throws Exception {
    super(COMPONENT_NAME, context);

    Utils.findDuccHome(); // add DUCC_HOME to System.properties

    // Running a real agent
    agentVirtual = System.getProperty("ducc.agent.virtual") == null ? false : true;

    this.nodeIdentity = nodeIdentity;
    this.launcher = launcher;
    this.configurationFactory = factory;
    this.commonProcessDispatcher = factory.getCommonProcessDispatcher(context);
    this.ORDispatcher = factory.getORDispatcher(context);

    // fetch Page Size from the OS and cache it
    pageSize = getOSPageSize();

    numProcessors = getNodeProcessors();

    logger.info("NodeAgent", null, "OS Page Size:" + pageSize);

    cpuClockRate = getOSClockRate();
    logger.info("NodeAgent", null, "OS Clock Rate:" + cpuClockRate);

    if (System.getProperty("ducc.rm.share.quantum") != null
            && System.getProperty("ducc.rm.share.quantum").trim().length() > 0) {
      shareQuantum = Integer.parseInt(System.getProperty("ducc.rm.share.quantum").trim());
    }
    /* Enable CGROUPS */
    String cgroups;
    String cgUtilsPath = null;
    boolean excludeNodeFromCGroups = false;
    if (!isVirtual()
            && (cgroups = System.getProperty("ducc.agent.launcher.cgroups.enable")) != null) {
      if (cgroups.equalsIgnoreCase("true")) {
        logger.info("nodeAgent", null, "ducc.properties [ducc.agent.launcher.cgroups.enable=true]");
        // Load exclusion file. Some nodes may be excluded from cgroups
        String exclusionFile;

        // get the name of the exclusion file from ducc.properties
        if ((exclusionFile = System.getProperty("ducc.agent.exclusion.file")) != null) {
          logger.info("nodeAgent", null,
                  "Ducc configured with cgroup node exclusion file - ducc.properties [ducc.agent.exclusion.file="
                          + exclusionFile + "]");
          // Parse node exclusion file and determine if cgroups and AP
          // deployment
          // is allowed on this node
          NodeExclusionParser exclusionParser = new NodeExclusionParser();
          exclusionParser.parse(exclusionFile);
          excludeNodeFromCGroups = exclusionParser.cgroupsExcluded();
          excludeAPs = exclusionParser.apExcluded();
          if (excludeNodeFromCGroups) {
            logger.info("nodeAgent", null,
                    "------- Node Explicitly Excluded From Using CGroups. Check File:"
                            + exclusionFile);
            cgroupFailureReason = "------- Node Explicitly Excluded From Using CGroups. Check File:"
                    + exclusionFile;
          }
          System.out.println(
                  "excludeNodeFromCGroups=" + excludeNodeFromCGroups + " excludeAPs=" + excludeAPs);
        } else {
          logger.info("nodeAgent", null, "Agent node *not* excluded from using cgroups");
        }
        // node not in the exclusion list for cgroups
        if (!excludeNodeFromCGroups) {
          // fetch a list of paths the agent will search to find cgroups utils
          // like cgexec. The default location is /usr/bin
          logger.info("nodeAgent", null,
                  "Testing cgroups to check if runtime utilities (cgexec) exist in expected locations in the filesystem");
          String cgroupsUtilsDirs = System.getProperty("ducc.agent.launcher.cgroups.utils.dir");
          if (cgroupsUtilsDirs == null) {
            cgUtilsPath = "/usr/bin"; // default
          } else {
            String[] paths = cgroupsUtilsDirs.split(",");
            for (String path : paths) {
              File file = new File(path.trim() + "/cgexec");
              if (file.exists()) {
                cgUtilsPath = path;
                break;
              }
            }
          }
          // scan /proc/mounts for base cgroup dir
          String cgroupsBaseDir = fetchCgroupsBaseDir("/proc/mounts");

          if (cgUtilsPath == null) {
            useCgroups = false;
            logger.info("nodeAgent", null,
                    "------- CGroups Disabled - Unable to Find Cgroups Utils Directory. Add/Modify ducc.agent.launcher.cgroups.utils.dir property in ducc.properties");
          } else if (cgroupsBaseDir == null || cgroupsBaseDir.trim().length() == 0) {
            useCgroups = false;
            logger.info("nodeAgent", null,
                    "------- CGroups Disabled - Unable to Find Cgroups Root Directory in /proc/mounts");

          } else {
            logger.info("nodeAgent", null, "Agent found cgroups runtime in " + cgUtilsPath
                    + " cgroups base dir=" + cgroupsBaseDir);
            // if cpuacct is configured in cgroups, the subsystems list will be updated
            String cgroupsSubsystems = "memory,cpu";

            long maxTimeToWaitForProcessToStop = 60000; // default 1 minute
            if (configurationFactory.processStopTimeout != null) {
              maxTimeToWaitForProcessToStop = Long.valueOf(configurationFactory.processStopTimeout);
            }

            cgroupsManager = new CGroupsManager(cgUtilsPath, cgroupsBaseDir, cgroupsSubsystems,
                    logger, maxTimeToWaitForProcessToStop);
            cgroupsManager.configure(this);
            // check if cgroups base directory exists in the filesystem
            // which means that cgroups
            // and cgroups convenience package are installed and the
            // daemon is up and running.
            if (cgroupsManager.cgroupExists(cgroupsBaseDir)) {
              logger.info("nodeAgent", null,
                      "Agent found cgroup base directory in " + cgroupsBaseDir);
              try {
                String containerId = "test";
                // validate cgroups by creating a dummy cgroup. The code checks if cgroup actually
                // got created by
                // verifying existence of test cgroup file. The second step in verification is to
                // check if
                // CPU control is working. Configured in cgconfig.conf, the CPU control allows for
                // setting
                // cpu.shares. The code will attempt to set the shares and subsequently tries to
                // read the
                // value from cpu.shares file to make sure the values match. Any exception in the
                // above steps
                // will cause cgroups to be disabled.
                //
                cgroupsManager.validator(cgroupsBaseDir, containerId,
                        System.getProperty("user.name"), false).cgcreate().cgset(100); // write
                                                                                       // cpu.shares=100
                                                                                       // and
                                                                                       // validate

                // cleanup dummy cgroup
                cgroupsManager.destroyContainer(containerId, System.getProperty("user.name"),
                        SIGKILL);
                useCgroups = true;
              } catch (CGroupsManager.CGroupsException ee) {
                logger.info("nodeAgent", null, ee);
                cgroupFailureReason = ee.getMessage();
                useCgroups = false;
              }
              if (useCgroups) {
                try {
                  // remove stale CGroups
                  cgroupsManager.cleanup();
                } catch (Exception e) {
                  logger.error("nodeAgent", null, e);
                  useCgroups = false;
                  logger.info("nodeAgent", null,
                          "Agent cgroup cleanup failed on this machine base directory in "
                                  + cgroupsBaseDir
                                  + ". Check if cgroups is installed on this node, Agent has correct permissions (consistent with cgconfig.conf), and the cgroup daemon is running");
                  cgroupFailureReason = "------- CGroups Not Working on this Machine";
                }
              } else {
                logger.info("nodeAgent", null,
                        "Agent cgroup test failed on this machine base directory in "
                                + cgroupsBaseDir
                                + ". Check if cgroups is installed on this node, Agent has correct permissions (consistent with cgconfig.conf), and the cgroup daemon is running");
                cgroupFailureReason = "------- CGroups Not Working on this Machine";
              }

            } else {
              logger.info("nodeAgent", null, "Agent failed to find cgroup base directory in "
                      + cgroupsBaseDir
                      + ". Check if cgroups is installed on this node and the cgroup daemon is running");
              // logger.info("nodeAgent", null, "------- CGroups Not Installed on this Machine");
              cgroupFailureReason = "------- CGroups Not Installed on this Machine";
            }
          }
        }
      }
    } else {
      logger.info("nodeAgent", null, "------- CGroups Not Enabled on this Machine");
      cgroupFailureReason = "------- CGroups Not Enabled on this Machine - check ducc.properties: ducc.agent.launcher.cgroups.enable ";
    }

    // begin publishing node metrics
    factory.startNodeMetrics(this);

    logger.info("nodeAgent", null,
            "CGroup Support=" + useCgroups + " excludeNodeFromCGroups=" + excludeNodeFromCGroups
                    + " excludeAPs=" + excludeAPs + " CGroups utils Dir:" + cgUtilsPath);

    String useSpawn = System.getProperty("ducc.agent.launcher.use.ducc_spawn");
    if (useSpawn != null && useSpawn.toLowerCase().equals("true")) {
      runWithDuccLing = true;
      String c_launcher_path = Utils.resolvePlaceholderIfExists(
              System.getProperty("ducc.agent.launcher.ducc_spawn_path"), System.getProperties());
      try {
        File duccLing = new File(c_launcher_path);
        if (duccLing.exists()) {
          duccLingExists = true;
        }
      } catch (Exception e) {
        logger.info("nodeAgent", null,
                "------- Agent failed while checking for existence of ducc_ling", e);
      }

    }
  }

  private String fetchCgroupsBaseDir(String mounts) {
    String cbaseDir = null;
    BufferedReader br = null;
    try {
      FileInputStream fis = new FileInputStream(mounts);
      // Construct BufferedReader from InputStreamReader
      br = new BufferedReader(new InputStreamReader(fis));

      String line = null;
      while ((line = br.readLine()) != null) {
        System.out.println(line);
        if (line.trim().startsWith("cgroup")) {
          String[] cgroupsInfo = line.split(" ");
          if (cgroupsInfo[1].trim().equals("/cgroup")) {
            cbaseDir = cgroupsInfo[1].trim();
            break;
          } else if (cgroupsInfo[1].trim().endsWith("/memory")) {
            // return the mount point minus the memory part
            cbaseDir = cgroupsInfo[1].substring(0, cgroupsInfo[1].indexOf("/memory"));
            break;
          }
        }
      } // while

    } catch (Exception e) {
      logger.info("nodeAgent", null,
              "------- Agent failed while checking for existence of ducc_ling", e);
    } finally {
      if (br != null) {
        try {
          br.close();
        } catch (Exception ex) {
        }
      }
    }
    return cbaseDir;
  }

  public int getNodeProcessors() {
    return runOSCommand(new String[] { "/usr/bin/getconf", "_NPROCESSORS_ONLN" });
  }

  public int getOSPageSize() {
    return runOSCommand(new String[] { "/usr/bin/getconf", "PAGESIZE" });
  }

  public int getOSClockRate() {
    return runOSCommand(new String[] { "/usr/bin/getconf", "CLK_TCK" });
  }

  private int runOSCommand(String[] cmd) {
    InputStreamReader in = null;
    BufferedReader reader = null;
    int retVal = 0;
    try {
      ProcessBuilder pb = new ProcessBuilder();
      pb.command(cmd);
      pb.redirectErrorStream(true);
      Process p = pb.start();
      in = new InputStreamReader(p.getInputStream());
      reader = new BufferedReader(in);
      String line = null;

      while ((line = reader.readLine()) != null) {
        retVal = Integer.parseInt(line.trim());
      }
    } catch (Exception e) {
      logger.error("runOSCommand", null, e);
    } finally {
      if (reader != null) {
        try {
          reader.close();
        } catch (Exception ex) {
        }
      }
    }
    return retVal;
  }

  public void setNodeInfo(Node node) {
    this.node = node;
  }

  public Node getNodeInfo() {
    return node;
  }

  public int getNodeTotalNumberOfShares() {
    int shareQuantum = 0;
    int shares = 1;
    if (System.getProperty("ducc.rm.share.quantum") != null
            && System.getProperty("ducc.rm.share.quantum").trim().length() > 0) {
      shareQuantum = Integer.parseInt(System.getProperty("ducc.rm.share.quantum").trim());
      shares = (int) getNodeInfo().getNodeMetrics().getNodeMemory().getMemTotal() / shareQuantum; // get
                                                                                                  // number
                                                                                                  // of
                                                                                                  // shares
      if ((getNodeInfo().getNodeMetrics().getNodeMemory().getMemTotal() % shareQuantum) > 0)
        shares++; // ciel
    }

    return shares;
  }

  public void start(DuccService service) throws Exception {
    super.start(service, null);
    String methodName = "start";
    String name = nodeIdentity.getShortName();
    String ip = nodeIdentity.getIp();
    String jmxUrl = getProcessJmxUrl();
    DuccDaemonRuntimeProperties.getInstance().bootAgent(name, ip, jmxUrl);
    String key = "ducc.broker.url";
    String value = System.getProperty(key);
    logger.info(methodName, null, key + "=" + value);
    stateChange(EventType.BOOT);
  }

  public DuccEventDispatcher getEventDispatcherForRemoteProcess() {
    return commonProcessDispatcher;
  }

  public boolean duccLingExists() {
    return duccLingExists;
  }

  public void duccLingExists(boolean duccLingExists) {
    this.duccLingExists = duccLingExists;
  }

  public boolean runWithDuccLing() {
    return runWithDuccLing;
  }

  public void runWithDuccLing(boolean runWithDuccLing) {
    this.runWithDuccLing = runWithDuccLing;
  }

  /**
   * Returns deep copy (by way of java serialization) of the Agents inventory.
   */
  @SuppressWarnings("unchecked")
  public Map<DuccId, IDuccProcess> getInventoryCopy() {
    Object deepCopy = null;
    try {
      inventorySemaphore.acquire();
      deepCopy = SerializationUtils.clone((ConcurrentHashMap<DuccId, IDuccProcess>) inventory);
    } catch (InterruptedException e) {
    } finally {
      inventorySemaphore.release();
    }
    return (Map<DuccId, IDuccProcess>) deepCopy;
  }

  /**
   * Returns shallow copy of the Agent's inventory
   */
  public Map<DuccId, IDuccProcess> getInventoryRef() {
    return inventory;
  }

  /*
   * Check if both the command and its args are missing, since the command defaults to the DUCC JVM.
   */
  private boolean invalidCommand(ICommandLine commandLine) {
    if (commandLine != null) {
      if (commandLine.getExecutable() != null && commandLine.getExecutable().length() > 0)
        return false;
      if (commandLine.getCommandLine() != null && commandLine.getCommandLine().length > 0)
        return false;
    }
    return true;
  }

  private boolean isProcessDeallocated(IDuccProcess process) {
    return (process.getProcessState().equals(ProcessState.Undefined) && process.isDeallocated());
  }

  /**
   * Stops any process that is in agent's inventory but not in provided job list sent by the PM.
   *
   * @param lifecycleController
   *          - instance implementing stopProcess() method
   * @param jobDeploymentList
   *          - all DUCC jobs sent by PM
   */
  public void takeDownProcessWithNoJob(ProcessLifecycleController lifecycleController,
          List<IDuccJobDeployment> jobDeploymentList) {
    String methodName = "takeDownProcessWithNoJob";
    try {
      inventorySemaphore.acquire();
      List<IDuccProcess> purgeList = new ArrayList<IDuccProcess>();
      boolean hasAjob = false;
      // Check if every process in agent's inventory is associated with a
      // job in a given
      // jobDeploymentList
      for (Entry<DuccId, IDuccProcess> processEntry : getInventoryRef().entrySet()) {
        // if a job list is empty, take down all agent processes that
        // are in the inventory
        if (jobDeploymentList.isEmpty()) {
          logger.info(methodName, null, "...Agent Process:" + processEntry.getValue().getDuccId()
                  + " Not in JobDeploymentList. Ducc Currently Has No Jobs Running");
          hasAjob = false;
        } else {
          // iterate over all jobs
          for (IDuccJobDeployment job : jobDeploymentList) {
            // check if current process is a JD
            if (job.getJdProcess() != null) {
              if (processEntry.getValue().getDuccId().equals(job.getJdProcess().getDuccId())) {
                hasAjob = true;
                break;
              }
            }
            // check if current process is a JP
            for (IDuccProcess jProcess : job.getJpProcessList()) {
              if (processEntry.getValue().getDuccId().equals(jProcess.getDuccId())) {
                hasAjob = true;
                break;
              }
            }
            if (hasAjob) {
              break;
            }
          }
        }
        if (!hasAjob) {
          // if a process in agent inventory has no job and is still
          // alive, stop it
          if (isAlive(processEntry.getValue())) {
            logger.error(methodName, null,
                    "<<<<<<<<< Stopping Process with no Job Assignement (Ghost Process) - DuccId:"
                            + processEntry.getValue().getDuccId() + " PID:"
                            + processEntry.getValue().getPID());
            processEntry.getValue().setReasonForStoppingProcess(
                    ReasonForStoppingProcess.JPHasNoActiveJob.toString());
            lifecycleController.stopProcess(processEntry.getValue());
          } else {
            // add process to purge list
            purgeList.add(processEntry.getValue());
          }
        } else {
          hasAjob = false;
        }
      }
      for (IDuccProcess processToPurge : purgeList) {
        logger.error(methodName, null,
                "XXXXXXXXXX Purging Process:" + processToPurge.getDuccId() + " Process State:"
                        + processToPurge.getProcessState() + " Process Resource State:"
                        + processToPurge.getResourceState());
        getInventoryRef().remove(processToPurge.getDuccId());
      }
    } catch (Exception e) {

    } finally {
      inventorySemaphore.release();
    }
  }

  private void stopProcessIfAlive(IDuccProcess process,
          ProcessLifecycleController lifecycleController) {
    String methodName = "stopProcessIfAlive";
    if (isAlive(process)) {
      logger.error(methodName, null,
              "<<<<<<<<< Stopping Process with no Job Assignement (Ghost Process) - DuccId:"
                      + process.getDuccId() + " PID:" + process.getPID());
      process.setReasonForStoppingProcess(ReasonForStoppingProcess.JPHasNoActiveJob.toString());
      lifecycleController.stopProcess(process);
    } else {
      logger.error(methodName, null,
              "XXXXXXXXXX Purging Process:" + process.getDuccId() + " Process State:"
                      + process.getProcessState() + " Process Resource State:"
                      + process.getResourceState());

      getInventoryRef().remove(process.getDuccId());

      Iterator<ManagedProcess> it = deployedProcesses.iterator();
      while (it.hasNext()) {
        ManagedProcess deployedProcess = it.next();
        if (deployedProcess.getDuccProcess().getDuccId().equals(process.getDuccId())) {
          it.remove();
          break;
        }
      }
      logger.info(methodName, null, "After Purge Inventory size:" + inventory.size()
              + " deployedProcesses size:" + deployedProcesses.size());
    }
  }

  /*
   * Valid process exists in agent inventory and in an incoming OR state. If process exists in agent
   * inventory but not in OR state than such process is invalid
   */
  private boolean validProcess(IDuccProcess process, List<IDuccJobDeployment> jobDeploymentList) {
    // iterate over all jobs
    for (IDuccJobDeployment job : jobDeploymentList) {
      // check if current process is a JD
      if ((job.getJdProcess() != null
              && process.getDuccId().equals(job.getJdProcess().getDuccId()))) {
        return true;
      } else {
        // check if current process is a JP
        for (IDuccProcess jProcess : job.getJpProcessList()) {
          if (process.getDuccId().equals(jProcess.getDuccId())) {
            return true;
          }
        }
      }
    }
    return false;
  }

  public void takeDownProcessWithNoJobV2(ProcessLifecycleController lifecycleController,
          List<IDuccJobDeployment> jobDeploymentList) {
    String methodName = "takeDownProcessWithNoJobV2";
    try {
      inventorySemaphore.acquire();

      // iterate over all processes in agent inventory
      for (Entry<DuccId, IDuccProcess> processEntry : getInventoryRef().entrySet()) {
        // if OR deployment list is empty, take down all agent processes that
        // are in the inventory
        if (jobDeploymentList.isEmpty()
                || !validProcess(processEntry.getValue(), jobDeploymentList)) {
          logger.info(methodName, null, "...Agent Process:" + processEntry.getValue().getDuccId()
                  + " Not in OR JobDeploymentList");
          stopProcessIfAlive(processEntry.getValue(), lifecycleController);
        }
      }
    } catch (Exception e) {
      logger.error(methodName, null, e);
    } finally {
      inventorySemaphore.release();
    }
  }

  /**
   * Reconciles agent inventory with job processes sent by PM
   *
   * @param lifecycleController
   *          - instance implementing stopProcess and startProcess
   * @param process
   *          - job process from a Job List
   * @param commandLine
   *          - in case this process is not in agents inventory we need cmd line to start it
   * @param info
   *          - DUCC common info including user log dir, user name, etc
   * @param workDuccId
   *          - job id
   */
  public void reconcileProcessStateAndTakeAction(ProcessLifecycleController lifecycleController,
          IDuccProcess process, ICommandLine commandLine, IDuccStandardInfo info,
          ProcessMemoryAssignment processMemoryAssignment, DuccId workDuccId,
          boolean isPreemptable) {
    String methodName = "reconcileProcessStateAndTakeAction";
    try {
      inventorySemaphore.acquire();
      // Check if process exists in agent's inventory
      if (getInventoryRef().containsKey(process.getDuccId())) {
        IDuccProcess agentManagedProcess = getInventoryRef().get(process.getDuccId());
        // check if process is Running, Initializing, Started, or Starting
        if (isAlive(agentManagedProcess)) {
          // Stop the process if it has been deallocated
          if (process.isDeallocated()) {
            // if agent is in stopping state, it will try to stop
            // its processes.
            if (stopping && isPreemptable) {
              logger.info(methodName, workDuccId, ">>>>>>>> Agent is stopping - process with PID:"
                      + process.getPID() + " is stopping");
              return; // agent is stopping. All processes are stopping
            } else {
              agentManagedProcess.setResourceState(ResourceState.Deallocated);
              logger.info(methodName, workDuccId,
                      "<<<<<<<< Agent Stopping Process:" + process.getDuccId() + " PID:"
                              + process.getPID() + " Reason: Ducc Deallocated the Process.");
              lifecycleController.stopProcess(agentManagedProcess);
            }
          }
          // else nothing to do. Process has been deallocated
        }
      } else { // Process not in agent's inventory
        // Add this process to the inventory so that it gets published.
        getInventoryRef().put(process.getDuccId(), process);
        if (process.isFailed()) {
          // When a process scheduling class is invalid, the AgentEventListener will
          // tag it as FAILED
          process.setReasonForStoppingProcess(
                  IDuccProcess.ReasonForStoppingProcess.InvalidSchedulingClass.name());
          ITimeWindow twr = new TimeWindow();

          process.setTimeWindowRun(twr);
          twr.setStartLong(0);
          twr.setEndLong(0);

          ITimeWindow twi = new TimeWindow();
          process.setTimeWindowInit(twi);
          twi.setStartLong(0);
          twi.setEndLong(0);

        } else if (process.isDeallocated()) {
          // process not in agent's inventory and it is marked as
          // deallocated. This can happen when an agent is restarted
          // while the rest of DUCC is running.
          // markAsStopped(process);
          process.setProcessState(ProcessState.Stopped);
        } else if (process.getResourceState().equals(ResourceState.Allocated)) {
          // check if OR thinks that this process is still running. If
          // so, this agent was restarted while the OR was running and
          // we need to mark the process as Failed.
          if (process.getProcessState().equals(ProcessState.Initializing)
                  || process.getProcessState().equals(ProcessState.Running)) {
            process.setProcessState(ProcessState.Failed);
          } else {
            // enforce presence of command line
            if (invalidCommand(commandLine)) {
              process.setProcessState(ProcessState.Failed);
              logger.info(methodName, workDuccId,
                      "Rejecting Process Start Request. Command line not provided for Process ID:"
                              + process.getDuccId());
              process.setReasonForStoppingProcess(
                      IDuccProcess.ReasonForStoppingProcess.CommandLineMissing.name());
            } else {
              if (stopping) {
                process.setProcessState(ProcessState.Rejected);
                logger.info(methodName, workDuccId, ">>>>>>> Agent Rejected Process:"
                        + process.getDuccId() + " Start Request - Agent is stopping");
                return;
              }
              process.setProcessState(ProcessState.Starting);
              logger.info(methodName, workDuccId,
                      ">>>>>>> Agent Starting Process:" + process.getDuccId() + " Process State:"
                              + process.getProcessState() + " Process Resource State:"
                              + process.getResourceState());
              lifecycleController.startProcess(process, commandLine, info, workDuccId,
                      processMemoryAssignment, isPreemptable);
            }
          }
        }
      }
    } catch (Exception e) {
      logger.error(methodName, workDuccId, e);
    } finally {
      inventorySemaphore.release();
    }
  }

  public void doStartProcess(IDuccProcess process, ICommandLine commandLine, IDuccStandardInfo info,
          DuccId workDuccId, boolean isPreemptable) {
    String methodName = "doStartProcess";
    try {
      inventorySemaphore.acquire();
      if (getInventoryRef().containsKey(process.getDuccId())) {
        logger.error(methodName, null, "Rejecting Process Start Request. Process with a Ducc ID:"
                + process.getDuccId() + " is already in agent's inventory.");
        return;
      }
      startProcess(process, commandLine, info, workDuccId, new ProcessMemoryAssignment(),
              isPreemptable);
    } catch (InterruptedException e) {
      logger.error(methodName, null, e);
    } finally {
      inventorySemaphore.release();
    }
  }

  private boolean isProcessRunning(IDuccProcess process) {
    if (process.getProcessState().equals(ProcessState.Running)
            || process.getProcessState().equals(ProcessState.Initializing)) {
      return true;
    }
    return false;
  }

  private boolean isOverSwapLimit(IDuccProcess process) {
    Iterator<ManagedProcess> it = deployedProcesses.iterator();
    while (it.hasNext()) {
      ManagedProcess deployedProcess = it.next();
      // for (ManagedProcess deployedProcess : deployedProcesses) {
      // Check if this process exceeds its alloted max swap usage
      if (deployedProcess.getDuccProcess().getDuccId().equals(process.getDuccId())
              && process.getSwapUsage() > deployedProcess.getMaxSwapThreshold()) {
        return true;
      }
    }
    return false;
  }

  private long getSwapOverLimit(IDuccProcess process) {
    long overLimit = 0;
    Iterator<ManagedProcess> it = deployedProcesses.iterator();
    while (it.hasNext()) {
      // for (ManagedProcess deployedProcess : deployedProcesses) {
      ManagedProcess deployedProcess = it.next();
      if (deployedProcess.getDuccProcess().getDuccId().equals(process.getDuccId())) {
        overLimit = deployedProcess.getMaxSwapThreshold() - process.getSwapUsage();
      }
    }
    if (overLimit < 0) {
      overLimit = 0;
    }
    return overLimit;
  }

  /**
   * Called when swap space on a node reached minimum as defined by ducc.node.min.swap.threshold in
   * ducc.properties. The agent will find the biggest (in terms of memory) process in its inventory
   * and stop it.
   */
  public void killProcessDueToLowSwapSpace(long minSwapThreshold) {
    String methodName = "killProcessDueToLowSwapSpace";
    IDuccProcess biggestProcess = null;
    try {
      inventorySemaphore.acquire();
      // find the fattest process in terms of absolute use of swap over the process limit
      for (Entry<DuccId, IDuccProcess> processEntry : getInventoryRef().entrySet()) {
        if (isProcessRunning(processEntry.getValue()) && isOverSwapLimit(processEntry.getValue())
                && (biggestProcess == null || getSwapOverLimit(biggestProcess) < getSwapOverLimit(
                        processEntry.getValue()))) {
          biggestProcess = processEntry.getValue();
        }
      }
    } catch (InterruptedException e) {
      logger.error(methodName, null, e);
    } finally {
      inventorySemaphore.release();
    }
    if (biggestProcess != null) {
      biggestProcess.setReasonForStoppingProcess(ReasonForStoppingProcess.LowSwapSpace.toString());
      logger.info(methodName, null, "Stopping Process:" + biggestProcess.getDuccId() + " PID:"
              + biggestProcess.getPID()
              + " Due to a low swap space. Process' RSS exceeds configured swap threshold of "
              + minSwapThreshold
              + " Defined in ducc.properties. Check ducc.node.min.swap.threshold property");
      stopProcess(biggestProcess);
    }
  }

  public void interruptThreadInWaitFor(String pid) throws Exception {
    String methodName = "interruptZombieProcess";
    synchronized (monitor) {
      Iterator<ManagedProcess> it = deployedProcesses.iterator();
      while (it.hasNext()) {
        // for (ManagedProcess dProcess : deployedProcesses) {
        ManagedProcess dProcess = it.next();
        if (dProcess.getPid() != null && dProcess.getPid().equals(pid)) {
          Future<?> future = dProcess.getFuture();
          if (future != null && !future.isDone() && !future.isCancelled()) {
            future.cancel(true); // interrupt the thread blocked on waitFor()
            logger.info(methodName, dProcess.getDuccProcess().getDuccId(),
                    "Interrupted Thread - Zombie Process with PID:" + dProcess.getPid());
          }
          break;
        }
      }
    }
  }

  /**
   * Called when Agent receives request to start a process.
   *
   * @param process
   *          - IDuccProcess instance with identity (DuccId)
   * @param commandLine
   *          - fully defined command line that will be used to exec the process.
   *
   */
  public void startProcess(IDuccProcess process, ICommandLine commandLine, IDuccStandardInfo info,
          DuccId workDuccId, ProcessMemoryAssignment processMemoryAssignment,
          boolean isPreemptable) {
    String methodName = "startProcess";

    try {
      // Add process to the Agent's inventory before it is started
      getInventoryRef().put(process.getDuccId(), process);
      // enforce presence of command line
      if (invalidCommand(commandLine)) {
        process.setProcessState(ProcessState.Failed);
        logger.info(methodName, null,
                "Rejecting Process Start Request. Command line not provided for Process ID:"
                        + process.getDuccId());
      } else if (isProcessDeallocated(process)) {
        process.setProcessState(ProcessState.Stopped);
        logger.info(methodName, null, "Rejecting Process Start Request. Process ID:"
                + process.getDuccId() + " hava already been deallocated due to Shrink");
      } else {
        deployProcess(process, commandLine, info, workDuccId, processMemoryAssignment,
                isPreemptable);
      }

    } catch (Exception e) {
      logger.error(methodName, null, e);
    }

  }

  public boolean isAlive(IDuccProcess invProcess) {
    return invProcess.getProcessState().equals(ProcessState.Initializing)
            || invProcess.getProcessState().equals(ProcessState.Running)
            || invProcess.getProcessState().equals(ProcessState.Stopping)
            || invProcess.getProcessState().equals(ProcessState.Starting)
            || invProcess.getProcessState().equals(ProcessState.Started);
  }

  public void doStopProcess(IDuccProcess process) {
    String methodName = "stopProcess";
    try {
      inventorySemaphore.acquire();
      stopProcess(process);
    } catch (InterruptedException e) {
      logger.error(methodName, null, e);
    } finally {
      inventorySemaphore.release();
    }

  }

  /**
   * Called when Agent receives request to stop a process.
   *
   * @param process
   *          - IDuccProcess instance with identity (DuccId)
   *
   */
  public void stopProcess(IDuccProcess process) {
    String methodName = "stopProcess";
    try {
      IDuccProcess invProcess = null;
      if ((invProcess = getInventoryRef().get(process.getDuccId())) != null
              && isAlive(invProcess)) {
        logger.info(methodName, null, "Undeploing Process with PID:" + process.getPID());
        undeployProcess(process);
      } else if (invProcess == null) { // process not in inventory
        logger.info(methodName, null,
                "Agent received Stop request for a process which is not in the Agent's inventory. "
                        + "It looks like this Agent was killed along with its child processes. Adding stale process to the inventory. PID:"
                        + process.getPID() + " DuccId:" + process.getDuccId() + "");
        // Received a request to stop a process that this is not in the
        // current
        // inventory. Most likely this agent was killed while its
        // processes were
        // still running. Add the process to the agent's inventory so
        // that its
        // included in the next published inventory. This is done so
        // that the
        // orchestrator can cleanup its state.
        if (process.getProcessState() != ProcessState.Stopped
                && process.getProcessState() != ProcessState.Failed
                && process.getProcessState() != ProcessState.InitializationTimeout
                && process.getProcessState() != ProcessState.FailedInitialization) {
          // Force the Stopped state if not already stopped or failed
          process.setProcessState(ProcessState.Stopped);
        }
        // Add stale process to the inventory. This will eventually be
        // cleaned up
        // when the PM sends purge request.
        getInventoryRef().put(process.getDuccId(), process);
      }
    } catch (Exception e) {
      logger.error(methodName, null, e);
    }
  }

  /**
   * Checks if process with a given PID has already registered memory collector Camel route. This
   * route periodically fetches resident memory of a process with a given PID. Each process
   * collector route is identified by process PID.
   *
   * @param pid
   *          - process PID also id of its route
   * @return - true if memory collector route has already been created. False, otherwise
   */
  private boolean addProcessMemoryCollector(String pid) {
    // search all camel routes for one with a given id
    for (Route route : super.getContext().getRoutes()) {
      if (route.getId().equals(pid)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Remove given process from Agent's inventory
   *
   * @param process
   *          - process to purge from inventory
   * @throws Exception
   */
  public void purgeProcess(IDuccProcess process) throws Exception {
    String methodName = "purgeProcess";
    DuccId key = null;
    String pid = "";
    try {
      inventorySemaphore.acquire();
      for (Entry<DuccId, IDuccProcess> processEntry : getInventoryRef().entrySet()) {
        // Check if process with a given unique DuccId exist in the
        // local map
        if (processEntry.getKey().equals(process.getDuccId())) {
          key = processEntry.getKey();
          pid = processEntry.getValue().getPID();
          break;
        }
      }
      if (key != null) {
        getInventoryRef().remove(key);
        logger.info(methodName, null, ">>>> Agent Purged Process with PID:" + pid);
      }
    } catch (InterruptedException e) {
    } finally {
      inventorySemaphore.release();
    }
    Iterator<ManagedProcess> it = deployedProcesses.iterator();
    while (it.hasNext()) {
      // for (ManagedProcess deployedProcess : deployedProcesses) {
      ManagedProcess deployedProcess = it.next();
      // Find ManagedProcess instance the DuccProcess instance is
      // associated with
      if (deployedProcess.getDuccProcess().getDuccId().equals(process.getDuccId())) {
        processUndeploy(deployedProcess, it);
        break;
      }
    }
  }

  private boolean changeState(ProcessState state) {
    switch (state) {
      case FailedInitialization:
      case InitializationTimeout:
      case Stopped:
      case Stopping:
        return false;
      case Starting:
      case Started:
      case Initializing:
      case Running:
        return true;
      default:
        break;
    }
    return false;
  }

  /**
   * Called when a service wrapper sends status update
   *
   * @param duccEvent
   *          - Ducc event object
   *
   * @throws Exception
   */

  public void updateProcessStatus(ProcessStateUpdateDuccEvent duccEvent) throws Exception {
    String methodName = "updateProcessStatus";

    try {
      inventorySemaphore.acquire();
      for (Entry<DuccId, IDuccProcess> processEntry : getInventoryRef().entrySet()) {
        // Check if process with a given unique DuccId exist in the
        // local map
        if (processEntry.getKey().getUnique().equals(duccEvent.getDuccProcessId())) {
          // found it. Update pid and state of the process
          if (duccEvent.getPid() != null && processEntry.getValue().getPID() == null) {
            processEntry.getValue().setPID(duccEvent.getPid());
          }

          if (duccEvent.getProcessJmxUrl() != null
                  && processEntry.getValue().getProcessJmxUrl() == null) {
            processEntry.getValue().setProcessJmxUrl(duccEvent.getProcessJmxUrl());
          }
          ITimeWindow tw = processEntry.getValue().getTimeWindowInit();
          if (tw != null) {
            if (!duccEvent.getState().equals(ProcessState.Initializing)) {
              // Mark the time the process ended initialization. It also
              // covers a case when the process terminates while initializing
              tw.setEnd(TimeStamp.getCurrentMillis());
              if (duccEvent.getState().equals(ProcessState.Running)) {
                ITimeWindow twr = new TimeWindow();
                String millis;
                millis = TimeStamp.getCurrentMillis();
                // Mark the time the process started running
                processEntry.getValue().setTimeWindowRun(twr);
                twr.setStart(millis);
              }
            }
          } else {
            logger.info(methodName, null,
                    "++++++++++++ Agent Init TimeWindow not available - tw==null");
          }
          ManagedProcess deployedProcess = null;
          synchronized (monitor) {
            Iterator<ManagedProcess> it = deployedProcesses.iterator();
            while (it.hasNext()) {
              // Find ManagedProcess instance the DuccProcess
              // instance is associated with
              ManagedProcess dProcess = it.next();
              if (dProcess.getDuccProcess().getDuccId().getUnique()
                      .equals(duccEvent.getDuccProcessId())) {
                deployedProcess = dProcess;
                break;
              }
            }
          }
          if (processEntry.getValue().getProcessState() != ProcessState.Running
                  && duccEvent.getState().equals(ProcessState.Running) && deployedProcess != null) {
            // cancel process initialization timer.
            deployedProcess.stopInitializationTimer();
          }

          logger.info(methodName, null,
                  ">>>> Agent Handling Process Update - Ducc Id: "
                          + processEntry.getValue().getDuccId() + " PID:"
                          + processEntry.getValue().getPID() + " Status:" + duccEvent.getState()
                          + " Deallocated:" + processEntry.getValue().isDeallocated());
          if (deployedProcess != null && deployedProcess.getSocketEndpoint() == null
                  && duccEvent.getServiceEdnpoint() != null) {
            deployedProcess.setSocketEndpoint(duccEvent.getServiceEdnpoint());
          }

          // This is a delayed stop. Previously a request to stop the
          // process was received
          // but the PID was not available yet. Instead a flag was set
          // to initiate a
          // stop after the process reports the PID.
          if (deployedProcess != null && deployedProcess.killAfterLaunch()) {
            logger.info(methodName, null,
                    ">>>> Process Ducc Id:" + processEntry.getValue().getDuccId()
                            + " Was Previously Tagged for Kill While It Was Starting");
            undeployProcess(processEntry.getValue());
          } else if (deployedProcess != null && deployedProcess.doKill() && deployedProcess
                  .getDuccProcess().getProcessState().equals(ProcessState.Stopped)) {
            deployedProcess.getDuccProcess()
                    .setReasonForStoppingProcess(ReasonForStoppingProcess.KilledByDucc.toString());
          } else if (deployedProcess != null && (deployedProcess.doKill()
                  || deployedProcess.getDuccProcess().getProcessState().equals(ProcessState.Failed)
                  || deployedProcess.getDuccProcess().getProcessState()
                          .equals(ProcessState.Killed))) {
            // The process has already stopped, but managed to send
            // the last update before dying. Ignore the update
            return;
          } else if (changeState(processEntry.getValue().getProcessState())) {
            logger.info(methodName, null, "=============== PID:" + processEntry.getValue().getPID()
                    + " Changing State - current state:" + processEntry.getValue().getProcessState()
                    + " New State:" + duccEvent.getState());
            processEntry.getValue().setProcessState(duccEvent.getState());
            DuccEventDispatcher dispatcher = configurationFactory
                    .getORDispatcher(super.getContext());
            try {
              DefaultNodeInventoryProcessor processor = configurationFactory
                      .nodeInventoryProcessor(this);
              Map<DuccId, IDuccProcess> inventoryCopy = (Map<DuccId, IDuccProcess>) SerializationUtils
                      .clone((ConcurrentHashMap<DuccId, IDuccProcess>) inventory);

              processor.dispatchInventoryUpdate(dispatcher,
                      configurationFactory.getInventoryUpdateEndpoint(), inventoryCopy);
              logger.info(methodName, null, "Sent Node Inventory Update to the OR - process PID:"
                      + processEntry.getValue().getPID());

            } catch (Exception e) {
              logger.warn("", null, e);
            }

            // if the process is Stopping, it must have hit an error threshold
          }
          // Check if MemoryCollector should be created for this
          // process. It collects
          // resident memory of the process at regular intervals.
          // Should only be added
          // once for each process. This route will have its id set to
          // process PID.
          if (addProcessMemoryCollector(duccEvent.getPid())
                  && (duccEvent.getState().equals(ProcessState.Initializing)
                          || duccEvent.getState().equals(ProcessState.Running))) {
            if (duccEvent.getState().equals(ProcessState.Running)) {
              if (processEntry.getValue().getUimaPipelineComponents() != null
                      && processEntry.getValue().getUimaPipelineComponents().size() > 0) {
                processEntry.getValue().getUimaPipelineComponents().clear();
                if (duccEvent.getUimaPipeline() != null) {
                  duccEvent.getUimaPipeline().clear();
                }
              }
            }

          } else if (duccEvent.getState().equals(ProcessState.Stopped)
                  || duccEvent.getState().equals(ProcessState.Failed)
                  || duccEvent.getState().equals(ProcessState.Killed)) {
            if (deployedProcess.getMetricsProcessor() != null) {
              deployedProcess.getMetricsProcessor().close(); // close open fds (stat and statm
                                                             // files)
            }
            logger.info(methodName, null,
                    "----------- Agent Stopped ProcessMemoryUsagePollingRouter for Process:"
                            + duccEvent.getPid());
          } else if (duccEvent.getState().equals(ProcessState.FailedInitialization)) {
            logger.info(methodName, null,
                    ">>>> Agent Handling Process FailedInitialization. PID:" + duccEvent.getPid());
            deployedProcess.getDuccProcess().setReasonForStoppingProcess(
                    ReasonForStoppingProcess.FailedInitialization.toString());
            deployedProcess.getDuccProcess().setProcessState(ProcessState.Stopping);
            deployedProcess.setStopping();

            deployedProcess.kill();
            logger.info(methodName, null, ">>>> Agent Handling Process FailedInitialization. PID:"
                    + duccEvent.getPid() + " Killing Process");

            undeployProcess(processEntry.getValue());

          } else if (duccEvent.getState().equals(ProcessState.InitializationTimeout)) {
            deployedProcess.getDuccProcess().setReasonForStoppingProcess(
                    ReasonForStoppingProcess.InitializationTimeout.toString());
            deployedProcess.getDuccProcess().setProcessState(ProcessState.Stopping);
            deployedProcess.setStopping();

            // Mark process for death. Doesnt actually kill the process

            deployedProcess.kill();
            logger.info(methodName, null, ">>>> Agent Handling Process InitializationTimeout. PID:"
                    + duccEvent.getPid() + " Killing Process");

            undeployProcess(processEntry.getValue());

          } else if (duccEvent.getState().equals(ProcessState.Stopping)) {
            if (duccEvent.getMessage() != null && duccEvent.getMessage()
                    .equals(ReasonForStoppingProcess.ExceededErrorThreshold.toString())) {
              processEntry.getValue().setReasonForStoppingProcess(
                      ReasonForStoppingProcess.ExceededErrorThreshold.toString());
            }
            if (!deployedProcess.getDuccProcess().getProcessState().equals(ProcessState.Stopped)
                    && !deployedProcess.getDuccProcess().getProcessState()
                            .equals(ProcessState.Stopping)) {
              deployedProcess.getDuccProcess().setProcessState(ProcessState.Stopping);
              deployedProcess.setStopping();
            }
          }
          if (duccEvent.getUimaPipeline() != null) {
            StringBuffer buffer = new StringBuffer("\t\tUima Pipeline -");
            for (IUimaPipelineAEComponent uimaAeState : duccEvent.getUimaPipeline()) {
              buffer.append("\n\t\tAE:").append(uimaAeState.getAeName()).append(" state:")
                      .append(uimaAeState.getAeState()).append(" InitTime:")
                      .append(uimaAeState.getInitializationTime() / 1000).append(" secs. Thread:")
                      .append(uimaAeState.getAeThreadId());
            }
            logger.info(methodName, null, buffer.toString());
            ((DuccProcess) processEntry.getValue())
                    .setUimaPipelineComponents(duccEvent.getUimaPipeline());
          }
          return; // found it. Done
        }
      }
    } catch (InterruptedException e) {
    } finally {
      inventorySemaphore.release();
    }
  }

  /**
   * Deploys process using supplied command line
   *
   * @param process
   *          - Process with identity (DuccId)
   * @param commandLine
   *          - fully defined command line that will be used to exec the process.
   */
  private void deployProcess(IDuccProcess process, ICommandLine commandLine, IDuccStandardInfo info,
          DuccId workDuccId, ProcessMemoryAssignment processMemoryAssignment, boolean preemptable) {
    String methodName = "deployProcess";
    synchronized (monitor) {
      boolean deployProcess = true;
      Iterator<ManagedProcess> it = deployedProcesses.iterator();
      while (it.hasNext()) {
        // for (ManagedProcess deployedProcess : deployedProcesses) {
        ManagedProcess deployedProcess = it.next();
        // ignore duplicate start request for the same process
        if (deployedProcess.getDuccId().equals(process.getDuccId())) {
          deployProcess = false;
          break;
        }
      }
      if (deployProcess) {
        try {
          logger.info(methodName, workDuccId,
                  "Agent [" + getIdentity().getIp() + "] Deploying Process - DuccID:"
                          + process.getDuccId().getFriendly() + " Unique DuccID:"
                          + process.getDuccId().getUnique() + " Node Assignment:"
                          + process.getNodeIdentity().getIp() + " Process Memory Assignment:"
                          + processMemoryAssignment + " MBs");
          TimeWindow tw = new TimeWindow();
          tw.setStart(TimeStamp.getCurrentMillis());
          tw.setEnd(null);
          process.setTimeWindowInit(tw);
          ManagedProcess managedProcess = new ManagedProcess(process, commandLine, this, logger,
                  processMemoryAssignment, preemptable);
          managedProcess.setProcessInfo(info);
          managedProcess.setWorkDuccId(workDuccId);

          // enrich process spec with unique ducc id which will be
          // used to correlate message
          // exchanges
          // between the agent and launched process

          ManagedProcess deployedProcess = launcher.launchProcess(this, getIdentity(), process,
                  commandLine, this, managedProcess);
          processDeploy(deployedProcess);
        } catch (Exception e) {
          logger.error(methodName, null, e);
        }
      } else {
        logger.info(methodName, workDuccId,
                "Ignoring duplicate request to start process - DuccID:"
                        + process.getDuccId().getFriendly() + " Unique DuccID:"
                        + process.getDuccId().getUnique());
      }
    }
  }

  class AgentStreamConsumer implements Runnable {
    private InputStream theStream;

    AgentStreamConsumer(InputStream is) {
      theStream = is;
    }

    public void run() {
      String methodName = "AgentStreamConsumer.run";

      BufferedReader bufferedReader = null;
      try {
        bufferedReader = new BufferedReader(new InputStreamReader(theStream));
        String line = null;
        while ((line = bufferedReader.readLine()) != null) {
          StringBuffer outputBuffer = new StringBuffer();
          outputBuffer.append(line + "\n");
        }
      } catch (Throwable t) {
        logger.warn(methodName, null, t);
        t.printStackTrace();
      } finally {
        try {
          bufferedReader.close();
        } catch (Exception e) {
        }
      }
    }
  }

  enum SIGNAL {
    SIGTERM("-15"), SIGKILL("-9");

    String signal = "";

    SIGNAL(String kind) {
      signal = kind;
    }

    public String get() {
      return signal;
    }
  };

  class ProcessRunner implements Runnable {
    ManagedProcess deployedProcess;

    public ProcessRunner(final ManagedProcess deployedProcess) {// final String pid, SIGNAL signal )
                                                                // {
      this.deployedProcess = deployedProcess;
    }

    public void run() {
      stopProcess(deployedProcess.getDuccProcess());
    }
  }

  private boolean runnable(ManagedProcess process) {
    return (process.getDuccProcess().getProcessState().equals(ProcessState.Initializing)
            || process.getDuccProcess().getProcessState().equals(ProcessState.Starting)
            || process.getDuccProcess().getProcessState().equals(ProcessState.Started)
            || process.getDuccProcess().getProcessState().equals(ProcessState.Running));
  }

  /**
   * This method is called when an agent receives a STOP request. It sends SIGTERM to all
   * non-preemptable child processes and starts a timer. If the timer pops and child processes are
   * still running, the agent takes itself out via halt()
   */
  private boolean stopChildProcesses(boolean quiesceMode) {
    String methodName = "stopNow";
    boolean wait = false;
    try {
      Iterator<ManagedProcess> it = deployedProcesses.iterator();
      while (it.hasNext()) {
        ManagedProcess deployedProcess = it.next();
        String pid = deployedProcess.getDuccProcess().getPID();
        logger.info(methodName, null, "....Process:" + pid + " is JD=" + deployedProcess.isJd()
                + " Preemptable:" + deployedProcess.isPreemptable());
        // dont send SIGTERM to non-preemptable processes in quiesce mode
        if ((quiesceMode && !deployedProcess.isPreemptable()) || deployedProcess.isStopping()
                || pid == null || pid.trim().length() == 0 || !runnable(deployedProcess)) {
          continue;
        }

        logger.info(methodName, null,
                "....Stopping Process - DuccId:" + deployedProcess.getDuccProcess().getDuccId()
                        + " PID:" + pid + " Sending SIGTERM Process State:"
                        + deployedProcess.getDuccProcess().getProcessState().toString()
                        + " Process Type:" + deployedProcess.getDuccProcess().getProcessType()
                        + " Uima AS:" + deployedProcess.isUimaAs() + " Preemtable:"
                        + deployedProcess.isPreemptable());
        wait = true;
        deployedProcess.setStopPriority(StopPriority.DONT_WAIT);
        // Stop each child process in its own thread to parallelize SIGTERM requests
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.execute(new ProcessRunner(deployedProcess));

      }

    } catch (Exception e) {
      logger.warn(methodName, null, e);
    }
    return wait;
  }

  private void killChildProcesses(boolean killOnlyUimaAs, boolean quiesce) {
    String methodName = "killChildProcesses";

    try {
      if (useCgroups) {
        logger.info(methodName, null, "CgroupsManager.cleanup() before ");
        if (killOnlyUimaAs) {
          Set<String> pidsToKill = new HashSet<>();
          Iterator<ManagedProcess> it = deployedProcesses.iterator();
          while (it.hasNext()) {
            ManagedProcess p = it.next();
            if (!p.isPreemptable() && p.getPid() != null && p.isUimaAs()) {
              pidsToKill.add(p.getPid());
            }
          }
          if (!pidsToKill.isEmpty()) {
            logger.info(methodName, null, ">>>>>>>> Found " + pidsToKill.size()
                    + " UIMA-AS processes still running - killing all non-preemptables via kill -9");
            // Since SIGTERM may not be enough to take down a process, use cgroups to find
            // any UIMA-AS process still standing and do hard kill
            cgroupsManager.cleanupPids(pidsToKill);

          }

        } else {
          Set<String> pidsToKill = new HashSet<>();
          Iterator<ManagedProcess> it = deployedProcesses.iterator();
          // Since SIGTERM may not be enough to take down a process, use cgroups to find
          // any process still standing and do hard kill
          while (it.hasNext()) {
            ManagedProcess p = it.next();
            if ((!quiesce && !p.isPreemptable()) && p.getPid() != null && p.isUimaAs()) {
              pidsToKill.add(p.getPid());
            }
          }
          // cgroupsManager.cleanup();
          cgroupsManager.cleanupPids(pidsToKill);

        }
        logger.info(methodName, null, "CgroupsManager.cleanup() after ");
      } else {
        Iterator<ManagedProcess> it = deployedProcesses.iterator();
        while (it.hasNext()) {
          ManagedProcess deployedProcess = it.next();

          String pid = deployedProcess.getDuccProcess().getPID();
          if ((quiesce && !deployedProcess.isPreemptable()) || pid == null
                  || pid.trim().length() == 0 || !runnable(deployedProcess)) {
            continue;
          }
          logger.info(methodName, null,
                  "....Stopping Process - DuccId:" + deployedProcess.getDuccProcess().getDuccId()
                          + " PID:" + pid + " Sending SIGKILL Process State:"
                          + deployedProcess.getDuccProcess().getProcessState().toString());
          ICommandLine cmdLine;
          if (Utils.isWindows()) {
            cmdLine = new NonJavaCommandLine("taskkill");
            cmdLine.addArgument("/PID");
          } else {
            cmdLine = new NonJavaCommandLine("/bin/kill");
            cmdLine.addArgument("-9");
          }
          cmdLine.addArgument(pid);

          deployedProcess.setStopping();
          deployedProcess.setStopPriority(StopPriority.DONT_WAIT);

          launcher.launchProcess(this, getIdentity(), deployedProcess.getDuccProcess(), cmdLine,
                  this, deployedProcess);
        }

      }
    } catch (Exception e) {
      logger.warn(methodName, null, e);
    }

  }

  private void handleSigTermTimeout(ManagedProcess deployedProcess) {
    String methodName = "handleSigTermTimeout";
    if (!deployedProcess.getDuccProcess().getProcessState().equals(ProcessState.Stopped)) {
      logger.info(methodName, deployedProcess.getDuccId(),
              "------------ Agent Timed-out Waiting for Process with PID:"
                      + deployedProcess.getDuccProcess().getPID() + " to Stop. Process State:"
                      + deployedProcess.getDuccProcess().getProcessState()
                      + " .Process did not stop in allotted time of "
                      + maxTimeToWaitForProcessToStop + " millis");
      logger.info(methodName, deployedProcess.getDuccId(),
              ">>>>>>>>>>>>>>> Killing Process:" + deployedProcess.getDuccProcess().getPID()
                      + " .Process State:" + deployedProcess.getDuccProcess().getProcessState());
    }

    ICommand sigKillCommand = new SigKillCommand(deployedProcess, logger);
    launcher.launchOSCommand(sigKillCommand);
  }

  /**
   * Kills a given process
   *
   * @param process
   *          - process to kill
   */
  private void undeployProcess(IDuccProcess process) {
    String methodName = "undeployProcess";

    synchronized (monitor) {

      ManagedProcess deployedProcess = getDeployedProcess(process.getDuccId());

      // Given process does not exist in agent's inventory
      if (Objects.isNull(deployedProcess)) {
        logger.info(methodName, null,
                ".... Process - DuccId:" + process.getDuccId() + " PID:" + process.getPID()
                        + " Not in Agent's inventory. Adding to the inventory with state=Stopped");
        process.setProcessState(ProcessState.Stopped);
        inventory.put(process.getDuccId(), process);
        processDeploy(new ManagedProcess(process, null, this, logger, new ProcessMemoryAssignment(),
                true));
        return;
      }

      if (Objects.isNull(deployedProcess.getPid())) {
        if (!deployedProcess.getDuccProcess().getProcessState().equals(ProcessState.Stopped)) {
          // process not reported its PID yet when the process
          // reports its PID, check if it should be killed.
          logger.info(methodName, deployedProcess.getDuccId(), ".... Process - Ducc ID:"
                  + deployedProcess.getDuccId()
                  + " Has Not Started Yet. PID Not Available. Tagging Process For Kill When It Reports Status");
          deployedProcess.killAfterLaunch(true);
        }
        return;
      }

      ICommand sigTermCommand = new SigTermCommand(deployedProcess, logger);

      // Mark the process as stopping. When the process exits,
      // the agent can determine if the process died on its own
      // (due to say, user code problem) or if it died
      // due to Agent initiated stop.
      deployedProcess.setStopping();

      try {
        logger.info(methodName, deployedProcess.getDuccId(),
                "------------ Agent Starting Killer Timer Task For Process with PID:"
                        + deployedProcess.getDuccProcess().getPID() + " Process State: "
                        + deployedProcess.getDuccProcess().getProcessState());
        // launch SIGTERM
        launcher.launchOSCommand(sigTermCommand);
        // fetch deployed process Future object which
        // will be used to determine if its still
        // running
        Future<?> future = deployedProcess.getFuture();
        // future.get() blocks until process stops or timeout occurs
        future.get(maxTimeToWaitForProcessToStop, TimeUnit.MILLISECONDS);
        logger.info(methodName, deployedProcess.getDuccId(), ">>>>>>>>>>>>> Process with PID:"
                + deployedProcess.getDuccProcess().getPID() + " Terminated");
      } catch (TimeoutException e) {
        logger.info(methodName, deployedProcess.getDuccId(),
                ">>>>>>>>> Timed Out Waiting For Process To Terminate After SIGTERM Was Sent");
        handleSigTermTimeout(deployedProcess);
      } catch (Exception e) {
        logger.error(methodName, deployedProcess.getDuccId(), e);
      }

      try {
        monitor.wait(500);
      } catch (InterruptedException ee) {

      }

      // check if defunct process
      if (isProcessRunning(deployedProcess.getDuccProcess())) {
        // wait for a short while before running DefunctProcessDetector
        // Sometimes when a process is killed via kill -9 it shows as
        // defunct in ps output. Not sure why this is so.

        // spin a thread where we check if the process is defunct. If true,
        // the process state is changed to Stopped and reason set to 'defunct'.
        // Next inventory publication will include this new state and the OR
        // can terminate a job.
        defunctDetectorExecutor.execute(new DefunctProcessDetector(deployedProcess, logger));
      }
      logger.info(methodName, deployedProcess.getDuccId(), "Inventory size:" + inventory.size()
              + " deployedProcesses size:" + deployedProcesses.size());
    }
  }

  public NodeIdentity getIdentity() {
    return nodeIdentity;
  }

  /**
   * Called when a process exits.
   */
  public void onProcessExit(IDuccProcess process) {
    String methodName = "onProcessExit";
    if (process == null) {
      return;
    }
    try {
      ProcessStateUpdate processStateUpdate = new ProcessStateUpdate(process.getProcessState(),
              process.getPID(), process.getDuccId().getUnique());
      ProcessStateUpdateDuccEvent event = new ProcessStateUpdateDuccEvent(processStateUpdate);
      // cleanup Camel route associated with a process that just stopped
      if (process.getPID() != null && super.getContext().getRoute(process.getPID()) != null) {
        try {
          // stop collecting process stats from /proc/<pid>/statm
          super.getContext().stopRoute(process.getPID());
        } catch (Exception e) {
          logger.error(methodName, null,
                  "....Unable to stop Camel route for PID:" + process.getPID());
        }
        // remove route from context, otherwise the routes accumulate over time causing memory leak
        super.getContext().removeRoute(process.getPID());
        StringBuilder sb = new StringBuilder("\n");
        logger.info(methodName, null,
                "Removed Camel Route from Context for PID:" + process.getPID());

        for (Route route : super.getContext().getRoutes()) {
          sb.append("Camel Context - RouteId:" + route.getId() + "\n");
        }
        logger.info(methodName, null, sb.toString());
      }
      updateProcessStatus(event);
    } catch (Exception e) {
      logger.error(methodName, null, e);
    } finally {

    }
  }

  public void onJPInitTimeout(IDuccProcess process, long timeout) {
    String methodName = "onJPInitTimeout";
    try {
      System.out.println("--------- Agent Timedout While Waiting For JP (PID:" + process.getPID()
              + ") to initialize. The JP exceeded configured timeout of " + timeout / (60 * 1000)
              + " minutes");
      ProcessStateUpdate processStateUpdate = new ProcessStateUpdate(
              ProcessState.InitializationTimeout, process.getPID(),
              process.getDuccId().getUnique());
      ProcessStateUpdateDuccEvent event = new ProcessStateUpdateDuccEvent(processStateUpdate);
      updateProcessStatus(event);
    } catch (Exception e) {
      logger.error(methodName, null, e);
    }
  }

  public void shutdown(String reason) {
    String methodName = "shutdown";
    Iterator<ManagedProcess> it = deployedProcesses.iterator();
    while (it.hasNext()) {
      ManagedProcess deployedProcess = it.next();
      try {

        undeployProcess(deployedProcess.getDuccProcess());
      } catch (Exception e) {
        logger.error(methodName, null, e);
      }
    }
  }

  public static void lock() throws Exception {
    agentLock.acquire();
  }

  public static void unlock() throws Exception {
    agentLock.release();
  }

  public boolean isManagedProcess(Set<NodeUsersCollector.ProcessInfo> processList,
          NodeUsersCollector.ProcessInfo cpi) {
    synchronized (monitor) {
      Iterator<ManagedProcess> it = deployedProcesses.iterator();
      while (it.hasNext()) {
        ManagedProcess deployedProcess = it.next();
        if (deployedProcess.getDuccProcess() != null) {
          // Check if process has been deployed but has not yet
          // reported its PID.
          // This is normal. It takes a bit of time until the JP
          // reports
          // its PID to the Agent. If there is at least one process in
          // Agent
          // deploy list with no PID we assume it is the one.
          String dppid = deployedProcess.getDuccProcess().getPID();
          if (dppid == null || dppid.equals(String.valueOf(cpi.getPid()))) {
            return true;
          }
        }
      }
      for (NodeUsersCollector.ProcessInfo pi : processList) {
        if (pi.getPid() == cpi.getPPid() && pi.getChildren().size() > 0) { // is
          // the current process a child of another java
          // Process?
          return isManagedProcess(pi.getChildren(), pi);
        }
      }
    }
    return false;
  }

  public boolean isRogueProcess(String uid, Set<NodeUsersCollector.ProcessInfo> processList,
          NodeUsersCollector.ProcessInfo cpi) throws Exception {

    synchronized (monitor) {
      // if cgroups are enabled, check if a given PID (cpi) exists in any of
      // the containers. If so, the process is not rogue.
      if (useCgroups) {
        if (cgroupsManager.isPidInCGroup(String.valueOf(cpi.getPid()))) {
          return false;
        }
      }
      // Agent adds a process to its inventory before launching it. So it
      // is
      // possible that the inventory contains a process with no PID. If
      // there
      // is such process in the inventory we cannot determine that a given
      // pid is rogue yet. Eventually, the launched process reports its
      // PID
      boolean foundDeployedProcessWithNoPID = false;
      Iterator<ManagedProcess> it = deployedProcesses.iterator();
      while (it.hasNext()) {
        ManagedProcess deployedProcess = it.next();
        if (deployedProcess.getDuccProcess() != null) {
          // Check if process has been deployed but has not yet
          // reported its PID.
          // This is normal. It takes a bit of time until the JP
          // reports
          // its PID to the Agent. If there is at least one process in
          // Agent
          // deploy list with no PID we assume it is the one.
          if (deployedProcess.getDuccProcess().getPID() == null) {
            foundDeployedProcessWithNoPID = true;
            break;
          }
          String dppid = deployedProcess.getDuccProcess().getPID();
          // process in inventory, not rogue
          if (dppid != null && dppid.equals(String.valueOf(cpi.getPid()))) {
            return false;
          }
        }
      }
      // not found
      if (foundDeployedProcessWithNoPID) {
        return false;
      } else if (cpi.getPPid() == 1) { // Any process owned by init is rogue
        // interrupt agent's thread blocking in waitFor() awaiting process termination.
        // This process is a zombie and there is no need to waste the thread.
        interruptThreadInWaitFor(String.valueOf(cpi.getPid()));
        return true;
      } else {
        return isParentProcessRogue(processList, cpi);
      }
    }
  }

  private boolean isParentProcessRogue(Set<NodeUsersCollector.ProcessInfo> processList,
          NodeUsersCollector.ProcessInfo cpi) {
    // boolean found = false;
    for (NodeUsersCollector.ProcessInfo pi : processList) {
      if (pi.getPid() == cpi.getPPid()) { // is the current process a
        if (pi.isRogue()) { // if parent is rogue, a child is rogue as
          // well
          return true;
        }
        return false;
      } else {
        if (pi.getChildren().size() > 0) {
          return isParentProcessRogue(pi.getChildren(), cpi);
        }
      }
    }
    return true;

  }

  /**
   * Process resident memory collector routes. Collects resident memory at fixed interval from the
   * OS.
   *
   */
  public class ProcessMemoryUsageRoute extends RouteBuilder {
    private NodeAgent agent;

    private IDuccProcess process;

    private ManagedProcess managedProcess;

    public ProcessMemoryUsageRoute(NodeAgent agent, IDuccProcess process,
            ManagedProcess managedProcess) {
      this.process = process;
      this.managedProcess = managedProcess;
      this.agent = agent;
    }

    public void configure() throws Exception {
      Processor nmp = configurationFactory.processMetricsProcessor(agent, process, managedProcess);
      int fixedRate = configurationFactory.getNodeInventoryPublishDelay();
      from("timer:processMemPollingTimer?fixedRate=true&delay=100&period=" + fixedRate)
              .routeId(process.getPID()).autoStartup(true).process(nmp);
    }
  }

  @Override
  public boolean isStopping() {
    return stopping;
  }

  @Override
  public void quiesceAndStop() throws Exception {
    stop(true, -1);
  }

  private void stop(boolean quiesce, long waitTimeInSecs) throws Exception {
    synchronized (stopLock) {
      logger.info("stop", null, "Agent stop() - quiesce:" + quiesce);
      if (stopping) {
        return;
      }
      stopping = true;
      stateChange(EventType.SHUTDOWN);
      // Dispatch SIGTERM to all processes. If this is quiesce mode we dont try to stop
      // non-preemptable processes
      boolean wait = stopChildProcesses(quiesce);
      if (quiesce) {
        logger.info("stop", null, "Agent stopping managed processes");
        long waitTime = 60; // default
        try {
          waitTime = Long.valueOf(configurationFactory.processStopTimeout);
          // Normalize. The configurationFactory.processStopTimeout from
          // ducc.properties is in millis. The code below expects secs.
          waitTime = (waitTime / 1000);
        } catch (Exception e) {
        }
        // Version 2.10.2 of UIMA-AS is not supporting quiesce and stop
        // so we need to implement wait than kill -9 strategy.
        waitForChildProcessesToTerminateAndKill(wait, waitTime, true, quiesce);
        logger.info("stop", null,
                ">>>>>>>>>>>> stop() waitForChildProcessesToTerminateAndKill() completed");
        // wait for JD processes to terminate. Return only when all non-preemptables
        // terminate.
        waitForChildProcessesToTerminate(false);
        logger.info("stop", null,
                ">>>>>>>>>>>> stop() waitForChildProcessesToTerminate() completed");
      } else {
        logger.info("stop", null, "Agent stopping managed processes with reaper delay of "
                + waitTimeInSecs + " secs");

        // wait for 60 secs and sends SIGKILL to any process still standing
        waitForChildProcessesToTerminateAndKill(wait, waitTimeInSecs, false, quiesce);
      }
      // Send an empty process map as the final inventory
      DuccEvent duccEvent = new NodeInventoryUpdateDuccEvent(inventory, getLastORSequence(),
              getIdentity());
      ORDispatcher.dispatch(duccEvent);
      logger.info("stop", null, "Agent published final inventory");

      logger.info("stop", null, "Stopping Publishing Metrics and Inventory");

      configurationFactory.stop();
      logger.info("stop", null, "Reaper thread finished - calling super.stop()");
      super.stop();
    }

  }

  @Override
  public void stop() throws Exception {
    // not quiesce, 60=default timeout before kill -9 is used
    stop(false, 60);
  }

  private void waitForChildProcessesToTerminateAndKill(boolean wait, long waitTimeInSecs,
          boolean killJustUimaAs, boolean quiesce) throws Exception {
    if (wait && !deployedProcesses.isEmpty()) {
      logger.info("waitForChildProcessesToTerminateAndKill", null,
              "Agent Sent SIGTERM to ALL Non-Preemptable Child Processes - Number of Deployed Processes:"
                      + deployedProcesses.size());

      Timer timer = new Timer(true);
      logger.info("waitForChildProcessesToTerminateAndKill", null, "Waiting", waitTimeInSecs,
              " secs before sending kill -9 to all ***non-preemptable*** child processes still running");
      CountDownLatch completionLatch = new CountDownLatch(1);
      // start a timer task which when triggered kills processes via kill -9
      timer.schedule(new KillTimerTask(completionLatch, killJustUimaAs, quiesce),
              waitTimeInSecs * 1000);

      // block this thread until killer task finishes its work
      completionLatch.await();

    }
    stopLock.wait(1000);
    logger.info("waitForChildProcessesToTerminateAndKill", null, "Done");

  }

  private void waitForChildProcessesToTerminate(boolean quiesceMode) throws Exception {
    logger.info("waitForChildProcessesToTerminate", null,
            "Agent Sent SIGTERM to Child Processes - Waiting for them to Quiesce - Number of Deployed Processes:"
                    + deployedProcesses.size());
    Iterator<ManagedProcess> it = deployedProcesses.iterator();
    while (it.hasNext()) {
      ManagedProcess p = it.next();
      // dont wait for JDs to stop. In quiesce mode we keep them
      // running until all JPs terminate and only than we stop them
      // if ( quiesceMode && ( p.isJd() || p.isUimaAs() ) ) {
      // in quiesce mode, skip UIMA-AS processes since currently
      // there is no support for quiesce there. So only wait for
      // POPs (JD), UIMA based JPs, and Services
      if (quiesceMode && p.isUimaAs()) {
        continue;
      }
      // block waiting for process to terminate
      p.getFuture().get();

    }
  }

  public Future<?> getDeployedJPFuture(IDuccId duccId) {
    Iterator<ManagedProcess> it = deployedProcesses.iterator();
    while (it.hasNext()) {
      // for (ManagedProcess deployedProcess : deployedProcesses) {
      ManagedProcess deployedProcess = it.next();
      // ignore duplicate start request for the same process
      if (deployedProcess.getDuccId().equals(duccId)) {
        return deployedProcess.getFuture();
      }
    }
    return null;
  }

  public ManagedProcess getDeployedProcess(IDuccId duccId) {
    Iterator<ManagedProcess> it = deployedProcesses.iterator();
    while (it.hasNext()) {
      ManagedProcess deployedProcess = it.next();
      // ignore duplicate start request for the same process
      if (deployedProcess.getDuccId().equals(duccId)) {
        return deployedProcess;
      }
    }
    return null;
  }

  /**
   * Copies reservations sent by the PM. It copies reservations associated with this node.
   *
   * @param reserves
   *          - list of ALL reservations
   * @throws Exception
   */
  public void setReservations(List<DuccUserReservation> reserves) throws Exception {
    try {
      reservationsSemaphore.acquire();
      if (reserves != null) {
        // clear old entries
        reservations.clear();
        // Only copy reservations for this node
        IDuccReservationMap reserveMap = new DuccReservationMap();
        for (DuccUserReservation r : reserves) {
          reserveMap.clear();
          for (Map.Entry<DuccId, IDuccReservation> entry : r.getUserReservations().entrySet()) {
            if (Utils.isThisNode(getIdentity().getIp(),
                    entry.getValue().getNodeIdentity().getIp())) {
              reserveMap.addReservation(entry.getValue());
            }
          }
          if (reserveMap.getMap().size() > 0) {
            DuccUserReservation reserve = new DuccUserReservation(r.getUserId(), r.getReserveID(),
                    reserveMap);
            reservations.add(reserve);
          }
        }
      }

      // this.reservations = reservations;
      logger.debug("setReservations", null,
              "+++++++++++ Copied User Reservations - List Size:" + reservations.size());
    } catch (InterruptedException e) {
    } finally {
      reservationsSemaphore.release();
    }
  }

  public List<DuccId> getUserReservations(String uid) {
    List<DuccId> reservationIds = new ArrayList<DuccId>();
    try {
      reservationsSemaphore.acquire();
      if (reservations != null) {
        for (DuccUserReservation r : reservations) {
          if (r.getUserId().equals(uid)) {
            for (Map.Entry<DuccId, IDuccReservation> entry : r.getUserReservations().entrySet()) {
              reservationIds.add(entry.getValue().getDuccId());
            }
            break;
          }
        }
      }
    } catch (InterruptedException e) {
    } finally {
      reservationsSemaphore.release();
    }
    return reservationIds;
  }

  public void copyAllUserReservations(TreeMap<String, NodeUsersInfo> map) {
    try {
      reservationsSemaphore.acquire();
      if (reservations != null) {
        logger.debug("copyAllUserReservations", null,
                "+++++++++++ Copying User Reservations - List Size:" + reservations.size());
        for (DuccUserReservation r : reservations) {
          if ("System".equals(r.getUserId())) {
            continue;
          }
          NodeUsersInfo nui = null;
          if (map.containsKey(r.getUserId())) {
            nui = map.get(r.getUserId());
          } else {
            nui = new NodeUsersInfo(r.getUserId());
            map.put(r.getUserId(), nui);
          }
          nui.addReservation(r.getReserveID());
        }
      } else {
        logger.debug("copyAllUserReservations", null, " ***********  No Reservations");
      }
    } catch (InterruptedException e) {
    } finally {
      reservationsSemaphore.release();
    }

  }

  public boolean userHasReservation(String uid) throws Exception {
    try {
      reservationsSemaphore.acquire();

      for (DuccUserReservation r : reservations) {
        if (r.getUserId().equals(uid)) {
          return true;
        }
      }
    } catch (InterruptedException e) {
    } finally {
      reservationsSemaphore.release();
    }
    return false;
  }

  public Object deepCopy(Object original) throws Exception {
    ObjectInputStream ois = null;
    ObjectOutputStream oos;
    ByteArrayInputStream bis;
    ByteArrayOutputStream bos;
    Object copy;
    try {
      // serialize object to bytes
      bos = new ByteArrayOutputStream();
      oos = new ObjectOutputStream(bos);
      oos.writeObject(original);
      oos.close();

      // construct an object from the bytes
      bis = new ByteArrayInputStream(bos.toByteArray());
      ois = new ObjectInputStream(bis);
      copy = ois.readObject();
      return copy;
    } catch (Exception e) {
      throw e;
    } finally {
      if (ois != null) {
        ois.close();
      }
    }
  }

  public RogueProcessReaper getRogueProcessReaper() {
    return rogueProcessReaper;
  }

  /**
   * Called when an Agent receives self dispatched Ping message.
   */
  // public void ping(AgentPingEvent agentPing) {
  // nodeMonitor.nodeArrives(agentPing.getNode());
  // }
  /*
   * public boolean excludeUser(String userId ) { if ( configurationFactory.userExclusionList !=
   * null ) { // exclusion list contains comma separated user ids String[] excludedUsers =
   * configurationFactory.userExclusionList.split(","); for ( String excludedUser : excludedUsers )
   * { if ( excludedUser.equals(userId)) { return true; } } } return false; } public boolean
   * excludeProcess(String process ) { if ( configurationFactory.processExclusionList != null ) { //
   * exclusion list contains comma separated user ids String[] excludedProcesses =
   * configurationFactory.processExclusionList.split(","); for ( String excludedProcess :
   * excludedProcesses ) { if ( excludedProcess.equals(process)) { return true; } } } return false;
   * }
   */
  public static void main(String[] args) {
    try {
      NodeIdentity node = new NodeIdentity(InetAddress.getLocalHost().getHostAddress(),
              InetAddress.getLocalHost().getHostName());
      NodeAgent agent = new NodeAgent(node);

      List<DuccUserReservation> reserves = new ArrayList<DuccUserReservation>();

      IDuccReservationMap reserveMap = new DuccReservationMap();
      IDuccReservationMap reserveMap2 = new DuccReservationMap();

      NodeIdentity ni1 = node;
      // new NodeIdentity(, name);
      NodeIdentity ni2 = new NodeIdentity("111.111.111.111", "node100");
      NodeIdentity ni3 = node;
      NodeIdentity ni4 = new NodeIdentity("222.222.222.222", "node102");

      DuccId id1 = new DuccId(100);
      DuccId id2 = new DuccId(101);
      DuccId id3 = new DuccId(102);
      DuccId id4 = new DuccId(103);

      IDuccReservation reservation1 = new DuccReservation(id1, ni1, 1);
      reserveMap.addReservation(reservation1);
      IDuccReservation reservation2 = new DuccReservation(id2, ni2, 1);
      reserveMap.addReservation(reservation2);
      IDuccReservation reservation4 = new DuccReservation(id4, ni4, 1);
      reserveMap.addReservation(reservation4);

      IDuccReservation reservation3 = new DuccReservation(id3, ni3, 1);
      reserveMap2.addReservation(reservation3);

      DuccUserReservation reserve = new DuccUserReservation("joe", new DuccId(500), reserveMap);
      DuccUserReservation reserve2 = new DuccUserReservation("jane", new DuccId(500), reserveMap2);
      reserves.add(reserve);
      reserves.add(reserve2);

      agent.setReservations(reserves);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private class NodeExclusionParser {
    private boolean excludeNodeFromCGroups = false;

    private boolean excludeAP = false;

    public void parse(String exclFile) throws Exception {
      // <node>=cgroup,ap
      File exclusionFile = new File(exclFile);
      if (!exclusionFile.exists()) {
        return;
      }
      BufferedReader br = new BufferedReader(new FileReader(exclusionFile));
      String line;
      NodeIdentity node = getIdentity();
      String nodeName = node.getShortName();

      while ((line = br.readLine()) != null) {
        if (line.startsWith(nodeName)) {
          String exclusions = line.substring(line.indexOf("=") + 1);
          String[] parsedExclusions = exclusions.split(",");
          for (String exclusion : parsedExclusions) {

            if (exclusion.trim().equals("cgroup")) {
              excludeNodeFromCGroups = true;

            } else if (exclusion.trim().equals("ap")) {
              excludeAP = true;

            }
          }
          break;
        }
      }
      br.close();
    }

    public boolean apExcluded() {
      return excludeAP;
    }

    public boolean cgroupsExcluded() {
      return excludeNodeFromCGroups;
    }
  }

  public DuccLogger getLogger() {
    return logger;
  }

  private void handleQuiesceAndStopEvent(DuccAdminEventQuiesceAndStop event) {
    logger.info("handleQuiesceAndStopEvent", null, "... Agent Received an Admin Request to Stop");
    try {
      stop(true, -1);
    } catch (Exception e) {
      logger.info("handleQuiesceAndStopEvent", null, e);
    }

  }

  private void handleStopEvent(DuccAdminEventStop event) {
    logger.info("handleStopEvent", null, "... Agent Received an Admin Request to Stop");
    try {

      stop(false, event.getTimeout());
    } catch (Exception e) {
      logger.info("handleStopEvent", null, e);
    }

  }

  private void handleStopPublishingEvent(DuccAdminEventStopMetrics event) {
    if (isThisTargetNode(getTargetNodes(event.getTargets()))) {
      logger.info("handleStopPublishingEvent", null,
              "... Agent Received an Admin Request to Stop Metrics Collection and Publishing");
      // Stop Camel route responsible for driving collection and publishing of metrics
      configurationFactory.stopMetricsRoute();
      logger.info("handleStopPublishingEvent", null,
              "... Agent Stopped Metrics Collection and Publishing");
    }

  }

  private String[] getTargetNodes(String targets) {
    logger.info("getTargetNodes", null, " Targets for Admin Command:" + targets);

    return targets.split(",");
  }

  private boolean isThisTargetNode(String[] nodes) {
    for (String targetNode : nodes) {
      if (Utils.isMachineNameMatch(targetNode.trim(), getIdentity().getCanonicalName())) {
        return true;
      }
    }
    return false;
  }

  private boolean isTarget(String[] targets) {
    for (String target : targets) {
      String[] targetParts = target.trim().split("@");
      logger.info("isTarget", null,
              " Targets for Admin Command:" + target + " This agent canonical identity:"
                      + getIdentity().getCanonicalName() + " short name:"
                      + getIdentity().getShortName());
      if ("agent".equals(targetParts[0])) {
        if (Utils.isMachineNameMatch(targetParts[1].trim(), getIdentity().getShortName())) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public void handleAdminEvent(DuccAdminEvent event) throws Exception {

    Thread t = new Thread() {
      public void run() {
        if (event instanceof DuccAdminEventStop) {
          if (isTarget(getTargetNodes(((DuccAdminEventStop) event).getTargets()))) {
            handleStopEvent((DuccAdminEventStop) event);
          }
        } else if (event instanceof DuccAdminEventQuiesceAndStop) {
          if (isTarget(getTargetNodes(((DuccAdminEventQuiesceAndStop) event).getTargets()))) {
            logger.info("handleAdminEvent", null, "Node a target for quiesce");
            handleQuiesceAndStopEvent((DuccAdminEventQuiesceAndStop) event);
          }
        } else if (event instanceof DuccAdminEventStopMetrics) {
          handleStopPublishingEvent((DuccAdminEventStopMetrics) event);
        } else {
          logger.info("handleAdminEvent", null,
                  "... Agent Received Unexpected Message of Type:" + event.getClass().getName());
        }
      }
    };
    t.start();

    /*
     * if (event instanceof DuccAdminEventStopMetrics) { // Get target machines from the message
     * String[] nodes = ((DuccAdminEventStopMetrics) event).getTargetNodes().split(","); // Check if
     * this message applies to this node for (String targetNode : nodes) { if
     * (Utils.isMachineNameMatch(targetNode.trim(), getIdentity().getCanonicalName())) {
     * logger.info("handleAdminEvent", null,
     * "... Agent Received an Admin Request to Stop Metrics Collection and Publishing"); // Stop
     * Camel route responsible for driving collection and publishing of metrics
     * configurationFactory.stopMetricsRoute(); logger.info("handleAdminEvent", null,
     * "... Agent Stopped Metrics Collection and Publishing"); break; } else {
     * logger.info("handleAdminEvent", null, "... Agent Not Target For Message:" +
     * event.getClass().getName()); } } } else { logger.info("handleAdminEvent", null,
     * "... Agent Received Unexpected Message of Type:" + event.getClass().getName());
     * 
     * }
     */
  }

  private class KillTimerTask extends TimerTask {

    private CountDownLatch completionLatch;

    private boolean killJustUimaAs;

    private boolean quiesce;

    public KillTimerTask(CountDownLatch completionLatch, boolean killOnlyUimaAs, boolean quiesce) {
      this.completionLatch = completionLatch;
      this.killJustUimaAs = killOnlyUimaAs;
      this.quiesce = quiesce;
    }

    @Override
    public void run() {
      try {
        // send kill -9 to any child process still running
        killChildProcesses(killJustUimaAs, quiesce);
      } finally {
        completionLatch.countDown();
      }
    }

  }
}
