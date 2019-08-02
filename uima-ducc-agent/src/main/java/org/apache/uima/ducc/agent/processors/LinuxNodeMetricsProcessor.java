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
package org.apache.uima.ducc.agent.processors;

import java.io.RandomAccessFile;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.camel.Exchange;
import org.apache.uima.ducc.agent.Agent;
import org.apache.uima.ducc.agent.NodeAgent;
import org.apache.uima.ducc.agent.metrics.collectors.NodeCpuCollector;
import org.apache.uima.ducc.agent.metrics.collectors.NodeLoadAverageCollector;
import org.apache.uima.ducc.agent.metrics.collectors.NodeMemInfoCollector;
import org.apache.uima.ducc.agent.metrics.collectors.NodeUsersCollector;
import org.apache.uima.ducc.common.DuccNode;
import org.apache.uima.ducc.common.Node;
import org.apache.uima.ducc.common.agent.metrics.memory.NodeMemory;
import org.apache.uima.ducc.common.node.metrics.NodeCpuInfo;
import org.apache.uima.ducc.common.node.metrics.NodeLoadAverage;
import org.apache.uima.ducc.common.node.metrics.NodeMetrics;
import org.apache.uima.ducc.common.node.metrics.NodeUsersInfo;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.transport.event.NodeMetricsUpdateDuccEvent;

public class LinuxNodeMetricsProcessor extends BaseProcessor implements NodeMetricsProcessor {
  DuccLogger logger = DuccLogger.getLogger(this.getClass(), Agent.COMPONENT_NAME);

  public static String[] MeminfoTargetFields = new String[] { "MemTotal:", "MemFree:", "SwapTotal:",
      "SwapFree:" };

  private NodeAgent agent;

  private String osname;

  private String osversion;

  private String osarch;

  private final ExecutorService pool;

  private RandomAccessFile memInfoFile;

  private RandomAccessFile loadAvgFile;

  // private Node node;
  private int swapThreshold = 0;

  // public LinuxNodeMetricsProcessor(NodeAgent agent, String memInfoFilePath,
  // String loadAvgFilePath) throws FileNotFoundException {
  public LinuxNodeMetricsProcessor() {
    super();
    // this.agent = agent;
    pool = Executors.newCachedThreadPool();
    // open files and keep them open until stop() is called
    // memInfoFile = new RandomAccessFile(memInfoFilePath, "r");
    // loadAvgFile = new RandomAccessFile(loadAvgFilePath, "r");
    // node = new DuccNode(agent.getIdentity(), null);

    osname = System.getProperty("os.name");
    osversion = System.getProperty("os.version");
    osarch = System.getProperty("os.arch");

    if (System.getProperty("ducc.node.min.swap.threshold") != null) {
      try {
        swapThreshold = Integer.valueOf(System.getProperty("ducc.node.min.swap.threshold"));
        logger.info("ctor", null, "Ducc Node Min Swap Threshold:" + swapThreshold);
      } catch (Exception e) {
      }
    }
  }

  public void setAgent(NodeAgent agent) {
    this.agent = agent;
  }

  public void initMemInfo(String memInfoFilePath) throws Exception {
    this.memInfoFile = new RandomAccessFile(memInfoFilePath, "r");

  }

  public void initLoadAvg(String loadAvgFilePath) throws Exception {
    this.loadAvgFile = new RandomAccessFile(loadAvgFilePath, "r");
  }

  public void stop() {
    try {
      if (memInfoFile != null) {
        memInfoFile.close();
      }
      if (loadAvgFile != null) {
        loadAvgFile.close();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Collects node's metrics and dumps it to a JMS topic. Currently collects memory utilization from
   * /proc/meminfo and average load from /proc/loadavg. This method is called from
   * NodeAgentStatsGenerator at fixed intervals.
   * 
   */
  public void process(Exchange e) {
    String methodName = "process";
    try {
      // every 10th node metrics publication log the status of CGroups
      if ((NodeAgent.logCounter.incrementAndGet() % 10) == 0) {
        if (agent.useCgroups) {
          logger.info(methodName, null, "\t****\n\t**** Agent CGroups status: enabled");

        } else {
          logger.info(methodName, null, "\t****\n\t**** Agent CGroups status: disabled. Reason:"
                  + NodeAgent.cgroupFailureReason);

        }
      }

      NodeMemInfoCollector memCollector = new NodeMemInfoCollector(MeminfoTargetFields);
      Future<NodeMemory> nmiFuture = pool.submit(memCollector);

      NodeLoadAverageCollector loadAvgCollector = new NodeLoadAverageCollector();

      Future<NodeLoadAverage> loadFuture = pool.submit(loadAvgCollector);
      NodeCpuCollector cpuCollector = new NodeCpuCollector();
      // Future<NodeCpuInfo> cpuFuture = pool.submit(cpuCollector);
      NodeCpuInfo cpuInfo = new NodeCpuInfo(agent.numProcessors,
              String.valueOf(cpuCollector.call()));

      e.getIn().setHeader("node", agent.getIdentity().getCanonicalName());
      NodeMemory memInfo = nmiFuture.get();
      TreeMap<String, NodeUsersInfo> users = null;
      // begin collecting user processes and activate rogue process detector
      // only after the agent receives the first Ducc state publication.
      if (agent.receivedDuccState) {
        NodeUsersCollector nodeUsersCollector = new NodeUsersCollector(agent, logger);

        logger.debug(methodName, null, "... Agent Collecting User Processes");

        Future<TreeMap<String, NodeUsersInfo>> nuiFuture = pool.submit(nodeUsersCollector);
        users = nuiFuture.get();
      } else {
        users = new TreeMap<String, NodeUsersInfo>();
      }
      NodeLoadAverage lav = loadFuture.get();
      boolean cpuReportingEnabled = false;
      if (agent.cgroupsManager != null) {
        cpuReportingEnabled = agent.cgroupsManager.isCpuReportingEnabled();
      }
      NodeMetrics nodeMetrics = new NodeMetrics(agent.getIdentity(), memInfo, lav, cpuInfo, users,
              cpuReportingEnabled);
      if (agent.isStopping()) {
        nodeMetrics.disableNode(); // sends Unavailable status to clients (RM,WS)
      }

      Node node = new DuccNode(agent.getIdentity(), nodeMetrics, agent.useCgroups);
      // Make the agent aware how much memory is available on the node. Do this once.
      if (agent.getNodeInfo() == null) {
        agent.setNodeInfo(node);
      }

      ((DuccNode) node).duccLingExists(agent.duccLingExists());
      ((DuccNode) node).runWithDuccLing(agent.runWithDuccLing());
      logger.info(methodName, null,
              "... Agent " + node.getNodeIdentity().getCanonicalName() + " OS Name:" + osname
                      + " OS Version:" + osversion + " OS Arch:" + osarch + " CPU Count:"
                      + cpuInfo.getAvailableProcessors() + " CPU Load Average:" + lav.getLoadAvg1()
                      + " Posting Memory (KB):"
                      + node.getNodeMetrics().getNodeMemory().getMemTotal() + " Memory Free (KB):"
                      + node.getNodeMetrics().getNodeMemory().getMemFree() + " Swap Total (KB):"
                      + node.getNodeMetrics().getNodeMemory().getSwapTotal() + " Swap Free (KB):"
                      + node.getNodeMetrics().getNodeMemory().getSwapFree()
                      + " Low Swap Threshold Defined in ducc.properties (KB):" + swapThreshold
                      + " CPU Reporting Enabled:" + cpuReportingEnabled + " Node Status:"
                      + nodeMetrics.getNodeStatus());

      logger.trace(methodName, null, "... Agent " + node.getNodeIdentity().getCanonicalName()
              + " Posting Users:" + node.getNodeMetrics().getNodeUsersMap().size());
      // Check if swap free is less than defined minimum threshold (check ducc.properties)
      if (swapThreshold > 0
              && (node.getNodeMetrics().getNodeMemory().getSwapFree() < swapThreshold)) {
        agent.killProcessDueToLowSwapSpace(swapThreshold);
      }
      NodeMetricsUpdateDuccEvent updateEvent = new NodeMetricsUpdateDuccEvent(node,
              agent.getInventoryRef().size());
      e.getIn().setBody(updateEvent, NodeMetricsUpdateDuccEvent.class);

    } catch (Exception ex) {
      logger.error(methodName, null, ex, new Object[] { "Agent" });
    }
  }
}
