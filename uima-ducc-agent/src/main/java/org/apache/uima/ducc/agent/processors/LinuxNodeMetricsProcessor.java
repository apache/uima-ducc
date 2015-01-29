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

import java.io.FileNotFoundException;
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


public class LinuxNodeMetricsProcessor extends BaseProcessor implements
		NodeMetricsProcessor {
	DuccLogger logger = DuccLogger.getLogger(this.getClass(), Agent.COMPONENT_NAME);
    public static String[] MeminfoTargetFields = new String[] {"MemTotal:","MemFree:","SwapTotal:","SwapFree:"};

	private NodeAgent agent;
	private final ExecutorService pool;
	private RandomAccessFile memInfoFile;
	private RandomAccessFile loadAvgFile;
	//private Node node;
	private int swapThreshold = 0;
	public LinuxNodeMetricsProcessor(NodeAgent agent, String memInfoFilePath,
			String loadAvgFilePath) throws FileNotFoundException {
		super();
		this.agent = agent;
		pool = Executors.newCachedThreadPool();
		// open files and keep them open until stop() is called
		memInfoFile = new RandomAccessFile(memInfoFilePath, "r");
		loadAvgFile = new RandomAccessFile(loadAvgFilePath, "r");
		//node = new DuccNode(agent.getIdentity(), null);
		if ( System.getProperty("ducc.node.min.swap.threshold") != null ) {
	    try {
	      swapThreshold = Integer.valueOf(System.getProperty("ducc.node.min.swap.threshold"));
	      logger.info("ctor", null, "Ducc Node Min Swap Threshold:"+swapThreshold);
	    } catch( Exception e) {
	    }
		}
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
	 * Collects node's metrics and dumps it to a JMS topic. Currently collects
	 * memory utilization from /proc/meminfo and average load from
	 * /proc/loadavg. This method is called from NodeAgentStatsGenerator at
	 * fixed intervals.
	 * 
	 */
  public void process(Exchange e) {
		String methodName = "process";
		try {
		  

			NodeMemInfoCollector memCollector = new NodeMemInfoCollector(MeminfoTargetFields);
			Future<NodeMemory> nmiFuture = pool.submit(memCollector);
			NodeLoadAverageCollector loadAvgCollector = new NodeLoadAverageCollector(
					loadAvgFile, 5, 0);
			Future<NodeLoadAverage> loadFuture = pool.submit(loadAvgCollector);
			NodeCpuCollector cpuCollector = new NodeCpuCollector();
			Future<NodeCpuInfo> cpuFuture = pool.submit(cpuCollector);
			e.getIn().setHeader("node", agent.getIdentity().getName());
			NodeMemory memInfo = nmiFuture.get();
			TreeMap<String, NodeUsersInfo> users = null;
			// begin collecting user processes and activate rogue process detector
			// only after the agent receives the first Ducc state publication.
			if ( agent.receivedDuccState ) {
			    NodeUsersCollector nodeUsersCollector = new NodeUsersCollector(agent, logger);
			    
			    logger.info(methodName, null, "... Agent Collecting User Processes");
			    
			    Future<TreeMap<String,NodeUsersInfo>> nuiFuture = 
			            pool.submit(nodeUsersCollector);
			    users = nuiFuture.get();
			} else {
				users = new TreeMap<String, NodeUsersInfo>();
			}
	    
            NodeMetrics nodeMetrics = new NodeMetrics(agent.getIdentity(), memInfo, loadFuture.get(),
              cpuFuture.get(), users);
      
			Node node = new DuccNode(agent.getIdentity(), nodeMetrics, agent.useCgroups);
			// Make the agent aware how much memory is available on the node. Do this once.
			if ( agent.getNodeInfo() == null ) {
				agent.setNodeInfo(node);
			}
						
			((DuccNode)node).duccLingExists(agent.duccLingExists());
			((DuccNode)node).runWithDuccLing(agent.runWithDuccLing());
			logger.info(methodName, null, "... Agent "+node.getNodeIdentity().getName()+" Posting Memory:"
					+ node.getNodeMetrics().getNodeMemory().getMemTotal()+
					" Memory Free:"+node.getNodeMetrics().getNodeMemory().getMemFree()+
					" Swap Total:"+node.getNodeMetrics().getNodeMemory().getSwapTotal()+
					" Swap Free:"+node.getNodeMetrics().getNodeMemory().getSwapFree()+
					" Low Swap Threshold Defined in ducc.properties:"+swapThreshold);
			logger.trace(methodName, null, "... Agent "+node.getNodeIdentity().getName()+" Posting Users:"+
					node.getNodeMetrics().getNodeUsersMap().size());
			// Check if swap free is less than defined minimum threshold (check ducc.properties) 
			if ( swapThreshold > 0 && ( node.getNodeMetrics().getNodeMemory().getSwapFree() < swapThreshold)) {
			  agent.killProcessDueToLowSwapSpace(swapThreshold);
			}
			NodeMetricsUpdateDuccEvent updateEvent = new NodeMetricsUpdateDuccEvent(node,agent.getInventoryRef().size());
			e.getIn().setBody(updateEvent, NodeMetricsUpdateDuccEvent.class);

			//  Add header property which will allow the agent to filter in its pings
//			Map<String, Object> headers = new HashMap<String, Object>();
//			headers.put(agent.configurationFactory.agentPingSelectorName, 
//			        agent.getIdentity().getIp());
		
			// Dispatch ping to self via common agent ping topic. A property in the header 
			// allow the agent to filter in its pings
//			agent.
//			  configurationFactory.
//			    getAgentPingDispatcher().
//			      dispatch(new AgentPingEvent(EventType.AGENT_PING, node), headers);

		} catch (Exception ex) {
			logger.error(methodName, null, ex, new Object[] { "Agent" });
		}
	}

}
