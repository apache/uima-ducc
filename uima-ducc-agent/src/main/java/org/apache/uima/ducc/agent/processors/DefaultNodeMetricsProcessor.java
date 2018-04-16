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

import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.camel.Exchange;
import org.apache.uima.ducc.agent.Agent;
import org.apache.uima.ducc.agent.NodeAgent;
import org.apache.uima.ducc.agent.metrics.collectors.DefaultNodeLoadAverageCollector;
import org.apache.uima.ducc.agent.metrics.collectors.DefaultNodeMemoryCollector;
import org.apache.uima.ducc.agent.metrics.collectors.NodeCpuCollector;
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


public class DefaultNodeMetricsProcessor extends BaseProcessor implements
		NodeMetricsProcessor {
	private  NodeAgent agent;
	
	private ExecutorService pool = Executors.newFixedThreadPool(1);
	
	DuccLogger logger = DuccLogger.getLogger(this.getClass(), Agent.COMPONENT_NAME);
	
	/*
	public DefaultNodeMetricsProcessor(final NodeAgent agent) throws Exception {
		this.agent = agent;
	}
	*/
	public void setAgent(NodeAgent agent ) {
		this.agent = agent;
	}
	public void process(Exchange exchange) throws Exception {
	  String methodName = "process";
	  try {

	    DefaultNodeMemoryCollector collector = new DefaultNodeMemoryCollector();
	    Future<NodeMemory> nmiFuture = pool.submit(collector);

	    DefaultNodeLoadAverageCollector loadAvgCollector = 
	            new DefaultNodeLoadAverageCollector();
	    Future<NodeLoadAverage> loadFuture = pool.submit(loadAvgCollector);

	    NodeCpuCollector cpuCollector = new NodeCpuCollector();
//	    Future<NodeCpuInfo> cpuFuture = pool.submit(cpuCollector);

	    NodeCpuInfo cpuInfo = new NodeCpuInfo(agent.numProcessors, cpuCollector.call().getCurrentLoad());
	    
	    NodeUsersCollector nodeUsersCollector = new NodeUsersCollector(agent, logger);
	    Future<TreeMap<String,NodeUsersInfo>> nuiFuture = pool.submit(nodeUsersCollector);
		boolean cpuReportingEnabled = agent.cgroupsManager.isCpuReportingEnabled();

	    NodeMetrics nodeMetrics = 
	            new NodeMetrics(agent.getIdentity(), nmiFuture.get(), loadFuture.get(), 
	                    cpuInfo, nuiFuture.get(), cpuReportingEnabled);

	    //Node node = new DuccNode(new NodeIdentity(), nodeMetrics);
	    // jrc 2011-07-30 I think this needs to be agent.getIdentity(), not create a new identity.
	    Node node = new DuccNode(agent.getIdentity(), nodeMetrics, agent.useCgroups);
		
	    // Make the agent aware how much memory is available on the node. Do this once.
		if ( agent.getNodeInfo() == null ) {
			agent.setNodeInfo(node);
		}
	    logger.info(methodName, null, "... Agent "+node.getNodeIdentity().getCanonicalName()+" Posting Users:"+
	            node.getNodeMetrics().getNodeUsersMap().size());
	    
	    NodeMetricsUpdateDuccEvent event = new NodeMetricsUpdateDuccEvent(node,agent.getInventoryRef().size());
	    exchange.getIn().setBody(event, NodeMetricsUpdateDuccEvent.class);

	  } catch( Exception e) {
	    e.printStackTrace();
	  }

	}

}
