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
package org.apache.uima.ducc.transport.event;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.uima.ducc.common.Node;
import org.apache.uima.ducc.common.NodeIdentity;
import org.apache.uima.ducc.common.agent.metrics.memory.NodeMemory;
import org.apache.uima.ducc.common.node.metrics.NodeUsersInfo;
import org.apache.uima.ducc.common.node.metrics.NodeUsersInfo.NodeProcess;


public class NodeMetricsUpdateDuccEvent extends AbstractDuccEvent{

	private static final long serialVersionUID = -1066240477810440223L;
	private Node node;
	private int processCount=0;
	
	public NodeMetricsUpdateDuccEvent(Node node, int processCount) {
		super(EventType.NODE_METRICS);
		this.node = node;
		this.processCount = processCount;
	}
	public Node getNode() {
		return node;
	}
	public NodeIdentity getNodeIdentity() {
		return node.getNodeIdentity();
	}
	public NodeMemory getNodeMemory() {
		return node.getNodeMetrics().getNodeMemory();
	}
	public TreeMap<String,NodeUsersInfo> getNodeUsersMap() {
	  return node.getNodeMetrics().getNodeUsersMap();
	}
	public List<ProcessInfo> getRogueProcessInfoList() {
		List<ProcessInfo> retVal = new ArrayList<ProcessInfo>();
		TreeMap<String,NodeUsersInfo> nodeUsersMap = getNodeUsersMap();
		if(nodeUsersMap != null) {
			for(Entry<String, NodeUsersInfo> entry : nodeUsersMap.entrySet()) {
				NodeUsersInfo nodeUsersInfo = entry.getValue();
				String uid = nodeUsersInfo.getUid();
				List<NodeProcess> rogueList = nodeUsersInfo.getRogueProcesses();
				for( NodeProcess rogue : rogueList ) {
					ProcessInfo processInfo = new ProcessInfo(uid,rogue.getPid(), rogue.isJava());
					retVal.add(processInfo);
				}
			}
		}
		return retVal;
	}
	public int getProcessCount() {
		return processCount;
	}
	public boolean getCgroups() {
		return node.isCgroupEnabled();
	}
	public boolean getCgroupsCpuReportingEnabled() {
		return node.getNodeMetrics().isCpuReportingEnabled();
	}
}
