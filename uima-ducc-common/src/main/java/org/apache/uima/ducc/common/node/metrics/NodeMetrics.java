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
package org.apache.uima.ducc.common.node.metrics;

import java.io.Serializable;
import java.util.TreeMap;

import org.apache.uima.ducc.common.NodeIdentity;
import org.apache.uima.ducc.common.agent.metrics.memory.NodeMemory;


public class NodeMetrics implements Serializable {
  private static final long serialVersionUID = 4646364817769237774L;
  private NodeLoadAverage nodeLoadAverage;
  private NodeMemory nodeMemory;
  private NodeCpuInfo nodeCpu;
  private NodeIdentity nodeIdentity;
  TreeMap<String,NodeUsersInfo> nodeUsersMap;
  private boolean cpuReportingEnabled = false;
  
  public NodeMetrics(NodeIdentity nodeIdentity, NodeMemory nodeMemory, NodeLoadAverage nodeLoadAverage , NodeCpuInfo nodeCpu, TreeMap<String,NodeUsersInfo> userProcessMap, boolean cpuReportingEnabled) {
    this.nodeIdentity = nodeIdentity;
	  setNodeMemory(nodeMemory);
    setNodeLoadAverage(nodeLoadAverage);
    setNodeCpu(nodeCpu);
    //setNodeUsersInfo(nodeUsersInfo);
    this.nodeUsersMap = userProcessMap;
    this.cpuReportingEnabled = cpuReportingEnabled;
  }
  public TreeMap<String, NodeUsersInfo> getNodeUsersMap() {
    return nodeUsersMap;
  }
  public boolean isCpuReportingEnabled() {
	  return cpuReportingEnabled;
  }
   public NodeIdentity getNodeIdentity() {
	  return nodeIdentity;
  }
  public NodeCpuInfo getNodeCpu() {
    return nodeCpu;
  }
  public void setNodeCpu(NodeCpuInfo nodeCpu) {
    this.nodeCpu = nodeCpu;
  }
  public NodeLoadAverage getNodeLoadAverage() {
    return nodeLoadAverage;
  }
  public void setNodeLoadAverage(NodeLoadAverage nodeLoadAverage) {
    this.nodeLoadAverage = nodeLoadAverage;
  }
  public NodeMemory getNodeMemory() {
    return nodeMemory;
  }
  public void setNodeMemory(NodeMemory nodeMemory) {
    this.nodeMemory = nodeMemory;
  }

}
