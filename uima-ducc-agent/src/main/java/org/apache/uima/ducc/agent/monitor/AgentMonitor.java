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
package org.apache.uima.ducc.agent.monitor;

import java.util.Map;

import org.apache.uima.ducc.agent.NodeAgent;
import org.apache.uima.ducc.agent.ProcessReaperTask;
import org.apache.uima.ducc.common.ANodeStability;
import org.apache.uima.ducc.common.Node;
import org.apache.uima.ducc.common.utils.DuccLogger;


public class AgentMonitor extends ANodeStability{
  //private NodeAgent agent;
  private ProcessReaperTask reaperTask;
  DuccLogger logger;
  public AgentMonitor(NodeAgent agent, DuccLogger logger, int nodeStability, int agentMetricsRate) {
    super(nodeStability, agentMetricsRate);
    //this.agent = agent;
    this.logger = logger;
    reaperTask = new ProcessReaperTask(agent,logger);
  }

  public void nodeDeath(Map<Node, Node> nodes) {
    logger.warn("AgentMonitor.nodeDeath", null,"Agent detected a network/borker problem. Proceeding to shutdown JPs");
    Thread t = new Thread(reaperTask);
    t.setDaemon(true);
    t.start();
  }

  public void missedNode(Node n, int c) {
    logger.info("missedNode",null,"Agent missed a ping ("+c+")");
  }

  public void ping(Node node) {
    super.nodeArrives(node);
  }
}
