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

import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.apache.uima.ducc.agent.processors.LinuxNodeMetricsProcessor;



public class NodeMetricsGenerator {

  public NodeMetricsGenerator( int refreshRate, int timeToLive) {

  }
  protected LinuxNodeMetricsProcessor configure( CamelContext context, String brokerUrl, final String ducc_node_metrics_endpoint) throws Exception {

    context.addRoutes(new RouteBuilder() {
      public void configure() {
//        from("timer:nodeMetricsTimer?fixedRate=true&period=" + refreshRate).startupOrder(3)
//                .setHeader(DuccExchange.Event, constant(DuccExchange.NodeStatsEvent))
//                .setHeader(DuccExchange.DUCCNODENAME, constant(NodeAgent.getI().getHostname()))
//                .setHeader(DuccExchange.DUCCNODEIP, constant(NodeAgent.getAgentInfo().getIp()))
//                .process(nodeMetricsProcessor).to(
//                		ducc_node_metrics_endpoint+"?explicitQosEnabled=true&timeToLive="+timeToLive);
      }
    });
   // return nodeMetricsProcessor;
    return null;
  }
}
