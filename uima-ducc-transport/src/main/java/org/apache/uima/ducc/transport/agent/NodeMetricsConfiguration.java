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
package org.apache.uima.ducc.transport.agent;

import java.net.InetAddress;

import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.uima.ducc.transport.DuccExchange;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
public class NodeMetricsConfiguration {
//	@Value("${dataSource.driverClassName}") String someValue;
	@Value("#{ systemProperties['ducc.agent.node.metrics.generator'] }")String agentNodeMetricGenerator;
	@Value("#{ systemProperties['ducc.agent.node.metrics.target.endpoint'] }")String agentNodeMetricEndpointTarget;

	@Bean
    public RouteBuilder routeBuilderForNodeMetrics(final Processor nodeMetricsProcessor) { //, final String hostname, final String ip) {
        return new RouteBuilder() {
            public void configure() throws Exception {
            	System.out.println("............. Generator::"+agentNodeMetricGenerator);
            	System.out.println("............. Target::"+agentNodeMetricEndpointTarget);
            	
                from(agentNodeMetricGenerator)
                .setHeader(DuccExchange.Event, constant(DuccExchange.NodeStatsEvent))
                .setHeader(DuccExchange.DUCCNODENAME, constant(InetAddress.getLocalHost().getHostName()))
                .setHeader(DuccExchange.DUCCNODEIP, constant(InetAddress.getLocalHost().getHostAddress()))
                .process(nodeMetricsProcessor).to(agentNodeMetricEndpointTarget);
                
            }
        };
    }
}
