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
package org.apache.uima.ducc.rm.config;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.uima.ducc.common.config.CommonConfiguration;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.SystemPropertyResolver;
import org.apache.uima.ducc.rm.NodeStability;
import org.apache.uima.ducc.rm.ResourceManager;
import org.apache.uima.ducc.rm.ResourceManagerComponent;
import org.apache.uima.ducc.rm.event.ResourceManagerEventListener;
import org.apache.uima.ducc.rm.scheduler.SchedConstants;
import org.apache.uima.ducc.transport.DuccTransportConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;


@Configuration
    @Import({DuccTransportConfiguration.class,CommonConfiguration.class})

    public class ResourceManagerConfiguration 
    implements SchedConstants
    {
        @Autowired CommonConfiguration common;
        @Autowired DuccTransportConfiguration resourceManagerTransport;

        DuccLogger logger = DuccLogger.getLogger(this.getClass(), COMPONENT_NAME);
        
        public ResourceManagerEventListener resourceManagerDelegateListener(ResourceManagerComponent rm) {
            ResourceManagerEventListener jmel =  new ResourceManagerEventListener(rm);
            int nodeStability = SystemPropertyResolver.getIntProperty("ducc.rm.node.stability", DEFAULT_STABILITY_COUNT);
            int agentMetricsRate = SystemPropertyResolver.getIntProperty("ducc.agent.node.metrics.publish.rate", DEFAULT_NODE_METRICS_RATE);
            NodeStability ns = new NodeStability(rm, nodeStability, agentMetricsRate);            
            rm.setNodeStability(ns);
            jmel.setEndpoint(common.rmStateUpdateEndpoint);
            jmel.setNodeStability(ns);
            ns.start();
            return jmel;
        }
        
        public RouteBuilder routeBuilderForEndpoint(final String endpoint, final ResourceManagerEventListener delegate) {
            return new RouteBuilder() {
                public void configure() {
                    from(endpoint)
                        .bean(delegate);
                }
            };
        }


        // test and debug only - user routeBuilderForEndpoint normally
        public RouteBuilder routeBuilderForJmEndpoint(final String endpoint, final ResourceManagerEventListener delegate) {
            System.out.println("Starting JM endpoint " + endpoint + "  ???????????????????????");
            return new RouteBuilder() {
                public void configure() {
                    from(endpoint)
                        .threads(10)
                        .bean(delegate);
                }
            };
        }
        
        @Bean 
        public ResourceManagerComponent resourceManager() 
        throws Throwable 
        {
            String methodName = "resourceManager";
            ResourceManagerComponent rm = null;
            try {            
                rm = new ResourceManagerComponent(common.camelContext());

                // rm.init();

                rm.setTransportConfiguration(resourceManagerTransport.duccEventDispatcher(common.rmStateUpdateEndpoint, rm.getContext()), 
                                             common.rmStateUpdateEndpoint, common.daemonsStateChangeEndpoint);
        
                //  Instantiate Resource Manager delegate listener. This listener will receive
                //  incoming messages. 
                ResourceManagerEventListener delegateListener = this.resourceManagerDelegateListener(rm);

                //  Inject a dispatcher into the listener in case it needs to send
                //  a message to another component. 
                delegateListener.setDuccEventDispatcher(resourceManagerTransport.duccEventDispatcher(common.orchestratorStateUpdateEndpoint,rm.getContext()));

                //  Inject Camel Router that will generate state updates at regular intervals
                // jrc rm.getContext().addRoutes(this.routeBuilderForRMStateUpdate(rm, common.rmStateUpdateEndpoint, Integer.parseInt(common.rmStatePublishRate)));

                //  Inject Camel Router that will handle Orchestrator state update messages
                rm.getContext().addRoutes(this.routeBuilderForEndpoint(common.orchestratorStateUpdateEndpoint, delegateListener));

                //  Inject Camel Router that will handle Agent Node inventory update messages
                // rm.getContext().addRoutes(this.routeBuilderForEndpoint(common.nodeInventoryEndpoint,delegateListener));

                //  Inject Camel Router that will handle Node Metrics messages
                rm.getContext().addRoutes(this.routeBuilderForEndpoint(common.nodeMetricsEndpoint, delegateListener));
                
                return rm;
            } catch ( Throwable t ) {
                logger.fatal(methodName, null, t);
                throw new IllegalStateException("Can't start RM: " + t.getMessage());
            }

        }

        public class ResourceManagerStateUpdateProcessor implements Processor {
            private ResourceManager resourceManager;
            public ResourceManagerStateUpdateProcessor(ResourceManager resourceManager) {
                this.resourceManager = resourceManager;
            }
            public void process(Exchange exchange) throws Exception {
                exchange.getIn().setBody(resourceManager.getState()); //new RmStateDuccEvent());
            }
                
        }

        public class NodeInventoryProcessor implements Processor {

            public void process(Exchange exchange) throws Exception {
                //                      System.out.println("... transport received Event. Body Type:"+exchange.getIn().getBody().getClass().getName());
                //Destination replyTo = exchange.getIn().getHeader("JMSReplyTo", Destination.class); 
                //                      System.out.println("... transport - value of replyTo:" + replyTo);
            }
                
        }

        public class NodeMetricsProcessor implements Processor {

            public void process(Exchange exchange) throws Exception {
                //                      System.out.println("... transport received Event. Body Type:"+exchange.getIn().getBody().getClass().getName());
                //Destination replyTo = exchange.getIn().getHeader("JMSReplyTo", Destination.class); 
                //                      System.out.println("... transport - value of replyTo:" + replyTo);
            }
                
        }

    }
