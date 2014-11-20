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
package org.apache.uima.ducc.transport.configuration.jd;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.jetty.JettyHttpComponent;
import org.apache.uima.ducc.common.config.CommonConfiguration;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.Utils;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.container.net.iface.IMetaCasTransaction;
import org.apache.uima.ducc.container.net.iface.IMetaCasTransaction.Direction;
import org.apache.uima.ducc.container.net.impl.MetaCasTransaction;
import org.apache.uima.ducc.transport.DuccTransportConfiguration;
import org.apache.uima.ducc.transport.configuration.jd.iface.IJobDriverComponent;
import org.apache.uima.ducc.transport.event.JdStateDuccEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

	/**
	 * A {@link JobDriverConfiguration} to configure JobDriver component. Depends on 
	 * properties loaded by a main program into System properties. 
	 * 
	 */
	@Configuration
	@Import({DuccTransportConfiguration.class,CommonConfiguration.class})
	public class JobDriverConfiguration {
		
		private static DuccLogger logger = DuccLoggerComponents.getJdOut(JobDriverConfiguration.class.getName());
		private static DuccId jobid = null;
		
		//	use Spring magic to autowire (instantiate and bind) CommonConfiguration to a local variable
		@Autowired CommonConfiguration common;
		//	use Spring magic to autowire (instantiate and bind) DuccTransportConfiguration to a local variable
		@Autowired DuccTransportConfiguration jobDriverTransport;
		
		/**
		 * Instantiate {@link JobDriverEventListener} which will handle incoming messages.
		 * 
		 * @param jd - {@link JobDriverComponent} instance
		 * @return - {@link JobDriverEventListener}
		 */
		public JobDriverEventListener jobDriverDelegateListener(IJobDriverComponent jdc) {
			JobDriverEventListener jdel =  new JobDriverEventListener(jdc);
			return jdel;
		}
		/**
		 * Create a Router to handle incoming messages from a given endpoint. All messages are delegated
		 * to a provided listener. Note: Camel uses introspection to determine which method to call when
		 * delegating a message. The name of the method doesnt matter it is the argument that needs
		 * to match the type of object in the message. If there is no method with a matching argument
		 * type the message will not be delegated.
		 * 
		 * @param endpoint - endpoint where messages are expected
		 * @param delegate - {@link JobDriverEventListener} instance
		 * @return - initialized {@link RouteBuilder} instance
		 * 
		 */
		public synchronized RouteBuilder routeBuilderForIncomingRequests(final String endpoint, final JobDriverEventListener delegate) {
	        return new RouteBuilder() {
	            public void configure() {
	            	from(endpoint)
	            	.bean(delegate);
	            }
	        };
	    }

		
		/**
		 * Creates Camel router that will publish Dispatched Job state at regular intervals.
		 * 
		 * @param targetEndpointToReceiveJdStateUpdate - endpoint where to publish Jd state 
		 * @param statePublishRate - how often to publish state
		 * @return
		 * @throws Exception
		 */
		private RouteBuilder routeBuilderForJdStatePost(final IJobDriverComponent jdc, final String targetEndpointToReceiveJdStateUpdate, final int statePublishRate) throws Exception {
			final JobDriverStateProcessor jdsp =  // an object responsible for generating the state 
				new JobDriverStateProcessor(jdc);
			
			return new RouteBuilder() {
			      public void configure() {
			        from("timer:jdStateDumpTimer?fixedRate=true&period=" + statePublishRate)
			                .process(jdsp)
			                .to(targetEndpointToReceiveJdStateUpdate);
			      }
			    };

		}
		private RouteBuilder routeBuilderForJpIncomingRequests(final JobDriverComponent jdc, final int port, final String app) throws Exception {
		    return new RouteBuilder() {
		        public void configure() throws Exception {
		        	CamelContext camelContext = jdc.getContext();
		            JettyHttpComponent jetty = new JettyHttpComponent();
		            jetty.setMaxThreads(4);  // Need to parameterize
		            jetty.setMinThreads(1);
		            camelContext.addComponent("jetty", jetty);
		            // listen on all interfaces.
		            from("jetty:http://0.0.0.0:" + port + "/"+app)
		            .unmarshal().xstream().
		            process(new JobDriverProcessor(jdc)).marshal().xstream();
		        }
		    };
		}
		
		public static class JobDriverProcessor  implements Processor {
			private 	IJobDriverComponent jdc;
			
			private JobDriverProcessor(IJobDriverComponent jdc) {
				this.jdc = jdc;
			}
		    public void process(Exchange exchange) throws Exception {
		        // Get the transaction object sent by the JP
		    	IMetaCasTransaction imt = 
		        		exchange.getIn().getBody(MetaCasTransaction.class);
		        
		    	// process JP's request
		    	jdc.handleJpRequest(imt);
		    	
		    	// setup reply 
		    	imt.setDirection(Direction.Response);

		        exchange.getOut().setHeader("content-type", "text/xml");
		        // ship it!
		        exchange.getOut().setBody(imt);
		    }
		} 
		/**
		 * Camel Processor responsible for generating Dispatched Job's state.
		 * 
		 */
		private class JobDriverStateProcessor implements Processor {
			private IJobDriverComponent jdc;
			
			private JobDriverStateProcessor(IJobDriverComponent jdc) {
				this.jdc = jdc;
			}
			public void process(Exchange exchange) throws Exception {
				// Fetch new state from Dispatched Job
				JdStateDuccEvent sse = jdc.getState();
				//	Add the state object to the Message
				exchange.getIn().setBody(sse);
			}
			
		}
		
		/**
		 * Creates and initializes {@link JobDriverComponent} instance. @Bean annotation identifies {@link JobDriverComponent}
		 * as a Spring framework Bean which will be managed by Spring container.  
		 * 
		 * @return {@link JobDriverComponent} instance
		 * 
		 * @throws Exception
		 */
		@Bean 
		public JobDriverComponent jobDriver() throws Exception {
			String location = "jobDriver";
			JobDriverComponent jdc = new JobDriverComponent("JobDriver", common.camelContext(), this);
	        //	Instantiate delegate listener to receive incoming messages. 
	        JobDriverEventListener delegateListener = this.jobDriverDelegateListener(jdc);
			//	Inject a dispatcher into the listener in case it needs to send
			//  a message to another component
	        delegateListener.setDuccEventDispatcher(jobDriverTransport.duccEventDispatcher(common.orchestratorStateUpdateEndpoint, jdc.getContext()));
			//	Inject Camel Router that will delegate messages to JobDriver delegate listener
			jdc.getContext().addRoutes(this.routeBuilderForIncomingRequests(common.orchestratorAbbreviatedStateUpdateEndpoint, delegateListener));
			
			int port = Utils.findFreePort();
			String jdUniqueId = "jdApp";
			jdc.getContext().addRoutes(this.routeBuilderForJpIncomingRequests(jdc, port, jdUniqueId));
			logger.debug(location, jobid, "endpoint: "+common.jdStateUpdateEndpoint+" "+"rate: "+common.jdStatePublishRate);
			jdc.getContext().addRoutes(this.routeBuilderForJdStatePost(jdc, common.jdStateUpdateEndpoint, Integer.parseInt(common.jdStatePublishRate)));
			return jdc;
		}

}
