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
package org.apache.uima.ducc.transport.configuration.jp;

import java.net.InetAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Predicate;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.uima.ducc.common.config.CommonConfiguration;
import org.apache.uima.ducc.common.utils.Utils;
import org.apache.uima.ducc.container.jp.JobProcessManager;
import org.apache.uima.ducc.container.jp.UimaProcessor;
import org.apache.uima.ducc.transport.DuccExchange;
import org.apache.uima.ducc.transport.DuccTransportConfiguration;
import org.apache.uima.ducc.transport.agent.ProcessStateUpdate;
import org.apache.uima.ducc.transport.dispatcher.DuccEventDispatcher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({ DuccTransportConfiguration.class, CommonConfiguration.class })
public class JobProcessConfiguration  {
	@Autowired
	DuccTransportConfiguration transport;
	@Autowired
	CommonConfiguration common;
	JobProcessComponent duccComponent = null;
	JobProcessManager jobProcessManager = null;
	AgentSession agent = null;
	//protected ProcessState currentState = ProcessState.Undefined;
	//protected ProcessState previousState = ProcessState.Undefined;
	RouteBuilder routeBuilder;
	CamelContext camelContext;

	/**
	 * Creates Camel Router to handle incoming messages
	 * 
	 * @param delegate
	 *            - {@code AgentEventListener} to delegate messages to
	 * 
	 * @return {@code RouteBuilder} instance
	 */
	public synchronized RouteBuilder routeBuilderForIncomingRequests(
			final String thisNodeIP, final JobProcessEventListener delegate) {
		return new RouteBuilder() {
			// Custom filter to select messages that are targeted for this
			// process. Checks the Node IP in a message to determine if 
			// this process is the target.
			Predicate filter = new DuccProcessFilter(thisNodeIP);

			public void configure() throws Exception {
				System.out
						.println("Service Wrapper Starting Request Channel on Endpoint:"
								+ common.managedServiceEndpoint);
				onException(Exception.class).handled(true)
						.process(new ErrorProcessor()).end();

				from(common.managedServiceEndpoint)

				.choice().when(filter).bean(delegate).end()
						.setId(common.managedServiceEndpoint);

			}
		};
	}

	public class ErrorProcessor implements Processor {

		public void process(Exchange exchange) throws Exception {
			// the caused by exception is stored in a property on the exchange
			Throwable caused = exchange.getProperty(Exchange.EXCEPTION_CAUGHT,
					Throwable.class);
			caused.printStackTrace();
			// System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!1");
			// assertNotNull(caused);
			// here you can do what you want, but Camel regard this exception as
			// handled, and
			// this processor as a failurehandler, so it wont do redeliveries.
			// So this is the
			// end of this route. But if we want to route it somewhere we can
			// just get a
			// producer template and send it.

			// send it to our mock endpoint
			// exchange.getContext().createProducerTemplate().send("mock:myerror",
			// exchange);
		}
	}

	
	@Bean
	public JobProcessComponent getProcessManagerInstance() throws Exception {
		try {
			// Assume IP address provided from environment. In production this
			// will be the actual node IP. In testing, the IP can be virtual
			// when running multiple agents on the same node. The agent is
			// responsible for providing the IP in this process environment.
			String thisNodeIP = (System.getenv("IP") == null) ? InetAddress
					.getLocalHost().getHostAddress() : System.getenv("IP");
			camelContext = common.camelContext();
			int serviceSocketPort = 0;
			String agentSocketParams = "";
			String jpSocketParams = "";
			if (common.managedServiceEndpointParams != null) {
				jpSocketParams = "?" + common.managedServiceEndpointParams;
			}

			if (common.managedProcessStateUpdateEndpointParams != null) {
				agentSocketParams = "?"
						+ common.managedProcessStateUpdateEndpointParams;
			}
			// set up agent socket endpoint where this UIMA AS service will send
			// state updates
			if (common.managedProcessStateUpdateEndpointType != null
					&& common.managedProcessStateUpdateEndpointType
							.equalsIgnoreCase("socket")) {
				common.managedProcessStateUpdateEndpoint = "mina:tcp://localhost:"
						+ System.getProperty(ProcessStateUpdate.ProcessStateUpdatePort)
						+ agentSocketParams;
			}
			// set up a socket endpoint where the UIMA AS service will receive
			// events sent from its agent
			if (common.managedServiceEndpointType != null
					&& common.managedServiceEndpointType
							.equalsIgnoreCase("socket")) {
				serviceSocketPort = Utils.findFreePort();
				// service is on the same node as the agent
				common.managedServiceEndpoint = "mina:tcp://localhost:"
						+ serviceSocketPort + jpSocketParams;
			}

			DuccEventDispatcher eventDispatcher = transport
					.duccEventDispatcher(
							common.managedProcessStateUpdateEndpoint,
							camelContext);

			// Create Agent proxy which will be used to notify Agent
			// of state changes.
			agent = new AgentSession(eventDispatcher,
					System.getenv("ProcessDuccId"), common.managedServiceEndpoint);
			
			System.out
					.println("#######################################################");
			System.out.println("## Agent Service State Update Endpoint:"
					+ common.managedProcessStateUpdateEndpoint + " ##");
			System.out
					.println("#######################################################");
			jobProcessManager = new JobProcessManager();
			duccComponent = 
					new JobProcessComponent("UimaProcess", camelContext, this);
			duccComponent.setAgentSession(agent);
			duccComponent.setJobProcessManager(jobProcessManager);
			duccComponent.setSaxonJarPath(common.saxonJarPath);
			duccComponent.setDd2SpringXslPath(common.dd2SpringXslPath);
			duccComponent.setTimeout(10000);  //common.jpTimeout);
			
			
			JobProcessEventListener eventListener = 
					new JobProcessEventListener(duccComponent);
			routeBuilder = this.routeBuilderForIncomingRequests(thisNodeIP, eventListener);

			camelContext.addRoutes(routeBuilder);

			return duccComponent;

		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
	}

	private class DuccProcessFilter implements Predicate {
		String thisNodeIP;

		public DuccProcessFilter(final String thisNodeIP) {
			this.thisNodeIP = thisNodeIP;
		}

		public synchronized boolean matches(Exchange exchange) {
			// String methodName="DuccProcessFilter.matches";
			boolean result = false;
			try {
				String pid = (String) exchange.getIn().getHeader(
						DuccExchange.ProcessPID);
				String targetIP = (String) exchange.getIn().getHeader(
						DuccExchange.DUCCNODEIP);
				// check if this message is targeting this process. Check if the
				// process PID
				// and the node match target process.
				if (Utils.getPID().equals(pid) && thisNodeIP.equals(targetIP)) { 
					result = true;
					System.out
							.println(">>>>>>>>> Process Received a Message. Is Process target for message:"
									+ result + ". Target PID:" + pid);
				}
			} catch (Throwable e) {
				e.printStackTrace();
			}
			return result;
		}
	}
}
