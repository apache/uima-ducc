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
package org.apache.uima.ducc.transport.configuration.service;

import java.net.InetAddress;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Predicate;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.uima.ducc.common.IDuccUser;
import org.apache.uima.ducc.common.config.CommonConfiguration;
import org.apache.uima.ducc.common.container.FlagsHelper;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.Utils;
import org.apache.uima.ducc.transport.DuccExchange;
import org.apache.uima.ducc.transport.DuccTransportConfiguration;
import org.apache.uima.ducc.transport.configuration.jp.AgentSession;
import org.apache.uima.ducc.transport.dispatcher.DuccEventDispatcher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({ DuccTransportConfiguration.class, CommonConfiguration.class })
public class ServiceConfiguration {
	public static final String AGENT_ENDPOINT = "mina:tcp://localhost:";
	protected static DuccLogger logger =
			new DuccLogger(ServiceConfiguration.class);

	@Autowired
	DuccTransportConfiguration transport;
	@Autowired
	CommonConfiguration common;
	ServiceComponent duccComponent = null;
	CamelContext camelContext;
	AgentSession agent = null;
	RouteBuilder routeBuilder;
	/**
	 * Creates Camel Router to handle incoming messages
	 *
	 * @param delegate
	 *            - {@code AgentEventListener} to delegate messages to
	 *
	 * @return {@code RouteBuilder} instance
	 */
	public synchronized RouteBuilder routeBuilderForIncomingRequests(
			final String thisNodeIP, final ServiceEventListener delegate) {
		return new RouteBuilder() {
			// Custom filter to select messages that are targeted for this
			// process. Checks the Node IP in a message to determine if
			// this process is the target.
			Predicate filter = new DuccProcessFilter(thisNodeIP);

			public void configure() throws Exception {
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
			logger.error("process", null, caused);
		}
	}


	private void checkPrereqs() {

	}

	public String getUserContainerClassForJob(String key) {
		if (key.equals("uima-as")) {
			if (common.uimaASProcessContainerClass == null) {
				// use default
				return "org.apache.uima.ducc.user.jp.UimaASProcessContainer";
			} else {
				return common.uimaASProcessContainerClass;
			}
		} else {
			if (common.uimaProcessContainerClass == null) {
				// use default
				return "org.apache.uima.ducc.user.jp.UimaProcessContainer";
			} else {
				return common.uimaProcessContainerClass;
			}
		}
	}

	@Bean
	public ServiceComponent getServiceInstance() throws Exception {
		try {
			checkPrereqs();
		} catch(Exception e) {
			throw e;
		}
		try {

			// Assume IP address provided from environment. In production this
			// will be the actual node IP. In testing, the IP can be virtual
			// when running multiple agents on the same node. The agent is
			// responsible for providing the IP in this process environment.
			String thisNodeIP = (System.getenv(IDuccUser.EnvironmentVariable.DUCC_IP.value()) == null) ? InetAddress
					.getLocalHost().getHostAddress() : System.getenv(IDuccUser.EnvironmentVariable.DUCC_IP.value());
			camelContext = common.camelContext();

			// custom processor class can be provided in the command line.
			// Its not required. If not present, this code will assign one
			// based on jobType
			if ( System.getProperty(FlagsHelper.Name.JpProcessorClass.pname() ) == null  ) {
				String containerClass = "org.apache.uima.ducc.user.service.UimaASServiceContainer";
				// Save the container class. This will be referenced from the
				// DuccJobService.initialize()
				System.setProperty(FlagsHelper.Name.JpProcessorClass.pname(),//"ducc.deploy.processor.class",
						containerClass);
			}

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
			boolean disableAgentUpdates = false;
			// set up agent socket endpoint where this UIMA AS service will send
			// state updates
			if (common.managedProcessStateUpdateEndpointType != null
					&& common.managedProcessStateUpdateEndpointType
							.equalsIgnoreCase("socket")) {
			  String updatePort = System.getenv(IDuccUser.EnvironmentVariable.DUCC_UPDATE_PORT.value());
			  if ( updatePort == null || updatePort.trim().isEmpty()) {
				  disableAgentUpdates = true;
			  }
			  common.managedProcessStateUpdateEndpoint = AGENT_ENDPOINT + updatePort + agentSocketParams;
			}
			// set up a socket endpoint where the UIMA AS service will receive
			// events sent from its agent
			if (common.managedServiceEndpointType != null
					&& common.managedServiceEndpointType
							.equalsIgnoreCase("socket")) {
				serviceSocketPort = Utils.findFreePort();
				// service is on the same node as the agent
				common.managedServiceEndpoint = AGENT_ENDPOINT
						+ serviceSocketPort + jpSocketParams;
			}

			DuccEventDispatcher eventDispatcher = transport
					.duccEventDispatcher(
							common.managedProcessStateUpdateEndpoint,
							camelContext);

			// Create Agent proxy which will be used to notify Agent
			// of state changes.
			String processId = 
					System.getProperty(IDuccUser.DashD.DUCC_ID_PROCESS_UNIQUE.value());
			if ( processId == null) {
				processId = 
						System.getenv(IDuccUser.EnvironmentVariable.DUCC_PROCESS_UNIQUEID.value());
			}
			agent = new AgentSession(eventDispatcher,
					processId,
					common.managedServiceEndpoint);
			if ( disableAgentUpdates ) {
				agent.disable(disableAgentUpdates);
			}

			duccComponent = new ServiceComponent("UimaProcess",
					camelContext, this);

			duccComponent.setAgentSession(agent);
			duccComponent.setSaxonJarPath(common.saxonJarPath);
			duccComponent.setDd2SpringXslPath(common.dd2SpringXslPath);

			ServiceEventListener eventListener =
					new ServiceEventListener(duccComponent);

			routeBuilder = this.routeBuilderForIncomingRequests(thisNodeIP,
					eventListener);

			camelContext.addRoutes(routeBuilder);

			return duccComponent;

		} catch (Exception e) {
			logger.error("getServiceInstance",null,e);
		    throw e;
		}
	}
	private class DuccProcessFilter implements Predicate {
		String thisNodeIP;

		public DuccProcessFilter(final String thisNodeIP) {
			this.thisNodeIP = thisNodeIP;
		}

		public synchronized boolean matches(Exchange exchange) {
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
				}
			} catch (Throwable e) {
				logger.error("matches",null,e);
			}
			return result;
		}
	}
}
