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

import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.apache.uima.ducc.common.IDuccUser;
import org.apache.uima.ducc.common.config.CommonConfiguration;
import org.apache.uima.ducc.common.container.FlagsHelper;
import org.apache.uima.ducc.common.utils.Utils;
import org.apache.uima.ducc.container.jp.JobProcessManager;
import org.apache.uima.ducc.transport.DuccTransportConfiguration;
import org.apache.uima.ducc.transport.dispatcher.DuccEventDispatcher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({ DuccTransportConfiguration.class, CommonConfiguration.class })
public class JobProcessConfiguration {
	public static final String AGENT_ENDPOINT = "mina:tcp://localhost:";
	@Autowired
	DuccTransportConfiguration transport;
	@Autowired
	CommonConfiguration common;
	JobProcessComponent duccComponent = null;
	JobProcessManager jobProcessManager = null;
	AgentSession agent = null;
	RouteBuilder routeBuilder;
	CamelContext camelContext;


	private void checkPrereqs() {
		boolean uimaAsJob = false;

		if (null == System.getProperty(FlagsHelper.Name.JpType.pname())) { // "Ducc.Job.Type")
			throw new RuntimeException("Missing Job Type. Add -D"
					+ FlagsHelper.Name.JpType.pname() + "=uima-as or "
					+ FlagsHelper.Name.JpType.pname()
					+ "=uima. Check your command line");
		} else {
			String jobType = System
					.getProperty(FlagsHelper.Name.JpType.pname());
			if (jobType.trim().equals("uima-as")) {
				uimaAsJob = true;
			} else if (!jobType.trim().equals("uima") && !jobType.trim().equals("custom")) {
				throw new RuntimeException("Invalid value for -D"
						+ FlagsHelper.Name.JpType.pname()
						+ ". Expected uima-as,uima or custom, Instead it is "
						+ jobType);
			}
		}

		if (null == System.getProperty(FlagsHelper.Name.DuccClasspath.pname())) {
			throw new RuntimeException("Missing the -D"
					+ FlagsHelper.Name.DuccClasspath.pname() + "=XXXX property");
		}
		if (uimaAsJob && null == common.saxonJarPath) {
			throw new RuntimeException(
					"Missing saxon jar path. Check your ducc.properties");
		}
		if (uimaAsJob && null == common.dd2SpringXslPath) {
			throw new RuntimeException(
					"Missing dd2spring xsl path. Check your ducc.properties");
		}
		if (null == System.getProperty(FlagsHelper.Name.JdURL.pname())) {
			throw new RuntimeException("Missing the -D"
					+ FlagsHelper.Name.JdURL.pname() + " property");
		}

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
	public JobProcessComponent getProcessManagerInstance() throws Exception {
		try {
			checkPrereqs();
		} catch(Exception e) {
			throw e;
		}
		try {
			camelContext = common.camelContext();

			// currently supported jobType values:
			// uima-as, DD jobs
			// uima, pieces parts
			String jobType = System
					.getProperty(FlagsHelper.Name.JpType.pname());

			// custom processor class can be provided in the command line.
			// Its not required. If not present, this code will assign one
			// based on jobType
			if ( System.getProperty(FlagsHelper.Name.JpProcessorClass.pname() ) == null  ) { //"ducc.deploy.processor.class") == null ) {
				String containerClass = getUserContainerClassForJob(jobType);
				// Save the container class. This will be referenced from the
				// DuccJobService.initialize()
				System.setProperty(FlagsHelper.Name.JpProcessorClass.pname(),//"ducc.deploy.processor.class",
						containerClass);
			}

			duccComponent = new JobProcessComponent("UimaProcess",
					camelContext, this);

			// check if required configuration is provided. This method throws
			// Exceptions if
			// there is something missing.
			// checkPrereqs(duccComponent.getLogger());

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
			// If not is system properties try environment
			if (common.managedProcessStateUpdateEndpointType != null
					&& common.managedProcessStateUpdateEndpointType
							.equalsIgnoreCase("socket")) {
			  String updatePort = System.getenv(IDuccUser.EnvironmentVariable.DUCC_UPDATE_PORT.value());
			  // if agent did not launch this process, the DUCC_UPDATE_PORT will not be present
			  // In such case, don't send updates
			  if ( updatePort == null || updatePort.trim().isEmpty()) {
				  disableAgentUpdates = true;
			  }
				common.managedProcessStateUpdateEndpoint = AGENT_ENDPOINT	+ updatePort + agentSocketParams;
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
			if ( common.jpFrameworkErrorLimit != null ) {
				int limit = Integer.parseInt(common.jpFrameworkErrorLimit);
				duccComponent.setMaxFrameworkFailures(limit);
			} else {
				duccComponent.setMaxFrameworkFailures(2);
			}
			DuccEventDispatcher eventDispatcher = transport
					.duccEventDispatcher(
							common.managedProcessStateUpdateEndpoint,
							camelContext);
			String processId = 
					System.getProperty(IDuccUser.DashD.DUCC_ID_PROCESS_UNIQUE.value());
			if ( processId == null) {
				processId = 
						System.getenv(IDuccUser.EnvironmentVariable.DUCC_PROCESS_UNIQUEID.value());
			}
			// Create Agent proxy which will be used to notify Agent
			// of state changes.
			agent = new AgentSession(eventDispatcher,
					processId,
					common.managedServiceEndpoint);
			if ( disableAgentUpdates ) {
				agent.disable(disableAgentUpdates);
			} else {
				duccComponent.getLogger().info("getProcessManagerInstance", null,"#######################################################");
				duccComponent.getLogger().info("getProcessManagerInstance", null,"## Agent Service State Update Endpoint:"
						+ common.managedProcessStateUpdateEndpoint + " ##");
				duccComponent.getLogger().info("getProcessManagerInstance", null,"#######################################################");
			}
			duccComponent.setAgentSession(agent);
			duccComponent.setSaxonJarPath(common.saxonJarPath);
			duccComponent.setDd2SpringXslPath(common.dd2SpringXslPath);
			if ( common.processThreadSleepTime != null ) {
			  duccComponent.setThreadSleepTime(Integer.parseInt(common.processThreadSleepTime));
			  duccComponent.getLogger().info("getProcessManagerInstance", null,
			      "Overriding Default Thread Sleep Time - New Value "+common.processThreadSleepTime+" ms");
			}
			if ( common.processRequestTimeout != null ) {
	          duccComponent.setTimeout(Integer.valueOf(common.processRequestTimeout));
			  duccComponent.getLogger().info("getProcessManagerInstance", null,
			     "Overriding Default Process Request Timeout - New Timeout "+common.processRequestTimeout+" ms");
			}

			return duccComponent;

		} catch (Exception e) {
			throw e;
		}
	}

}
