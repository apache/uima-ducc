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

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Predicate;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.uima.ducc.common.config.CommonConfiguration;
import org.apache.uima.ducc.common.utils.Utils;
import org.apache.uima.ducc.container.jp.JobProcessManager;
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

//			ManagedUimaService service = 
//		        	new ManagedUimaService(common.saxonJarPath,
//		        			common.dd2SpringXslPath, 
//		        			serviceAdapter(eventDispatcher,common.managedServiceEndpoint), camelContext);
			
//			service.setConfigFactory(this);
//		    service.setAgentStateUpdateEndpoint(common.managedProcessStateUpdateEndpoint);
            
			// Create an Agent proxy. This is used to notify the Agent
			// of state changes.
			agent = new AgentSession(eventDispatcher,
					System.getenv("ProcessDuccId"), common.managedServiceEndpoint);

			
			System.out
					.println("#######################################################");
			System.out.println("## Agent Service State Update Endpoint:"
					+ common.managedProcessStateUpdateEndpoint + " ##");
			System.out
					.println("#######################################################");

//			JobProcessEventListener delegateListener = processDelegateListener(jobProcessManager);
//			delegateListener.setDuccEventDispatcher(eventDispatcher);
			
			jobProcessManager = new JobProcessManager();
			// Create Lifecycle manager responsible for handling start event
			// initiated by the Ducc framework. It will eventually call the
			// start(String[] args) method on JobProcessConfiguration object
			// which kicks off initialization of UIMA pipeline and processing
			// begins.
			duccComponent = 
					new JobProcessComponent("UimaProcess", camelContext, this);
			duccComponent.setAgentSession(agent);
			duccComponent.setJobProcessManager(jobProcessManager);
			
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
/*
	public void start(String[] args) {
		try {
			String jps = System.getProperty("org.apache.uima.ducc.userjarpath");
			if (null == jps) {
				System.err
						.println("Missing the -Dorg.apache.uima.jarpath=XXXX property");
				System.exit(1);
			}
			String processJmxUrl = duccComponent.getProcessJmxUrl();
			agent.notify(ProcessState.Initializing, processJmxUrl);
			IUimaProcessor uimaProcessor = null; 
			ScheduledThreadPoolExecutor executor = null;
			
			try {
				executor = new ScheduledThreadPoolExecutor(1);
				executor.prestartAllCoreThreads();
				// Instantiate a UIMA AS jmx monitor to poll for status of the AE.
				// This monitor checks if the AE is initializing or ready.
				JmxAEProcessInitMonitor monitor = new JmxAEProcessInitMonitor(agent);
				executor.scheduleAtFixedRate(monitor, 20, 30, TimeUnit.SECONDS);

		    	// Deploy UIMA pipelines. This blocks until the pipelines initializes or
		    	// there is an exception. The IUimaProcessor is a wrapper around
		    	// processing container where the analysis is being done.
		    	uimaProcessor =
		    			jobProcessManager.deploy(jps, args, "org.apache.uima.ducc.user.jp.UserProcessContainer");
				
		    	// pipelines deployed and initialized. This is process is Ready
		    	// for processing
		    	currentState = ProcessState.Running;
				// Update agent with the most up-to-date state of the pipeline
			//	monitor.run();
				// all is well, so notify agent that this process is in Running state
				agent.notify(currentState, processJmxUrl);
                // Create thread pool and begin processing
				
				
				
		    } catch( Exception ee) {
		    	currentState = ProcessState.FailedInitialization;
				System.out
						.println(">>> Failed to Deploy UIMA Service. Check UIMA Log for Details");
				agent.notify(ProcessState.FailedInitialization);
		    } finally {
				// Stop executor. It was only needed to poll AE initialization status.
				// Since deploy() completed
				// the UIMA AS service either succeeded initializing or it failed. In
				// either case we no longer
				// need to poll for initialization status
		    	if ( executor != null ) {
			    	executor.shutdownNow();
		    	}
		    	
		    }
			


		} catch( Exception e) {
			currentState = ProcessState.FailedInitialization;
			agent.notify(currentState);

			
		}
	}
	*/

/*
	public void stop() {
        try {
        	//agent.stop();
        	
        	if (camelContext != null) {
    			for (Route route : camelContext.getRoutes()) {

    				route.getConsumer().stop();
    				System.out.println(">>> configFactory.stop() - stopped route:"
    						+ route.getId());
    			}
    		}
		} catch( Exception e) {
			
		}
		
		
	}
*/
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
