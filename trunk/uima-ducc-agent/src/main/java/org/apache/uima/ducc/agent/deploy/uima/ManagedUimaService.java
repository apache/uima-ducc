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
package org.apache.uima.ducc.agent.deploy.uima;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.management.InstanceNotFoundException;
import javax.management.JMX;
import javax.management.MBeanServer;
import javax.management.ObjectInstance;
import javax.management.ObjectName;

import org.apache.camel.CamelContext;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.uima.aae.UimaASApplicationEvent.EventTrigger;
import org.apache.uima.aae.UimaASApplicationExitEvent;
import org.apache.uima.aae.controller.AnalysisEngineController;
import org.apache.uima.adapter.jms.activemq.SpringContainerDeployer;
import org.apache.uima.adapter.jms.service.UIMA_Service;
import org.apache.uima.analysis_engine.AnalysisEngineManagement;
import org.apache.uima.ducc.agent.deploy.AbstractManagedService;
import org.apache.uima.ducc.agent.deploy.ServiceStateNotificationAdapter;
import org.apache.uima.ducc.common.main.DuccService;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.Utils;
import org.apache.uima.ducc.common.utils.XStreamUtils;
import org.apache.uima.ducc.transport.agent.IUimaPipelineAEComponent;
import org.apache.uima.ducc.transport.agent.UimaPipelineAEComponent;
import org.apache.uima.ducc.transport.event.common.DuccUimaDeploymentDescriptor;
import org.apache.uima.ducc.transport.event.common.IProcessState.ProcessState;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;

/**
 * Service wrapper for UIMA AS service. Deploys UIMA AS using Spring deployer
 * component. Reports UIMA AS state to an agent using
 * {@code ServiceStateNotificationAdapter}.
 * 
 */
public class ManagedUimaService extends AbstractManagedService implements
		ApplicationListener<ApplicationEvent> {

	private SpringContainerDeployer serviceDeployer;
	private String saxonJarPath;
	private String dd2SpringXslPath;
	private String processJmxUrl = null;
	protected static DuccLogger logger;
	private String agentStateUpdateEndpoint = "";
    private UimaAsServiceConfiguration  configFactory;
    
	public static void main(String[] args) {
		try {
			ManagedUimaService ms = new ManagedUimaService(
					"${DUCC_HOME}/lib/saxon8/saxon8.jar",
					"${DUCC_HOME}/bin/dd2spring.xsl", null,
					new DefaultCamelContext());
			ms.deploy(new String[] { XStreamUtils
					.marshall(new DuccUimaDeploymentDescriptor(args[0])) });
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public ManagedUimaService(String saxonJarPath, String dd2SpringXslPath,
			ServiceStateNotificationAdapter serviceAdapter, CamelContext context) {
		super(serviceAdapter, context);
		this.saxonJarPath = saxonJarPath;
		this.dd2SpringXslPath = dd2SpringXslPath;
		// Fetch uima logger and inject UIMALogFormatter to show thread ids
		// Logger l = java.util.logging.Logger.getLogger("org.apache.uima");
		// ConsoleHandler ch = new ConsoleHandler();
		// ch.setFormatter(new UIMALogFormatter());
		// l.addHandler(ch);
	}

	public DuccLogger getLogger()
	{
		return new DuccLogger(DuccService.class);
	}
	
	public void onServiceStateChange(ProcessState state) {
		super.notifyAgentWithStatus(state);
	}
    public void setConfigFactory(UimaAsServiceConfiguration  configFactory) {
    	this.configFactory = configFactory;
    }
	public void setAgentStateUpdateEndpoint(String agentUpdateEndpoint) {
		this.agentStateUpdateEndpoint = agentUpdateEndpoint;
	}

	public void quiesceAndStop() {
		try {
			if ( configFactory != null ) {
				configFactory.stop();  // stop Camel Routes
			}
			if (serviceDeployer != null) {
				AnalysisEngineController topLevelController = serviceDeployer
						.getTopLevelController();
				if (topLevelController != null
						&& !topLevelController.isStopped()) {
					serviceDeployer
							.undeploy(SpringContainerDeployer.QUIESCE_AND_STOP);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public void terminate() {
		currentState = ProcessState.Stopped;
		System.out.println("Service STOPPED");
		try {
			if ( configFactory != null ) {
				configFactory.stop();  // stop Camel Routes
			}

			super.notifyAgentWithStatus(currentState);
			if (serviceDeployer != null) {
				// Use top level controller to stop all components
				serviceDeployer.getTopLevelController().stop();
			}
			stopIt();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void killService() {
		logger.info("killService", null,
				"Ducc UIMA Service process received STOP event. Stopping UIMA AS ...");
		if (serviceDeployer != null) {
			// Use top level controller to stop all components. This method
			// doesnt wait
			// for inflight CASes to be processed
			serviceDeployer.getTopLevelController().stop();
		}
		logger.info("killService", null,
				"Ducc UIMA Service process stopped UIMA AS and exiting via System.exit()");
		System.exit(-1);
	}

	public void stopService() {
		quiesceAndStop();
		currentState = ProcessState.Stopped;
		System.exit(0);
	}

	/**
	 * Returns UIMA AS service arguments: saxonURL, xslt parser
	 * 
	 * @param args
	 *            - commandline args
	 * @throws Exception
	 */
	public String[] getServiceArgs(String[] args) throws Exception {
		String ddPath = args[0];
		ddPath = Utils.resolvePlaceholderIfExists(ddPath,
				System.getProperties());
		return new String[] {
				"-saxonURL",
				Utils.resolvePlaceholderIfExists(saxonJarPath,
						System.getProperties()),
				"-xslt",
				Utils.resolvePlaceholderIfExists(dd2SpringXslPath,
						System.getProperties()), "-dd", ddPath };
	}

	/*
	 * private void setupLogging() throws Exception { Properties props = new
	 * Properties(); try { InputStream configStream =
	 * getClass().getResourceAsStream ("Logger.properties");
	 * props.load(configStream); configStream.close(); } catch(IOException e) {
	 * System.out.println("Error"); }
	 * //props.setProperty("log4j.rootLogger","INFO, stdout");
	 * Enumeration<Logger> en = LogManager.getCurrentLoggers(); while
	 * (en.hasMoreElements()) {
	 * System.out.println("Logger Appender Class:"+en.nextElement().getName());
	 * } LogManager.resetConfiguration(); PropertyConfigurator.configure(props);
	 * }
	 */
	/**
	 * deploys UIMA AS service
	 */
	public void deploy(String[] args) throws Exception {
		// Instrument this process with JMX Agent. The Agent will
		// find an open port and start JMX Connector allowing
		// jmx clients to connect to this jvm using standard
		// jmx connect url. This process does not require typical
		// -D<jmx params> properties. Currently the JMX does not
		// use security allowing all clients to connect.
		processJmxUrl = super.getProcessJmxUrl();
		System.out.println("Connect jConsole to this process using JMX URL:"
				+ processJmxUrl);

		UIMA_Service service = new UIMA_Service();

		StringBuffer sb = new StringBuffer("Deploying UIMA AS with args:\n");

		for (String arg : args) {
			sb.append(arg + "\n");
		}
		System.out.println(sb.toString());
		String[] serviceArgs = getServiceArgs(args);

		sb.setLength(0);
		sb.append("Service Args:\n");
		for (String arg : serviceArgs) {
			sb.append(" " + arg);
		}
		System.out.println(sb.toString());

		System.out.println("ManagedUimaService initializing...");

		// parse command args and run dd2spring to generate spring context
		// files from deployment descriptors
		String[] contextFiles = service.initialize(serviceArgs);
		if (contextFiles == null) {
			throw new Exception(
					"Spring Context Files Not Generated. Unable to Launch Uima AS Service");
		}
		// Make sure that the dd2spring generated file exists
		File generatedFile = new File(contextFiles[0]);
		while (!generatedFile.exists()) {
			synchronized (generatedFile) {
				generatedFile.wait(500);
			}
		}
		System.out
				.println("ManagedUimaService initialized - ready to process. Agent State Update endpoint:"
						+ agentStateUpdateEndpoint);
		System.out
				.println(".... Verified dd2spring generated spring context file:"
						+ contextFiles[0]);
		// Let the Agent know that the service is entering Initialization
		// state. This is an initial state of a service, covering
		// process bootstrapping(startup) and initialization of UIMA
		// components.
		super.notifyAgentWithStatus(ProcessState.Initializing, processJmxUrl);

		ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(
				1);
		executor.prestartAllCoreThreads();
		// Instantiate a UIMA AS jmx monitor to poll for status of the AE.
		// This monitor checks if the AE is initializing or ready.
		UimaAEJmxMonitor monitor = new UimaAEJmxMonitor(this, serviceArgs);
		/*
		 * This will execute the UimaAEJmxMonitor continuously for every 15
		 * seconds with an initial delay of 20 seconds. This monitor polls
		 * initialization status of AE deployed in UIMA AS.
		 */
		executor.scheduleAtFixedRate(monitor, 20, 30, TimeUnit.SECONDS);
		// Future<Integer> future = executor.submit(callable);

		// Deploy components defined in Spring context files.
		// !!!! NOTE:This method blocks until the container is fully
		// initialized and all UIMA-AS components are successfully deployed
		// or there is a failure.
		try {
			serviceDeployer = service.deploy(contextFiles, this);
		} catch (Throwable t) {
			t.printStackTrace();
		}
		// Stop executor. It was only needed to poll AE initialization status.
		// Since deploy() completed
		// the UIMA AS service either succeeded initializing or it failed. In
		// either case we no longer
		// need to poll for initialization status
		executor.shutdownNow();

		if (serviceDeployer == null || serviceDeployer.initializationFailed()) {
			currentState = ProcessState.FailedInitialization;
			System.out
					.println(">>> Failed to Deploy UIMA Service. Check UIMA Log for Details");
			super.notifyAgentWithStatus(ProcessState.FailedInitialization);
		} else {
			currentState = ProcessState.Running;
			// Update agent with the most up-to-date state of the pipeline
			monitor.run();
			super.notifyAgentWithStatus(currentState, processJmxUrl);
		}

	}

	public void updateAgent(List<IUimaPipelineAEComponent> pipeline) {
		super.notifyAgentWithStatus(pipeline);
	}

	public static class UimaAEJmxMonitor implements Runnable {
		MBeanServer server = null;
		ManagedUimaService service;
		static int howManySeenSoFar = 1;
		public List<IUimaPipelineAEComponent> aeStateList = new ArrayList<IUimaPipelineAEComponent>();

		public UimaAEJmxMonitor(ManagedUimaService service, String[] serviceArgs)
				throws Exception {
			server = ManagementFactory.getPlatformMBeanServer();
			this.service = service;
		}

		private IUimaPipelineAEComponent getUimaAeByName(String name) {
			for (IUimaPipelineAEComponent aeState : aeStateList) {
				if (aeState.getAeName().equals(name)) {
					return aeState;
				}
			}
			return null;
		}

		public void run() {
			try {
				// create an ObjectName with UIMA As JMS naming convention to
				// enable
				// finding deployed uima components.
				ObjectName uimaServicePattern = new ObjectName(
						"org.apache.uima:type=ee.jms.services,*");
				// Fetch UIMA AS MBean names from JMX Server that match above
				// name pattern
				Set<ObjectInstance> mbeans = new HashSet<ObjectInstance>(
						server.queryMBeans(uimaServicePattern, null));
				List<IUimaPipelineAEComponent> componentsToDelete = new ArrayList<IUimaPipelineAEComponent>();
				boolean updateAgent = false;
				for (ObjectInstance instance : mbeans) {
					String targetName = instance.getObjectName()
							.getKeyProperty("name");
					if (targetName.endsWith("FlowController")) { // skip FC
						continue;
					}
					// Only interested in AEs
					if (instance
							.getClassName()
							.equals("org.apache.uima.analysis_engine.impl.AnalysisEngineManagementImpl")) {
						String[] aeObjectNameParts = instance.getObjectName()
								.toString().split(",");
						if (aeObjectNameParts.length == 3) {
							// this is uima aggregate MBean. Skip it. We only
							// care about this
							// aggregate's pipeline components.
							continue;
						}
						StringBuffer sb = new StringBuffer();
						// int partCount = 0;
						// compose component name from jmx ObjectName
						for (String part : aeObjectNameParts) {
							// partCount++;
							if (part.startsWith("org.apache.uima:type")
									|| part.startsWith("s=")) {
								continue; // skip service name part of the name
							} else {
								sb.append("/");
								if (part.endsWith("Components")) {
									part = part.substring(0,
											part.indexOf("Components")).trim();
								}
								sb.append(part.substring(part.indexOf("=") + 1));
							}
						}
						// Fetch a proxy to the AE Management object which holds
						// AE stats
						AnalysisEngineManagement proxy = JMX.newMBeanProxy(
								server, instance.getObjectName(),
								AnalysisEngineManagement.class);

						IUimaPipelineAEComponent aeState = null;
						// if ((aeState = getUimaAeByName(aeStateList,
						// sb.toString())) == null) {
						if ((aeState = getUimaAeByName(sb.toString())) == null) {
							// Not interested in AEs that are in a Ready State
							if (AnalysisEngineManagement.State.valueOf(
									proxy.getState()).equals(
									AnalysisEngineManagement.State.Ready)) {
								continue;
							}
							aeState = new UimaPipelineAEComponent(
									sb.toString(), proxy.getThreadId(),
									AnalysisEngineManagement.State
											.valueOf(proxy.getState()));
							aeStateList.add(aeState);
							((UimaPipelineAEComponent) aeState).startInitialization = System
									.currentTimeMillis();
							aeState.setAeState(AnalysisEngineManagement.State.Initializing);
							updateAgent = true;
						} else {
							// continue publishing AE state while the AE is
							// initializing
							if (AnalysisEngineManagement.State
									.valueOf(proxy.getState())
									.equals(AnalysisEngineManagement.State.Initializing)) {
								updateAgent = true;
								aeState.setInitializationTime(System
										.currentTimeMillis()
										- ((UimaPipelineAEComponent) aeState).startInitialization);
								// publish state if the AE just finished
								// initializing and is now in Ready state
							} else if (aeState
									.getAeState()
									.equals(AnalysisEngineManagement.State.Initializing)
									&& AnalysisEngineManagement.State
											.valueOf(proxy.getState())
											.equals(AnalysisEngineManagement.State.Ready)) {
								aeState.setAeState(AnalysisEngineManagement.State.Ready);
								updateAgent = true;
								synchronized (this) {
									try {
										wait(5);
									} catch (InterruptedException ex) {
									}
								}
								aeState.setInitializationTime(proxy
										.getInitializationTime());
								// AE reached ready state we no longer need to
								// publish its state
								componentsToDelete.add(aeState);
							}
						}
						DuccService.getDuccLogger(this.getClass().getName()).debug(
								"UimaAEJmxMonitor.run()",
								null,
								"---- AE Name:" + proxy.getName()
										+ " AE State:" + proxy.getState()
										+ " AE init time="
										+ aeState.getInitializationTime()
										+ " Proxy Init time="
										+ proxy.getInitializationTime()
										+ " Proxy Thread ID:"
										+ proxy.getThreadId());
					}
				}
				howManySeenSoFar = 1; // reset error counter
				if (updateAgent) {
					DuccService.getDuccLogger(this.getClass().getName()).debug("UimaAEJmxMonitor.run()", null,
							"---- Publishing UimaPipelineAEComponent List - size="
									+ aeStateList.size());
					try {
						service.updateAgent(aeStateList);
					} catch (Exception ex) {
						throw ex;
					} finally {
						// remove components that reached Ready state
						for (IUimaPipelineAEComponent aeState : componentsToDelete) {
							aeStateList.remove(aeState);
						}
					}
				}

			} catch (UndeclaredThrowableException e) {
				if (!(e.getCause() instanceof InstanceNotFoundException)) {
					if (howManySeenSoFar > 3) { // allow up three errors of this
												// kind
						DuccService.getDuccLogger(this.getClass().getName()).info("UimaAEJmxMonitor.run()", null, e);
						howManySeenSoFar = 1;
						throw e;
					}
					howManySeenSoFar++;
				} else {
					// AE not fully initialized yet, ignore the exception
				}
			} catch (Throwable e) {
				howManySeenSoFar = 1;
				DuccService.getDuccLogger(this.getClass().getName()).info("UimaAEJmxMonitor.run()", null, e);
			}
		}
	}

	public void onApplicationEvent(ApplicationEvent event) {
		if (event instanceof UimaASApplicationExitEvent) {
			String reason = "ProcessCASFailed";
			if (!((UimaASApplicationExitEvent) event).getEventTrigger().equals(
					EventTrigger.ExceededErrorThreshold)) {
				reason = "ExceededErrorThreshold";
			}
			notifyAgentWithStatus(ProcessState.Stopping, reason);
		}
	}

}
