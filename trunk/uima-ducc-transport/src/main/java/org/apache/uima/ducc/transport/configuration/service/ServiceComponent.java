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

import java.lang.reflect.Method;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.camel.CamelContext;
import org.apache.uima.ducc.common.component.AbstractDuccComponent;
import org.apache.uima.ducc.common.component.IJobProcessor;
import org.apache.uima.ducc.common.main.DuccService;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.transport.configuration.jp.AgentSession;
import org.apache.uima.ducc.transport.configuration.jp.JmxAEProcessInitMonitor;
import org.apache.uima.ducc.transport.event.common.IProcessState.ProcessState;

public class ServiceComponent extends AbstractDuccComponent implements
		IJobProcessor {

	private AgentSession agent = null;
	ScheduledThreadPoolExecutor executor = null;

	private String jmxConnectString = "";
	protected ProcessState currentState = ProcessState.Undefined;
	protected ProcessState previousState = ProcessState.Undefined;
	protected static DuccLogger logger =
			new DuccLogger(ServiceComponent.class);
	protected String saxonJarPath;
	protected String dd2SpringXslPath;
	protected String dd;
	private Object processorInstance = null;
    private CountDownLatch exitLatch = new CountDownLatch(1);
	private Lock stateLock = new ReentrantLock();
	
	public ServiceComponent(String componentName, CamelContext ctx,
			ServiceConfiguration jpc) {
		super(componentName, ctx);
		jmxConnectString = super.getProcessJmxUrl();

	}

	public void setProcessor(Object pc, String[] args) {
		this.processorInstance = pc;
	}

	public void setState(ProcessState state) {
		try {
			stateLock.lock();
			if (currentState.name().equals(
					ProcessState.FailedInitialization.name())) {
				return;
			}
			if (!state.name().equals(currentState.name())) {
				currentState = state;
				agent.notify(currentState, super.getProcessJmxUrl());
			}
		} finally {
			stateLock.unlock();
		}
	}

	protected void setDD(String dd) {
		this.dd = dd;
	}

	public void setDd2SpringXslPath(String dd2SpringXslPath) {
		this.dd2SpringXslPath = dd2SpringXslPath;
	}

	public void setSaxonJarPath(String saxonJarPath) {
		this.saxonJarPath = saxonJarPath;
	}

	protected void setAgentSession(AgentSession session) {
		agent = session;
	}

	public String getProcessJmxUrl() {
		return jmxConnectString;
	}

	public DuccLogger getLogger() {
		return logger;
	}

	/**
	 * This method is called by super during ducc framework boot sequence. It
	 * creates all the internal components and worker threads and initiates
	 * processing. When threads exit, this method shuts down the components and
	 * returns.
	 */
	public void start(DuccService service, String[] args) throws Exception {
		super.start(service, args);

		if (args == null || args.length == 0 || args[0] == null
				|| args[0].trim().length() == 0) {
			logger.warn(
					"start",
					null,
					"Missing Deployment Descriptor - Service Requires argument. Add DD for UIMA-AS job");
			throw new RuntimeException(
					"Missing Deployment Descriptor - Service Requires argument. Add DD for UIMA-AS job");
		}
		String processJmxUrl = super.getProcessJmxUrl();
		// tell the agent that this process is initializing
		agent.notify(ProcessState.Initializing, processJmxUrl);

		try {
			executor = new ScheduledThreadPoolExecutor(1);
			executor.prestartAllCoreThreads();
			// Instantiate a UIMA AS jmx monitor to poll for status of the AE.
			// This monitor checks if the AE is initializing or ready.
			JmxAEProcessInitMonitor monitor = new JmxAEProcessInitMonitor(agent);
			/*
			 * This will execute the UimaAEJmxMonitor continuously for every 15
			 * seconds with an initial delay of 20 seconds. This monitor polls
			 * initialization status of AE deployed in UIMA AS.
			 */
			executor.scheduleAtFixedRate(monitor, 20, 30, TimeUnit.SECONDS);
			String[] jpArgs;
			jpArgs = new String[] { "-dd", args[0], "-saxonURL", saxonJarPath,
					"-xslt", dd2SpringXslPath };
			
			Properties props = new Properties();
			// Using java reflection, initialize instance of IProcessContainer
			Method initMethod = processorInstance
					.getClass()
					.getSuperclass()
					.getDeclaredMethod("initialize", Properties.class,
							String[].class);
			initMethod.invoke(processorInstance, props, jpArgs);

			//getLogger().info("start", null, "Ducc JP JobType=" + jobType);

			System.out.println("JMX Connect String:" + processJmxUrl);
			// getLogger().info("start", null,
			// "Starting "+scaleout+" Process Threads - JMX Connect String:"+
			// processJmxUrl);

			Method deployMethod = processorInstance.getClass().getSuperclass()
					.getDeclaredMethod("deploy");
			deployMethod.invoke(processorInstance);
			System.out
					.println(".... Deployed Processing Container - Initialization Successful - Thread "
							+ Thread.currentThread().getId());

			// if initialization was successful, tell the agent that the JP is
			// running
			if (!currentState.equals(ProcessState.FailedInitialization)) {
				// pipelines deployed and initialized. This process is Ready
				currentState = ProcessState.Running;
				// Update agent with the most up-to-date state of the pipeline
				// all is well, so notify agent that this process is in Running
				// state
				agent.notify(currentState, processJmxUrl);
				// SUCCESSFUL DEPLOY - Now wait until the agent sends stop
				// request. Processing continues in UIMA-AS without DUCCs 
				// involvement.
				// In this class stop() method, the latch will
				// count down and allow the process to exit.
				exitLatch.await();
			}
			
		} catch (Exception ee) {
			ee.printStackTrace();
			currentState = ProcessState.FailedInitialization;
			getLogger()
					.info("start", null,
							">>> Failed to Deploy UIMA Service. Check UIMA Log for Details");
			agent.notify(ProcessState.FailedInitialization);
			Runtime.getRuntime().halt(0);   // hard stop. Initialization failed
		}

	}

	public void setRunning() {
		currentState = ProcessState.Running;
	}

	public boolean isRunning() {
		return currentState.equals(ProcessState.Running);
	}

	public void stop() {
		currentState = ProcessState.Stopping;
		if ( agent != null ) {
			agent.notify(currentState);
		}
		if (super.isStopping()) {
			return; // already stopping - nothing to do
		}

		System.out.println("... ServiceComponent - Stopping Service Adapter");
		try {
			exitLatch.countDown();   // count down the exit latch so this process can exit
			// Stop executor. It was only needed to poll AE initialization
			// status.
			// Since deploy() completed
			// the UIMA AS service either succeeded initializing or it failed.
			// In
			// either case we no longer
			// need to poll for initialization status
			if (executor != null) {
				executor.shutdownNow();
			}

			if (agent != null) {
				agent.stop();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {

			try {

				Method stopMethod = processorInstance.getClass()
						.getSuperclass().getDeclaredMethod("stop");
				stopMethod.invoke(processorInstance);

				super.stop();
			} catch (Exception ee) {
			}
		}
	}

	public void resetInvestment(String key) throws Exception {
		// TODO Auto-generated method stub
		
	}

}
