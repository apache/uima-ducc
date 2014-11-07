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

import java.lang.management.ManagementFactory;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.management.InstanceNotFoundException;
import javax.management.JMX;
import javax.management.MBeanServer;
import javax.management.ObjectInstance;
import javax.management.ObjectName;

import org.apache.uima.analysis_engine.AnalysisEngineManagement;
import org.apache.uima.ducc.common.main.DuccService;
import org.apache.uima.ducc.transport.agent.IUimaPipelineAEComponent;
import org.apache.uima.ducc.transport.agent.UimaPipelineAEComponent;


public class JmxAEProcessInitMonitor implements Runnable {
	MBeanServer server = null;
	AgentSession agent;
	static int howManySeenSoFar = 1;
	public List<IUimaPipelineAEComponent> aeStateList = new ArrayList<IUimaPipelineAEComponent>();

	public JmxAEProcessInitMonitor(AgentSession agent)
			throws Exception {
		server = ManagementFactory.getPlatformMBeanServer();
		this.agent = agent;
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
					agent.notify(aeStateList);
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