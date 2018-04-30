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

package org.apache.uima.ducc.ps.service.jmx;

import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import javax.management.InstanceNotFoundException;
import javax.management.JMX;
import javax.management.MBeanServer;
import javax.management.ObjectInstance;
import javax.management.ObjectName;

import org.apache.uima.analysis_engine.AnalysisEngineManagement;
import org.apache.uima.analysis_engine.AnalysisEngineManagement.State;
import org.apache.uima.ducc.ps.service.monitor.IServiceMonitor;
import org.apache.uima.util.Level;
import org.apache.uima.util.Logger;


public class JmxAEProcessInitMonitor implements Runnable {
	private volatile boolean running = false;
	private MBeanServer server = null;
	private IServiceMonitor monitor;
	private static int howManySeenSoFar = 1;
	private Logger logger;
	
	public List<IUimaPipelineAEComponent> aeStateList = 
			new ArrayList<>();

	public JmxAEProcessInitMonitor(IServiceMonitor monitor,Logger logger )
			{
		server = ManagementFactory.getPlatformMBeanServer();
		this.monitor = monitor;
		this.logger = logger;
	}

	private IUimaPipelineAEComponent getUimaAeByName(String name) {
		for (IUimaPipelineAEComponent aeState : aeStateList) {
			if (aeState.getAeName().equals(name)) {
				return aeState;
			}
		}
		
		return null;
	}
    public void updateAgentWhenRunning() {
    	running = true;
		try {
		    run();
		} catch (Exception ex) {
			logger.log(Level.WARNING, "", ex);
		}
    }
	public void run() {
		if ( running ) {
			return; // the process is in Running state
		}
		try {
			// create an ObjectName with UIMA JMX naming convention to
			// enable
			// finding deployed uima components.
			ObjectName uimaServicePattern = new ObjectName(
					"org.apache.uima:*");
			// Fetch UIMA MBean names from JMX Server that match above
			// name pattern
			Set<ObjectInstance> mbeans = new HashSet<>(
					server.queryMBeans(uimaServicePattern, null));
			List<IUimaPipelineAEComponent> componentsToDelete = new ArrayList<>();
			boolean updateMonitor = false;
			for (ObjectInstance instance : mbeans) {
				String targetName = instance.getObjectName()
						.getKeyProperty("name");
				if (targetName.endsWith("FlowController") || targetName.trim().endsWith("DUCC.Job")) { // skip FC
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
					StringBuilder sb = new StringBuilder();
					// compose component name from jmx ObjectName
					for (String part : aeObjectNameParts) {
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
						updateMonitor = true;
					} else {
						// continue publishing AE state while the AE is
						// initializing
						if (AnalysisEngineManagement.State
								.valueOf(proxy.getState())
								.equals(AnalysisEngineManagement.State.Initializing)) {
							updateMonitor = true;
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
							updateMonitor = true;
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
					if (logger.isLoggable(Level.FINE)) {
						logger.log(Level.FINE,
								"UimaAEJmxMonitor.run()---- AE Name:" + proxy.getName()
										+ " AE State:" + proxy.getState()
										+ " AE init time="
										+ aeState.getInitializationTime()
										+ " Proxy Init time="
										+ proxy.getInitializationTime()
										+ " Proxy Thread ID:"
										+ proxy.getThreadId()) ;
					}
				}
			}
			howManySeenSoFar = 1; // reset error counter
			if (updateMonitor && !running ) {
				if ( logger.isLoggable(Level.FINE))  {
					logger.log(Level.FINE,"UimaAEJmxMonitor.run() ---- Publishing UimaPipelineAEComponent List - size="
									+ aeStateList.size());
				}
				try {
					if ( monitor != null ) {
						StringBuilder sb = new StringBuilder();
						for( IUimaPipelineAEComponent ae : aeStateList ) {
							sb.append("[").
							append(ae.getAeName()).
							append(",").
							append(ae.getAeState()).
							append(",").
							append(ae.getInitializationTime()).
							append(",").
							append(ae.getAeThreadId()).append("]");
						}
						Properties initState = new Properties();
						initState.setProperty("SERVICE_UIMA_INIT_STATE", sb.toString());
						monitor.onStateChange( initState);
						//agent.notify(false, aeStateList);
					}
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
					if ( logger.isLoggable(Level.INFO) ) {
						logger.log(Level.INFO,"", e);
					}
					howManySeenSoFar = 1;
					throw e;
				}
				howManySeenSoFar++;
			} else {
				// AE not fully initialized yet, ignore the exception
			}
		} catch (Throwable e) {
			howManySeenSoFar = 1;
			logger.log(Level.WARNING, "", e);
		}
	}
	
	
	public class UimaPipelineAEComponent implements IUimaPipelineAEComponent {
		
		private static final long serialVersionUID = 1L;
		
		String name;
		State state;
		long threadId;
		long initializationTime;
		public transient long startInitialization;
		
		public UimaPipelineAEComponent(String name, long threadId, State state) {
			this.name = name;
			this.threadId = threadId;
			this.state = state;
		}
		public long getInitializationTime() {
			return initializationTime;
		}
		public void setInitializationTime(long initializationTime) {
			this.initializationTime = initializationTime;
		}

		
		
		public String getAeName() {
			// TODO Auto-generated method stub
			return name;
		}

		
		public State getAeState() {
			// TODO Auto-generated method stub
			return state;
		}

		public void setAeState(State state ){
			this.state = state;
		}
		public long getAeThreadId() {
			// TODO Auto-generated method stub
			return threadId;
		}

	}
	public interface IUimaPipelineAEComponent extends Serializable{
		public String getAeName();
		public 	State getAeState();	
		public void setAeState(State state );
		public long getAeThreadId();
		public long getInitializationTime();
		public void setInitializationTime(long initializationTime);
	}
}