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
package org.apache.uima.ducc.orchestrator.event;

import java.util.Properties;

import org.apache.camel.Body;
import org.apache.camel.CamelContext;
import org.apache.uima.ducc.common.internationalization.Messages;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.orchestrator.Orchestrator;
import org.apache.uima.ducc.orchestrator.OrchestratorCommonArea;
import org.apache.uima.ducc.orchestrator.system.events.log.SystemEventsLogger;
import org.apache.uima.ducc.transport.dispatcher.DuccEventDispatcher;
import org.apache.uima.ducc.transport.event.AServiceRequest;
import org.apache.uima.ducc.transport.event.AgentProcessLifecycleReportDuccEvent;
import org.apache.uima.ducc.transport.event.CancelJobDuccEvent;
import org.apache.uima.ducc.transport.event.CancelReservationDuccEvent;
import org.apache.uima.ducc.transport.event.CancelServiceDuccEvent;
import org.apache.uima.ducc.transport.event.DaemonDuccEvent;
import org.apache.uima.ducc.transport.event.DuccEvent.EventType;
import org.apache.uima.ducc.transport.event.DuccWorkRequestEvent;
import org.apache.uima.ducc.transport.event.JdRequestEvent;
import org.apache.uima.ducc.transport.event.NodeInventoryUpdateDuccEvent;
import org.apache.uima.ducc.transport.event.RmStateDuccEvent;
import org.apache.uima.ducc.transport.event.ServiceReplyEvent;
import org.apache.uima.ducc.transport.event.SmHeartbeatDuccEvent;
import org.apache.uima.ducc.transport.event.SmStateDuccEvent;
import org.apache.uima.ducc.transport.event.SubmitJobDuccEvent;
import org.apache.uima.ducc.transport.event.SubmitReservationDuccEvent;
import org.apache.uima.ducc.transport.event.SubmitServiceDuccEvent;
import org.apache.uima.ducc.transport.event.cli.JobReplyProperties;
import org.apache.uima.ducc.transport.event.delegate.DuccEventDelegateListener;


public class OrchestratorEventListener implements DuccEventDelegateListener {
	private static final DuccLogger logger = DuccLogger.getLogger(OrchestratorEventListener.class);

	private OrchestratorCommonArea orchestratorCommonArea = OrchestratorCommonArea.getInstance();
	private Messages messages = orchestratorCommonArea.getSystemMessages();
	
	private Orchestrator orchestrator;
	
	public OrchestratorEventListener(Orchestrator orchestrator) {
		this.orchestrator = orchestrator;
	}
	public void setDuccEventDispatcher( DuccEventDispatcher eventDispatcher ) {
	}
	public void setEndpoint( String endpoint ) {
	}
	public void onSubmitJobEvent(@Body SubmitJobDuccEvent duccEvent) throws Exception {
		String methodName = "onSubmitJobEvent";
		logger.trace(methodName, null, messages.fetch("enter"));
		try {
			orchestrator.startJob(duccEvent);
		}
		catch(Throwable t) {
			logger.error(methodName, null, t);
		}
		logger.trace(methodName, null, messages.fetch("exit"));
	}
	public void onCancelJobEvent(@Body CancelJobDuccEvent duccEvent) throws Exception {
		String methodName = "onCancelJobEvent";
		logger.trace(methodName, null, messages.fetch("enter"));
		try {
			Properties properties = duccEvent.getProperties();
			String dpid = properties.getProperty(JobReplyProperties.key_dpid);
			if(dpid != null) {
				orchestrator.stopJobProcess(duccEvent);
			}
			else {
				orchestrator.stopJob(duccEvent);
			}
		}
		catch(Throwable t) {
			logger.error(methodName, null, t);
		}
		logger.trace(methodName, null, messages.fetch("exit"));
	}
	public void onSubmitReservationEvent(@Body SubmitReservationDuccEvent duccEvent) throws Exception {
		String methodName = "onSubmitReservationEvent";
		logger.trace(methodName, null, messages.fetch("enter"));
		try {
			orchestrator.startReservation(duccEvent);
		}
		catch(Throwable t) {
			logger.error(methodName, null, t);
		}
		logger.trace(methodName, null, messages.fetch("exit"));
	}
	public void onCancelReservationEvent(@Body CancelReservationDuccEvent duccEvent) throws Exception {
		String methodName = "onCancelReservationEvent";
		logger.trace(methodName, null, messages.fetch("enter"));
		try {
			orchestrator.stopReservation(duccEvent);
		}
		catch(Throwable t) {
			logger.error(methodName, null, t);
		}
		logger.trace(methodName, null, messages.fetch("exit"));
	}
	public void onSubmitServiceEvent(@Body SubmitServiceDuccEvent duccEvent) throws Exception {
		String methodName = "onSubmitServiceEvent";
		logger.trace(methodName, null, messages.fetch("enter"));
		try {
			orchestrator.startService(duccEvent);
		}
		catch(Throwable t) {
			logger.error(methodName, null, t);
		}
		logger.trace(methodName, null, messages.fetch("exit"));
	}
	public void onCancelServiceEvent(@Body CancelServiceDuccEvent duccEvent) throws Exception {
		String methodName = "onCancelServiceEvent";
		logger.trace(methodName, null, messages.fetch("enter"));
		try {
			orchestrator.stopService(duccEvent);
		}
		catch(Throwable t) {
			logger.error(methodName, null, t);
		}
		logger.trace(methodName, null, messages.fetch("exit"));
	}
	public void onSmStateUpdateEvent(@Body SmStateDuccEvent duccEvent) throws Exception {
		String methodName = "onSmStateUpdateEvent";
		logger.trace(methodName, null, messages.fetch("enter"));
		try {
			orchestrator.reconcileSmState(duccEvent);
		}
		catch(Throwable t) {
			logger.error(methodName, null, t);
		}
		logger.trace(methodName, null, messages.fetch("exit"));
	}
	public void onSmHeartbeatUpdateEvent(@Body SmHeartbeatDuccEvent duccEvent) throws Exception {
		String methodName = "onSmHeartbeatUpdateEvent";
		logger.trace(methodName, null, messages.fetch("enter"));
		try {
			orchestrator.reconcileSmHeartbeat(duccEvent);
		}
		catch(Throwable t) {
			logger.error(methodName, null, t);
		}
		logger.trace(methodName, null, messages.fetch("exit"));
	}
	public void onRmStateUpdateEvent(@Body RmStateDuccEvent duccEvent) throws Exception {
		String methodName = "onRmStateUpdateEvent";
		logger.trace(methodName, null, messages.fetch("enter"));
		try {
			RMStateEventLogger.receiver(duccEvent);
			orchestrator.reconcileRmState(duccEvent);
		}
		catch(Throwable t) {
			logger.error(methodName, null, t);
		}
		logger.trace(methodName, null, messages.fetch("exit"));
	}
	public void onDwStateExchangeEvent(@Body DuccWorkRequestEvent duccEvent) throws Exception {
		String methodName = "onDwStateUpdateEvent";
		logger.trace(methodName, null, messages.fetch("enter"));
		try {
			orchestrator.reconcileDwState(duccEvent);
		}
		catch(Throwable t) {
			logger.error(methodName, null, t);
		}
		logger.trace(methodName, null, messages.fetch("exit"));
	}
	public void onJdStateExchangeEvent(@Body JdRequestEvent duccEvent) throws Exception {
		String methodName = "onJdStateUpdateEvent";
		logger.trace(methodName, null, messages.fetch("enter"));
		try {
			orchestrator.reconcileJdState(duccEvent);
		}
		catch(Throwable t) {
			logger.error(methodName, null, t);
		}
		logger.trace(methodName, null, messages.fetch("exit"));
	}
	public void onNodeInventoryUpdateDuccEvent(@Body NodeInventoryUpdateDuccEvent duccEvent) throws Exception {
		String methodName = "onNodeInventoryUpdateDuccEvent";
		logger.trace(methodName, null, messages.fetch("enter"));
		try {
			NodeInventoryEventLogger.receiver(duccEvent);
			orchestrator.reconcileNodeInventory(duccEvent);
		}
		catch(Throwable t) {
			logger.error(methodName, null, t);
		}
		logger.trace(methodName, null, messages.fetch("exit"));
	}
	public void onAgentProcessLifecycleReportDuccEvent(@Body AgentProcessLifecycleReportDuccEvent duccEvent) throws Exception {
		String methodName = "onAgentProcessLifecycleReportDuccEvent";
		logger.trace(methodName, null, messages.fetch("enter"));
		try {
			orchestrator.reconcileAgentProcessLifecycleReport(duccEvent);
		}
		catch(Throwable t) {
			logger.error(methodName, null, t);
		}
		logger.trace(methodName, null, messages.fetch("exit"));
	}
	private SmChannel smChannel = null;
	
	public void initSmChannel(CamelContext context, String endpoint) {
		smChannel = new SmChannel(context, endpoint);
	}
	
	public void onServiceRequestEvent(@Body AServiceRequest request) throws Exception {
		String methodName = "onServiceRequestEvent";
		logger.trace(methodName, null, messages.fetch("enter"));
		try {
			ServiceReplyEvent reply = smChannel.exchange(request);
			request.setReply(reply);
		}
		catch(Throwable t) {
			logger.error(methodName, null, t);
		}
		logger.trace(methodName, null, messages.fetch("exit"));
	}
	
	public void onDaemonDuccEvent(@Body DaemonDuccEvent daemonDuccEvent) throws Exception {
		String methodName = "onDaemonDuccEvent";
		logger.trace(methodName, null, messages.fetch("enter"));
		try {
			EventType type = daemonDuccEvent.getEventType();
			switch(type) {
			case SWITCH_TO_BACKUP:
			case SWITCH_TO_MASTER:
				logger.warn(methodName, null, daemonDuccEvent.getDaemon().getAbbrev(), daemonDuccEvent.getNodeIdentity().getCanonicalName());
				SystemEventsLogger.warn(daemonDuccEvent);
				break;
			default:
				logger.info(methodName, null, daemonDuccEvent.getDaemon().getAbbrev(), daemonDuccEvent.getNodeIdentity().getCanonicalName());
				SystemEventsLogger.info(daemonDuccEvent);
				break;
			}
		}
		catch(Throwable t) {
			logger.error(methodName, null, t);
		}
		logger.trace(methodName, null, messages.fetch("exit"));
	}
}
