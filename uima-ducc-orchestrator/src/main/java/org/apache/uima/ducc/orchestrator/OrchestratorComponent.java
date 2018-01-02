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
package org.apache.uima.ducc.orchestrator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.uima.ducc.common.NodeIdentity;
import org.apache.uima.ducc.common.admin.event.DuccAdminEvent;
import org.apache.uima.ducc.common.admin.event.RmAdminReply;
import org.apache.uima.ducc.common.boot.DuccDaemonRuntimeProperties;
import org.apache.uima.ducc.common.boot.DuccDaemonRuntimeProperties.DaemonName;
import org.apache.uima.ducc.common.component.AbstractDuccComponent;
import org.apache.uima.ducc.common.crypto.Crypto;
import org.apache.uima.ducc.common.crypto.CryptoException;
import org.apache.uima.ducc.common.internationalization.Messages;
import org.apache.uima.ducc.common.main.DuccRmAdmin;
import org.apache.uima.ducc.common.main.DuccService;
import org.apache.uima.ducc.common.persistence.or.IDbDuccWorks;
import org.apache.uima.ducc.common.persistence.or.TypedProperties;
import org.apache.uima.ducc.common.system.SystemState;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccPropertiesResolver;
import org.apache.uima.ducc.common.utils.IDuccLoggerComponents;
import org.apache.uima.ducc.common.utils.TimeStamp;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.database.DbDuccWorks;
import org.apache.uima.ducc.orchestrator.OrchestratorConstants.StartType;
import org.apache.uima.ducc.orchestrator.authentication.DuccWebAdministrators;
import org.apache.uima.ducc.orchestrator.exceptions.ResourceUnavailableForJobDriverException;
import org.apache.uima.ducc.orchestrator.factory.IJobFactory;
import org.apache.uima.ducc.orchestrator.factory.JobFactory;
import org.apache.uima.ducc.orchestrator.jd.scheduler.JdScheduler;
import org.apache.uima.ducc.orchestrator.maintenance.MaintenanceThread;
import org.apache.uima.ducc.orchestrator.maintenance.NodeAccounting;
import org.apache.uima.ducc.orchestrator.system.events.log.SystemEventsLogger;
import org.apache.uima.ducc.orchestrator.utilities.TrackSync;
import org.apache.uima.ducc.transport.event.AgentProcessLifecycleReportDuccEvent;
import org.apache.uima.ducc.transport.event.AgentProcessLifecycleReportDuccEvent.LifecycleEvent;
import org.apache.uima.ducc.transport.event.CancelJobDuccEvent;
import org.apache.uima.ducc.transport.event.CancelReservationDuccEvent;
import org.apache.uima.ducc.transport.event.CancelServiceDuccEvent;
import org.apache.uima.ducc.transport.event.DuccEvent.EventType;
import org.apache.uima.ducc.transport.event.DuccWorkRequestEvent;
import org.apache.uima.ducc.transport.event.IDuccContext.DuccContext;
import org.apache.uima.ducc.transport.event.JdRequestEvent;
import org.apache.uima.ducc.transport.event.NodeInventoryUpdateDuccEvent;
import org.apache.uima.ducc.transport.event.OrchestratorStateDuccEvent;
import org.apache.uima.ducc.transport.event.RmStateDuccEvent;
import org.apache.uima.ducc.transport.event.SmHeartbeatDuccEvent;
import org.apache.uima.ducc.transport.event.SmStateDuccEvent;
import org.apache.uima.ducc.transport.event.SubmitJobDuccEvent;
import org.apache.uima.ducc.transport.event.SubmitReservationDuccEvent;
import org.apache.uima.ducc.transport.event.SubmitServiceDuccEvent;
import org.apache.uima.ducc.transport.event.cli.JobReplyProperties;
import org.apache.uima.ducc.transport.event.cli.JobRequestProperties;
import org.apache.uima.ducc.transport.event.cli.ReservationReplyProperties;
import org.apache.uima.ducc.transport.event.cli.ReservationRequestProperties;
import org.apache.uima.ducc.transport.event.cli.ServiceRequestProperties;
import org.apache.uima.ducc.transport.event.cli.SpecificationProperties;
import org.apache.uima.ducc.transport.event.common.DuccProcessMap;
import org.apache.uima.ducc.transport.event.common.DuccWorkJob;
import org.apache.uima.ducc.transport.event.common.DuccWorkMap;
import org.apache.uima.ducc.transport.event.common.DuccWorkReservation;
import org.apache.uima.ducc.transport.event.common.IDuccCompletionType.JobCompletionType;
import org.apache.uima.ducc.transport.event.common.IDuccCompletionType.ReservationCompletionType;
import org.apache.uima.ducc.transport.event.common.IDuccProcess;
import org.apache.uima.ducc.transport.event.common.IDuccProcess.ReasonForStoppingProcess;
import org.apache.uima.ducc.transport.event.common.IDuccProcessMap;
import org.apache.uima.ducc.transport.event.common.IDuccProcessType.ProcessType;
import org.apache.uima.ducc.transport.event.common.IDuccReservation;
import org.apache.uima.ducc.transport.event.common.IDuccReservationMap;
import org.apache.uima.ducc.transport.event.common.IDuccState.JobState;
import org.apache.uima.ducc.transport.event.common.IDuccState.ReservationState;
import org.apache.uima.ducc.transport.event.common.IDuccTypes.DuccType;
import org.apache.uima.ducc.transport.event.common.IDuccWork;
import org.apache.uima.ducc.transport.event.common.IDuccWorkJob;
import org.apache.uima.ducc.transport.event.common.IDuccWorkReservation;
import org.apache.uima.ducc.transport.event.common.IProcessState.ProcessState;
import org.apache.uima.ducc.transport.event.common.IRationale;
import org.apache.uima.ducc.transport.event.common.IResourceState.ProcessDeallocationType;
import org.apache.uima.ducc.transport.event.common.IResourceState.ResourceState;
import org.apache.uima.ducc.transport.event.common.ITimeWindow;
import org.apache.uima.ducc.transport.event.common.Rationale;
import org.apache.uima.ducc.transport.event.jd.IDriverStatusReport;
import org.apache.uima.ducc.transport.event.rm.IRmJobState;
import org.apache.uima.ducc.transport.event.sm.ServiceMap;

public class OrchestratorComponent extends AbstractDuccComponent 
implements Orchestrator {
	private static final DuccLogger logger = DuccLogger.getLogger(OrchestratorComponent.class);
	private static DuccId jobid = null;
	
	private OrchestratorCommonArea orchestratorCommonArea = OrchestratorCommonArea.getInstance();
	private Messages messages = orchestratorCommonArea.getSystemMessages();
	private DuccWorkMap workMap = orchestratorCommonArea.getWorkMap();
	private StateManager stateManager = StateManager.getInstance();
	//private HealthMonitor healthMonitor = HealthMonitor.getInstance();
	//private MqReaper mqReaper = MqReaper.getInstance();
	private IJobFactory jobFactory = JobFactory.getInstance();
	private ReservationFactory reservationFactory = ReservationFactory.getInstance();
	private JdScheduler jdScheduler = orchestratorCommonArea.getJdScheduler();
	private StateJobAccounting stateJobAccounting = StateJobAccounting.getInstance();
	private DuccPropertiesResolver dpr = DuccPropertiesResolver.getInstance();
	
	private IDbDuccWorks dbDuccWorks = null;
	
	public OrchestratorComponent(CamelContext context) {
		super("Orchestrator", context);
		init();
	}
	
	/*
	 * Initialize DB access, and create table(s) if not already present
	 */
	private void init() {
		String location = "init";
		try {
			dbDuccWorks = new DbDuccWorks(logger);
			dbDuccWorks.dbInit();
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
		}
	}
	
	public void onDuccAdminKillEvent(DuccAdminEvent event) throws Exception {
		OrchestratorCheckpoint.getInstance().saveState();
		SystemEventsLogger.warn(IDuccLoggerComponents.abbrv_orchestrator, EventType.SHUTDOWN.name(), "");
		super.onDuccAdminKillEvent(event);
	}
	
	private void force(IDuccWorkJob job, IRationale rationale){
		String methodName = "force";
		if(!job.isCompleted()) {
			stateJobAccounting.stateChange(job, JobState.Completed);
			job.getStandardInfo().setDateOfCompletion(TimeStamp.getCurrentMillis());
			stateJobAccounting.complete(job, JobCompletionType.CanceledBySystem, rationale);
			OrchestratorCommonArea.getInstance().getProcessAccounting().deallocateAndStop(job,ProcessDeallocationType.JobCanceled);
			logger.info(methodName, job.getDuccId(),JobCompletionType.CanceledBySystem);
		}
	}
	
	/*
	private void cancel(IDuccWorkJob job) {
		String methodName = "cancel";
		if(!job.isFinished()) {
			stateJobAccounting.stateChange(job, JobState.Completing);
			job.getStandardInfo().setDateOfCompletion(TimeStamp.getCurrentMillis());
			stateJobAccounting.complete(job, JobCompletionType.CanceledBySystem);
			OrchestratorCommonArea.getInstance().getProcessAccounting().deallocateAndStop(job,ProcessDeallocationType.JobCanceled);
			logger.info(methodName, job.getDuccId(),JobCompletionType.CanceledBySystem);
		}
	}
	*/
	
	private void cancel(IDuccWorkReservation reservation) {
		String methodName = "cancel";
		reservation.getStandardInfo().setDateOfCompletion(TimeStamp.getCurrentMillis());
		reservation.stateChange(ReservationState.Completed);
		reservation.complete(ReservationCompletionType.CanceledBySystem);
		logger.info(methodName, reservation.getDuccId(), ReservationCompletionType.CanceledBySystem);
	}
	
	private StartType getStartTypeProperty() 
	{
		String methodName = "getStartTypeProperty";
		logger.trace(methodName, null, messages.fetch("enter"));
		StartType startType = StartType.warm;
		String property = dpr.getProperty(DuccPropertiesResolver.ducc_orchestrator_start_type);
		if(property != null) {
			String startTypeProperty = property.trim().toLowerCase();
			if(startTypeProperty.equals("cold")) {
				startType = StartType.cold;
			}
			else if(startTypeProperty.equals("warm")) {
				startType = StartType.warm;
			}
			else {
				logger.warn(methodName, null, "ducc.orchestrator.start.type value in ducc.properties not recognized: "+property);
			}
		}
		else {
			logger.warn(methodName, null, "ducc.orchestrator.start.type not found in ducc.properties");
		}
		logger.trace(methodName, null, messages.fetch("exit"));
		return startType;
	}
	
	private void resolveSignatureRequired() throws CryptoException 
	{
		String methodName = "resolveSignatureRequired";
		logger.trace(methodName, null, messages.fetch("enter"));
		String property = dpr.getProperty(DuccPropertiesResolver.ducc_signature_required);
		if(property != null) {
			String signatureRequiredProperty = property.trim().toLowerCase();
			if(signatureRequiredProperty.equals("on")) {
				orchestratorCommonArea.setSignatureRequired();
				logger.info(methodName, null, "ducc.signature.required: "+property);
			}
			else if(signatureRequiredProperty.equals("off")) {
				orchestratorCommonArea.resetSignatureRequired();
				logger.info(methodName, null, "ducc.signature.required: "+property);
			}
			else {
				logger.warn(methodName, null, "ducc.signature.required value in ducc.properties not recognized: "+property);
			}
		}
		else {
			logger.warn(methodName, null, "ducc.signature.required not found in ducc.properties");
		}
		logger.trace(methodName, null, messages.fetch("exit"));
		return;
	}
	
	private StartType getStartTypeOverride(String[] args) 
	{
		String methodName = "getStartTypeOverride";
		logger.trace(methodName, null, messages.fetch("enter"));
		StartType startType = null;
		// override start type if specified on command line
		if(args != null) {
			for( String arg : args) {
				logger.debug(methodName, null, "arg: "+arg);
				String flag = arg.trim();
				while(flag.startsWith("-")) {
						flag = flag.replaceFirst("-", "");
				}
				if(flag.equals(StartType.cold.toString())) {
					startType = StartType.cold;
				}
				else if(flag.equals(StartType.warm.toString())) {
					startType = StartType.warm;
				}
				else {
					logger.warn(methodName, null, "unrecognized arg: "+arg);
				}
			}
		}
		logger.trace(methodName, null, messages.fetch("exit"));
		return startType;
	}
	
	private StartType getStartType(String[] args) 
	{
		String methodName = "getStartType";
		logger.trace(methodName, null, messages.fetch("enter"));
		StartType startType = OrchestratorConstants.startTypeDefault;
		StartType property = getStartTypeProperty();
		StartType override = getStartTypeOverride(args) ;
		StringBuffer sb = new StringBuffer();
		sb.append("start type: ");
		if(override != null) {
			startType = override;
			sb.append(startType);
			sb.append(", "+"override");
		}
		else if(property != null) {
			startType = property;
			sb.append(startType);
			sb.append(", "+"property");
		}
		else {
			sb.append(startType);
			sb.append(startType);
			sb.append(", "+"default");
		}
		logger.info(methodName, null, sb);
		logger.trace(methodName, null, messages.fetch("exit"));
		return startType;
	}
		
	public DuccLogger getLogger() {
	    return logger;
	  }
	public void start(DuccService service, String[] args) throws Exception {
		String methodName = "start";
		logger.trace(methodName, null, messages.fetch("enter"));
		try {
			StartType startType = getStartType(args);
			logger.info(methodName, null, "##### "+startType+" #####");
			String key = "ducc.broker.url";
			String value = System.getProperty(key);
			logger.debug(methodName, null, key+"="+value);
			boolean saveState = false;
			TrackSync ts = TrackSync.await(workMap, this.getClass(), methodName);
			synchronized(workMap) {
				ts.using();
				Iterator<IDuccWork> iterator = workMap.values().iterator();
				while(iterator.hasNext()) {
					IDuccWork duccWork = iterator.next();
					switch(duccWork.getDuccType()) {
					case Job:
					case Service:
						IDuccWorkJob job = (IDuccWorkJob) duccWork;
						switch(startType) {
						case cold:
							force(job, new Rationale("system cold start"));
							saveState = true;
							break;
						case warm:
							break;
						}
						break;
					case Reservation:
						IDuccWorkReservation reservation = (IDuccWorkReservation) duccWork;
						switch(startType) {
						case cold:
							cancel(reservation);
							saveState = true;
							break;
						case warm:
							break;
						}
						break;
					default:
						break;
					}
				}
			}
			ts.ended();
			if(saveState) {
				OrchestratorCheckpoint.getInstance().saveState();
			}
			switch(startType) {
			case cold:
				jdScheduler = JdScheduler.getInstance();
				break;
			case warm:
				jdScheduler = JdScheduler.getInstance();
				jdScheduler.restore();
				break;
			}
			resolveSignatureRequired();
			MaintenanceThread.getInstance().start();
		} 
		catch(Throwable t) {
			logger.error(methodName, null, t);
		}
		super.start(service, args);
		
		/*
		 * Route requests destined for RM though OR for recording in system-events.log.
		 */
		String adminEndpoint = System.getProperty("ducc.rm.via.or.admin.endpoint");
        if ( adminEndpoint == null ) {
            logger.warn(methodName, null, "No admin endpoint configured.  Not starting admin channel.");
        } else {
            startRmAdminChannel(adminEndpoint, this);
        }
		
		DuccDaemonRuntimeProperties.getInstance().boot(DaemonName.Orchestrator,getProcessJmxUrl());
		SystemEventsLogger.warn(IDuccLoggerComponents.abbrv_orchestrator, EventType.BOOT.name(), "");
		logger.trace(methodName, null, messages.fetch("exit"));
	}
	
	
	/**
	 * DuccWork State Reconciliation
	 */
	
	public void reconcileDwState(DuccWorkRequestEvent duccEvent) {
		String methodName = "reconcileDwState";
		logger.trace(methodName, null, messages.fetch("enter"));
		if(duccEvent != null) {
			DuccId duccId = duccEvent.getDuccId();
			if(duccId != null) {
				if(workMap != null) {
					IDuccWork dw = workMap.findDuccWork(duccId);
					duccEvent.setDw(dw);
					if(dw == null) {
						logger.warn(methodName, duccId, "dw==null");
					}
				}
				else {
					logger.warn(methodName, duccId, "workMap==null");
				}
			}
			else {
				logger.warn(methodName, jobid, "duccId==null");
			}
		}
		else {
			logger.warn(methodName, jobid, "duccEvent==null");
		}
		logger.trace(methodName, null, messages.fetch("exit"));
	}
	
	/**
	 * Job Driver State Reconciliation
	 */
	
	public void reconcileJdState(JdRequestEvent duccEvent) {
		String methodName = "reconcileJdState";
		IDriverStatusReport dsr = duccEvent.getDriverStatusReport();
		DuccId duccId = null;
		if(dsr != null) {
			duccId = dsr.getDuccId();
		}
		logger.trace(methodName, null, messages.fetch("enter"));
		if(dsr != null) {
			logger.info(methodName, duccId, dsr.getLogReport());
			stateManager.reconcileState(dsr);
			String sid = ""+duccId.getFriendly();
			DuccWorkJob duccWorkJob = (DuccWorkJob) WorkMapHelper.cloneDuccWork(workMap, sid, this, methodName);
			if(duccWorkJob != null) {
				IDuccProcessMap processMap = duccWorkJob.getProcessMap();
				duccEvent.setProcessMap(new DuccProcessMap(processMap));
				
			}
			else {
				String text = "not found in map";
				duccEvent.setKillDriverReason(text);
				logger.warn(methodName, duccId, text);
			}
		}
		logger.trace(methodName, null, messages.fetch("exit"));
	}
	
	/**
	 * Resources Manager State Reconciliation
	 */
	
	public void reconcileRmState(RmStateDuccEvent duccEvent) {
		String methodName = "reconcileRmState";
		logger.trace(methodName, null, messages.fetch("enter"));
		Map<DuccId, IRmJobState> resourceMap = duccEvent.getJobState();
		try {
			stateManager.reconcileState(resourceMap);
			jdScheduler.handle(workMap);
		}
		catch(Exception e) {
			logger.error(methodName, null, e);
		}
		
		logger.trace(methodName, null, messages.fetch("exit"));
	}
	/**
	 * Services Manager State Reconciliation
	 */
	
	public void reconcileSmState(SmStateDuccEvent duccEvent) {
		String methodName = "reconcileSmState";
		logger.trace(methodName, null, messages.fetch("enter"));
		ServiceMap serviceMap = duccEvent.getServiceMap();
		stateManager.reconcileState(serviceMap);
		logger.trace(methodName, null, messages.fetch("exit"));
	}
	
	@Override
	public void reconcileSmHeartbeat(SmHeartbeatDuccEvent duccEvent) {
		String methodName = "reconcileSmHeartbeat";
		logger.trace(methodName, null, messages.fetch("enter"));
		logger.trace(methodName, null, messages.fetch("exit"));
	}

	/**
	 * Node Inventory State Reconciliation
	 */
	
	public void reconcileNodeInventory(NodeInventoryUpdateDuccEvent duccEvent) {
		String methodName = "reconcileNodeInventory";
		logger.trace(methodName, null, messages.fetch("enter"));
		HashMap<DuccId, IDuccProcess> processMap = duccEvent.getProcesses();
		stateManager.reconcileState(processMap);
		NodeAccounting.getInstance().heartbeat(processMap);
		adjustPublicationSequenceNumber(duccEvent);
		logger.trace(methodName, null, messages.fetch("exit"));
	}
	
	/*
	 * If an Agent has seen a higher sequence number than that currently
	 * published by Orchestrator, then use the higher value.  The likely
	 * cause is the loss of access to the orchestrator-state.properties file.
	 */
	private void adjustPublicationSequenceNumber(NodeInventoryUpdateDuccEvent duccEvent) {
		NodeIdentity nodeIdentity = duccEvent.getNodeIdentity();
		long seqNo = duccEvent.getSequence();
		OrchestratorState.getInstance().setNextSequenceNumberStateIfGreater(nodeIdentity, seqNo);
	}
	
	/*
	 * Safely obtain Node from the arriving event
	 */
	private String getNode(AgentProcessLifecycleReportDuccEvent duccEvent) {
		String node = "?";
		NodeIdentity nodeIdentity = duccEvent.getNodeIdentity();
		if(nodeIdentity != null) {
			node = nodeIdentity.getName();
		}
		return node;
	}
	
	/*
	 * Safely obtain LifecycleEvent from the arriving event
	 */
	private LifecycleEvent getLifecycleEvent(AgentProcessLifecycleReportDuccEvent duccEvent) {
		LifecycleEvent lifecycleEvent = duccEvent.getLifecycleEvent();
		if(lifecycleEvent == null) {
			lifecycleEvent = LifecycleEvent.Undefined;
		}
		return lifecycleEvent;
	}
	
	/*
	 * Safely obtain DuccId from the arriving event
	 */
	private DuccId getProcessDuccId(AgentProcessLifecycleReportDuccEvent duccEvent) {
		DuccId duccid = null;
		IDuccProcess process = duccEvent.getProcess();
		if(process != null) {
			duccid = process.getDuccId();
		}
		return duccid;
	}
	
	/*
	 * Safely obtain ProcessId from the arriving event
	 */
	private String getProcessId(AgentProcessLifecycleReportDuccEvent duccEvent) {
		String id = null;
		DuccId duccid = getProcessDuccId(duccEvent);
		if(duccid != null) {
			id = duccid.toString();
		}
		return id;
	}
	
	/*
	 * Safely obtain ProcessType from the arriving event
	 */
	private ProcessType getProcessType(AgentProcessLifecycleReportDuccEvent duccEvent) {
		ProcessType processType = null;
		IDuccProcess process = duccEvent.getProcess();
		if(process != null) {
			processType = process.getProcessType();
		}
		return processType;
	}
	
	/*
	 * Validate arriving agent's process lifecycle event and record to system events log
	 */
	@Override
	public void reconcileAgentProcessLifecycleReport(AgentProcessLifecycleReportDuccEvent duccEvent) {
		String location = "reconcileAgentProcessLifecycleReport";
		StringBuffer sb = new StringBuffer();
		String id = getProcessId(duccEvent);
		String node = getNode(duccEvent);
		LifecycleEvent lifecycleEvent = getLifecycleEvent(duccEvent);
		ProcessType processType = getProcessType(duccEvent);
		IDuccProcess process = duccEvent.getProcess();
		DuccId processDuccId = getProcessDuccId(duccEvent);
		if(process == null) {
			sb.append("process:"+process+" ");
			sb.append("node:"+node+" ");
			sb.append("lifefcycleEvent:"+lifecycleEvent.name()+" ");
			logger.error(location, jobid, sb.toString());
		}
		else if(id == null) {
			sb.append("id:"+id+" ");
			sb.append("node:"+node+" ");
			sb.append("lifefcycleEvent:"+lifecycleEvent.name()+" ");
			logger.error(location, jobid, sb.toString());
		}
		else if(node == null) {
			sb.append("id:"+id+" ");
			sb.append("node:"+node+" ");
			sb.append("lifefcycleEvent:"+lifecycleEvent.name()+" ");
			logger.error(location, jobid, sb.toString());
		}
		else if(lifecycleEvent == LifecycleEvent.Undefined) {
			sb.append("id:"+id+" ");
			sb.append("node:"+node+" ");
			sb.append("lifefcycleEvent:"+lifecycleEvent.name()+" ");
			logger.error(location, jobid, sb.toString());
		}
		else if(processType == null) {
			sb.append("id:"+id+" ");
			sb.append("node:"+node+" ");
			sb.append("lifefcycleEvent:"+lifecycleEvent.name()+" ");
			sb.append("processType:"+processType+" ");
			logger.error(location, jobid, sb.toString());
		}
		else {
			DuccId dwId = OrchestratorCommonArea.getInstance().getProcessAccounting().getJobId(processDuccId);
			IDuccWork dw = workMap.findDuccWork(dwId);
			DuccType dwType = dw.getDuccType();
			sb.append("dwId:"+dwId+" ");
			sb.append("dwType:"+dwType+" ");
			sb.append("id:"+id+" ");
			sb.append("node:"+node+" ");
			sb.append("lifefcycleEvent:"+lifecycleEvent.name()+" ");
			sb.append("processType:"+processType.name()+" ");
			logger.debug(location, jobid, sb.toString());
			SystemEventsLogger.info(dw, process, node, lifecycleEvent, processType);
		}
	}
	
	/**
	 * Publish Orchestrator State
	 */
	
	public OrchestratorStateDuccEvent getState() {
		String methodName = "getState";
		logger.trace(methodName, null, messages.fetch("enter"));
		OrchestratorStateDuccEvent orchestratorStateDuccEvent = new OrchestratorStateDuccEvent(logger);
		try {
			DuccWorkMap workMapCopy = WorkMapHelper.deepCopy(workMap, this, methodName);
			int activeJobs = workMapCopy.getJobCount();
			int activeReservations = workMapCopy.getReservationCount();
			int activeServices = workMapCopy.getServiceCount();
			logger.debug(methodName, null, messages.fetch("publishing state")+" "+
											messages.fetchLabel("active job count")+activeJobs
											+" "+
											messages.fetchLabel("active reservation count")+activeReservations
											+" "+
											messages.fetchLabel("active service count")+activeServices
											);
			if(jdScheduler.isMinimalAllocateRequirementMet()) {
				workMapCopy.setJobDriverMinimalAllocateRequirementMet();
			}
			else {
				workMapCopy.resetJobDriverMinimalAllocateRequirementMet();
			}
			logger.debug(methodName, jobid, "isJobDriverMinimalAllocateRequirementMet="+workMapCopy.isJobDriverMinimalAllocateRequirementMet());
			orchestratorStateDuccEvent.setWorkMap(workMapCopy);
			//stateManager.prune(workMapCopy);
			//healthMonitor.cancelNonViableJobs();
			//mqReaper.removeUnusedJdQueues(workMapCopy);
		}
		catch(Throwable t) {
			logger.error(methodName, null, t);
		}
		logger.trace(methodName, null, messages.fetch("exit"));
		return orchestratorStateDuccEvent;
	}

	@SuppressWarnings("unchecked")
	private void submitError(Properties properties, String error_message) {
		String key = SpecificationProperties.key_submit_errors;
		ArrayList<String> value_submit_errors = (ArrayList<String>) properties.get(key);
		if(value_submit_errors == null) {
			value_submit_errors = new ArrayList<String>();
			properties.put(key, value_submit_errors);
		}
		value_submit_errors.add(error_message);
	}
	private boolean isSignatureValid(Properties properties) {
		String methodName = "isSignatureValid";
		boolean retVal = true;
		try {
			if(orchestratorCommonArea.isSignatureRequired()) {
			  // Check that the signature is valid
			  String user = properties.getProperty(SpecificationProperties.key_user);
			  byte[] signature = (byte[]) properties.get(SpecificationProperties.key_signature);
			  Crypto crypto = new Crypto(user);
			  retVal = crypto.isValid(signature);
			}
		}
		catch(Throwable t) {
			logger.error(methodName, null, t);
		}
		return retVal;
	}
	private String getRole(Properties properties) {
		String methodName = "isAuthorized";
		String role = SpecificationProperties.key_role_user;
		try {
			if(properties.containsKey(SpecificationProperties.key_role_administrator)) {
				role = SpecificationProperties.key_role_administrator;
			}
		}
		catch(Exception e) {
			logger.error(methodName, null, e);
		}
		return role;
	}
	private boolean isAuthorized(DuccId duccId, String reqUser, String tgtUser, String role) {
		String methodName = "isAuthorized";
		boolean retVal = false;
		try {
			if(reqUser.equals(tgtUser)) {
				logger.info(methodName, duccId, reqUser+" is "+tgtUser);
				retVal = true;
			}
			else {
				if(role.equals(SpecificationProperties.key_role_administrator)) {
					DuccWebAdministrators dwa = DuccWebAdministrators.getInstance();
					if(dwa.isAdministrator(reqUser)) {
						logger.info(methodName, duccId, reqUser+" is "+SpecificationProperties.key_role_administrator);
						retVal = true;
					}
				}
				else {
					logger.info(methodName, duccId, "role"+" is not "+SpecificationProperties.key_role_administrator);
				}
			}
		}
		catch(Exception e) {
			logger.error(methodName, duccId, e);
		}
		return retVal;
	}
	/**
	 * Handle Job Submit
	 */
	
	public void startJob(SubmitJobDuccEvent duccEvent) {
		String methodName = "startJob";
		logger.trace(methodName, null, messages.fetch("enter"));
		try {
			OrchestratorHelper.assignDefaults(duccEvent);
			JobRequestProperties properties = (JobRequestProperties) duccEvent.getProperties();
			if(!isSignatureValid(properties)) {
				String error_message = messages.fetch(" type=authentication error, text=signature not valid.");
				logger.error(methodName, null, error_message);
				submitError(properties, error_message);
			}
			else if(!SystemState.getInstance().isAcceptJobs()) {
				String error_message = messages.fetch(" type=system error, text=system is not accepting new work at this time.");
				logger.error(methodName, null, error_message);
				submitError(properties, error_message);
			}
			else {
				if(Validate.request(duccEvent)) {
					try {
						IDuccWorkJob duccWorkJob = jobFactory.createJob(properties);
						WorkMapHelper.addDuccWork(workMap, duccWorkJob, this, methodName);
						// state: Received
						stateJobAccounting.stateChange(duccWorkJob, JobState.Received);
						OrchestratorCheckpoint.getInstance().saveState();
						// state: WaitingForDriver
						stateJobAccounting.stateChange(duccWorkJob, JobState.WaitingForDriver);
						OrchestratorCheckpoint.getInstance().saveState();
						// prepare for reply to submitter
						properties.put(JobRequestProperties.key_id, duccWorkJob.getId());
						duccEvent.setProperties(properties);
						// save specification to DB
						TypedProperties tp = new TypedProperties();
						for(Entry<Object, Object> entry : properties.entrySet()) {
							String name = (String) entry.getKey();
							if(name.equals("signature")) {
								// skip it
							}
							else {
								String type = TypedProperties.PropertyType.system.name();
								if(properties.isUserProvided(name)) {
									type = TypedProperties.PropertyType.user.name();
								}
								tp.add(type, entry.getKey(), entry.getValue());
							}
						}
						String specificationType = TypedProperties.SpecificationType.Job.name();
						Long id = duccWorkJob.getDuccId().getFriendly();
						dbDuccWorks.upsertSpecification(specificationType, id, tp);
					}
					catch(ResourceUnavailableForJobDriverException e) {
						String error_message = messages.fetch(" type=system error, text=job driver node unavailable.");
						logger.error(methodName, null, error_message);
						submitError(properties, error_message);
					}
				}
				else {
					logger.info(methodName, null, messages.fetch("TODO")+" prepare error reply");
					//TODO
				}
			}
		}
		catch(Throwable t) {
			logger.error(methodName, null, messages.fetch("TODO")+" prepare error reply",t);
			//TODO
		}
		logger.trace(methodName, null, messages.fetch("exit"));
		return;
	}
	/**
	 * Handle Job Cancel
	 */
	
	public void stopJob(CancelJobDuccEvent duccEvent) {
		String methodName = "stopJob";
		DuccId dwid = null;
		logger.trace(methodName, dwid, messages.fetch("enter"));
		Properties properties = duccEvent.getProperties();
		if(!isSignatureValid(properties)) {
			String error_message = messages.fetch(" type=authentication error, text=signature not valid.");
			logger.error(methodName, dwid, error_message);
			submitError(properties, error_message);
		}
		else if(Validate.request(duccEvent)) {
			String jobId = properties.getProperty(JobRequestProperties.key_id);
			logger.info(methodName, jobid, JobRequestProperties.key_id+"="+jobId);
			long t0 = System.currentTimeMillis();
			DuccWorkJob duccWorkJob = (DuccWorkJob) WorkMapHelper.findDuccWork(workMap, DuccType.Job, jobId, this, methodName);
			long t1 = System.currentTimeMillis();
			long elapsed = t1 - t0;
			if(elapsed > Constants.SYNC_LIMIT) {
				logger.debug(methodName, dwid, "elapsed msecs: "+elapsed);
			}
			if(duccWorkJob != null) {
				dwid = duccWorkJob.getDuccId();
				String reqUser = properties.getProperty(JobRequestProperties.key_user).trim();
				String reqRole = getRole(properties);
				String tgtUser = duccWorkJob.getStandardInfo().getUser().trim();
				if(isAuthorized(dwid, reqUser, tgtUser, reqRole)) {
					logger.debug(methodName, dwid, "reqUser:"+reqUser+" "+"reqRole:"+reqRole+" "+"tgtUser:"+tgtUser);
					String givenReason = properties.getProperty(SpecificationProperties.key_reason);
					Reason reason = new Reason(dwid, reqUser, reqRole, givenReason);
					IRationale rationale = new Rationale(reason.toString());
					JobCompletionType jobCompletionType = JobCompletionType.CanceledByUser;
					if(reqRole.equals(SpecificationProperties.key_role_administrator)) {
						jobCompletionType = JobCompletionType.CanceledByAdministrator;
					}
					stateManager.jobTerminate(duccWorkJob, jobCompletionType, rationale, ProcessDeallocationType.JobCanceled);
					OrchestratorCheckpoint.getInstance().saveState();
					// prepare for reply to canceler
					properties.put(JobReplyProperties.key_message, JobReplyProperties.msg_canceled);
					duccEvent.setProperties(properties);
					logger.info(methodName, dwid, messages.fetchLabel("job state")+duccWorkJob.getJobState());
				}
				else {
					// prepare not authorized reply 
					properties.put(JobReplyProperties.key_message, JobReplyProperties.msg_user_not_authorized);
					duccEvent.setProperties(properties);
					logger.info(methodName, dwid, jobId+" : "+messages.fetch(JobReplyProperties.msg_user_not_authorized));
				}
			}
			else {
				// prepare undefined reply 
				properties.put(JobReplyProperties.key_message, JobReplyProperties.msg_job_not_found);
				duccEvent.setProperties(properties);
				logger.info(methodName, dwid, jobId+" : "+messages.fetch(JobReplyProperties.msg_job_not_found));
			}
		}
		else {
			logger.info(methodName, dwid, messages.fetch("TODO")+" prepare error reply");
			//TODO
		}
		logger.trace(methodName, dwid, messages.fetch("exit"));
		return;
	}
	
	
	public void stopJobProcess(CancelJobDuccEvent duccEvent) {
		String methodName = "stopJobProcess";
		DuccId dwid = null;
		logger.trace(methodName, dwid, messages.fetch("enter"));
		Properties properties = duccEvent.getProperties();
		if(!isSignatureValid(properties)) {
			String error_message = messages.fetch(" type=authentication error, text=signature not valid.");
			logger.error(methodName, dwid, error_message);
			submitError(properties, error_message);
		}
		else if(Validate.request(duccEvent)) {
			String dpid = null;
			String jobId = properties.getProperty(JobRequestProperties.key_id);
			DuccWorkJob duccWorkJob = (DuccWorkJob) WorkMapHelper.findDuccWork(workMap, DuccType.Job, jobId, this, methodName);
			if(duccWorkJob != null) {
				dwid = duccWorkJob.getDuccId();
				String reqUser = properties.getProperty(JobRequestProperties.key_user).trim();
				String reqRole = getRole(properties);
				String tgtUser = duccWorkJob.getStandardInfo().getUser().trim();
				if(isAuthorized(dwid, reqUser, tgtUser, reqRole)) {
					logger.debug(methodName, dwid, "reqUser:"+reqUser+" "+"reqRole:"+reqRole+" "+"tgtUser:"+tgtUser);
					dpid = properties.getProperty(JobReplyProperties.key_dpid);
					IDuccProcess idp = duccWorkJob.getProcess(dpid);
					if(idp != null) {
						switch(idp.getProcessState()) {
						case Starting:
						case Started:
						case Initializing:
						case Running:
							OrUtil.setResourceState(duccWorkJob, idp, ResourceState.Deallocated);
							idp.setProcessState(ProcessState.Abandoned);
							idp.setProcessDeallocationType(ProcessDeallocationType.Canceled);
							idp.setReasonForStoppingProcess(ReasonForStoppingProcess.UserInitiated.toString());
							if(reqRole != null) {
								if(reqRole.equalsIgnoreCase(SpecificationProperties.key_role_administrator)) {
									idp.setReasonForStoppingProcess(ReasonForStoppingProcess.AdministratorInitiated.toString());
								}
							}
							long now = System.currentTimeMillis();
							ITimeWindow twi = idp.getTimeWindowInit();
							if(twi != null) {
								if(twi.getStartLong() > 0) {
									twi.setEndLong(now);
								}
							}
							ITimeWindow twr = idp.getTimeWindowRun();
							if(twr != null) {
								if(twr.getStartLong() > 0) {
									twr.setEndLong(now);
								}
							}
							// prepare process not active 
							properties.put(JobReplyProperties.key_message, JobReplyProperties.msg_process_canceled);
							duccEvent.setProperties(properties);
							logger.info(methodName, dwid, dpid, messages.fetch(JobReplyProperties.msg_process_canceled));
							break;
						default:
							// prepare process not active 
							properties.put(JobReplyProperties.key_message, JobReplyProperties.msg_process_not_active);
							duccEvent.setProperties(properties);
							logger.info(methodName, dwid, dpid, messages.fetch(JobReplyProperties.msg_process_not_active));
							break;
						}
					}
					else {
						// prepare process not found reply 
						properties.put(JobReplyProperties.key_message, JobReplyProperties.msg_process_not_found);
						duccEvent.setProperties(properties);
						logger.info(methodName, dwid, dpid, messages.fetch(JobReplyProperties.msg_process_not_found));
					}
				}
				else {
					// prepare not authorized reply 
					properties.put(JobReplyProperties.key_message, JobReplyProperties.msg_user_not_authorized);
					duccEvent.setProperties(properties);
					logger.info(methodName, dwid, jobId+" : "+messages.fetch(JobReplyProperties.msg_user_not_authorized));
				}
			}
			else {
				// prepare job not found 
				properties.put(JobReplyProperties.key_message, JobReplyProperties.msg_job_not_found);
				duccEvent.setProperties(properties);
				logger.info(methodName, dwid, dpid, messages.fetch(JobReplyProperties.msg_job_not_found));
			}
		}
		else {
			logger.info(methodName, dwid, messages.fetch("TODO")+" prepare error reply");
			//TODO
		}
		logger.trace(methodName, dwid, messages.fetch("exit"));
		return;
	}
	
	
	public void startReservation(SubmitReservationDuccEvent duccEvent) {
		String methodName = "startReservation";
		logger.trace(methodName, null, messages.fetch("enter"));	
		try {
			OrchestratorHelper.assignDefaults(duccEvent);
			Properties properties = duccEvent.getProperties();
			if(!isSignatureValid(properties)) {
				String error_message = messages.fetch(" type=authentication error, text=signature not valid.");
				logger.error(methodName, null, error_message);
				submitError(properties, error_message);
			}
			else if(!Validate.accept(duccEvent)) {
				//String error_message = messages.fetch(" type=accept error, text=system is not configured to accept requests of this type.");
				String error_message = messages.fetch("Request was not accepted: System is configured to refuse reservations.");
				logger.error(methodName, null, error_message);
				submitError(properties, error_message);
			}
			else if(Validate.request(duccEvent)) {
				DuccWorkReservation duccWorkReservation = reservationFactory.create((ReservationRequestProperties)properties);
				WorkMapHelper.addDuccWork(workMap, duccWorkReservation, this, methodName);
				// state: Received
				duccWorkReservation.stateChange(ReservationState.Received);
				OrchestratorCheckpoint.getInstance().saveState();
				// state: WaitingForResources
				duccWorkReservation.stateChange(ReservationState.WaitingForResources);
				OrchestratorCheckpoint.getInstance().saveState();
				if(duccWorkReservation.isWaitForAssignment()) {
					int counter = 0;
					while(duccWorkReservation.isPending()) {
						counter++;
						if(counter > 5) {
							counter = 0;
							logger.info(methodName, duccWorkReservation.getDuccId(), "waiting for allocation...");
						}
						Thread.sleep(1000);
					}
					try {
						properties.put(ReservationReplyProperties.key_message, duccWorkReservation.getCompletionRationale().getText());
					}
					catch(Throwable t) {
					}
				}
				properties.put(ReservationRequestProperties.key_id, duccWorkReservation.getId());
				// node list
				properties.put(ReservationRequestProperties.key_node_list, "");
				if(!duccWorkReservation.getReservationMap().isEmpty()) {
					StringBuffer sb = new StringBuffer();
					IDuccReservationMap map = duccWorkReservation.getReservationMap();
					for (DuccId key : map.keySet()) { 
						IDuccReservation value = duccWorkReservation.getReservationMap().get(key);
						String node = value.getNodeIdentity().getName();
						sb.append(node);
						sb.append(" ");
					}
					properties.put(ReservationRequestProperties.key_node_list,sb.toString().trim());
				}
				duccEvent.setProperties(properties);
			}
			else {
				logger.info(methodName, null, messages.fetch("TODO")+" prepare error reply");
				//TODO
			}
		}
		catch(Exception e) {
			logger.error(methodName, null, messages.fetch("TODO")+" prepare error reply",e);
			//TODO
		}
		logger.trace(methodName, null, messages.fetch("exit"));
		return;
	}
	
	
	public void stopReservation(CancelReservationDuccEvent duccEvent) {
		String methodName = "stopReservation";
		DuccId dwid = null;
		logger.trace(methodName, dwid, messages.fetch("enter"));
		Properties properties = duccEvent.getProperties();
		if(!isSignatureValid(properties)) {
			String error_message = messages.fetch(" type=authentication error, text=signature not valid.");
			logger.error(methodName, dwid, error_message);
			submitError(properties, error_message);
		}
		else {
			String id = properties.getProperty(ReservationRequestProperties.key_id);
			logger.info(methodName, jobid, ReservationRequestProperties.key_id+"="+id);
			long t0 = System.currentTimeMillis();
			DuccWorkReservation duccWorkReservation = (DuccWorkReservation) WorkMapHelper.findDuccWork(workMap, DuccType.Reservation, id, this, methodName);
			long t1 = System.currentTimeMillis();
			long elapsed = t1 - t0;
			if(elapsed > Constants.SYNC_LIMIT) {
				logger.debug(methodName, dwid, "elapsed msecs: "+elapsed);
			}
			if(duccWorkReservation != null) {
				if(Validate.request(duccEvent,duccWorkReservation)) {
					dwid = duccWorkReservation.getDuccId();
					String reqUser = properties.getProperty(JobRequestProperties.key_user).trim();
					String reqRole = getRole(properties);
					String tgtUser = duccWorkReservation.getStandardInfo().getUser().trim();
					if(isAuthorized(dwid, reqUser, tgtUser, reqRole)) {
						logger.debug(methodName, dwid, "reqUser:"+reqUser+" "+"reqRole:"+reqRole+" "+"tgtUser:"+tgtUser);
						duccWorkReservation.getStandardInfo().setCancelUser(reqUser);
						duccWorkReservation.getStandardInfo().setDateOfCompletion(TimeStamp.getCurrentMillis());
						duccWorkReservation.stateChange(ReservationState.Completed);
						duccWorkReservation.complete(ReservationCompletionType.CanceledByUser);
						String u1 = duccWorkReservation.getStandardInfo().getUser();
						String u2 = duccWorkReservation.getStandardInfo().getCancelUser();
						if(u1 != null) {
							if(u2 != null) {
								if(!u1.equals(u2)) {
									duccWorkReservation.complete(ReservationCompletionType.CanceledByAdmin);
								}
							}
						}
						OrchestratorCheckpoint.getInstance().saveState();
						// prepare for reply to canceler
						properties.put(ReservationReplyProperties.key_message, ReservationReplyProperties.msg_canceled);
						duccEvent.setProperties(properties);
						logger.info(methodName, dwid, messages.fetchLabel("reservation state")+duccWorkReservation.getReservationState());
					}
					else {
						// prepare not authorized reply 
						properties.put(ReservationReplyProperties.key_message, ReservationReplyProperties.msg_user_not_authorized);
						duccEvent.setProperties(properties);
						logger.info(methodName, dwid, dwid+" : "+messages.fetch(ReservationReplyProperties.msg_user_not_authorized));
					}
				}
				else {
					properties.put(ReservationReplyProperties.key_message, ReservationReplyProperties.msg_user_not_authorized);
					duccEvent.setProperties(properties);
					logger.info(methodName, dwid, id+" : "+messages.fetch("not authorized"));
				}
			}
			else {
				// prepare undefined reply 
				properties.put(ReservationReplyProperties.key_message, ReservationReplyProperties.msg_not_found);
				duccEvent.setProperties(properties);
				logger.info(methodName, dwid, id+" : "+messages.fetch("reservation not found"));
			}
		}
		logger.trace(methodName, dwid, messages.fetch("exit"));
		return;
	}
	
	/**
	 * Handle Service Submit
	 */
	
	public void startService(SubmitServiceDuccEvent duccEvent) {
		String methodName = "startService";
		logger.trace(methodName, null, messages.fetch("enter"));
		try {
			OrchestratorHelper.assignDefaults(duccEvent);
			JobRequestProperties properties = (JobRequestProperties) duccEvent.getProperties();
			if(!isSignatureValid(properties)) {
				String error_message = messages.fetch(" type=authentication error, text=signature not valid.");
				logger.error(methodName, null, error_message);
				submitError(properties, error_message);
			}
			else if(!SystemState.getInstance().isAcceptJobs()) {
				String error_message = messages.fetch(" type=system error, text=system is not accepting new work at this time.");
				logger.error(methodName, null, error_message);
				submitError(properties, error_message);
			}
			else {
				if(Validate.request(duccEvent)) {
					IDuccWorkJob duccWorkJob = jobFactory.createService(properties);
					WorkMapHelper.addDuccWork(workMap, duccWorkJob, this, methodName);
					// state: Received
					stateJobAccounting.stateChange(duccWorkJob, JobState.Received);
					OrchestratorCheckpoint.getInstance().saveState();
					// state: WaitingForServices
					JobState nextState = JobState.WaitingForServices;
					if(duccWorkJob.getServiceDependencies() == null) {
						String message = messages.fetch("bypass")+" "+nextState;
						logger.debug(methodName, duccWorkJob.getDuccId(), message);
						nextState = JobState.WaitingForResources;
					}
					stateJobAccounting.stateChange(duccWorkJob, nextState);
					OrchestratorCheckpoint.getInstance().saveState();
					// prepare for reply to submitter
					properties.put(JobRequestProperties.key_id, duccWorkJob.getId());
					duccEvent.setProperties(properties);
					// save specification to DB
					TypedProperties tp = new TypedProperties();
					for(Entry<Object, Object> entry : properties.entrySet()) {
						String name = (String) entry.getKey();
						if(name.equals("signature")) {
							// skip it
						}
						else {
							String type = TypedProperties.PropertyType.system.name();
							if(properties.isUserProvided(name)) {
								type = TypedProperties.PropertyType.user.name();
							}
							tp.add(type, entry.getKey(), entry.getValue());
						}
					}
					long id = duccWorkJob.getDuccId().getFriendly();
					if(properties.containsKey(ServiceRequestProperties.key_service_type_other)) {
						String specificationType = TypedProperties.SpecificationType.ManagedReservation.name();
						dbDuccWorks.upsertSpecification(specificationType, id, tp);
						/* 
						 * save managed reservation specification instances
						 */
						logger.trace(methodName, duccWorkJob.getDuccId(), "type="+specificationType );
					}
					else {
						String specificationType = TypedProperties.SpecificationType.Service.name();
						long instance_id = 0;
						try {
							String value = properties.getProperty("id");
							instance_id = Long.valueOf(value);
						}
						catch(Exception e) {
							// oh well...
						}
						/* 
						 * do not save service specification instances
						 */
						logger.trace(methodName, duccWorkJob.getDuccId(), "type="+specificationType, "instance_id="+instance_id );
					}
				}
				else {
					logger.info(methodName, null, messages.fetch("TODO")+" prepare error reply");
					//TODO
				}
			}
		}
		catch(Throwable t) {
			logger.error(methodName, null, messages.fetch("TODO")+" prepare error reply",t);
			//TODO
		}
		logger.trace(methodName, null, messages.fetch("exit"));
		return;
	}
	
	/**
	 * Handle Service Cancel
	 */
	
	public void stopService(CancelServiceDuccEvent duccEvent) {
		String methodName = "stopService";
		DuccId dwid = null;
		logger.trace(methodName, dwid, messages.fetch("enter"));
		Properties properties = duccEvent.getProperties();
		if(!isSignatureValid(properties)) {
			String error_message = messages.fetch(" type=authentication error, text=signature not valid.");
			logger.error(methodName, dwid, error_message);
			submitError(properties, error_message);
		}
		else if(Validate.request(duccEvent)) {
			// update state
			String jobId = properties.getProperty(JobRequestProperties.key_id);
			logger.info(methodName, jobid, JobRequestProperties.key_id+"="+jobId);
			long t0 = System.currentTimeMillis();
			DuccWorkJob duccWorkJob = (DuccWorkJob) WorkMapHelper.findDuccWork(workMap, DuccType.Service, jobId, this, methodName);
			long t1 = System.currentTimeMillis();
			long elapsed = t1 - t0;
			if(elapsed > Constants.SYNC_LIMIT) {
				logger.debug(methodName, dwid, "elapsed msecs: "+elapsed);
			}
			DuccContext context = duccEvent.getContext();
			if(duccWorkJob != null) {
				dwid = duccWorkJob.getDuccId();
				String reqUser = properties.getProperty(JobRequestProperties.key_user).trim();
				String reqRole = getRole(properties);
				String tgtUser = duccWorkJob.getStandardInfo().getUser().trim();
				if(isAuthorized(dwid, reqUser, tgtUser, reqRole)) {
					logger.debug(methodName, dwid, "reqUser:"+reqUser+" "+"reqRole:"+reqRole+" "+"tgtUser:"+tgtUser);
					String type;
					switch(context) {
					case ManagedReservation:
						type = "managed reservation";
						break;
					default:
						type = "service";
						break;
					}
					String givenReason = properties.getProperty(SpecificationProperties.key_reason);
					Reason reason = new Reason(dwid, reqUser, reqRole, givenReason);
					IRationale rationale = new Rationale(reason.toString());
					JobCompletionType jobCompletionType = JobCompletionType.CanceledByUser;
					if(reqRole.equals(SpecificationProperties.key_role_administrator)) {
						jobCompletionType = JobCompletionType.CanceledByAdministrator;
					}
					stateManager.jobTerminate(duccWorkJob, jobCompletionType, rationale, ProcessDeallocationType.ServiceStopped);
					OrchestratorCheckpoint.getInstance().saveState();
					// prepare for reply to canceler
					properties.put(JobReplyProperties.key_message, JobReplyProperties.msg_canceled);
					duccEvent.setProperties(properties);
					logger.info(methodName, dwid, messages.fetchLabel(type+" state")+duccWorkJob.getJobState());
				}
				else {
					// prepare not authorized reply 
					properties.put(JobReplyProperties.key_message, JobReplyProperties.msg_user_not_authorized);
					duccEvent.setProperties(properties);
					logger.info(methodName, dwid, jobId+" : "+messages.fetch(JobReplyProperties.msg_user_not_authorized));
				}
			}
			else {
				// prepare undefined reply 
				String message;
				switch(context) {
				case ManagedReservation:
					message = JobReplyProperties.msg_managed_reservation_not_found;
					break;
				default:
					message = JobReplyProperties.msg_service_not_found;
					break;
				}
				properties.put(JobReplyProperties.key_message, message);
				duccEvent.setProperties(properties);
				logger.info(methodName, dwid, jobId+" : "+messages.fetch(message));
			}
		}
		else {
			logger.info(methodName, dwid, messages.fetch("TODO")+" prepare error reply");
			//TODO
		}
		logger.trace(methodName, dwid, messages.fetch("exit"));
		return;
	}
	
	//==================================================
	
	/**
	 * See ResourceManagerComponent.  This code is a replication of that code for 
	 * the purpose of routing requests CLI/API -> OR -> RM to support a single point
	 * of entry and to record request/responses in the system-events.log.
	 */
	
	private String RMAdminRoute = "RMAdminRoute";
	
    /**
     * Creates Camel Router for Ducc RM admin events.
     * 
     * @param endpoint
     *          - ducc admin endpoint
     * @param delegate
     *          - who to call when admin event arrives
     * @throws Exception
     */
    private void startRmAdminChannel(final String endpoint, final AbstractDuccComponent delegate)
        throws Exception 
    {
    	String methodName = "startRmAdminChannel";
        getContext().addRoutes(new RouteBuilder() {
                public void configure() {
                	String methodName = "startRmAdminChannel.configure";
                	if (logger != null) {
                        logger.info(methodName, null, "Admin Channel Configure on endpoint:" + endpoint);
                    }
                    onException(Exception.class).handled(true).process(new ErrorProcessor());
                    
                    from(endpoint).routeId(RMAdminRoute).unmarshal().xstream()
                         .process(new RmAdminEventProcessor(delegate));
                }
            });

        getContext().startRoute(RMAdminRoute);
        if (logger != null) {
            logger.info(methodName, null, "Admin Channel Activated on endpoint:" + endpoint);
        }
    }

    class RmAdminEventProcessor implements Processor 
    {
        final AbstractDuccComponent delegate;

        private DuccRmAdmin admin = new DuccRmAdmin(new DefaultCamelContext(), "ducc.rm.admin.endpoint");
        
        public RmAdminEventProcessor(final AbstractDuccComponent delegate) 
        {
            this.delegate = delegate;
        }

        public void process(final Exchange exchange) 
            throws Exception 
        {            
            String location = "RmAdminEventProcessor.process";
            Object body = exchange.getIn().getBody();
            logger.info(location, jobid, "Received Admin Message of Type:",  body.getClass().getName());
            RmAdminReply reply = null;
            if ( body instanceof DuccAdminEvent ) {
                DuccAdminEvent dae = (DuccAdminEvent) body;
                try {
                	logger.debug(location, jobid, "dispatch");
                	SystemEventsLogger.info(IDuccLoggerComponents.abbrv_resourceManager, dae);
                	reply = admin.dispatchAndWaitForReply(dae);
                	logger.debug(location, jobid, "dispatch completed");
                	SystemEventsLogger.info(IDuccLoggerComponents.abbrv_resourceManager, dae, reply);
                }
                catch(Exception e) {
                	logger.error(location, jobid, e);
                }
            } 
            else {
                logger.info(location, jobid, "Invalid RM event:", body.getClass().getName());
                reply = new RmAdminReply();
                reply.setMessage("Unrecognized RM event.");
            }
            exchange.getIn().setBody(reply);
        }
    }

}
