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
import org.apache.uima.ducc.common.NodeIdentity;
import org.apache.uima.ducc.common.boot.DuccDaemonRuntimeProperties;
import org.apache.uima.ducc.common.boot.DuccDaemonRuntimeProperties.DaemonName;
import org.apache.uima.ducc.common.component.AbstractDuccComponent;
import org.apache.uima.ducc.common.config.CommonConfiguration;
import org.apache.uima.ducc.common.crypto.Crypto;
import org.apache.uima.ducc.common.crypto.Crypto.AccessType;
import org.apache.uima.ducc.common.crypto.CryptoException;
import org.apache.uima.ducc.common.internationalization.Messages;
import org.apache.uima.ducc.common.main.DuccService;
import org.apache.uima.ducc.common.system.SystemState;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.DuccPropertiesResolver;
import org.apache.uima.ducc.common.utils.LinuxUtils;
import org.apache.uima.ducc.common.utils.TimeStamp;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.orchestrator.OrchestratorConstants.StartType;
import org.apache.uima.ducc.orchestrator.maintenance.MaintenanceThread;
import org.apache.uima.ducc.orchestrator.maintenance.NodeAccounting;
import org.apache.uima.ducc.transport.event.CancelJobDuccEvent;
import org.apache.uima.ducc.transport.event.CancelReservationDuccEvent;
import org.apache.uima.ducc.transport.event.CancelServiceDuccEvent;
import org.apache.uima.ducc.transport.event.JdStateDuccEvent;
import org.apache.uima.ducc.transport.event.NodeInventoryUpdateDuccEvent;
import org.apache.uima.ducc.transport.event.OrchestratorAbbreviatedStateDuccEvent;
import org.apache.uima.ducc.transport.event.OrchestratorStateDuccEvent;
import org.apache.uima.ducc.transport.event.RmStateDuccEvent;
import org.apache.uima.ducc.transport.event.SmStateDuccEvent;
import org.apache.uima.ducc.transport.event.SubmitJobDuccEvent;
import org.apache.uima.ducc.transport.event.SubmitReservationDuccEvent;
import org.apache.uima.ducc.transport.event.SubmitServiceDuccEvent;
import org.apache.uima.ducc.transport.event.cli.JobReplyProperties;
import org.apache.uima.ducc.transport.event.cli.JobRequestProperties;
import org.apache.uima.ducc.transport.event.cli.JobSpecificationProperties;
import org.apache.uima.ducc.transport.event.cli.ReservationReplyProperties;
import org.apache.uima.ducc.transport.event.cli.ReservationRequestProperties;
import org.apache.uima.ducc.transport.event.cli.SpecificationProperties;
import org.apache.uima.ducc.transport.event.common.DuccWorkJob;
import org.apache.uima.ducc.transport.event.common.DuccWorkMap;
import org.apache.uima.ducc.transport.event.common.DuccWorkReservation;
import org.apache.uima.ducc.transport.event.common.IDuccCompletionType.JobCompletionType;
import org.apache.uima.ducc.transport.event.common.IDuccCompletionType.ReservationCompletionType;
import org.apache.uima.ducc.transport.event.common.IDuccProcess;
import org.apache.uima.ducc.transport.event.common.IDuccProcess.ReasonForStoppingProcess;
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
import org.apache.uima.ducc.transport.event.common.Rationale;
import org.apache.uima.ducc.transport.event.jd.DriverStatusReport;
import org.apache.uima.ducc.transport.event.rm.IRmJobState;
import org.apache.uima.ducc.transport.event.sm.ServiceMap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({CommonConfiguration.class})
public class OrchestratorComponent extends AbstractDuccComponent 
implements Orchestrator {
	//	Springframework magic to inject instance of {@link CommonConfiguration}
	@Autowired CommonConfiguration common;
	private static final DuccLogger logger = DuccLoggerComponents.getOrLogger(OrchestratorComponent.class.getName());
	
	private OrchestratorCommonArea orchestratorCommonArea = OrchestratorCommonArea.getInstance();
	private Messages messages = orchestratorCommonArea.getSystemMessages();
	private DuccWorkMap workMap = orchestratorCommonArea.getWorkMap();
	private StateManager stateManager = StateManager.getInstance();
	//private HealthMonitor healthMonitor = HealthMonitor.getInstance();
	//private MqReaper mqReaper = MqReaper.getInstance();
	private JobFactory jobFactory = JobFactory.getInstance();
	private ReservationFactory reservationFactory = ReservationFactory.getInstance();
	private CommonConfiguration commonConfiguration = orchestratorCommonArea.getCommonConfiguration();
	private JobDriverHostManager hostManager = orchestratorCommonArea.getHostManager();
	private StateJobAccounting stateJobAccounting = StateJobAccounting.getInstance();
	
	public OrchestratorComponent(CamelContext context) {
		super("Orchestrator", context);
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
		String property = commonConfiguration.orchestratorStartType;
		if(property != null) {
			String startTypeProperty = property.trim().toLowerCase();
			if(startTypeProperty.equals("cold")) {
				startType = StartType.cold;
			}
			else if(startTypeProperty.equals("warm")) {
				startType = StartType.warm;
			}
			else if(startTypeProperty.equals("hot")) {
				startType = StartType.hot;
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
		String property = commonConfiguration.signatureRequired;
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
				else if(flag.equals(StartType.hot.toString())) {
					startType = StartType.hot;
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
		
	@Override
	public void start(DuccService service, String[] args) throws Exception {
		String methodName = "start";
		logger.trace(methodName, null, messages.fetch("enter"));
		try {
			StartType startType = getStartType(args);
			logger.info(methodName, null, "##### "+startType+" #####");
			boolean saveState = false;
			long t0 = System.currentTimeMillis();
			synchronized(workMap) {
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
							force(job, new Rationale("system warm start"));
							saveState = true;
							break;
						case hot:
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
							if(commonConfiguration.jdHostClass.equals(reservation.getSchedulingInfo().getSchedulingClass())) {
								cancel(reservation);
								saveState = true;
							}
							break;
						case hot:
							if(commonConfiguration.jdHostClass.equals(reservation.getSchedulingInfo().getSchedulingClass())) {
								IDuccReservationMap map = reservation.getReservationMap();
								Iterator<Entry<DuccId, IDuccReservation>> entries = map.entrySet().iterator();
								while(entries.hasNext()) {
									Entry<DuccId, IDuccReservation> entry = entries.next();
									NodeIdentity node = entry.getValue().getNodeIdentity();
									hostManager.addNode(node);
								}
							}
							break;
						}
						break;
					}
				}
			}
			long t1 = System.currentTimeMillis();
			long elapsed = t1 - t0;
			if(elapsed > Constants.SYNC_LIMIT) {
				logger.debug(methodName, null, "elapsed msecs: "+elapsed);
			}
			if(saveState) {
				OrchestratorCheckpoint.getInstance().saveState();
			}
			switch(startType) {
			case cold:
			case warm:
				hostManager = JobDriverHostManager.getInstance();
				hostManager.init();
				break;
			case hot:
				hostManager = JobDriverHostManager.getInstance();
				hostManager.conditional();
				break;
			}
			resolveSignatureRequired();
			MaintenanceThread.getInstance().start();
		} 
		catch(Throwable t) {
			logger.error(methodName, null, t);
		}
		super.start(service, args);
		DuccDaemonRuntimeProperties.getInstance().boot(DaemonName.Orchestrator,getProcessJmxUrl());
		logger.trace(methodName, null, messages.fetch("exit"));
	}
	
	/**
	 * Job Driver State Reconciliation
	 */
	@Override
	public void reconcileJdState(JdStateDuccEvent duccEvent) {
		String methodName = "reconcileJdState";
		DriverStatusReport dsr = duccEvent.getState();
		DuccId duccId = null;
		if(dsr != null) {
			duccId = dsr.getDuccId();
		}
		logger.trace(methodName, null, messages.fetch("enter"));
		if(dsr != null) {
			logger.info(methodName, duccId, dsr.getLogReport());
			stateManager.reconcileState(dsr);
		}
		logger.trace(methodName, null, messages.fetch("exit"));
	}
	/**
	 * Resources Manager State Reconciliation
	 */
	@Override
	public void reconcileRmState(RmStateDuccEvent duccEvent) {
		String methodName = "reconcileRmState";
		logger.trace(methodName, null, messages.fetch("enter"));
		Map<DuccId, IRmJobState> resourceMap = duccEvent.getJobState();
		try {
			stateManager.reconcileState(resourceMap);
		}
		catch(Exception e) {
			logger.error(methodName, null, e);
		}
		
		logger.trace(methodName, null, messages.fetch("exit"));
	}
	/**
	 * Services Manager State Reconciliation
	 */
	@Override
	public void reconcileSmState(SmStateDuccEvent duccEvent) {
		String methodName = "reconcileSmState";
		logger.trace(methodName, null, messages.fetch("enter"));
		ServiceMap serviceMap = duccEvent.getServiceMap();
		stateManager.reconcileState(serviceMap);
		logger.trace(methodName, null, messages.fetch("exit"));
	}
	/**
	 * Node Inventory State Reconciliation
	 */
	@Override
	public void reconcileNodeInventory(NodeInventoryUpdateDuccEvent duccEvent) {
		String methodName = "reconcileNodeInventory";
		logger.trace(methodName, null, messages.fetch("enter"));
		HashMap<DuccId, IDuccProcess> processMap = duccEvent.getProcesses();
		stateManager.reconcileState(processMap);
		NodeAccounting.getInstance().heartbeat(processMap);
		logger.trace(methodName, null, messages.fetch("exit"));
	}
	/**
	 * Publish Orchestrator State
	 */
	@Override
	public OrchestratorStateDuccEvent getState() {
		String methodName = "getState";
		logger.trace(methodName, null, messages.fetch("enter"));
		OrchestratorStateDuccEvent orchestratorStateDuccEvent = new OrchestratorStateDuccEvent();
		try {
			long t0 = System.currentTimeMillis();
			DuccWorkMap workMapCopy = workMap.deepCopy();
			long t1 = System.currentTimeMillis();
			long elapsed = t1 - t0;
			if(elapsed > Constants.SYNC_LIMIT) {
				logger.debug(methodName, null, "elapsed msecs: "+elapsed);
			}
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
	/**
	 * Publish Orchestrator Abbreviated State
	 */
	@Override
	public OrchestratorAbbreviatedStateDuccEvent getAbbreviatedState() {
		String methodName = "getAbbreviatedState";
		logger.trace(methodName, null, messages.fetch("enter"));
		OrchestratorAbbreviatedStateDuccEvent orchestratorAbbreviatedStateDuccEvent = new OrchestratorAbbreviatedStateDuccEvent();
		try {
			long t0 = System.currentTimeMillis();
			DuccWorkMap workMapCopy = workMap.deepCopy();
			long t1 = System.currentTimeMillis();
			long elapsed = t1 - t0;
			if(elapsed > Constants.SYNC_LIMIT) {
				logger.debug(methodName, null, "elapsed msecs: "+elapsed);
			}
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
			long t2 = System.currentTimeMillis();
			orchestratorAbbreviatedStateDuccEvent.setWorkMap(workMapCopy);
			long t3 = System.currentTimeMillis();
			long elapsed2 = t3 - t2;
			if(elapsed > Constants.SYNC_LIMIT) {
				logger.debug(methodName, null, "elapsed msecs: "+elapsed2);
			}
			//stateManager.prune(workMapCopy);
			//healthMonitor.cancelNonViableJobs();
			//mqReaper.removeUnusedJdQueues(workMapCopy);
		}
		catch(Throwable t) {
			logger.error(methodName, null, t);
		}
		logger.trace(methodName, null, messages.fetch("exit"));
		return orchestratorAbbreviatedStateDuccEvent;
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
	private boolean isSignatureInvalid(Properties properties) {
		String methodName = "isSignatureInvalid";
		boolean retVal = false;
		try {
			if(orchestratorCommonArea.isSignatureRequired()) {
				String user = properties.getProperty(SpecificationProperties.key_user);
				String userHome = LinuxUtils.getUserHome(user);
				String runmode = DuccPropertiesResolver.getInstance().getProperty(DuccPropertiesResolver.ducc_runmode);
				if(runmode != null) {
					if(runmode.equals("Test")) {
						userHome = System.getProperty("user.home");
					}
				}
				Crypto crypto = new Crypto(userHome,AccessType.READER);
				String signature = (String)crypto.decrypt((byte[])properties.get(SpecificationProperties.key_signature));
				if(!user.equals(signature)) {
					logger.warn(methodName, null, "user:"+user+" signature:"+signature+" valid:n");
				}
				else {
					logger.debug(methodName, null, "user:"+user+" signature:"+signature+" valid:y");
				}
				retVal = !user.equals(signature);
			}
		}
		catch(Throwable t) {
			logger.error(methodName, null, t);
		}
		return retVal;
	}
	/**
	 * Handle Job Submit
	 */
	@Override
	public void startJob(SubmitJobDuccEvent duccEvent) {
		String methodName = "startJob";
		logger.trace(methodName, null, messages.fetch("enter"));
		try {
			JobRequestProperties properties = (JobRequestProperties) duccEvent.getProperties();
			int jdNodes = hostManager.nodes();
			if(isSignatureInvalid(properties)) {
				String error_message = messages.fetch(" type=authentication error, text=signature not valid.");
				logger.error(methodName, null, error_message);
				submitError(properties, error_message);
			}
			else if(jdNodes <= 0) {
				String error_message = messages.fetch(" type=system error, text=job driver node unavailable.");
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
					DuccWorkJob duccWorkJob = jobFactory.create(common,properties);
					long t0 = System.currentTimeMillis();
					workMap.addDuccWork(duccWorkJob);
					long t1 = System.currentTimeMillis();
					long elapsed = t1 - t0;
					if(elapsed > Constants.SYNC_LIMIT) {
						logger.debug(methodName, null, "elapsed msecs: "+elapsed);
					}
					// state: Received
					stateJobAccounting.stateChange(duccWorkJob, JobState.Received);
					OrchestratorCheckpoint.getInstance().saveState();
					// state: WaitingForDriver
					stateJobAccounting.stateChange(duccWorkJob, JobState.WaitingForDriver);
					OrchestratorCheckpoint.getInstance().saveState();
					// prepare for reply to submitter
					properties.put(JobRequestProperties.key_id, duccWorkJob.getId());
					duccEvent.setProperties(properties);
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
	@Override
	public void stopJob(CancelJobDuccEvent duccEvent) {
		String methodName = "stopJob";
		logger.trace(methodName, null, messages.fetch("enter"));
		Properties properties = duccEvent.getProperties();
		if(isSignatureInvalid(properties)) {
			String error_message = messages.fetch(" type=authentication error, text=signature not valid.");
			logger.error(methodName, null, error_message);
			submitError(properties, error_message);
		}
		else if(Validate.request(duccEvent)) {
			// update state
			String jobId = properties.getProperty(JobRequestProperties.key_id);
			long t0 = System.currentTimeMillis();
			DuccWorkJob duccWorkJob = (DuccWorkJob) workMap.findDuccWork(DuccType.Job,jobId);
			long t1 = System.currentTimeMillis();
			long elapsed = t1 - t0;
			if(elapsed > Constants.SYNC_LIMIT) {
				logger.debug(methodName, null, "elapsed msecs: "+elapsed);
			}
			if(duccWorkJob != null) {
				String userid = properties.getProperty(JobSpecificationProperties.key_user);
				IRationale rationale = new Rationale("job canceled by userid "+userid);
				stateManager.jobTerminate(duccWorkJob, JobCompletionType.CanceledByUser, rationale, ProcessDeallocationType.JobCanceled);
				OrchestratorCheckpoint.getInstance().saveState();
				// prepare for reply to canceler
				properties.put(JobReplyProperties.key_message, JobReplyProperties.msg_canceled);
				duccEvent.setProperties(properties);
				logger.info(methodName, duccWorkJob.getDuccId(), messages.fetchLabel("job state")+duccWorkJob.getJobState());
			}
			else {
				// prepare undefined reply 
				properties.put(JobReplyProperties.key_message, JobReplyProperties.msg_job_not_found);
				duccEvent.setProperties(properties);
				logger.info(methodName, null, jobId+" : "+messages.fetch(JobReplyProperties.msg_job_not_found));
			}
		}
		else {
			logger.info(methodName, null, messages.fetch("TODO")+" prepare error reply");
			//TODO
		}
		logger.trace(methodName, null, messages.fetch("exit"));
		return;
	}
	
	@Override
	public void stopJobProcess(CancelJobDuccEvent duccEvent) {
		String methodName = "stopJobProcess";
		logger.trace(methodName, null, messages.fetch("enter"));
		Properties properties = duccEvent.getProperties();
		if(isSignatureInvalid(properties)) {
			String error_message = messages.fetch(" type=authentication error, text=signature not valid.");
			logger.error(methodName, null, error_message);
			submitError(properties, error_message);
		}
		else if(Validate.request(duccEvent)) {
			DuccId djid = null;
			String dpid = null;
			String jobId = properties.getProperty(JobRequestProperties.key_id);
			DuccWorkJob duccWorkJob = (DuccWorkJob) workMap.findDuccWork(DuccType.Job,jobId);
			if(duccWorkJob != null) {
				djid = duccWorkJob.getDuccId();
				dpid = properties.getProperty(JobReplyProperties.key_dpid);
				IDuccProcess idp = duccWorkJob.getProcess(dpid);
				if(idp != null) {
					switch(idp.getProcessState()) {
					case Starting:
					case Initializing:
					case Running:
						idp.setResourceState(ResourceState.Deallocated);
						idp.setProcessState(ProcessState.Abandoned);
						idp.setProcessDeallocationType(ProcessDeallocationType.Canceled);
						idp.setReasonForStoppingProcess(ReasonForStoppingProcess.UserInitiated.toString());
						// prepare process not active 
						properties.put(JobReplyProperties.key_message, JobReplyProperties.msg_process_canceled);
						duccEvent.setProperties(properties);
						logger.info(methodName, djid, dpid, messages.fetch(JobReplyProperties.msg_process_canceled));
						break;
					default:
						// prepare process not active 
						properties.put(JobReplyProperties.key_message, JobReplyProperties.msg_process_not_active);
						duccEvent.setProperties(properties);
						logger.info(methodName, djid, dpid, messages.fetch(JobReplyProperties.msg_process_not_active));
						break;
					}
				}
				else {
					// prepare process not found reply 
					properties.put(JobReplyProperties.key_message, JobReplyProperties.msg_process_not_found);
					duccEvent.setProperties(properties);
					logger.info(methodName, djid, dpid, messages.fetch(JobReplyProperties.msg_process_not_found));
				}
			}
			else {
				// prepare job not found 
				properties.put(JobReplyProperties.key_message, JobReplyProperties.msg_job_not_found);
				duccEvent.setProperties(properties);
				logger.info(methodName, djid, dpid, messages.fetch(JobReplyProperties.msg_job_not_found));
			}
		}
		else {
			logger.info(methodName, null, messages.fetch("TODO")+" prepare error reply");
			//TODO
		}
		logger.trace(methodName, null, messages.fetch("exit"));
		return;
	}
	
	@Override
	public void startReservation(SubmitReservationDuccEvent duccEvent) {
		String methodName = "startReservation";
		logger.trace(methodName, null, messages.fetch("enter"));	
		try {
			Properties properties = duccEvent.getProperties();
			if(isSignatureInvalid(properties)) {
				String error_message = messages.fetch(" type=authentication error, text=signature not valid.");
				logger.error(methodName, null, error_message);
				submitError(properties, error_message);
			}
			else if(Validate.request(duccEvent)) {
				DuccWorkReservation duccWorkReservation = reservationFactory.create(common,(ReservationRequestProperties)properties);
				long t0 = System.currentTimeMillis();
				workMap.addDuccWork(duccWorkReservation);
				long t1 = System.currentTimeMillis();
				long elapsed = t1 - t0;
				if(elapsed > Constants.SYNC_LIMIT) {
					logger.debug(methodName, null, "elapsed msecs: "+elapsed);
				}
				// state: Received
				duccWorkReservation.stateChange(ReservationState.Received);
				OrchestratorCheckpoint.getInstance().saveState();
				// state: WaitingForResources
				duccWorkReservation.stateChange(ReservationState.WaitingForResources);
				OrchestratorCheckpoint.getInstance().saveState();
				int counter = 0;
				while(duccWorkReservation.isPending()) {
					counter++;
					if(counter > 5) {
						counter = 0;
						logger.info(methodName, duccWorkReservation.getDuccId(), "waiting for allocation...");
					}
					Thread.sleep(1000);
				}
				// prepare for reply to submitter
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
	
	@Override
	public void stopReservation(CancelReservationDuccEvent duccEvent) {
		String methodName = "stopReservation";
		logger.trace(methodName, null, messages.fetch("enter"));
		Properties properties = duccEvent.getProperties();
		if(isSignatureInvalid(properties)) {
			String error_message = messages.fetch(" type=authentication error, text=signature not valid.");
			logger.error(methodName, null, error_message);
			submitError(properties, error_message);
		}
		else {
			// update state
			String id = properties.getProperty(ReservationRequestProperties.key_id);
			long t0 = System.currentTimeMillis();
			DuccWorkReservation duccWorkReservation = (DuccWorkReservation) workMap.findDuccWork(DuccType.Reservation,id);
			long t1 = System.currentTimeMillis();
			long elapsed = t1 - t0;
			if(elapsed > Constants.SYNC_LIMIT) {
				logger.debug(methodName, null, "elapsed msecs: "+elapsed);
			}
			if(Validate.request(duccEvent,duccWorkReservation)) {
				if(duccWorkReservation != null) {
					String cancelUser = properties.getProperty(SpecificationProperties.key_user);
					duccWorkReservation.getStandardInfo().setCancelUser(cancelUser);
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
					logger.info(methodName, duccWorkReservation.getDuccId(), messages.fetchLabel("reservation state")+duccWorkReservation.getReservationState());
				}
				else {
					// prepare undefined reply 
					properties.put(ReservationReplyProperties.key_message, ReservationReplyProperties.msg_not_found);
					duccEvent.setProperties(properties);
					logger.info(methodName, null, id+" : "+messages.fetch("reservation not found"));
				}
			}
			else {
				properties.put(ReservationReplyProperties.key_message, ReservationReplyProperties.msg_user_not_authorized);
				duccEvent.setProperties(properties);
				logger.info(methodName, null, id+" : "+messages.fetch("not authorized"));
			}
		}
		logger.trace(methodName, null, messages.fetch("exit"));
		return;
	}
	
	/**
	 * Handle Service Submit
	 */
	@Override
	public void startService(SubmitServiceDuccEvent duccEvent) {
		String methodName = "startService";
		logger.trace(methodName, null, messages.fetch("enter"));
		try {
			JobRequestProperties properties = (JobRequestProperties) duccEvent.getProperties();
			NodeIdentity nodeIdentity = hostManager.getNode();
			if(isSignatureInvalid(properties)) {
				String error_message = messages.fetch(" type=authentication error, text=signature not valid.");
				logger.error(methodName, null, error_message);
				submitError(properties, error_message);
			}
			else if(nodeIdentity == null) {
				String error_message = messages.fetch(" type=system error, text=job driver node unavailable.");
				logger.error(methodName, null, error_message);
				submitError(properties, error_message);
			}
			else if(!SystemState.getInstance().isAcceptJobs()) {
				String error_message = messages.fetch(" type=system error, text=system is not accepting new work at this time.");
				logger.error(methodName, null, error_message);
				submitError(properties, error_message);
			}
			else {
				logger.debug(methodName, null, messages.fetch("job driver host")+" "+messages.fetchLabel("IP")+nodeIdentity.getIp()+" "+messages.fetchLabel("name")+nodeIdentity.getName());
				if(Validate.request(duccEvent)) {
					DuccWorkJob duccWorkJob = jobFactory.create(common,properties);
					long t0 = System.currentTimeMillis();
					workMap.addDuccWork(duccWorkJob);
					long t1 = System.currentTimeMillis();
					long elapsed = t1 - t0;
					if(elapsed > Constants.SYNC_LIMIT) {
						logger.debug(methodName, null, "elapsed msecs: "+elapsed);
					}
					// state: Received
					stateJobAccounting.stateChange(duccWorkJob, JobState.Received);
					OrchestratorCheckpoint.getInstance().saveState();
					// state: WaitingForServices
					stateJobAccounting.stateChange(duccWorkJob, JobState.WaitingForServices);
					OrchestratorCheckpoint.getInstance().saveState();
					// prepare for reply to submitter
					properties.put(JobRequestProperties.key_id, duccWorkJob.getId());
					duccEvent.setProperties(properties);
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
	@Override
	public void stopService(CancelServiceDuccEvent duccEvent) {
		String methodName = "stopService";
		logger.trace(methodName, null, messages.fetch("enter"));
		Properties properties = duccEvent.getProperties();
		if(isSignatureInvalid(properties)) {
			String error_message = messages.fetch(" type=authentication error, text=signature not valid.");
			logger.error(methodName, null, error_message);
			submitError(properties, error_message);
		}
		else if(Validate.request(duccEvent)) {
			// update state
			String jobId = properties.getProperty(JobRequestProperties.key_id);
			long t0 = System.currentTimeMillis();
			DuccWorkJob duccWorkJob = (DuccWorkJob) workMap.findDuccWork(DuccType.Service,jobId);
			long t1 = System.currentTimeMillis();
			long elapsed = t1 - t0;
			if(elapsed > Constants.SYNC_LIMIT) {
				logger.debug(methodName, null, "elapsed msecs: "+elapsed);
			}
			if(duccWorkJob != null) {
				String userid = properties.getProperty(JobSpecificationProperties.key_user);
				IRationale rationale = new Rationale("service canceled by "+userid);
				stateManager.jobTerminate(duccWorkJob, JobCompletionType.CanceledByUser, rationale, ProcessDeallocationType.JobCanceled);
				OrchestratorCheckpoint.getInstance().saveState();
				// prepare for reply to canceler
				properties.put(JobReplyProperties.key_message, JobReplyProperties.msg_canceled);
				duccEvent.setProperties(properties);
				logger.info(methodName, duccWorkJob.getDuccId(), messages.fetchLabel("service state")+duccWorkJob.getJobState());
			}
			
			else {
				// prepare undefined reply 
				properties.put(JobReplyProperties.key_message, JobReplyProperties.msg_service_not_found);
				duccEvent.setProperties(properties);
				logger.info(methodName, null, jobId+" : "+messages.fetch(JobReplyProperties.msg_service_not_found));
			}
		}
		else {
			logger.info(methodName, null, messages.fetch("TODO")+" prepare error reply");
			//TODO
		}
		logger.trace(methodName, null, messages.fetch("exit"));
		return;
	}
}
