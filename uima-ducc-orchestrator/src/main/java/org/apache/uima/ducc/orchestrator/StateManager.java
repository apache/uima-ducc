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

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.uima.ducc.common.Node;
import org.apache.uima.ducc.common.NodeIdentity;
import org.apache.uima.ducc.common.internationalization.Messages;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.DuccPropertiesResolver;
import org.apache.uima.ducc.common.utils.TimeStamp;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.agent.IUimaPipelineAEComponent;
import org.apache.uima.ducc.transport.event.common.DuccProcess;
import org.apache.uima.ducc.transport.event.common.DuccReservation;
import org.apache.uima.ducc.transport.event.common.DuccWorkJob;
import org.apache.uima.ducc.transport.event.common.DuccWorkMap;
import org.apache.uima.ducc.transport.event.common.DuccWorkPopDriver;
import org.apache.uima.ducc.transport.event.common.DuccWorkReservation;
import org.apache.uima.ducc.transport.event.common.IDuccCompletionType.JobCompletionType;
import org.apache.uima.ducc.transport.event.common.IDuccCompletionType.ReservationCompletionType;
import org.apache.uima.ducc.transport.event.common.IDuccProcess;
import org.apache.uima.ducc.transport.event.common.IDuccProcessMap;
import org.apache.uima.ducc.transport.event.common.IDuccProcessType.ProcessType;
import org.apache.uima.ducc.transport.event.common.IDuccProcessWorkItems;
import org.apache.uima.ducc.transport.event.common.IDuccReservationMap;
import org.apache.uima.ducc.transport.event.common.IDuccState.JobState;
import org.apache.uima.ducc.transport.event.common.IDuccState.ReservationState;
import org.apache.uima.ducc.transport.event.common.IDuccTypes.DuccType;
import org.apache.uima.ducc.transport.event.common.IDuccUimaDeploymentDescriptor;
import org.apache.uima.ducc.transport.event.common.IDuccWork;
import org.apache.uima.ducc.transport.event.common.IDuccWorkJob;
import org.apache.uima.ducc.transport.event.common.IDuccWorkReservation;
import org.apache.uima.ducc.transport.event.common.IDuccWorkService;
import org.apache.uima.ducc.transport.event.common.IProcessState.ProcessState;
import org.apache.uima.ducc.transport.event.common.IRationale;
import org.apache.uima.ducc.transport.event.common.IResourceState.ProcessDeallocationType;
import org.apache.uima.ducc.transport.event.common.IResourceState.ResourceState;
import org.apache.uima.ducc.transport.event.common.Rationale;
import org.apache.uima.ducc.transport.event.common.history.HistoryPersistenceManager;
import org.apache.uima.ducc.transport.event.jd.DriverStatusReport;
import org.apache.uima.ducc.transport.event.jd.DuccProcessWorkItemsMap;
import org.apache.uima.ducc.transport.event.rm.IResource;
import org.apache.uima.ducc.transport.event.rm.IRmJobState;
import org.apache.uima.ducc.transport.event.sm.IService.ServiceState;
import org.apache.uima.ducc.transport.event.sm.ServiceDependency;
import org.apache.uima.ducc.transport.event.sm.ServiceMap;


public class StateManager {
	private static final DuccLogger logger = DuccLoggerComponents.getOrLogger(StateManager.class.getName());
	
	private static StateManager stateManager = new StateManager();
	
	public static StateManager getInstance() {
		return stateManager;
	}
	
	private long quantum_size_in_bytes = 0;
	
	public StateManager() {
		String ducc_rm_share_quantum = DuccPropertiesResolver.getInstance().getFileProperty(DuccPropertiesResolver.ducc_rm_share_quantum);
		long oneKB = 1024;
		long oneMB = 1024*oneKB;
		long oneGB = 1024*oneMB;
		quantum_size_in_bytes = Long.parseLong(ducc_rm_share_quantum) * oneGB;
	}
	
	private OrchestratorCommonArea orchestratorCommonArea = OrchestratorCommonArea.getInstance();
	private Messages messages = orchestratorCommonArea.getSystemMessages();
	private DuccWorkMap workMap = orchestratorCommonArea.getWorkMap();
	private ConcurrentHashMap<DuccId,DriverStatusReport> driverStatusReportMap = orchestratorCommonArea.getDriverStatusReportMap();
	private StateJobAccounting stateJobAccounting = StateJobAccounting.getInstance();
	
	HistoryPersistenceManager hpm = orchestratorCommonArea.getHistoryPersistencemanager();
	
	private boolean jobDriverTerminated(DuccWorkJob duccWorkJob) {
		String methodName = "jobDriverTerminated";
		boolean status = true;
		logger.trace(methodName, null, messages.fetch("enter"));
		IDuccProcessMap processMap = duccWorkJob.getDriver().getProcessMap();
		Iterator<DuccId> processMapIterator = processMap.keySet().iterator();
		while(processMapIterator.hasNext()) {
			DuccId duccId = processMapIterator.next();
			IDuccProcess process = processMap.get(duccId);
			if(process.isActive()) {
				logger.debug(methodName, duccId,  messages.fetch("processes active"));
				status = false;
			}
		}
		logger.trace(methodName, null, messages.fetch("exit"));
		return status;
	}
	
	private boolean jobProcessesTerminated(DuccWorkJob duccWorkJob) {
		String methodName = "jobProcessesTerminated";
		boolean status = true;
		logger.trace(methodName, null, messages.fetch("enter"));
		IDuccProcessMap processMap = duccWorkJob.getProcessMap();
		Iterator<DuccId> processMapIterator = processMap.keySet().iterator();
		while(processMapIterator.hasNext()) {
			DuccId duccId = processMapIterator.next();
			IDuccProcess process = processMap.get(duccId);
			if(process.isActive()) {
				logger.debug(methodName, duccId,  messages.fetch("processes active"));
				status = false;
			}
		}
		logger.trace(methodName, null, messages.fetch("exit"));
		return status;
	}
	
	private boolean allProcessesTerminated(DuccWorkJob duccWorkJob) {
		String methodName = "allProcessesTerminated";
		boolean status = false;
		logger.trace(methodName, null, messages.fetch("enter"));
		switch(duccWorkJob.getDuccType()) {
		case Job:
			if(jobDriverTerminated(duccWorkJob)) {
				if(jobProcessesTerminated(duccWorkJob)) {
					status = true;
					if(duccWorkJob.getStandardInfo().getDateOfShutdownProcessesMillis() <= 0) {
						duccWorkJob.getStandardInfo().setDateOfShutdownProcesses(TimeStamp.getCurrentMillis());
					}
				}
			}
			break;
		case Service:
			if(jobProcessesTerminated(duccWorkJob)) {
				status = true;
				if(duccWorkJob.getStandardInfo().getDateOfShutdownProcessesMillis() <= 0) {
					duccWorkJob.getStandardInfo().setDateOfShutdownProcesses(TimeStamp.getCurrentMillis());
				}
			}
			break;
		}
		logger.trace(methodName, null, messages.fetch("exit"));
		return status;
	}
	
	private long SECONDS = 1000;
	private long MINUTES = 60 * SECONDS;
	private long AgeTime = 1 * MINUTES;
	
	private boolean isAgedOut(IDuccWork duccWork) {
		String methodName = "isAgedOut";
		logger.trace(methodName, null, messages.fetch("enter"));
		boolean retVal = true;
		long endMillis = 0;
		long nowMillis = 0;
		long elapsed = 0;
		try {
			endMillis = duccWork.getStandardInfo().getDateOfCompletionMillis();
			nowMillis = System.currentTimeMillis();
			elapsed = (nowMillis - endMillis);
			if(elapsed <= AgeTime) {
				retVal = false;
			}
			endMillis = duccWork.getStandardInfo().getDateOfShutdownProcessesMillis();
			elapsed = (nowMillis - endMillis);
			if(elapsed <= AgeTime) {
				retVal = false;
			}
		}
		catch(Exception e) {
			logger.error(methodName, null, "nowMillis:"+endMillis+" "+"nowMillis:"+endMillis+" ", e);
		}
		logger.trace(methodName, null, messages.fetch("exit"));
		return retVal;
	}
	
	public boolean isSaved(IDuccWorkJob duccWorkJob) {
		String methodName = "isSaved";
		logger.trace(methodName, null, messages.fetch("enter"));
		boolean retVal = false;
		try {
			switch(duccWorkJob.getDuccType()) {
			case Job:
				hpm.jobSave(duccWorkJob);
				retVal = true;
				break;
			case Service:
				hpm.serviceSave((IDuccWorkService)duccWorkJob);
				retVal = true;
				break;
			}
		}
		catch(Exception e) {
			logger.error(methodName, duccWorkJob.getDuccId(), e);
		}
		logger.trace(methodName, null, messages.fetch("exit"));
		return retVal;
	}
	
	public boolean isSaved(IDuccWorkReservation duccWorkReservation) {
		String methodName = "isSaved";
		logger.trace(methodName, null, messages.fetch("enter"));
		boolean retVal = false;
		try {
			hpm.reservationSave(duccWorkReservation);
			retVal = true;
		}
		catch(Exception e) {
		}
		logger.trace(methodName, null, messages.fetch("exit"));
		return retVal;
	}
	
	public int prune(DuccWorkMap workMap) {
		String methodName = "prune";
		int changes = 0;
		logger.trace(methodName, null, messages.fetch("enter"));
		long t0 = System.currentTimeMillis();
		Iterator<DuccId> workMapIterator = workMap.keySet().iterator();
		while(workMapIterator.hasNext()) {
			DuccId duccId = workMapIterator.next();
			IDuccWork duccWork = workMap.findDuccWork(duccId);
			switch(duccWork.getDuccType()) {
			case Job:
			case Service:
				DuccWorkJob duccWorkJob = (DuccWorkJob)duccWork;
				if(duccWorkJob != null) {
					if(duccWorkJob.isCompleting() && allProcessesTerminated(duccWorkJob)) {
						stateJobAccounting.stateChange(duccWorkJob, JobState.Completed);
					}
					if(duccWorkJob.isCompleted() && allProcessesTerminated(duccWorkJob) && isSaved(duccWorkJob) && isAgedOut(duccWorkJob)) {
						workMap.removeDuccWork(duccId);
						driverStatusReportMap.remove(duccId);
						logger.info(methodName, duccId, messages.fetch("removed job"));
						changes ++;
						IDuccProcessMap processMap = duccWorkJob.getProcessMap();
						Iterator<DuccId> processMapIterator = processMap.keySet().iterator();
						while(processMapIterator.hasNext()) {
							DuccId processDuccId = processMapIterator.next();
							orchestratorCommonArea.getProcessAccounting().removeProcess(processDuccId);
							logger.info(methodName, duccId, messages.fetch("removed process")+" "+processDuccId.toString());
							changes ++;
						}
						logger.info(methodName, duccId, messages.fetch("processes inactive"));
					}
					else {
						logger.debug(methodName, duccId, messages.fetch("processes active"));
					}
				}
				break;
			case Reservation:
				DuccWorkReservation duccWorkReservation = (DuccWorkReservation)duccWork;
				if(duccWorkReservation != null) {
					if(duccWorkReservation.isCompleted() && isSaved(duccWorkReservation) && isAgedOut(duccWorkReservation)) {
						workMap.removeDuccWork(duccId);
						logger.info(methodName, duccId, messages.fetch("removed reservation"));
						changes ++;
					}
				}
				break;
			}
		}
		long t1 = System.currentTimeMillis();
		long elapsed = t1 - t0;
		if(elapsed > Constants.SYNC_LIMIT) {
			logger.debug(methodName, null, "elapsed msecs: "+elapsed);
		}
		logger.debug(methodName, null, "processToWorkMap.size()="+orchestratorCommonArea.getProcessAccounting().processCount());
		if(changes > 0) {
			OrchestratorCheckpoint.getInstance().saveState();
		}
		logger.trace(methodName, null, messages.fetch("exit"));
		return changes;
	}
	
	private int stateChange(DuccWorkJob duccWorkJob, JobState state) {
		stateJobAccounting.stateChange(duccWorkJob, state);
		return 1;
	}
	
	private int stateChange(DuccWorkReservation duccWorkReservation, ReservationState state) {
		duccWorkReservation.stateChange(state);
		return 1;
	}
	
	private void setJdJmxUrl(DuccWorkJob job, String jdJmxUrl) {
		if(jdJmxUrl != null) {
			DuccWorkPopDriver driver = job.getDriver();
			IDuccProcessMap processMap = driver.getProcessMap();
			if(processMap != null) {
				Collection<IDuccProcess> processCollection = processMap.values();
				Iterator<IDuccProcess> iterator = processCollection.iterator();
				while(iterator.hasNext()) {
					IDuccProcess process = iterator.next();
					process.setProcessJmxUrl(jdJmxUrl);
					
				}
			}
		}
	}
	
	private void copyProcessWorkItemsReport(DuccWorkJob job, DriverStatusReport jdStatusReport) {
		String methodName = "copyProcessWorkItemsReport";
		try {
			IDuccProcessMap processMap = job.getProcessMap();
			DuccProcessWorkItemsMap pwiMap = jdStatusReport.getDuccProcessWorkItemsMap();
			Iterator<DuccId> iterator = pwiMap.keySet().iterator();
			while(iterator.hasNext()) {
				DuccId processId = iterator.next();
				IDuccProcess process = processMap.get(processId);
				IDuccProcessWorkItems pwi = pwiMap.get(processId);
				process.setProcessWorkItems(pwi);
			}
		}
		catch(Throwable t) {
			logger.error(methodName, job.getDuccId(), t);
		}
	}
	
	private void copyDriverWorkItemsReport(DuccWorkJob job, DriverStatusReport jdStatusReport) {
		String methodName = "copyDriverWorkItemsReport";
		try {
			DuccProcessWorkItemsMap pwiMap = jdStatusReport.getDuccProcessWorkItemsMap();
			IDuccProcessWorkItems pwi = pwiMap.getTotals();
			DuccWorkPopDriver driver = job.getDriver();
			IDuccProcessMap processMap = driver.getProcessMap();
			if(processMap != null) {
				Iterator<DuccId> iterator = processMap.keySet().iterator();
				while(iterator.hasNext()) {
					DuccId processId = iterator.next();
					IDuccProcess process = processMap.get(processId);
					process.setProcessWorkItems(pwi);
				}
			}
		}
		catch(Throwable t) {
			logger.error(methodName, job.getDuccId(), t);
		}
	}
	
	/**
	 * JD reconciliation
	 */
	public void reconcileState(DriverStatusReport jdStatusReport) {
		String methodName = "reconcileState (JD)";
		logger.trace(methodName, null, messages.fetch("enter"));
		int changes = 0;
		long t0 = System.currentTimeMillis();
		synchronized(workMap) {
			DuccId duccId = jdStatusReport.getDuccId();
			DuccWorkJob duccWorkJob = (DuccWorkJob) workMap.findDuccWork(duccId);
			if(duccWorkJob != null) {
				String jdJmxUrl = jdStatusReport.getJdJmxUrl();
				setJdJmxUrl(duccWorkJob, jdJmxUrl);
				IDuccUimaDeploymentDescriptor uimaDeploymentDescriptor = jdStatusReport.getUimaDeploymentDescriptor();
				if(uimaDeploymentDescriptor != null) {
					boolean copyDD = true;
					if(copyDD) {
						duccWorkJob.setUimaDeployableConfiguration(uimaDeploymentDescriptor);
					}
				}
				//
				copyProcessWorkItemsReport(duccWorkJob, jdStatusReport);
				copyDriverWorkItemsReport(duccWorkJob, jdStatusReport);
				//
				switch(duccWorkJob.getJobState()) {
				case Completed:
					break;
				case Completing:
				default:
					driverStatusReportMap.put(duccId, jdStatusReport);
					break;
				}
				//
				if(jdStatusReport.getWorkItemsTotal() == 0) {
					jobTerminate(duccWorkJob, JobCompletionType.CanceledByDriver, new Rationale("no work items to process"), ProcessDeallocationType.JobCanceled);
				}
				else {
					switch(jdStatusReport.getDriverState()) {
					case NotRunning:
						break;
					case Initializing:	
						switch(duccWorkJob.getJobState()) {
						case WaitingForDriver:
							stateJobAccounting.stateChange(duccWorkJob, JobState.WaitingForServices);
							break;
						case Initializing:
							break;
						}
						break;
					case Running:
					case Idle:	
						if(jdStatusReport.isKillJob()) {
							IRationale rationale = jdStatusReport.getJobCompletionRationale();
							switch(duccWorkJob.getJobState()) {
							case WaitingForServices:
								if(rationale == null) {
									rationale = new Rationale("waiting for services");
								}
								else {
									if(rationale.isSpecified()) {
										String text = rationale.getText();
										rationale = new Rationale(text+": "+"waiting for services");
									}
									else {
										rationale = new Rationale("waiting for services");
									}
								}
								break;
							default:
								break;
							}
							jobTerminate(duccWorkJob, JobCompletionType.CanceledByDriver, rationale, ProcessDeallocationType.JobFailure);
							break;
						}
						switch(duccWorkJob.getJobState()) {
						case WaitingForDriver:
							stateJobAccounting.stateChange(duccWorkJob, JobState.WaitingForServices);
							break;
						case Initializing:
							stateJobAccounting.stateChange(duccWorkJob, JobState.Running);
							break;
						}
						break;
					case Completing:	
						if(!duccWorkJob.isFinished()) {
							stateJobAccounting.stateChange(duccWorkJob, JobState.Completing);
						}
						break;
					case Completed:
						if(!duccWorkJob.isCompleted()) {
							stateJobAccounting.stateChange(duccWorkJob, JobState.Completing);
							deallocateJobDriver(duccWorkJob, jdStatusReport);
							duccWorkJob.getStandardInfo().setDateOfCompletion(TimeStamp.getCurrentMillis());
							switch(jdStatusReport.getJobCompletionType()) {
							case EndOfJob:
								duccWorkJob.setCompletion(JobCompletionType.EndOfJob, new Rationale("state manager detected normal completion"));
								try {
									int errors = Integer.parseInt(duccWorkJob.getSchedulingInfo().getWorkItemsError());
									if(errors > 0) {
										duccWorkJob.setCompletion(JobCompletionType.Error, new Rationale("state manager detected errors="+errors));
									}
								}
								catch(Exception e) {
								}
								break;
							default:
								duccWorkJob.setCompletion(jdStatusReport.getJobCompletionType(),jdStatusReport.getJobCompletionRationale());
								break;
							}
						}
						break;
					case Undefined:
						break;
					}
				}
				//
				OrchestratorCommonArea.getInstance().getProcessAccounting().setStatus(jdStatusReport,duccWorkJob);
				if(deallocateIdleProcesses(duccWorkJob, jdStatusReport)) {
					changes++;
				}
				if(deallocateFailedProcesses(duccWorkJob, jdStatusReport)) {
					changes++;
				}
			}
			else {
				logger.warn(methodName, duccId, messages.fetch("not found"));
			}
		}
		long t1 = System.currentTimeMillis();
		long elapsed = t1 - t0;
		if(elapsed > Constants.SYNC_LIMIT) {
			logger.debug(methodName, null, "elapsed msecs: "+elapsed);
		}		
		if(changes > 0) {
			OrchestratorCheckpoint.getInstance().saveState();
		}
		logger.trace(methodName, null, messages.fetch("exit"));
	}
	
	public boolean isExcessCapacity(DuccWorkJob job, DriverStatusReport jdStatusReport) {
		String methodName = "isExcessCapacity";
		boolean retVal = false;
		if(jdStatusReport != null) {
			IDuccProcessMap processMap = job.getProcessMap();
			int threads_per_share = Integer.parseInt(job.getSchedulingInfo().getThreadsPerShare());
			long capacity = processMap.getUsableProcessCount() * threads_per_share;
			long total = jdStatusReport.getWorkItemsTotal();
			long done = jdStatusReport.getWorkItemsProcessingCompleted();
			long error = jdStatusReport.getWorkItemsProcessingError();
			long todo = total - (done + error);
			if(capacity > 0) {
				if(todo < capacity) {
				retVal = true;
				}
			}
			logger.info(methodName, job.getDuccId(), "todo:"+todo+" "+"capacity:"+capacity+" "+"excess:"+retVal);
		}
		else {
			logger.info(methodName, job.getDuccId(), "todo:"+"?"+" "+"capacity:"+"?"+" "+"excess:"+retVal);
		}
		return retVal;
	}
	
	private boolean deallocateIdleProcesses(DuccWorkJob job, DriverStatusReport jdStatusReport) {
		String methodName = "deallocateIdleProcesses";
		boolean retVal = false;
		if(!jdStatusReport.isPending()  && !jdStatusReport.isWorkItemPendingProcessAssignment()) {
			IDuccProcessMap processMap = job.getProcessMap();
			Iterator<DuccId> iterator = processMap.keySet().iterator();
			boolean excessCapacity = isExcessCapacity(job, jdStatusReport);
			while(iterator.hasNext() && excessCapacity) {
				DuccId duccId = iterator.next();
				IDuccProcess process = processMap.get(duccId);
				if(!process.isDeallocated()) {
					String nodeIP = process.getNodeIdentity().getIp();
					String PID = process.getPID();
					if(!jdStatusReport.isOperating(nodeIP, PID)) {
						process.setResourceState(ResourceState.Deallocated);
						process.setProcessDeallocationType(ProcessDeallocationType.Voluntary);
						logger.info(methodName, job.getDuccId(), process.getDuccId(), "deallocated");
						retVal = true;
						excessCapacity = isExcessCapacity(job, jdStatusReport);
					}
				}
			}
		}
		return retVal;
	}
	
	private boolean deallocateFailedProcesses(DuccWorkJob job, DriverStatusReport jdStatusReport) {
		String methodName = "deallocateFailedProcesses";
		boolean retVal = false;
		IDuccProcessMap processMap = job.getProcessMap();
		Iterator<DuccId> iterator = jdStatusReport.getKillDuccIds();
		while (iterator.hasNext()) {
			DuccId duccId = iterator.next();
			IDuccProcess process = processMap.get(duccId);
			if(process != null) {
				if(!process.isDeallocated()) {
					process.setResourceState(ResourceState.Deallocated);
					process.setProcessDeallocationType(ProcessDeallocationType.Exception);
					logger.info(methodName, job.getDuccId(), process.getDuccId(), "deallocated");
				}
			}
			else {
				logger.warn(methodName, job.getDuccId(), duccId, "not in process map");
			}
		}
		return retVal;
	}
	
	
	private boolean deallocateJobDriver(DuccWorkJob job, DriverStatusReport jdStatusReport) {
		String methodName = "deallocateJobDriver";
		boolean retVal = false;
		IDuccProcessMap processMap = job.getDriver().getProcessMap();
		Iterator<DuccId> iterator = processMap.keySet().iterator();
		while (iterator.hasNext()) {
			DuccId duccId = iterator.next();
			IDuccProcess process = processMap.get(duccId);
			if(process != null) {
				if(!process.isDeallocated()) {
					process.setResourceState(ResourceState.Deallocated);
					process.setProcessDeallocationType(ProcessDeallocationType.Voluntary);
					logger.info(methodName, job.getDuccId(), process.getDuccId(), "deallocated");
				}
			}
			else {
				logger.warn(methodName, job.getDuccId(), duccId, "not in process map");
			}
		}
		return retVal;
	}
	
	/**
	 * RM reconciliation
	 */
	public void reconcileState(Map<DuccId, IRmJobState> rmResourceStateMap) throws Exception {
		String methodName = "reconcileState (RM)";
		logger.trace(methodName, null, messages.fetch("enter"));
		logger.debug(methodName, null, messages.fetchLabel("size")+rmResourceStateMap.size());
		int changes = 0;
		long t0 = System.currentTimeMillis();
		synchronized(workMap) {
			Iterator<DuccId> rmResourceStateIterator = rmResourceStateMap.keySet().iterator();
			while(rmResourceStateIterator.hasNext()) {
				DuccId duccId = rmResourceStateIterator.next();
				IRmJobState rmResourceState = rmResourceStateMap.get(duccId);
				if(rmResourceState.getPendingAdditions() != null) {
					logger.debug(methodName, duccId, messages.fetchLabel("pending additions")+rmResourceState.getPendingAdditions().size());
				}
				if(rmResourceState.getPendingRemovals() != null) {
					logger.debug(methodName, duccId, messages.fetchLabel("pending removals")+rmResourceState.getPendingRemovals().size());
				}
				IDuccWork duccWork = workMap.findDuccWork(duccId);
				if(duccWork== null) {
					logger.debug(methodName, duccId, messages.fetch("not found"));
				}
				else {
					logger.debug(methodName, duccId, messages.fetchLabel("type")+duccWork.getDuccType());
					switch(duccWork.getDuccType()) {
					case Job:
						logger.debug(methodName, duccId, messages.fetch("processing job..."));
						DuccWorkJob duccWorkJob = (DuccWorkJob) duccWork;
						processPurger(duccWorkJob,rmResourceState.getResources());
						changes += processMapResourcesAdd(duccWorkJob,rmResourceState.getPendingAdditions());
						changes += processMapResourcesDel(duccWorkJob,rmResourceState.getPendingRemovals());
						JobState jobState = duccWorkJob.getJobState();
						logger.debug(methodName, duccId, messages.fetchLabel("job state")+jobState);
						switch(jobState) {
						case Received:
						case WaitingForDriver:
							logger.warn(methodName, duccId, messages.fetchLabel("unexpected state")+jobState);
							break;
						case WaitingForServices:
							logger.debug(methodName, duccId, messages.fetchLabel("unexpected state")+jobState);
							break;
						case WaitingForResources:
							if(rmResourceState.isRefused()) {
								duccWorkJob.getStandardInfo().setDateOfCompletion(TimeStamp.getCurrentMillis());
								duccWorkJob.setCompletionType(JobCompletionType.ResourcesUnavailable);
								duccWorkJob.setCompletionRationale(new Rationale("resource manager refused allocation: "+rmResourceState.getReason()));
								changes += stateChange(duccWorkJob,JobState.Completed);
								logger.warn(methodName, duccId, messages.fetchLabel("refused")+rmResourceState.getReason());
							}
							if(duccWorkJob.getProcessMap().size() > 0) {
								changes += stateChange(duccWorkJob,JobState.Initializing);
								logger.info(methodName, duccId, messages.fetchLabel("resources count")+duccWorkJob.getProcessMap().size());
							}
							break;
						case Initializing:
						case Running:
							if(duccWorkJob.getProcessMap().size() == 0) {
								changes += stateChange(duccWorkJob,JobState.WaitingForResources);
								logger.info(methodName, duccId, messages.fetchLabel("resources count")+duccWorkJob.getProcessMap().size());
							}
							break;
						case Completing:
						case Completed:
							logger.debug(methodName, duccId, messages.fetchLabel("unsuitable state")+jobState);
							break;
						case Undefined:
							logger.warn(methodName, duccId, messages.fetchLabel("unsuitable state")+jobState);
							break;
						}
						break;
					case Reservation:
						logger.debug(methodName, duccId, messages.fetch("processing reservation..."));
						DuccWorkReservation duccWorkReservation = (DuccWorkReservation) duccWork;
						changes += reservationMapResourcesAdd(duccWorkReservation,rmResourceState.getPendingAdditions());
						changes += reservationMapResourcesDel(duccWorkReservation,rmResourceState.getPendingRemovals());
						ReservationState reservationState = duccWorkReservation.getReservationState();
						switch(reservationState) {
						case Received:
							logger.warn(methodName, duccId, messages.fetchLabel("unexpected state")+reservationState);
							break;
						case WaitingForResources:
							if(rmResourceState.isRefused()) {
								duccWorkReservation.getStandardInfo().setDateOfCompletion(TimeStamp.getCurrentMillis());
								duccWorkReservation.setCompletionType(ReservationCompletionType.ResourcesUnavailable);
								duccWorkReservation.setCompletionRationale(new Rationale("resource manager refused allocation: "+rmResourceState.getReason()));
								changes += stateChange(duccWorkReservation,ReservationState.Completed);
								logger.warn(methodName, duccId, messages.fetchLabel("refused")+rmResourceState.getReason());
							}
							else {
								if(rmResourceState.getResources() != null) {
									if(!rmResourceState.getResources().isEmpty()) {
										changes += stateChange(duccWorkReservation,ReservationState.Assigned);
										logger.info(methodName, duccId, messages.fetchLabel("resources count")+rmResourceState.getResources().size());
									}
								}
								else {
									logger.info(methodName, duccId, messages.fetch("waiting...no resources?"));
								}
							}
							break;
						case Assigned:
							if(rmResourceState.getResources() != null) {
								if(rmResourceState.getResources().isEmpty()) {
									changes += stateChange(duccWorkReservation,ReservationState.Completed);
									logger.info(methodName, duccId, messages.fetchLabel("resources count")+rmResourceState.getResources().size());
								}
							}
							else {
								logger.info(methodName, duccId, messages.fetch("assigned...no resources?"));
							}
							break;
						case Completed:
							logger.debug(methodName, duccId, messages.fetchLabel("unsuitable state")+reservationState);
							break;
						case Undefined:
							logger.warn(methodName, duccId, messages.fetchLabel("unsuitable state")+reservationState);
							break;
						}
						break;
					case Service:
						logger.debug(methodName, duccId, messages.fetch("processing service..."));
						DuccWorkJob duccWorkService = (DuccWorkJob) duccWork;
						processPurger(duccWorkService,rmResourceState.getResources());
						changes += processMapResourcesAdd(duccWorkService,rmResourceState.getPendingAdditions());
						changes += processMapResourcesDel(duccWorkService,rmResourceState.getPendingRemovals());
						JobState serviceState = duccWorkService.getJobState();
						logger.debug(methodName, duccId, messages.fetchLabel("service state")+serviceState);
						switch(serviceState) {
						case Received:
							logger.warn(methodName, duccId, messages.fetchLabel("unexpected state")+serviceState);
							break;
						case WaitingForServices:
							logger.debug(methodName, duccId, messages.fetchLabel("unexpected state")+serviceState);
							break;
						case WaitingForResources:
							if(rmResourceState.isRefused()) {
								duccWorkService.getStandardInfo().setDateOfCompletion(TimeStamp.getCurrentMillis());
								duccWorkService.setCompletionType(JobCompletionType.ResourcesUnavailable);
								duccWorkService.setCompletionRationale(new Rationale("resource manager refused allocation: "+rmResourceState.getReason()));
								changes += stateChange(duccWorkService,JobState.Completed);
								logger.warn(methodName, duccId, messages.fetchLabel("refused")+rmResourceState.getReason());
							}
							if(duccWorkService.getProcessMap().size() > 0) {
								changes += stateChange(duccWorkService,JobState.Initializing);
								logger.info(methodName, duccId, messages.fetchLabel("resources count")+duccWorkService.getProcessMap().size());
							}
							break;
						case Initializing:
						case Running:
							if(duccWorkService.getProcessMap().size() == 0) {
								changes += stateChange(duccWorkService,JobState.WaitingForResources);
								logger.info(methodName, duccId, messages.fetchLabel("resources count")+duccWorkService.getProcessMap().size());
							}
							break;
						case Completing:
						case Completed:
							logger.debug(methodName, duccId, messages.fetchLabel("unsuitable state")+serviceState);
							break;
						case Undefined:
							logger.warn(methodName, duccId, messages.fetchLabel("unsuitable state")+serviceState);
							break;
						}
						break;
					}
				}
			}
			if(changes > 0) {
				OrchestratorCheckpoint.getInstance().saveState();
			}
		}
		long t1 = System.currentTimeMillis();
		long elapsed = t1 - t0;
		if(elapsed > Constants.SYNC_LIMIT) {
			logger.debug(methodName, null, "elapsed msecs: "+elapsed);
		}		logger.trace(methodName, null, messages.fetch("exit"));
	}
	
	private int processPurger(DuccWorkJob job,Map<DuccId, IResource> map) {
		String methodName = "processPurger";
		logger.trace(methodName, null, messages.fetch("enter"));
		int changes = 0;
		if(job != null) {
			if(map != null) {
				Iterator<DuccId> iterator = map.keySet().iterator();
				while(iterator.hasNext()) {
					DuccId duccId = iterator.next();
					IResource resource = map.get(duccId);
					if(resource.isPurged()) {
						IDuccProcess process = job.getProcessMap().get(duccId);
						if(!process.isDefunct()) {
							String rState = process.getResourceState().toString();
							String pState = process.getProcessState().toString();
							logger.info(methodName, job.getDuccId(), duccId, "rState:"+rState+" "+"pState"+pState);
							process.setResourceState(ResourceState.Deallocated);
							process.setProcessDeallocationType(ProcessDeallocationType.Purged);
							process.advanceProcessState(ProcessState.Stopped);
						}
					}
				}
			}
		}
		logger.trace(methodName, null, messages.fetch("exit"));
		return changes;
	}
	
	private int processMapResourcesAdd(DuccWorkJob duccWorkJob,Map<DuccId,IResource> resourceMap) {
		String methodName = "processMapResourcesAdd";
		logger.trace(methodName, null, messages.fetch("enter"));
		int changes = 0;
		if(resourceMap == null) {
			logger.info(methodName, duccWorkJob.getDuccId(), messages.fetch("no map found"));
		}
		else {
			IDuccProcessMap processMap = duccWorkJob.getProcessMap();
			Iterator<DuccId> resourceMapIterator = resourceMap.keySet().iterator();
			while(resourceMapIterator.hasNext()) {
				DuccId duccId = resourceMapIterator.next();
				IResource resource = resourceMap.get(duccId);
				Node node = resourceMap.get(duccId).getNode();
				NodeIdentity nodeId = node.getNodeIdentity();
				if(!processMap.containsKey(duccId)) {
					ProcessType processType = null;
					switch(duccWorkJob.getServiceDeploymentType()) {
					case custom:
					case other:
						processType = ProcessType.Pop;
						break;
					case uima:
					case unspecified:
						processType = ProcessType.Job_Uima_AS_Process;
						break;
					}
					DuccProcess process = new DuccProcess(duccId, node, processType);
					long process_max_size_in_bytes = quantum_size_in_bytes * resource.countShares();
					CGroupManager.assign(duccWorkJob.getDuccId(), process, process_max_size_in_bytes);
					orchestratorCommonArea.getProcessAccounting().addProcess(duccId, duccWorkJob.getDuccId());
					processMap.addProcess(process);
					process.setResourceState(ResourceState.Allocated);
					logger.info(methodName, duccWorkJob.getDuccId(), messages.fetch("resource added")
												+" "+messages.fetchLabel("process")+duccId.getFriendly()
												+" "+messages.fetchLabel("unique")+duccId.getUnique()
												+" "+messages.fetchLabel("name")+nodeId.getName()
												+" "+messages.fetchLabel("ip")+nodeId.getIp());
					changes++;
					// check on usefulness of recent allocation
					switch(duccWorkJob.getJobState()) {
					// allocation unnecessary if job is already completed
					case Completing:
					case Completed:
						process.setResourceState(ResourceState.Deallocated);
						process.setProcessDeallocationType(ProcessDeallocationType.Voluntary);
						process.advanceProcessState(ProcessState.Stopped);
						logger.warn(methodName, duccWorkJob.getDuccId(), 
								messages.fetch("resource allocated for completed job")
								+" "+
								messages.fetchLabel("process")+duccId.getFriendly()
								);
						break;
					default:
						// allocation unnecessary if job has excess capacity
						if(isExcessCapacity(duccWorkJob,driverStatusReportMap.get(duccId))) {
							process.setResourceState(ResourceState.Deallocated);
							process.setProcessDeallocationType(ProcessDeallocationType.Voluntary);
							process.advanceProcessState(ProcessState.Stopped);
							logger.warn(methodName, duccWorkJob.getDuccId(), 
									messages.fetch("resource allocated for over capacity job")
									+" "+
									messages.fetchLabel("process")+duccId.getFriendly()
									);
						}
						break;
					}
				}
				else {
					logger.warn(methodName, duccWorkJob.getDuccId(), messages.fetch("resource exists")
						+" "+messages.fetchLabel("process")+duccId.getFriendly()
						+" "+messages.fetchLabel("unique")+duccId.getUnique()
						+" "+messages.fetchLabel("name")+nodeId.getName()
						+" "+messages.fetchLabel("ip")+nodeId.getIp());
				}
			}
		}
		logger.trace(methodName, null, messages.fetch("exit"));
		return changes;
	}
	
	private int processMapResourcesDel(DuccWorkJob duccWorkJob,Map<DuccId,IResource> resourceMap) {
		String methodName = "processMapResourcesDel";
		logger.trace(methodName, duccWorkJob.getDuccId(), messages.fetch("enter"));
		int changes = 0;
		if(resourceMap == null) {
			logger.info(methodName, duccWorkJob.getDuccId(), messages.fetch("no map found"));
		}
		else {
			IDuccProcessMap processMap = duccWorkJob.getProcessMap();
			Iterator<DuccId> resourceMapIterator = resourceMap.keySet().iterator();
			logger.debug(methodName, duccWorkJob.getDuccId(), messages.fetchLabel("size")+processMap.size());
			while(resourceMapIterator.hasNext()) {
				DuccId duccId = resourceMapIterator.next();
				Node node = resourceMap.get(duccId).getNode();
				NodeIdentity nodeId = node.getNodeIdentity();
				logger.info(methodName, duccWorkJob.getDuccId(), messages.fetch("resource processing")
					+" "+messages.fetchLabel("process")+duccId.getFriendly()
					+" "+messages.fetchLabel("unique")+duccId.getUnique()
					+" "+messages.fetchLabel("name")+nodeId.getName()
					+" "+messages.fetchLabel("ip")+nodeId.getIp());
				if(processMap.containsKey(duccId)) {
					IDuccProcess process = processMap.get(duccId);
					process.setResourceState(ResourceState.Deallocated);
					process.setProcessDeallocationType(ProcessDeallocationType.Forced);
					logger.info(methodName, duccWorkJob.getDuccId(), messages.fetch("resource deallocated")
						+" "+messages.fetchLabel("process")+duccId.getFriendly()
						+" "+messages.fetchLabel("unique")+duccId.getUnique()
						+" "+messages.fetchLabel("name")+nodeId.getName()
						+" "+messages.fetchLabel("ip")+nodeId.getIp());
					/*
					if(process.isDefunct()) {
						orchestratorCommonArea.getProcessAccounting().removeProcess(duccId);
						processMap.removeProcess(duccId);
						logger.info(methodName, duccId, messages.fetch("remove resource")+" "+messages.fetchLabel("name")+nodeId.getName()+" "+messages.fetchLabel("ip")+nodeId.getIp());
						changes++;
					}
					*/
				}
				else {
					logger.info(methodName, duccWorkJob.getDuccId(), messages.fetch("resource not found")
						+" "+messages.fetchLabel("process")+duccId.getFriendly()
						+" "+messages.fetchLabel("unique")+duccId.getUnique()
						+" "+messages.fetchLabel("name")+nodeId.getName()
						+" "+messages.fetchLabel("ip")+nodeId.getIp());
				}
			}
		}
		logger.trace(methodName, duccWorkJob.getDuccId(), messages.fetch("exit"));
		return changes;
	}

	private int reservationMapResourcesAdd(DuccWorkReservation duccWorkReservation,Map<DuccId,IResource> resourceMap) {
		String methodName = "reservationMapResourcesAdd";
		logger.trace(methodName, null, messages.fetch("enter"));
		int changes = 0;
		IDuccReservationMap reservationMap = duccWorkReservation.getReservationMap();
		if(resourceMap != null) {
			Iterator<DuccId> resourceMapIterator = resourceMap.keySet().iterator();
			while(resourceMapIterator.hasNext()) {
				DuccId duccId = resourceMapIterator.next();
				IResource resource = resourceMap.get(duccId);
				Node node = resource.getNode();
				NodeIdentity nodeId = node.getNodeIdentity();
				int shares = resource.countShares();
				if(!reservationMap.containsKey(duccId)) {
					DuccReservation reservation = new DuccReservation(duccId, node, shares);
					reservationMap.addReservation(reservation);
					logger.info(methodName, duccId, messages.fetch("add resource")+" "+messages.fetchLabel("name")+nodeId.getName()+" "+messages.fetchLabel("ip")+nodeId.getIp());
					changes++;
				}
				else {
					logger.debug(methodName, duccId, messages.fetch("duplicate resource")+" "+messages.fetchLabel("name")+nodeId.getName()+" "+messages.fetchLabel("ip")+nodeId.getIp());
				}
			}
		}
		logger.trace(methodName, null, messages.fetch("exit"));
		return changes;
	}
	
	private int reservationMapResourcesDel(DuccWorkReservation duccWorkReservation,Map<DuccId,IResource> resourceMap) {
		String methodName = "processMapResourcesDel";
		logger.trace(methodName, null, messages.fetch("enter"));
		int changes = 0;
		IDuccReservationMap reservationMap = duccWorkReservation.getReservationMap();
		if(resourceMap != null) {
			Iterator<DuccId> resourceMapIterator = resourceMap.keySet().iterator();
			while(resourceMapIterator.hasNext()) {
				DuccId duccId = resourceMapIterator.next();
				Node node = resourceMap.get(duccId).getNode();
				NodeIdentity nodeId = node.getNodeIdentity();
				if(reservationMap.containsKey(duccId)) {
					reservationMap.removeReservation(duccId);
					logger.info(methodName, duccId, messages.fetch("remove resource")+" "+messages.fetchLabel("name")+nodeId.getName()+" "+messages.fetchLabel("ip")+nodeId.getIp());
					changes++;
				}
				else {
					logger.debug(methodName, duccId, messages.fetch("not found resource")+" "+messages.fetchLabel("name")+nodeId.getName()+" "+messages.fetchLabel("ip")+nodeId.getIp());
				}
			}
		}
		logger.trace(methodName, null, messages.fetch("exit"));
		return changes;
	}
	
	/**
	 * SM reconciliation
	 */
	private String getServiceDependencyMessages(ServiceDependency sd) {
		String retVal = null;
		Map<String, String> messages = sd.getMessages();
		if(messages != null) {
			StringBuffer sb = new StringBuffer();
			for(String endpoint : messages.keySet()) {
				sb.append(endpoint);
				sb.append(":");
				sb.append(messages.get(endpoint));
				sb.append(";");
			}
			retVal = sb.toString();
		}
		return retVal;
	}
	
	public void reconcileState(ServiceMap serviceMap) {
		String methodName = "reconcileState (SM)";
		logger.trace(methodName, null, messages.fetch("enter"));
		int changes = 0;
		Iterator<DuccId> serviceMapIterator = serviceMap.keySet().iterator();
		long t0 = System.currentTimeMillis();
		synchronized(workMap) {
			while(serviceMapIterator.hasNext()) {
				DuccId duccId = serviceMapIterator.next();
				ServiceDependency services = serviceMap.get(duccId);
				DuccWorkJob duccWorkJob = (DuccWorkJob) workMap.findDuccWork(duccId);
				if(duccWorkJob != null) {
					JobState jobState = duccWorkJob.getJobState();
					ServiceState serviceState = services.getState();
					switch(jobState) {
					case Received:
						logger.warn(methodName, duccId, messages.fetchLabel("unexpected job state")+jobState);
						break;
					case WaitingForDriver:
						logger.debug(methodName, duccId, messages.fetchLabel("pending job state")+jobState);
						break;
					case WaitingForServices:
						switch(serviceState) {
						case Waiting:
						case Initializing:
							break;
						case Available:
							stateJobAccounting.stateChange(duccWorkJob, JobState.WaitingForResources);
							changes++;
							logger.info(methodName, duccId, messages.fetchLabel("job state")+jobState+" "+messages.fetchLabel("services state")+serviceState);
							break;
						case NotAvailable:
                        case Stopping:
							stateJobAccounting.stateChange(duccWorkJob, JobState.Completing);
							duccWorkJob.getStandardInfo().setDateOfCompletion(TimeStamp.getCurrentMillis());
							String sdm = getServiceDependencyMessages(services);  
							IRationale rationale = new Rationale();
							if(sdm != null) {
								rationale = new Rationale("service manager reported "+sdm);
							}
							stateJobAccounting.complete(duccWorkJob, JobCompletionType.ServicesUnavailable, rationale);
							changes++;
							logger.info(methodName, duccId, messages.fetchLabel("job state")+jobState+" "+messages.fetchLabel("services state")+serviceState);
							break;
						case Undefined:
							logger.warn(methodName, duccId, messages.fetchLabel("job state")+jobState+" "+messages.fetchLabel("services state")+serviceState);
							break;
						}
						break;
					case WaitingForResources:
						logger.debug(methodName, duccId, messages.fetchLabel("job state")+jobState+" "+messages.fetchLabel("services state")+serviceState);
						break;
					case Initializing:
					case Running:
						logger.debug(methodName, duccId, messages.fetchLabel("job state")+jobState+" "+messages.fetchLabel("services state")+serviceState);
						break;
					case Completed:
						logger.debug(methodName, duccId, messages.fetchLabel("job state")+jobState+" "+messages.fetchLabel("services state")+serviceState);
						break;
					case Undefined:
						logger.warn(methodName, duccId, messages.fetchLabel("unexpected job state")+jobState);
						break;
					}
				}
				else {
					logger.debug(methodName, duccId, messages.fetch("job not found"));
				}
			}
			if(changes > 0) {
				OrchestratorCheckpoint.getInstance().saveState();
			}
		}
		long t1 = System.currentTimeMillis();
		long elapsed = t1 - t0;
		if(elapsed > Constants.SYNC_LIMIT) {
			logger.debug(methodName, null, "elapsed msecs: "+elapsed);
		}		
		logger.trace(methodName, null, messages.fetch("exit"));
	}
	
	/**
	 * Node Inventory reconciliation
	 */
	public void reconcileState(HashMap<DuccId, IDuccProcess> inventoryProcessMap) {
		String methodName = "reconcileState (Node Inventory)";
		logger.trace(methodName, null, messages.fetch("enter"));
		Iterator<DuccId> iterator = inventoryProcessMap.keySet().iterator();
		long t0 = System.currentTimeMillis();
		synchronized(workMap) {
			while(iterator.hasNext()) {
				DuccId processId = iterator.next();
				IDuccProcess inventoryProcess = inventoryProcessMap.get(processId);
				List<IUimaPipelineAEComponent> upcList = inventoryProcess.getUimaPipelineComponents();
				if(upcList != null) {
					Iterator<IUimaPipelineAEComponent> upcIterator = upcList.iterator();
					while(upcIterator.hasNext()) {
						IUimaPipelineAEComponent upc = upcIterator.next();
						logger.debug(methodName, null, processId, "pipelineInfo: "+inventoryProcess.getNodeIdentity()+" "+inventoryProcess.getPID()+" "+upc.getAeName()+" "+upc.getAeState()+" "+upc.getInitializationTime());
					}
				}
				ProcessType processType = inventoryProcess.getProcessType();
				if(processType != null) {
					DuccId jobId = OrchestratorCommonArea.getInstance().getProcessAccounting().getJobId(processId);
					if(jobId != null) {
						IDuccWork duccWork = workMap.findDuccWork(jobId);
						if(duccWork != null) {
							DuccType jobType = duccWork.getDuccType();
							switch(jobType) {
							case Job:
								switch(processType) {
								case Pop:
									OrchestratorCommonArea.getInstance().getProcessAccounting().setStatus(inventoryProcess);
									DuccWorkJob job = (DuccWorkJob) duccWork;
									switch(inventoryProcess.getProcessState()) {
									case Failed:
										if(inventoryProcess.getDuccId().getFriendly() == 0) {
											jobTerminate(job, JobCompletionType.DriverProcessFailed, new Rationale(inventoryProcess.getReasonForStoppingProcess()), inventoryProcess.getProcessDeallocationType());
										}
										else {
											jobTerminate(job, JobCompletionType.ProcessFailure, new Rationale(inventoryProcess.getReasonForStoppingProcess()), inventoryProcess.getProcessDeallocationType());
										}
										break;
									default:
										if(inventoryProcess.isComplete()) {
											OrchestratorCommonArea.getInstance().getProcessAccounting().deallocate(job,ProcessDeallocationType.Stopped);
											IRationale rationale = new Rationale("state manager reported as normal completion");
											String retVal = job.getSchedulingInfo().getWorkItemsError();
											if(!retVal.equals("0")) {
												rationale = new Rationale("state manager reported at least one work item error");
											}
											completeJob(job, rationale);
										}
										break;
									}
									break;
								case Service:
									logger.warn(methodName, jobId, processId, "unexpected process type: "+processType);
									break;
								case Job_Uima_AS_Process:
									OrchestratorCommonArea.getInstance().getProcessAccounting().setStatus(inventoryProcess);
									break;
								}
								break;
							case Service:
								DuccWorkJob service = (DuccWorkJob) duccWork;
								switch(processType) {
								case Pop:
									OrchestratorCommonArea.getInstance().getProcessAccounting().setStatus(inventoryProcess);
									if(inventoryProcess.isComplete()) {
										OrchestratorCommonArea.getInstance().getProcessAccounting().deallocate(service,ProcessDeallocationType.Stopped);
									}
									if(!service.hasAliveProcess()) {
										completeManagedReservation(service, new Rationale("state manager reported no viable service process exists, type="+processType));
									}
									break;
								case Service:
									OrchestratorCommonArea.getInstance().getProcessAccounting().setStatus(inventoryProcess);
									if(inventoryProcess.isComplete()) {
										OrchestratorCommonArea.getInstance().getProcessAccounting().deallocate(service,ProcessDeallocationType.Stopped);
									}
									if(!service.hasAliveProcess()) {
										completeJob(service, new Rationale("state manager reported no viable service process exists, type="+processType));
									}
									break;
								case Job_Uima_AS_Process:
									OrchestratorCommonArea.getInstance().getProcessAccounting().setStatus(inventoryProcess);
									if(inventoryProcess.isComplete()) {
										OrchestratorCommonArea.getInstance().getProcessAccounting().deallocate(service,ProcessDeallocationType.Stopped);
									}
									if(!service.hasAliveProcess()) {
										completeJob(service, new Rationale("state manager reported no viable service process exists, type="+processType));
									}
									break;
								}
								break;
							}
						}
						else {
							StringBuffer sb = new StringBuffer();
							sb.append("node:"+inventoryProcess.getNodeIdentity().getName());
							sb.append(" ");
							sb.append("PID:"+inventoryProcess.getPID());
							sb.append(" ");
							sb.append("type:"+inventoryProcess.getProcessType());
							logger.debug(methodName, jobId, sb);
						}
					}
					else {
						StringBuffer sb = new StringBuffer();
						sb.append("node:"+inventoryProcess.getNodeIdentity().getName());
						sb.append(" ");
						sb.append("PID:"+inventoryProcess.getPID());
						sb.append(" ");
						sb.append("type:"+inventoryProcess.getProcessType());
						logger.debug(methodName, jobId, sb);
					}
				}
				else {
					DuccId jobId = null;
					StringBuffer sb = new StringBuffer();
					sb.append("node:"+inventoryProcess.getNodeIdentity().getName());
					sb.append(" ");
					sb.append("PID:"+inventoryProcess.getPID());
					sb.append(" ");
					sb.append("type:"+inventoryProcess.getProcessType());
					logger.warn(methodName, jobId, sb);
				}
			}
		}
		long t1 = System.currentTimeMillis();
		long elapsed = t1 - t0;
		if(elapsed > Constants.SYNC_LIMIT) {
			logger.debug(methodName, null, "elapsed msecs: "+elapsed);
		}		
		logger.trace(methodName, null, messages.fetch("exit"));
	}
	
	private void advanceToCompleted(DuccWorkJob job) {
		switch(job.getJobState()) {
		case Completing:
		case Completed:
			break;
		default:
			if(job.getProcessMap().getAliveProcessCount() == 0) {
				stateJobAccounting.stateChange(job, JobState.Completing);
			}
		}
	}
	
	private void completeManagedReservation(DuccWorkJob service, IRationale rationale) {
		String location = "completeManagedReservation";
		DuccId jobid = null;
		try {
			
			jobid = service.getDuccId();
			Map<DuccId, IDuccProcess> map = service.getProcessMap().getMap();
			int size = map.size();
			if(size != 1) {
				logger.warn(location, jobid, "size: "+size);
				completeJob(service, rationale);
			}
			else {
				Iterator<DuccId> iterator = map.keySet().iterator();
				while(iterator.hasNext()) {
					DuccId key = iterator.next();
					IDuccProcess process = map.get(key);
					int code = process.getProcessExitCode();
					IRationale exitCode = new Rationale("code="+code);
					switch(service.getCompletionType()) {
					case Undefined:
						service.setCompletion(JobCompletionType.ProgramExit, exitCode);
						service.getStandardInfo().setDateOfCompletion(TimeStamp.getCurrentMillis());
						break;
					}
					advanceToCompleted(service);
					break;
				}
			}
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
			completeJob(service, rationale);
		}
	}
	
	private void completeJob(DuccWorkJob job, IRationale rationale) {
		String location = "completeJob";
		DuccId jobid = null;
		switch(job.getCompletionType()) {
		case Undefined:
			job.setCompletion(JobCompletionType.EndOfJob, rationale);
			job.getStandardInfo().setDateOfCompletion(TimeStamp.getCurrentMillis());
		case EndOfJob:
			if(job.getProcessFailureCount() > 0) {
				job.setCompletion(JobCompletionType.Error, rationale);
			}
			else if(job.getProcessInitFailureCount() > 0) {
				job.setCompletion(JobCompletionType.Error, rationale);
			}
			else {
				try {
					if(Integer.parseInt(job.getSchedulingInfo().getWorkItemsError()) > 0) {
						job.setCompletion(JobCompletionType.Error, rationale);
					}
				}
				catch(Exception e) {
					logger.error(location, jobid, e);
				}
			}
			break;
		default:
			break;
		}
		advanceToCompleted(job);
	}
	
	public void jobTerminate(IDuccWorkJob job, JobCompletionType jobCompletionType, IRationale rationale, ProcessDeallocationType processDeallocationType) {
		if(!job.isFinished()) {
			stateJobAccounting.stateChange(job, JobState.Completing);
			stateJobAccounting.complete(job, jobCompletionType, rationale);
			OrchestratorCommonArea.getInstance().getProcessAccounting().deallocate(job,processDeallocationType);
			job.getStandardInfo().setDateOfCompletion(TimeStamp.getCurrentMillis());
		}
	}
	
}
