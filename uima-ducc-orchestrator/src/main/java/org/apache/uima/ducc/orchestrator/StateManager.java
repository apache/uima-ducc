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
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.uima.ducc.common.Node;
import org.apache.uima.ducc.common.NodeIdentity;
import org.apache.uima.ducc.common.SizeBytes;
import org.apache.uima.ducc.common.container.FlagsHelper;
import org.apache.uima.ducc.common.internationalization.Messages;
import org.apache.uima.ducc.common.jd.files.workitem.IRemoteLocation;
import org.apache.uima.ducc.common.jd.files.workitem.RemoteLocation;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccPropertiesResolver;
import org.apache.uima.ducc.common.utils.DuccSchedulerClasses;
import org.apache.uima.ducc.common.utils.TimeStamp;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.orchestrator.ckpt.OrchestratorCheckpoint;
import org.apache.uima.ducc.orchestrator.user.UserLogging;
import org.apache.uima.ducc.orchestrator.utilities.TrackSync;
import org.apache.uima.ducc.transport.agent.IUimaPipelineAEComponent;
import org.apache.uima.ducc.transport.cmdline.ICommandLine;
import org.apache.uima.ducc.transport.cmdline.JavaCommandLine;
import org.apache.uima.ducc.transport.event.common.DuccProcess;
import org.apache.uima.ducc.transport.event.common.DuccReservation;
import org.apache.uima.ducc.transport.event.common.DuccUimaDeploymentDescriptor;
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
import org.apache.uima.ducc.transport.event.common.history.IHistoryPersistenceManager;
import org.apache.uima.ducc.transport.event.jd.IDriverStatusReport;
import org.apache.uima.ducc.transport.event.jd.IDuccProcessWorkItemsReport;
import org.apache.uima.ducc.transport.event.rm.IResource;
import org.apache.uima.ducc.transport.event.rm.IRmJobState;
import org.apache.uima.ducc.transport.event.sm.IService.ServiceState;
import org.apache.uima.ducc.transport.event.sm.ServiceDependency;
import org.apache.uima.ducc.transport.event.sm.ServiceMap;

public class StateManager {
	private static final DuccLogger logger = DuccLogger.getLogger(StateManager.class);
	private static final DuccId jobid = null;

	private static StateManager stateManager = new StateManager();

	public static StateManager getInstance() {
		return stateManager;
	}

	public StateManager() {
	}

	private OrchestratorCommonArea orchestratorCommonArea = OrchestratorCommonArea.getInstance();
	private Messages messages = orchestratorCommonArea.getSystemMessages();
	private StateJobAccounting stateJobAccounting = StateJobAccounting.getInstance();

	private IHistoryPersistenceManager hpm = orchestratorCommonArea.getHistoryPersistencemanager();

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
				logger.debug(methodName, duccWorkJob.getDuccId(),  messages.fetch("processes active:")+duccId);
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
				logger.debug(methodName, duccWorkJob.getDuccId(),  messages.fetch("processes active:")+duccId);
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
		default:
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
				logger.info(methodName, jobid, "Completion", elapsed, AgeTime);
				retVal = false;
			}
			endMillis = duccWork.getStandardInfo().getDateOfShutdownProcessesMillis();
			elapsed = (nowMillis - endMillis);
			if(elapsed <= AgeTime) {
				logger.info(methodName, jobid, "Shutdown", elapsed, AgeTime);
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
				hpm.saveJob(duccWorkJob);
				retVal = true;
				break;
			case Service:
				hpm.saveService((IDuccWorkService)duccWorkJob);
				retVal = true;
				break;
			default:
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
			hpm.saveReservation(duccWorkReservation);
			retVal = true;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		logger.trace(methodName, null, messages.fetch("exit"));
		return retVal;
	}

	private void dumper() {
		String location = "dumper";
		try {
			DuccWorkMap dwMap = orchestratorCommonArea.getWorkMap();
			for(DuccId duccId : dwMap.keySet()) {
				IDuccWork dw = dwMap.findDuccWork(duccId);
				if(dw != null) {
					logger.trace(location, duccId, "dw: "+dw.getDuccType());
				}
			}
			ConcurrentHashMap<DuccId, DuccId> p2jMap = ProcessToJobMap.getInstance().getMap();
			for(Entry<DuccId, DuccId> entry : p2jMap.entrySet()) {
				logger.trace(location, jobid, "p:"+entry.getKey()+" "+"j:"+entry.getValue());
			}
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
		}
	}
	
	
	private boolean isCompleting(DuccWorkJob dwj) {
		String location = "isCompleting";
		boolean retVal = false;
		DuccId duccId = null;
		if(dwj != null) {
			duccId = dwj.getDuccId();
			retVal = dwj.isCompleting();
		}
		logger.debug(location, duccId, retVal);
		return retVal;
	}
	
	private boolean isCompleted(DuccWorkJob dwj) {
		String location = "isCompleted";
		boolean retVal = false;
		DuccId duccId = null;
		if(dwj != null) {
			duccId = dwj.getDuccId();
			retVal = dwj.isCompleted();
		}
		logger.debug(location, duccId, retVal);
		return retVal;
	}

	public int prune(DuccWorkMap workMap) {
		String methodName = "prune";
		dumper();
		int changes = 0;
		logger.trace(methodName, null, messages.fetch("enter"));
		long t0 = System.currentTimeMillis();
		Set<DuccId> workMapSet = workMap.keySet();
		logger.debug(methodName, jobid, "size="+workMapSet.size());
		Iterator<DuccId> workMapIterator = workMapSet.iterator();
		while(workMapIterator.hasNext()) {
			DuccId duccId = workMapIterator.next();
			IDuccWork duccWork = WorkMapHelper.findDuccWork(workMap, duccId, this, methodName);
			if(duccWork == null) {
				logger.warn(methodName, duccId, "not found");
				continue;
			}
			DuccType duccType = duccWork.getDuccType();
			if(duccType == null) {
				logger.warn(methodName, duccId, "no type?");
				continue;
			}
			switch(duccType) {
			case Job:
			case Service:
				DuccWorkJob duccWorkJob = (DuccWorkJob)duccWork;
				if(duccWorkJob != null) {
					switch(duccType) {
					case Job:
						if(jobDriverTerminated(duccWorkJob)) {
							OrchestratorHelper.jdDeallocate(duccWorkJob);
							changes ++;
						}
						break;
					default:
						break;
					}
					if(isCompleting(duccWorkJob) && allProcessesTerminated(duccWorkJob)) {
						stateJobAccounting.stateChange(duccWorkJob, JobState.Completed);
						changes ++;
					}
					if(isCompleted(duccWorkJob) && allProcessesTerminated(duccWorkJob) && isSaved(duccWorkJob) && isAgedOut(duccWorkJob)) {
						WorkMapHelper.removeDuccWork(workMap, duccWorkJob, this, methodName);
						logger.info(methodName, duccId, messages.fetch("removed job"));
						changes ++;
						IDuccProcessMap processMap = null;
						DuccWorkPopDriver driver = duccWorkJob.getDriver();
						if(driver != null) {
							processMap = driver.getProcessMap();
						}
						if(processMap != null) {
							Iterator<DuccId> processMapIterator = processMap.keySet().iterator();
							while(processMapIterator.hasNext()) {
								DuccId processDuccId = processMapIterator.next();
								orchestratorCommonArea.getProcessAccounting().removeProcess(processDuccId);
								logger.info(methodName, duccId, messages.fetch("removed driver process")+" "+processDuccId.toString());
								changes ++;
							}
							logger.info(methodName, duccId, messages.fetch("processes driver inactive"));
						}
						processMap = duccWorkJob.getProcessMap();
						if(processMap != null) {
							Iterator<DuccId> processMapIterator = processMap.keySet().iterator();
							while(processMapIterator.hasNext()) {
								DuccId processDuccId = processMapIterator.next();
								orchestratorCommonArea.getProcessAccounting().removeProcess(processDuccId);
								logger.info(methodName, duccId, messages.fetch("removed process")+" "+processDuccId.toString());
								changes ++;
							}
							logger.info(methodName, duccId, messages.fetch("processes inactive"));
						}
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
						WorkMapHelper.removeDuccWork(workMap, duccWorkReservation, this, methodName);
						logger.info(methodName, duccId, messages.fetch("removed reservation"));
						changes ++;
					}
				}
				break;
			default:
				break;
			}
		}
		long t1 = System.currentTimeMillis();
		long elapsed = t1 - t0;
		if(elapsed > Constants.SYNC_LIMIT) {
			logger.debug(methodName, null, "elapsed msecs: "+elapsed);
		}
		logger.debug(methodName, null, "processToWorkMap.size="+orchestratorCommonArea.getProcessAccounting().processCount());
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

	private void copyInvestmentReport(DuccWorkJob job, IDriverStatusReport jdStatusReport) {
		String methodName = "copyInvestmentReport";
		try {
			Map<RemoteLocation, Long> omMap = jdStatusReport.getInvestmentMillisMap();
			IDuccProcessMap processMap = job.getProcessMap();
			for(Entry<DuccId, IDuccProcess> entry : processMap.entrySet()) {
				IDuccProcess process = entry.getValue();
				Node node = process.getNode();
				NodeIdentity nodeIdentity = node.getNodeIdentity();
				String nodeIP = nodeIdentity.getIp();
				String pid = process.getPID();
				RemoteLocation remoteLocation = new RemoteLocation(nodeIP, pid);
				long investment = 0;
				if(omMap.containsKey(remoteLocation)) {
					investment = omMap.get(remoteLocation).longValue();
				}
				process.setWiMillisInvestment(investment);
				logger.debug(methodName, job.getDuccId(), process.getDuccId(), "investment:"+investment+" "+"node(IP): "+nodeIP+" "+"pid: "+pid);
			}
		}
		catch(Exception e) {
			logger.error(methodName, job.getDuccId(), e);
		}
	}
	private void copyProcessWorkItemsReport(DuccWorkJob job, IDriverStatusReport jdStatusReport) {
		String methodName = "copyProcessWorkItemsReport";
		try {
			IDuccProcessMap processMap = job.getProcessMap();
			IDuccProcessWorkItemsReport pwiReport = jdStatusReport.getDuccProcessWorkItemsMap();
			if(pwiReport!= null) {
				ConcurrentHashMap<DuccId, IDuccProcessWorkItems> pwiMap = pwiReport.getMap();
				Iterator<DuccId> iterator = pwiMap.keySet().iterator();
				while(iterator.hasNext()) {
					DuccId processId = iterator.next();
					IDuccProcess process = processMap.get(processId);
					IDuccProcessWorkItems pwi = pwiMap.get(processId);
					process.setProcessWorkItems(pwi);
					logger.trace(methodName, job.getDuccId(), "done:"+pwi.getCountDone()+" "+"error:"+pwi.getCountError()+" "+"dispatch:"+pwi.getCountDispatch());
				}
			}
		}
		catch(Exception e) {
			logger.error(methodName, job.getDuccId(), e);
		}
	}

	private void copyDriverWorkItemsReport(DuccWorkJob job, IDriverStatusReport jdStatusReport) {
		String methodName = "copyDriverWorkItemsReport";
		try {
			IDuccProcessWorkItemsReport pwiReport = jdStatusReport.getDuccProcessWorkItemsMap();
			if(pwiReport != null) {
				IDuccProcessWorkItems pwi = pwiReport.getTotals();
				DuccWorkPopDriver driver = job.getDriver();
				IDuccProcessMap processMap = driver.getProcessMap();
				if(processMap != null) {
					Iterator<DuccId> iterator = processMap.keySet().iterator();
					while(iterator.hasNext()) {
						DuccId processId = iterator.next();
						IDuccProcess process = processMap.get(processId);
						process.setProcessWorkItems(pwi);
						logger.debug(methodName, job.getDuccId(), "done:"+pwi.getCountDone()+" "+"error:"+pwi.getCountError()+" "+"dispatch:"+pwi.getCountDispatch());
					}
				}
			}
			job.setWiMillisMin(jdStatusReport.getWiMillisMin());
			job.setWiMillisMax(jdStatusReport.getWiMillisMax());
			job.setWiMillisAvg(jdStatusReport.getWiMillisAvg());
			job.setWiMillisOperatingLeast(jdStatusReport.getWiMillisOperatingLeast());
			job.setWiMillisCompletedMost(jdStatusReport.getWiMillisCompletedMost());
		}
		catch(Exception e) {
			logger.error(methodName, job.getDuccId(), e);
		}
	}

	private void setCompletionIfNotAlreadySet(DuccWorkJob duccWorkJob, IDriverStatusReport jdStatusReport) {
		String methodName = "setCompletionIfNotAlreadySet";
		DuccId jobid = null;
		try {
			jobid = duccWorkJob.getDuccId();
			setCompletionIfNotAlreadySet(jobid, duccWorkJob, jdStatusReport.getJobCompletionType(), jdStatusReport.getJobCompletionRationale());
		}
		catch(Exception e) {
			logger.error(methodName, jobid, e);
		}
	}

	private void setCompletionIfNotAlreadySet(DuccWorkJob duccWorkJob, JobCompletionType jobCompletionType, IRationale rationale) {
		String methodName = "setCompletionIfNotAlreadySet";
		DuccId jobid = null;
		try {
			jobid = duccWorkJob.getDuccId();
			setCompletionIfNotAlreadySet(jobid, duccWorkJob, jobCompletionType,rationale);
		}
		catch(Exception e) {
			logger.error(methodName, jobid, e);
		}
	}

	private void setCompletionIfNotAlreadySet(DuccId jobid, DuccWorkJob duccWorkJob, JobCompletionType reqJobCompletionType, IRationale reqRationale) {
		String methodName = "setCompletionIfNotAlreadySet";
		logger.trace(methodName, null, messages.fetch("enter"));
		try {
			JobCompletionType curJobCompletionType = duccWorkJob.getCompletionType();
			switch(curJobCompletionType) {
			case Undefined:
				duccWorkJob.setCompletion(reqJobCompletionType, reqRationale);
				logger.debug(methodName, jobid, "changed: "+curJobCompletionType+" to "+reqJobCompletionType);
				break;
			default:
				logger.debug(methodName, jobid, "unchanged: "+curJobCompletionType+" to "+reqJobCompletionType);
				break;
			}

		}
		catch(Exception e) {
			logger.error(methodName, jobid, e);
		}
		logger.trace(methodName, null, messages.fetch("exit"));
	}

	private void addJdUrlToJpCommandLine(IDuccWorkJob dwj, IDriverStatusReport jdStatusReport) {
		String location = "addJdPortToJpCommandLine";
		DuccId jobid = null;
		if(!dwj.isJdURLSpecified()) {
			jobid = dwj.getDuccId();
			String node = jdStatusReport.getNode();
			int port = jdStatusReport.getPort();
			if(port > 0) {
				JavaCommandLine jcl = (JavaCommandLine) dwj.getCommandLine();
				// add port
				String opt;
				// format is http://<node>:<port>/jdApp
				String value = "http://"+node+":"+port+"/jdApp";
				opt = FlagsHelper.Name.JdURL.dname()+"="+value;
				jcl.addOption(opt);
				logger.info(location, jobid, opt);
				dwj.setJdURLSpecified();
			}
		}
	}

	private void addDeployableToJpCommandLine(IDuccWorkJob dwj, IDriverStatusReport jdStatusReport) {
		String location = "addDeployableToJpCommandLine";
		DuccId jobid = null;
		if(!dwj.isDdSpecified()) {
			//V1
			String jpDd = jdStatusReport.getUimaDeploymentDescriptor();
			if(jpDd != null) {
				IDuccUimaDeploymentDescriptor uimaDeploymentDescriptor = new DuccUimaDeploymentDescriptor(jpDd);
				dwj.setUimaDeployableConfiguration(uimaDeploymentDescriptor);
			}
			//V2
			boolean agentAddsDdToCommandLine = false;
			if(!agentAddsDdToCommandLine) {
				String jpAe = jdStatusReport.getUimaAnalysisEngine();
				if(jpAe != null) {
					ICommandLine jcl = dwj.getCommandLine();
					List<String> args = jcl.getArguments();
					String arg = jpAe;
					if(args == null) {
						jcl.addArgument(arg);
						logger.debug(location, jobid,  "add[null]:"+arg);
					}
					else if(args.isEmpty()) {
						jcl.addArgument(jpAe);
						logger.debug(location, jobid, "add[empty]:"+arg);
					}
					List<String> argList = jcl.getArguments();
					if(args != null) {
						int index = 0;
						for(String argument : argList) {
							logger.debug(location, jobid, "arg["+index+"]: "+argument);
							index++;
						}
					}
					dwj.setDdSpecified();
				}
			}
		}
	}

	/**
	 * JD reconciliation
	 */
	public void reconcileState(IDriverStatusReport jdStatusReport) {
		String methodName = "reconcileState (JD)";
		logger.trace(methodName, null, messages.fetch("enter"));
		DuccWorkMap workMap = orchestratorCommonArea.getWorkMap();
		int changes = 0;
		TrackSync ts = TrackSync.await(workMap, this.getClass(), methodName);
		synchronized(workMap) {
			ts.using();
			DuccId duccId = jdStatusReport.getDuccId();
			String sid = ""+duccId.getFriendly();
			DuccWorkJob duccWorkJob = (DuccWorkJob) WorkMapHelper.findDuccWork(workMap, sid, this, methodName);
			if(duccWorkJob != null) {
				//
				String jdJmxUrl = jdStatusReport.getJdJmxUrl();
				setJdJmxUrl(duccWorkJob, jdJmxUrl);
				//
				copyInvestmentReport(duccWorkJob, jdStatusReport);
				copyProcessWorkItemsReport(duccWorkJob, jdStatusReport);
				copyDriverWorkItemsReport(duccWorkJob, jdStatusReport);
				//
				switch(duccWorkJob.getJobState()) {
				case Completed:
					break;
				case Completing:
				default:
					duccWorkJob.setWiTotal(jdStatusReport.getWorkItemsTotal());
					duccWorkJob.setWiDone(jdStatusReport.getWorkItemsProcessingCompleted());
					duccWorkJob.setWiError(jdStatusReport.getWorkItemsProcessingError());
					break;
				}
				//
				IRationale rationale;
				if(jdStatusReport.getWorkItemsTotal() == 0) {
					jobTerminate(duccWorkJob, JobCompletionType.NoWorkItemsFound, new Rationale("job driver had no work items to process"), ProcessDeallocationType.JobCanceled);
				}
				else if(jdStatusReport.isKillJob()) {
					rationale = jdStatusReport.getJobCompletionRationale();
					jobTerminate(duccWorkJob, JobCompletionType.CanceledByDriver, rationale, ProcessDeallocationType.JobFailure);
				}
				else {
					switch(jdStatusReport.getDriverState()) {
					case Failed:
						rationale = jdStatusReport.getJobCompletionRationale();
						jobTerminate(duccWorkJob, JobCompletionType.CanceledByDriver, rationale, ProcessDeallocationType.JobFailure);
						break;
					case NotRunning:
						break;
					case Initializing:
						switch(duccWorkJob.getJobState()) {
						case WaitingForDriver:
						    addJdUrlToJpCommandLine(duccWorkJob, jdStatusReport);
						    addDeployableToJpCommandLine(duccWorkJob, jdStatusReport);
						    if(!duccWorkJob.isJdURLSpecified()) {
						        logger.debug(methodName, duccId, "No JdURL provided yet - still waitingForDriver");
						        break;
	                        }
							JobState nextState = JobState.WaitingForServices;
							if(duccWorkJob.getServiceDependencies() == null) {
								String message = messages.fetch("bypass")+" "+nextState;
								logger.debug(methodName, duccId, message);
								nextState = JobState.WaitingForResources;
							}
							stateJobAccounting.stateChange(duccWorkJob, nextState);
							break;
						case Assigned:
							stateJobAccounting.stateChange(duccWorkJob, JobState.Initializing);
							break;
						case Initializing:
							break;
						default:
							break;
						}
						break;
					case Running:
					case Idle:
						if(jdStatusReport.isKillJob()) {
							rationale = jdStatusReport.getJobCompletionRationale();
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
						case Assigned:
						case Initializing:
							stateJobAccounting.stateChange(duccWorkJob, JobState.Running);
							break;
						default:
							break;
						}
						break;
					case Completing:
						if(!duccWorkJob.isFinished()) {
							stateJobAccounting.stateChange(duccWorkJob, JobState.Completing);
						}
						stopJps(duccWorkJob);
						stopJd(duccWorkJob);
						break;
					case Completed:
						if(!duccWorkJob.isCompleted()) {
							if(!duccWorkJob.isFinished()) {
								stateJobAccounting.stateChange(duccWorkJob, JobState.Completing);
							}
							stopJps(duccWorkJob);
							stopJd(duccWorkJob);
							duccWorkJob.getStandardInfo().setDateOfCompletion(TimeStamp.getCurrentMillis());
							switch(jdStatusReport.getJobCompletionType()) {
							case EndOfJob:
								try {
									int errors = jdStatusReport.getWorkItemsProcessingError();
									int done = jdStatusReport.getWorkItemsProcessingCompleted();
									if(errors > 0) {
										setCompletionIfNotAlreadySet(duccWorkJob, JobCompletionType.Error, new Rationale("state manager detected error work items="+errors));
									}
									else if(done == 0) {
										setCompletionIfNotAlreadySet(duccWorkJob, JobCompletionType.EndOfJob, new Rationale("state manager detected no work items processed"));
									}
									else {
										setCompletionIfNotAlreadySet(duccWorkJob, JobCompletionType.EndOfJob, new Rationale("state manager detected normal completion"));
									}
								}
								catch(Exception e) {
									logger.error(methodName, duccId, e);
								}
								break;
							default:
								setCompletionIfNotAlreadySet(duccWorkJob, jdStatusReport);
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
		ts.ended();
		if(changes > 0) {
			OrchestratorCheckpoint.getInstance().saveState();
		}
		logger.trace(methodName, null, messages.fetch("exit"));
	}

	private boolean isExcessCapacity(DuccWorkJob job) {
		String methodName = "isExcessCapacity";
		boolean retVal = false;
		long total = job.getWiTotal();
		long done = job.getWiDone();
		long error = job.getWiError();
		long sum = total + done + error;
		if(sum > 0) {
			long capacity = job.getWorkItemCapacity();
			long todo = total - (done + error);
			long tpp = job.getSchedulingInfo().getIntThreadsPerProcess();
			long numShares = 0;
			if(todo%tpp > 0) {
				numShares = 1;
			}
			numShares += todo / tpp;
			long adjTodo = numShares * tpp;
			if(capacity > 0) {
				if(adjTodo < capacity) {
					retVal = true;
				}
			}
			logger.info(methodName, job.getDuccId(), "todo:"+todo+" "+"adjTodo:"+adjTodo+" "+"capacity:"+capacity+" "+"excess:"+retVal);
		}
		else {
			logger.info(methodName, job.getDuccId(), "todo:"+"?"+" "+"capacity:"+"?"+" "+"excess:"+retVal);
		}
		return retVal;
	}

	private boolean isDeallocatable(IDriverStatusReport jdStatusReport) {
		boolean retVal = false;
		if(!jdStatusReport.isPending()) {
			retVal = true;
		}
		return retVal;
	}

	private boolean deallocateIdleProcesses(DuccWorkJob job, IDriverStatusReport jdStatusReport) {
		String methodName = "deallocateIdleProcesses";
		boolean retVal = false;
		DuccId jobid = job.getDuccId();
		try {
			if(isDeallocatable(jdStatusReport)) {
				IDuccProcessMap processMap = job.getProcessMap();
				Iterator<DuccId> iterator = processMap.keySet().iterator();
				boolean excessCapacity = isExcessCapacity(job);
				int count = 0;
				while(iterator.hasNext() && excessCapacity) {
					count++;
					DuccId duccId = iterator.next();
					IDuccProcess process = processMap.get(duccId);
					if(!process.isDeallocated()) {
						String nodeIP = process.getNodeIdentity().getIp();
						String PID = process.getPID();
						if(!jdStatusReport.isOperating(nodeIP, PID)) {
							OrUtil.setResourceState(job, process, ResourceState.Deallocated);
							process.setProcessDeallocationType(ProcessDeallocationType.Voluntary);
							logger.info(methodName, job.getDuccId(), process.getDuccId(), "deallocated");
							retVal = true;
							excessCapacity = isExcessCapacity(job);
						}
						else {
							logger.debug(methodName, job.getDuccId(), process.getDuccId(), "operating");
						}
					}
					else {
						logger.trace(methodName, job.getDuccId(), process.getDuccId(), "already deallocated");
					}
				}
				logger.trace(methodName, jobid, "count="+count);
			}
			else {
				logger.debug(methodName, jobid, "not deallocatable");
			}
		}
		catch(Exception e) {
			logger.error(methodName, job.getDuccId(), e);
		}
		return retVal;
	}

	private DuccId getDuccId(IDuccProcessMap processMap, IRemoteLocation remoteLocation) {
		String methodName = "getDuccId";
		DuccId duccId = null;
		if(processMap != null) {
			if(remoteLocation != null) {
				String nodeIp = remoteLocation.getNodeIP();
				String processId = remoteLocation.getPid();
				logger.debug(methodName, null, "nodeIp:"+nodeIp+" "+"processId:"+processId);
				for(Entry<DuccId, IDuccProcess> entry : processMap.entrySet()) {
					IDuccProcess p = entry.getValue();
					logger.debug(methodName, null, "duccId:"+entry.getKey()+" "+"nodeId:"+p.getNodeIdentity().getIp()+" "+"processId:"+p.getPID());
				}
				IDuccProcess process = processMap.findProcess(nodeIp, processId);
				if(process != null) {
					duccId = process.getDuccId();
				}
			}
			else {
				logger.debug(methodName, null, "remoteLocation null?");
			}
		}
		else {
			logger.debug(methodName, null, "processMap null?");
		}
		return duccId;
	}

	private boolean deallocateFailedProcesses(DuccWorkJob job, IDriverStatusReport jdStatusReport) {
		String methodName = "deallocateFailedProcesses";
		boolean retVal = false;
		IDuccProcessMap processMap = job.getProcessMap();
		Map<IRemoteLocation, ProcessDeallocationType> map = jdStatusReport.getProcessKillMap();
		if(map != null) {
			logger.debug(methodName, job.getDuccId(), "size:"+map.size());
			for(Entry<IRemoteLocation, ProcessDeallocationType> entry : map.entrySet()) {
				DuccId duccId = getDuccId(processMap, entry.getKey());
				if(duccId != null) {
					IDuccProcess process = processMap.get(duccId);
					if(process != null) {
						if(!process.isDeallocated()) {
							OrUtil.setResourceState(job, process, ResourceState.Deallocated);
							process.setProcessDeallocationType(entry.getValue());
							logger.info(methodName, job.getDuccId(), process.getDuccId(), "deallocated");
						}
					}
					else {
						logger.warn(methodName, job.getDuccId(), duccId, "not in process map");
					}
				}
				else {
					logger.warn(methodName, job.getDuccId(), duccId, "null?");
				}
			}
		}
		return retVal;
	}

	private void stopJps(DuccWorkJob job) {
		String methodName = "stopJps";
		IDuccProcessMap processMap = job.getProcessMap();
		Iterator<DuccId> iterator = processMap.keySet().iterator();
		while (iterator.hasNext()) {
			DuccId duccId = iterator.next();
			IDuccProcess process = processMap.get(duccId);
			if(process != null) {
				if(!process.isDeallocated()) {
					OrUtil.setResourceState(job, process, ResourceState.Deallocated);
					process.setProcessDeallocationType(ProcessDeallocationType.Voluntary);
					logger.info(methodName, job.getDuccId(), process.getDuccId(), "deallocated");
				}
			}
		}
	}

	private boolean delayStopJd(DuccWorkJob job) {
		String methodName = "delayStopJd";
		boolean retVal = true;
		job.setCompletingTOD();
		if(!job.hasAliveProcess()) {
			long elapsed = System.currentTimeMillis() - job.getCompletingTOD();
			if((System.currentTimeMillis() - job.getCompletingTOD()) > 1000*60*5) {
				logger.debug(methodName, job.getDuccId(), ""+elapsed);
				retVal = false;
			}
			else if(!job.hasNoPidProcess()) {
				logger.debug(methodName, job.getDuccId(), "all JPs have PIDs");
				retVal = false;
			}
		}
		if(retVal) {
			logger.debug(methodName, job.getDuccId(), "");
		}
		return retVal;
	}

	private void stopJd(DuccWorkJob job) {
		String methodName = "stopJd";
		if(!delayStopJd(job)) {
			IDuccProcessMap processMap = job.getDriver().getProcessMap();
			Iterator<DuccId> iterator = processMap.keySet().iterator();
			while (iterator.hasNext()) {
				DuccId duccId = iterator.next();
				IDuccProcess process = processMap.get(duccId);
				if(process != null) {
					if(!process.isDeallocated()) {
						OrUtil.setResourceState(job, process, ResourceState.Deallocated);
						process.setProcessDeallocationType(ProcessDeallocationType.Voluntary);
						logger.info(methodName, job.getDuccId(), process.getDuccId(), "deallocated");
					}
				}
			}
		}
	}

	private static AtomicBoolean refusedLogged = new AtomicBoolean(false);

	/**
	 * RM reconciliation
	 */
	public void reconcileState(Map<DuccId, IRmJobState> rmResourceStateMap) throws Exception {
		String methodName = "reconcileState (RM)";
		logger.trace(methodName, null, messages.fetch("enter"));
		logger.debug(methodName, null, messages.fetchLabel("size")+rmResourceStateMap.size());
		DuccWorkMap workMap = orchestratorCommonArea.getWorkMap();
		int changes = 0;
		TrackSync ts = TrackSync.await(workMap, this.getClass(), methodName);
		synchronized(workMap) {
			ts.using();
			Iterator<DuccId> rmResourceStateIterator = rmResourceStateMap.keySet().iterator();
			while(rmResourceStateIterator.hasNext()) {
				DuccId duccId = rmResourceStateIterator.next();
				IRmJobState rmResourceState = rmResourceStateMap.get(duccId);
				Map<DuccId, IResource> mapAdditions = rmResourceState.getPendingAdditions();
				if(mapAdditions != null) {
					int mapSize = mapAdditions.size();
					if(mapSize > 0) {
						logger.info(methodName, duccId, messages.fetchLabel("pending additions")+mapSize);
					}
					else {
						logger.trace(methodName, duccId, messages.fetchLabel("pending additions")+mapSize);
					}

				}
				Map<DuccId, IResource> mapRemovals = rmResourceState.getPendingRemovals();
				if(mapRemovals != null) {
					int mapSize = mapRemovals.size();
					if(mapSize > 0) {
						logger.info(methodName, duccId, messages.fetchLabel("pending removals")+mapSize);
					}
					else {
						logger.trace(methodName, duccId, messages.fetchLabel("pending removals")+mapSize);
					}
				}
				IDuccWork duccWork = WorkMapHelper.findDuccWork(workMap, duccId, this, methodName);
				if(duccWork== null) {
					logger.debug(methodName, duccId, messages.fetch("not found"));
				}
				else {
					logger.trace(methodName, duccId, messages.fetchLabel("type")+duccWork.getDuccType());
					duccWork.setRmReason(null);
					switch(duccWork.getDuccType()) {
					case Job:
						logger.trace(methodName, duccId, messages.fetch("processing job..."));
						DuccWorkJob duccWorkJob = (DuccWorkJob) duccWork;
						processPurger(duccWorkJob,rmResourceState.getResources());
						changes += processMapResourcesAdd(duccWorkJob,rmResourceState.memoryGbPerProcess(),rmResourceState.getPendingAdditions());
						changes += processMapResourcesDel(duccWorkJob,rmResourceState.memoryGbPerProcess(),rmResourceState.getPendingRemovals());
						JobState jobState = duccWorkJob.getJobState();
						logger.trace(methodName, duccId, messages.fetchLabel("job state")+jobState);
						switch(jobState) {
						case Received:
						case WaitingForDriver:
							logger.warn(methodName, duccId, messages.fetchLabel("unexpected state")+jobState);
							break;
						case WaitingForServices:
							logger.debug(methodName, duccId, messages.fetchLabel("unexpected state")+jobState);
							break;
						case WaitingForResources:
							String rmReason = rmResourceState.getReason();
							logger.trace(methodName, duccId, messages.fetchLabel("rmReason")+rmReason);
							duccWork.setRmReason(rmReason);
							if(rmResourceState.isRefused()) {
								duccWorkJob.getStandardInfo().setDateOfCompletion(TimeStamp.getCurrentMillis());
								duccWorkJob.setCompletionType(JobCompletionType.ResourcesUnavailable);
								duccWorkJob.setCompletionRationale(new Rationale("resource manager refused allocation: "+rmResourceState.getReason()));
								changes += stateChange(duccWorkJob,JobState.Completed);
								logger.warn(methodName, duccId, messages.fetchLabel("refused")+rmResourceState.getReason());
								String text = rmResourceState.getReason();
								UserLogging.record(duccWorkJob, text);
							}
							if(duccWorkJob.getProcessMap().size() > 0) {
								changes += stateChange(duccWorkJob,JobState.Assigned);
								logger.info(methodName, duccId, messages.fetchLabel("resources count")+duccWorkJob.getProcessMap().size());
							}
							break;
						case Assigned:
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
						logger.trace(methodName, duccId, messages.fetch("processing reservation..."));
						DuccWorkReservation duccWorkReservation = (DuccWorkReservation) duccWork;
						changes += reservationMapResourcesAdd(duccWorkReservation,rmResourceState.memoryGbPerProcess(),rmResourceState.getPendingAdditions());
						changes += reservationMapResourcesDel(duccWorkReservation,rmResourceState.memoryGbPerProcess(),rmResourceState.getPendingRemovals());
						ReservationState reservationState = duccWorkReservation.getReservationState();
						logger.trace(methodName, duccId, messages.fetchLabel("reservation state")+reservationState);
						switch(reservationState) {
						case Received:
							logger.warn(methodName, duccId, messages.fetchLabel("unexpected state")+reservationState);
							break;
						case WaitingForResources:
							String rmReason = rmResourceState.getReason();
							logger.trace(methodName, duccId, messages.fetchLabel("rmReason")+rmReason);
							duccWork.setRmReason(rmReason);
							if(rmResourceState.isRefused()) {
								String schedulingClass = duccWorkReservation.getSchedulingInfo().getSchedulingClass().trim();
								if(schedulingClass.equals(DuccSchedulerClasses.JobDriver)) {
									if(!refusedLogged.get()) {
										logger.warn(methodName, duccId, messages.fetchLabel("refusal ignored")+rmResourceState.getReason());
										refusedLogged.set(true);
									}
								}
								else {
									duccWorkReservation.getStandardInfo().setDateOfCompletion(TimeStamp.getCurrentMillis());
									duccWorkReservation.setCompletionType(ReservationCompletionType.ResourcesUnavailable);
									duccWorkReservation.setCompletionRationale(new Rationale("resource manager refused allocation: "+rmResourceState.getReason()));
									changes += stateChange(duccWorkReservation,ReservationState.Completed);
									logger.warn(methodName, duccId, messages.fetchLabel("refused")+rmResourceState.getReason());
								}
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
						logger.trace(methodName, duccId, messages.fetch("processing service..."));
						DuccWorkJob duccWorkService = (DuccWorkJob) duccWork;
						int processPurged = processPurger(duccWorkService,rmResourceState.getResources());
						changes += processMapResourcesAdd(duccWorkService,rmResourceState.memoryGbPerProcess(),rmResourceState.getPendingAdditions());
						changes += processMapResourcesDel(duccWorkService,rmResourceState.memoryGbPerProcess(),rmResourceState.getPendingRemovals());
						JobState serviceState = duccWorkService.getJobState();
						logger.trace(methodName, duccId, messages.fetchLabel("service state")+serviceState);
						switch(serviceState) {
						case Received:
							logger.warn(methodName, duccId, messages.fetchLabel("unexpected state")+serviceState);
							break;
						case WaitingForServices:
							logger.debug(methodName, duccId, messages.fetchLabel("unexpected state")+serviceState);
							break;
						case WaitingForResources:
							String rmReason = rmResourceState.getReason();
							logger.trace(methodName, duccId, messages.fetchLabel("rmReason")+rmReason);
							duccWork.setRmReason(rmReason);
							if(rmResourceState.isRefused()) {
								duccWorkService.getStandardInfo().setDateOfCompletion(TimeStamp.getCurrentMillis());
								duccWorkService.setCompletionType(JobCompletionType.ResourcesUnavailable);
								duccWorkService.setCompletionRationale(new Rationale("resource manager refused allocation: "+rmResourceState.getReason()));
								changes += stateChange(duccWorkService,JobState.Completed);
								logger.warn(methodName, duccId, messages.fetchLabel("refused")+rmResourceState.getReason());
								String text = rmResourceState.getReason();
								UserLogging.record(duccWorkService, text);
							}
							if(duccWorkService.getProcessMap().size() > 0) {
								changes += stateChange(duccWorkService,JobState.Initializing);
								logger.info(methodName, duccId, messages.fetchLabel("resources count")+duccWorkService.getProcessMap().size());
							}
							if((processPurged > 0) && allProcessesTerminated(duccWorkService)) {
								duccWorkService.getStandardInfo().setDateOfCompletion(TimeStamp.getCurrentMillis());
								duccWorkService.setCompletionType(JobCompletionType.ResourcesUnavailable);
								duccWorkService.setCompletionRationale(new Rationale("resource manager purged allocation: "+rmResourceState.getReason()));
								changes += stateChange(duccWorkService,JobState.Completed);
								logger.warn(methodName, duccId, messages.fetchLabel("purged")+rmResourceState.getReason());
								String text = rmResourceState.getReason();
								UserLogging.record(duccWorkService, text);
							}
							break;
						case Assigned:
						case Initializing:
						case Running:
							if(duccWorkService.getProcessMap().size() == 0) {
								changes += stateChange(duccWorkService,JobState.WaitingForResources);
								logger.info(methodName, duccId, messages.fetchLabel("resources count")+duccWorkService.getProcessMap().size());
							}
							if((processPurged > 0) && allProcessesTerminated(duccWorkService)) {
								duccWorkService.getStandardInfo().setDateOfCompletion(TimeStamp.getCurrentMillis());
								duccWorkService.setCompletionType(JobCompletionType.ResourcesUnavailable);
								duccWorkService.setCompletionRationale(new Rationale("resource manager purged allocation: "+rmResourceState.getReason()));
								changes += stateChange(duccWorkService,JobState.Completed);
								logger.warn(methodName, duccId, messages.fetchLabel("purged")+rmResourceState.getReason());
								String text = rmResourceState.getReason();
								UserLogging.record(duccWorkService, text);
							}
							break;
						case Completing:
						case Completed:
							logger.debug(methodName, duccId, messages.fetchLabel("unsuitable state")+serviceState);
							break;
						case Undefined:
							logger.warn(methodName, duccId, messages.fetchLabel("unsuitable state")+serviceState);
							break;
						default:
							break;
						}
						break;
					default:
						break;
					}
				}
			}
			if(changes > 0) {
				OrchestratorCheckpoint.getInstance().saveState();
			}
		}
		ts.ended();
		logger.trace(methodName, null, messages.fetch("exit"));
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
							logger.info(methodName, job.getDuccId(), duccId, "rState:"+rState+" "+"pState:"+pState);
							OrUtil.setResourceState(job, process, ResourceState.Deallocated);
							process.setProcessDeallocationType(ProcessDeallocationType.Purged);
							process.advanceProcessState(ProcessState.Stopped);
							changes++;
						}
					}
				}
			}
		}
		logger.trace(methodName, null, messages.fetch("exit"));
		return changes;
	}

	private int processMapResourcesAdd(DuccWorkJob duccWorkJob, int memoryGbPerProcess, Map<DuccId,IResource> resourceMap) {
		String methodName = "processMapResourcesAdd";
		logger.trace(methodName, null, messages.fetch("enter"));
		int changes = 0;
		duccWorkJob.getSchedulingInfo().setMemorySizeAllocatedInBytes(memoryGbPerProcess*SizeBytes.GB);
		if(resourceMap == null) {
			logger.info(methodName, duccWorkJob.getDuccId(), messages.fetch("no map found"));
		}
		else {
			IDuccProcessMap processMap = duccWorkJob.getProcessMap();
			Iterator<DuccId> resourceMapIterator = resourceMap.keySet().iterator();
			while(resourceMapIterator.hasNext()) {
				DuccId duccId = resourceMapIterator.next();
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
					long process_max_size_in_bytes = memoryGbPerProcess * SizeBytes.GB;
					CGroupManager.assign(duccWorkJob.getDuccId(), process, process_max_size_in_bytes);
					orchestratorCommonArea.getProcessAccounting().addProcess(duccId, duccWorkJob.getDuccId());
					processMap.addProcess(process);
					OrUtil.setResourceState(duccWorkJob, process, ResourceState.Allocated);
					logger.info(methodName, duccWorkJob.getDuccId(), messages.fetch("resource added")
												+" "+messages.fetchLabel("process")+duccId.getFriendly()
												+" "+messages.fetchLabel("unique")+duccId.getUnique()
												+" "+messages.fetchLabel("name")+nodeId.getCanonicalName()
												+" "+messages.fetchLabel("ip")+nodeId.getIp());
					changes++;
					// check on usefulness of recent allocation
					switch(duccWorkJob.getJobState()) {
					// allocation unnecessary if job is already completed
					case Completing:
					case Completed:
						OrUtil.setResourceState(duccWorkJob, process, ResourceState.Deallocated);
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
						if(isExcessCapacity(duccWorkJob)) {
							OrUtil.setResourceState(duccWorkJob, process, ResourceState.Deallocated);
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
						+" "+messages.fetchLabel("name")+nodeId.getCanonicalName()
						+" "+messages.fetchLabel("ip")+nodeId.getIp());
				}
			}
		}
		logger.trace(methodName, null, messages.fetch("exit"));
		return changes;
	}

	private int processMapResourcesDel(DuccWorkJob duccWorkJob, int memoryGbPerProcess, Map<DuccId,IResource> resourceMap) {
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
					+" "+messages.fetchLabel("name")+nodeId.getCanonicalName()
					+" "+messages.fetchLabel("ip")+nodeId.getIp());
				if(processMap.containsKey(duccId)) {
					IDuccProcess process = processMap.get(duccId);
					switch(process.getResourceState()) {
					case Deallocated:
						break;
					default:
						OrUtil.setResourceState(duccWorkJob, process, ResourceState.Deallocated);
						process.setProcessDeallocationType(ProcessDeallocationType.Forced);
						logger.info(methodName, duccWorkJob.getDuccId(), messages.fetch("resource deallocated")
							+" "+messages.fetchLabel("process")+duccId.getFriendly()
							+" "+messages.fetchLabel("unique")+duccId.getUnique()
							+" "+messages.fetchLabel("name")+nodeId.getCanonicalName()
							+" "+messages.fetchLabel("ip")+nodeId.getIp());
						break;
					}
					/*
					if(process.isDefunct()) {
						orchestratorCommonArea.getProcessAccounting().removeProcess(duccId);
						processMap.removeProcess(duccId);
						logger.info(methodName, duccId, messages.fetch("remove resource")+" "+messages.fetchLabel("name")+nodeId.getCanonicalName()+" "+messages.fetchLabel("ip")+nodeId.getIp());
						changes++;
					}
					*/
				}
				else {
					logger.info(methodName, duccWorkJob.getDuccId(), messages.fetch("resource not found")
						+" "+messages.fetchLabel("process")+duccId.getFriendly()
						+" "+messages.fetchLabel("unique")+duccId.getUnique()
						+" "+messages.fetchLabel("name")+nodeId.getCanonicalName()
						+" "+messages.fetchLabel("ip")+nodeId.getIp());
				}
			}
		}
		logger.trace(methodName, duccWorkJob.getDuccId(), messages.fetch("exit"));
		return changes;
	}

	private int reservationMapResourcesAdd(DuccWorkReservation duccWorkReservation,int memoryGbPerProcess,Map<DuccId,IResource> resourceMap) {
		String methodName = "reservationMapResourcesAdd";
		logger.trace(methodName, null, messages.fetch("enter"));
		int changes = 0;
		duccWorkReservation.getSchedulingInfo().setMemorySizeAllocatedInBytes(memoryGbPerProcess*SizeBytes.GB);
		IDuccReservationMap reservationMap = duccWorkReservation.getReservationMap();
		if(resourceMap != null) {
			Iterator<DuccId> resourceMapIterator = resourceMap.keySet().iterator();
			while(resourceMapIterator.hasNext()) {
				DuccId duccId = resourceMapIterator.next();
				IResource resource = resourceMap.get(duccId);
				Node node = resource.getNode();
				NodeIdentity nodeId = node.getNodeIdentity();
				if(!reservationMap.containsKey(duccId)) {
					long bytes = memoryGbPerProcess * SizeBytes.GB;
					DuccReservation reservation = new DuccReservation(duccId, node, bytes);
					reservationMap.addReservation(reservation);
					logger.info(methodName, duccId, messages.fetch("add resource")+" "+messages.fetchLabel("name")+nodeId.getCanonicalName()+" "+messages.fetchLabel("ip")+nodeId.getIp());
					changes++;
				}
				else {
					logger.debug(methodName, duccId, messages.fetch("duplicate resource")+" "+messages.fetchLabel("name")+nodeId.getCanonicalName()+" "+messages.fetchLabel("ip")+nodeId.getIp());
				}
			}
		}
		logger.trace(methodName, null, messages.fetch("exit"));
		return changes;
	}

	private int reservationMapResourcesDel(DuccWorkReservation duccWorkReservation,int memoryGbPerProcess,Map<DuccId,IResource> resourceMap) {
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
					logger.info(methodName, duccId, messages.fetch("remove resource")+" "+messages.fetchLabel("name")+nodeId.getCanonicalName()+" "+messages.fetchLabel("ip")+nodeId.getIp());
					changes++;
				}
				else {
					logger.debug(methodName, duccId, messages.fetch("not found resource")+" "+messages.fetchLabel("name")+nodeId.getCanonicalName()+" "+messages.fetchLabel("ip")+nodeId.getIp());
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
		DuccWorkMap workMap = orchestratorCommonArea.getWorkMap();
		int changes = 0;
		Iterator<DuccId> serviceMapIterator = serviceMap.keySet().iterator();
		TrackSync ts = TrackSync.await(workMap, this.getClass(), methodName);
		synchronized(workMap) {
			ts.using();
			while(serviceMapIterator.hasNext()) {
				DuccId duccId = serviceMapIterator.next();
				ServiceDependency services = serviceMap.get(duccId);
				DuccWorkJob duccWorkJob = (DuccWorkJob) WorkMapHelper.findDuccWork(workMap, duccId, this, methodName);
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
                        case Pending:                   // UIMA-4223
						case Waiting:
                        case Starting:
						case Initializing:
							break;
						case Available:
							stateJobAccounting.stateChange(duccWorkJob, JobState.WaitingForResources);
							changes++;
							logger.info(methodName, duccId, messages.fetchLabel("job state")+jobState+" "+messages.fetchLabel("services state")+serviceState);
							break;
						case NotAvailable:
						case Stopped:
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
						default:
							break;
						}
						break;
					case WaitingForResources:
						logger.debug(methodName, duccId, messages.fetchLabel("job state")+jobState+" "+messages.fetchLabel("services state")+serviceState);
						break;
					case Assigned:
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
					default:
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
		ts.ended();
		logger.trace(methodName, null, messages.fetch("exit"));
	}

	private void inventoryJob(IDuccWork duccWork, IDuccProcess inventoryProcess) {
		String methodName = "inventoryJob";
		DuccWorkJob job = (DuccWorkJob) duccWork;
		DuccId jobId = job.getDuccId();
		DuccId processId = inventoryProcess.getDuccId();
		ProcessType processType = inventoryProcess.getProcessType();
		switch(processType) {
		case Pop:
			OrchestratorCommonArea.getInstance().getProcessAccounting().setStatus(inventoryProcess);
			if(jobDriverTerminated(job)) {
				OrchestratorHelper.jdDeallocate(job);
			}
			switch(inventoryProcess.getProcessState()) {
			case LaunchFailed:
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
					int errors = job.getSchedulingInfo().getIntWorkItemsError();
					int done = job.getSchedulingInfo().getIntWorkItemsCompleted();
					if(errors > 0) {
						setCompletionIfNotAlreadySet(job, JobCompletionType.Error, new Rationale("state manager detected error work items="+errors));
					}
					else if(done == 0) {
						setCompletionIfNotAlreadySet(job, JobCompletionType.EndOfJob, new Rationale("state manager detected no work items processed"));
					}
					// <UIMA-3337>
					else {
						setCompletionIfNotAlreadySet(job, JobCompletionType.EndOfJob, rationale);
					}
					// </UIMA-3337>
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
			try {
				if(!job.isInitialized()) {
					IDuccProcessMap map = job.getProcessMap();
					for(Entry<DuccId, IDuccProcess> entry : map.entrySet()) {
						IDuccProcess process = entry.getValue();
						StringBuffer sb = new StringBuffer();
						sb.append("pid:"+process.getPID()+" ");
						sb.append("state:"+process.getProcessState()+" ");
						sb.append("reason:"+process.getReasonForStoppingProcess()+" ");
						logger.trace(methodName, job.getDuccId(), sb.toString());
					}
					long initFailureCount = job.getProcessInitFailureCount();
					long startup_initialization_error_limit = DuccPropertiesResolver.get(DuccPropertiesResolver.ducc_jd_startup_initialization_error_limit, 1);
					if(initFailureCount >= startup_initialization_error_limit) {
						String reason = "process inititialization failure count["+initFailureCount+"] meets startup initialization error limit["+startup_initialization_error_limit+"]";
						logger.warn(methodName, job.getDuccId(), reason);
						JobCompletionType jobCompletionType = JobCompletionType.CanceledBySystem;
						Rationale rationale = new Rationale(reason);
						ProcessDeallocationType processDeallocationType = ProcessDeallocationType.JobCanceled;
						stateManager.jobTerminate(job, jobCompletionType, rationale, processDeallocationType);
					}
					else {
						String reason = "process failure count["+initFailureCount+"] does not exceed startup initialization error limit["+startup_initialization_error_limit+"]";
						logger.debug(methodName, job.getDuccId(), reason);
					}
				}
				else {
					logger.trace(methodName, job.getDuccId(), "job is initialized");
				}
			}
			catch(Exception e) {
				logger.error(methodName, jobId, e);
			}
			break;
		}
		// <UIMA-3923>
		advanceToCompleted(job);
		// </UIMA-3923>
	}

	private void inventoryService(IDuccWork duccWork, IDuccProcess inventoryProcess) {
		//String methodName = "inventoryService";
		DuccWorkJob service = (DuccWorkJob) duccWork;
		ProcessType processType = inventoryProcess.getProcessType();
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
				completeService(service, new Rationale("state manager reported no viable service process exists, type="+processType));
			}
			break;
		case Job_Uima_AS_Process:
			OrchestratorCommonArea.getInstance().getProcessAccounting().setStatus(inventoryProcess);
			if(inventoryProcess.isComplete()) {
				OrchestratorCommonArea.getInstance().getProcessAccounting().deallocate(service,ProcessDeallocationType.Stopped);
			}
			if(!service.hasAliveProcess()) {
				completeService(service, new Rationale("state manager reported no viable service process exists, type="+processType));
			}
			break;
		}
		// <UIMA-3923>
		advanceToCompleted(service);
		// </UIMA-3923>
	}

	/**
	 * Node Inventory reconciliation
	 */
	public void reconcileState(HashMap<DuccId, IDuccProcess> inventoryProcessMap) {
		String methodName = "reconcileState (Node Inventory)";
		logger.trace(methodName, null, messages.fetch("enter"));
		DuccWorkMap workMap = orchestratorCommonArea.getWorkMap();
		Iterator<DuccId> iterator = inventoryProcessMap.keySet().iterator();
		TrackSync ts = TrackSync.await(workMap, this.getClass(), methodName);
		synchronized(workMap) {
			ts.using();
			while(iterator.hasNext()) {
				DuccId processId = iterator.next();
				IDuccProcess inventoryProcess = inventoryProcessMap.get(processId);
				List<IUimaPipelineAEComponent> upcList = inventoryProcess.getUimaPipelineComponents();
				if(upcList != null) {
					Iterator<IUimaPipelineAEComponent> upcIterator = upcList.iterator();
					while(upcIterator.hasNext()) {
						IUimaPipelineAEComponent upc = upcIterator.next();
						logger.trace(methodName, null, processId, "pipelineInfo: "+inventoryProcess.getNodeIdentity()+" "+inventoryProcess.getPID()+" "+upc.getAeName()+" "+upc.getAeState()+" "+upc.getInitializationTime());
					}
				}
				ProcessType processType = inventoryProcess.getProcessType();
				if(processType != null) {
					DuccId jobId = OrchestratorCommonArea.getInstance().getProcessAccounting().getJobId(processId);
					if(jobId != null) {
						IDuccWork duccWork = WorkMapHelper.findDuccWork(workMap, jobId, this, methodName);
						if(duccWork != null) {
							DuccType jobType = duccWork.getDuccType();
							switch(jobType) {
							case Job:
								inventoryJob(duccWork, inventoryProcess);
								break;
							case Service:
								inventoryService(duccWork, inventoryProcess);
								break;
							default:
								break;
							}
						}
						else {
							StringBuffer sb = new StringBuffer();
							sb.append("node:"+inventoryProcess.getNodeIdentity().getCanonicalName());
							sb.append(" ");
							sb.append("PID:"+inventoryProcess.getPID());
							sb.append(" ");
							sb.append("type:"+inventoryProcess.getProcessType());
							logger.debug(methodName, jobId, sb);
						}
					}
					else {
						StringBuffer sb = new StringBuffer();
						sb.append("node:"+inventoryProcess.getNodeIdentity().getCanonicalName());
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
					sb.append("node:"+inventoryProcess.getNodeIdentity().getCanonicalName());
					sb.append(" ");
					sb.append("PID:"+inventoryProcess.getPID());
					sb.append(" ");
					sb.append("type:"+inventoryProcess.getProcessType());
					logger.warn(methodName, jobId, sb);
				}
			}
		}
		ts.ended();
		logger.trace(methodName, null, messages.fetch("exit"));
	}

	private void advanceToCompleted(DuccWorkJob job) {
		switch(job.getJobState()) {
		case Completing:
			if(job.getProcessMap().getAliveProcessCount() == 0) {
				stateJobAccounting.stateChange(job, JobState.Completed);
			}
			break;
		default:
			break;
		}
	}

	private void advanceToCompleting(DuccWorkJob job) {
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
						JobCompletionType completionType = JobCompletionType.ProgramExit;
						ProcessState processState = process.getProcessState();
						if(processState != null) {
							switch(processState) {
							case LaunchFailed:
								completionType = JobCompletionType.LaunchFailure;
								break;
							default:
								break;
							}
						}
						service.setCompletion(completionType, exitCode);
						service.getStandardInfo().setDateOfCompletion(TimeStamp.getCurrentMillis());
						break;
					default:
						break;
					}
					advanceToCompleting(service);
					break;
				}
			}
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
			completeJob(service, rationale);
		}
	}

	private void completeService(DuccWorkJob service, IRationale rationale) {
		String location = "completeService";
		DuccId jobid = service.getDuccId();
		if(service.getProcessFailureCount() > 0) {
			service.setCompletion(JobCompletionType.Warning, new Rationale("process failure(s) occurred"));
			logger.debug(location, jobid, service.getCompletionRationale().getText()+", "+"ProcessFailureCount="+service.getProcessFailureCount());
		}
		else if(service.getProcessInitFailureCount() > 0) {
			service.setCompletion(JobCompletionType.Warning, new Rationale("process initialization failure(s) occurred"));
			logger.debug(location, jobid, service.getCompletionRationale().getText()+", "+"ProcessInitFailureCount="+service.getProcessInitFailureCount());
		}
		else {
			setCompletionIfNotAlreadySet(service, JobCompletionType.EndOfJob, rationale);
			logger.debug(location, jobid, service.getCompletionRationale().getText()+", "+"no failures");
		}
		advanceToCompleting(service);
	}

	private void completeJob(DuccWorkJob job, IRationale rationale) {
		String location = "completeJob";
		DuccId jobid = null;
		switch(job.getCompletionType()) {
		case Undefined:
			job.setCompletion(JobCompletionType.Undefined, rationale);
			job.getStandardInfo().setDateOfCompletion(TimeStamp.getCurrentMillis());
			break;
		case EndOfJob:
			if(job.getProcessFailureCount() > 0) {
				job.setCompletion(JobCompletionType.Warning, new Rationale("all work items completed, but job process failure(s) occurred"));
			}
			else if(job.getProcessInitFailureCount() > 0) {
				job.setCompletion(JobCompletionType.Warning, new Rationale("all work items completed, but job process initialization failure(s) occurred"));
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
		advanceToCompleting(job);
	}

	public void jobTerminate(IDuccWorkJob job, JobCompletionType jobCompletionType, IRationale rationale, ProcessDeallocationType processDeallocationType) {
		if(!job.isFinished()) {
			stateJobAccounting.stateChange(job, JobState.Completing);
			stateJobAccounting.complete(job, jobCompletionType, rationale);
			OrchestratorCommonArea.getInstance().getProcessAccounting().deallocate(job,processDeallocationType);
		}
	}

}
