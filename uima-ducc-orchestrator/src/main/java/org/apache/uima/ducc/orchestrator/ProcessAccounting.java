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
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.uima.ducc.common.internationalization.Messages;
import org.apache.uima.ducc.common.node.metrics.ProcessGarbageCollectionStats;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.TimeStamp;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.agent.IUimaPipelineAEComponent;
import org.apache.uima.ducc.transport.event.common.DuccWorkJob;
import org.apache.uima.ducc.transport.event.common.DuccWorkMap;
import org.apache.uima.ducc.transport.event.common.IDuccProcess;
import org.apache.uima.ducc.transport.event.common.IDuccProcessMap;
import org.apache.uima.ducc.transport.event.common.IDuccState.JobState;
import org.apache.uima.ducc.transport.event.common.IDuccWork;
import org.apache.uima.ducc.transport.event.common.IDuccWorkExecutable;
import org.apache.uima.ducc.transport.event.common.IDuccWorkJob;
import org.apache.uima.ducc.transport.event.common.IProcessState.ProcessState;
import org.apache.uima.ducc.transport.event.common.IResourceState.ProcessDeallocationType;
import org.apache.uima.ducc.transport.event.common.IResourceState.ResourceState;
import org.apache.uima.ducc.transport.event.common.ITimeWindow;
import org.apache.uima.ducc.transport.event.common.TimeWindow;
import org.apache.uima.ducc.transport.event.jd.DriverStatusReport;


public class ProcessAccounting {
	
	private static final DuccLogger logger = DuccLoggerComponents.getOrLogger(ProcessAccounting.class.getName());
	
	private OrchestratorCommonArea orchestratorCommonArea = OrchestratorCommonArea.getInstance();
	private Messages messages = orchestratorCommonArea.getSystemMessages();
	private DuccWorkMap workMap = orchestratorCommonArea.getWorkMap();
	
	private ConcurrentHashMap<DuccId,DuccId> processToJobMap = new ConcurrentHashMap<DuccId,DuccId>();
	
	private StateJobAccounting stateJobAccounting = StateJobAccounting.getInstance();
	
	public ProcessAccounting() {
	}
	
	public ProcessAccounting(ConcurrentHashMap<DuccId,DuccId> processToJobMap) {
		setProcessToJobMap(processToJobMap);
	}
	
	public ConcurrentHashMap<DuccId,DuccId> getProcessToJobMap() {
		return this.processToJobMap;
	}
	
	private void setProcessToJobMap(ConcurrentHashMap<DuccId,DuccId> processToJobMap) {
		this.processToJobMap = processToJobMap;
	}
	
	public DuccId getJobId(DuccId processId) {
		String methodName = "getJobId";
		DuccId retVal;
		long t0 = System.currentTimeMillis();
		synchronized(workMap) {
			retVal = processToJobMap.get(processId);
		}
		long t1 = System.currentTimeMillis();
		long elapsed = t1 - t0;
		if(elapsed > Constants.SYNC_LIMIT) {
			logger.debug(methodName, null, "elapsed msecs: "+elapsed);
		}
		return retVal;
	}
	
	public int processCount() {
		String methodName = "processCount";
		int retVal;
		long t0 = System.currentTimeMillis();
		synchronized(workMap) {
			retVal = processToJobMap.size();
		}
		long t1 = System.currentTimeMillis();
		long elapsed = t1 - t0;
		if(elapsed > Constants.SYNC_LIMIT) {
			logger.debug(methodName, null, "elapsed msecs: "+elapsed);
		}
		return retVal;
	}
	
	public boolean addProcess(DuccId processId, DuccId jobId) {
		String methodName = "addProcess";
		logger.trace(methodName, null, messages.fetch("enter"));
		boolean retVal = false;
		long t0 = System.currentTimeMillis();
		synchronized(workMap) {
			if(!processToJobMap.containsKey(processId)) {
				processToJobMap.put(processId, jobId);
				retVal = true;
				logger.info(methodName, jobId, processId, messages.fetch("added"));
			}
			else {
				logger.warn(methodName, jobId, processId, messages.fetch("exists"));
			}
		}
		long t1 = System.currentTimeMillis();
		long elapsed = t1 - t0;
		if(elapsed > Constants.SYNC_LIMIT) {
			logger.debug(methodName, null, "elapsed msecs: "+elapsed);
		}
		logger.trace(methodName, null, messages.fetch("exit"));
		return retVal;
	}
	
	public boolean removeProcess(DuccId processId) {
		String methodName = "removeProcess";
		logger.trace(methodName, null, messages.fetch("enter"));
		boolean retVal = false;
		long t0 = System.currentTimeMillis();
		synchronized(workMap) {
			if(processToJobMap.containsKey(processId)) {
				DuccId jobId = processToJobMap.remove(processId);
				retVal = true;
				logger.info(methodName, jobId, processId, messages.fetch("removed"));
			}
			else {
				logger.info(methodName, null, processId, messages.fetch("not found"));
			}
		}
		long t1 = System.currentTimeMillis();
		long elapsed = t1 - t0;
		if(elapsed > Constants.SYNC_LIMIT) {
			logger.debug(methodName, null, "elapsed msecs: "+elapsed);
		}
		logger.trace(methodName, null, messages.fetch("exit"));
		return retVal;
	}
	
	private boolean compare(String a, String b) {
		boolean retVal = false;
		if(a == null) {
			if(b == null) {
				retVal = true;
			}
		}
		else {
			return a.equals(b);
		}
		return retVal;
	}
	
	private boolean compare(ITimeWindow a, ITimeWindow b) {
		boolean retVal = false;
		if((a == null) && (b == null)) {
			retVal = true;
		}
		else if((a != null) && (b != null)) {
			retVal = compare(a.getStart(),b.getStart()) && compare(a.getEnd(),b.getEnd());
		}
		return retVal;
	}
	
	public void copyInventoryPID(IDuccWork dw, IDuccProcess inventoryProcess, IDuccProcess process) {
		String methodName = "copyInventoryPID";
		logger.trace(methodName, null, messages.fetch("enter"));
		String newValue = inventoryProcess.getPID();
		String oldValue = process.getPID();
		logger.debug(methodName, dw.getDuccId(), inventoryProcess.getDuccId(), ""+newValue);
		if(newValue == null) {
			if(oldValue != null) {
				logger.warn(methodName, dw.getDuccId(), inventoryProcess.getDuccId(), "PID"+" "+"old:"+oldValue+" "+"new:"+newValue+" "+"keeping old");
			}
		}
		else {
			if(oldValue == null) {
				process.setPID(newValue);
			}
			else {
				if(oldValue.equals(newValue)) {
					//OK
				}
				else {
					logger.warn(methodName, dw.getDuccId(), inventoryProcess.getDuccId(), "PID"+" "+"old:"+oldValue+" "+"new:"+newValue+" "+"keeping old");
				}
			}
		}
		logger.trace(methodName, null, messages.fetch("exit"));
		return;
	}
	
	public void copyInventorySwapUsage(IDuccWork dw, IDuccProcess inventoryProcess, IDuccProcess process) {
		String methodName = "copyInventorySwapUsage";
		logger.trace(methodName, null, messages.fetch("enter"));
		long value = inventoryProcess.getSwapUsage();
		logger.debug(methodName, dw.getDuccId(), inventoryProcess.getDuccId(), ""+value);
		process.setSwapUsage(value);
		if(process.getSwapUsageMax() < process.getSwapUsage()) {
			process.setSwapUsageMax(process.getSwapUsage());
		}
		logger.trace(methodName, null, messages.fetch("exit"));
		return;
	}
	
	public void copyInventoryMajorFaults(IDuccWork dw, IDuccProcess inventoryProcess, IDuccProcess process) {
		String methodName = "copyInventoryMajorFaults";
		logger.trace(methodName, null, messages.fetch("enter"));
		process.setMajorFaults(inventoryProcess.getMajorFaults());
		logger.trace(methodName, null, messages.fetch("exit"));
		return;
	}
	
	public void copyInventoryRss(IDuccWork dw, IDuccProcess inventoryProcess, IDuccProcess process) {
		String methodName = "copyInventoryRss";
		logger.trace(methodName, null, messages.fetch("enter"));
		process.setResidentMemory(inventoryProcess.getResidentMemory());
		logger.trace(methodName, null, messages.fetch("exit"));
		return;
	}
	
	/*
	private void injectError(DuccId jobid, DuccId processId, ProcessGarbageCollectionStats newGCS, ProcessGarbageCollectionStats oldGCS) {
		String location = "injectError";
		Random random = new Random();
		if(random.nextBoolean()) {
			long falseCount = oldGCS.getCollectionCount()+newGCS.getCollectionCount()+1;
			logger.warn(location, jobid, processId, "falseCount: "+falseCount);
			oldGCS.setCollectionCount(falseCount);
			long falseTime = oldGCS.getCollectionTime()+newGCS.getCollectionTime()+10*1000;
			logger.warn(location, jobid, processId, "falseTime: "+falseTime);
			oldGCS.setCollectionTime(falseTime);
		}
	}
	*/
	
	private boolean validateGCStats(DuccId jobid, DuccId processId, ProcessGarbageCollectionStats newGCS, ProcessGarbageCollectionStats oldGCS) {
		String location = "validateGCStats";
		boolean retVal = true;
		if(oldGCS == null) {
			//retVal = true;
		}
		else if(newGCS == null) {
			logger.warn(location, jobid, processId, "ProcessGarbageCollectionStats missing?");
			retVal = false;
		}
		else {
			long newCC = newGCS.getCollectionCount();
			long oldCC = oldGCS.getCollectionCount();
			if(newCC < oldCC) {
				logger.warn(location, jobid, processId, "CollectionCount "+newCC+" < "+oldCC);
				retVal = false;
			}
			long newCT = newGCS.getCollectionTime();
			long oldCT = oldGCS.getCollectionTime();
			if(newCT < oldCT) {
				logger.warn(location, jobid, processId, "CollectionTime "+newCT+" < "+oldCT);
				retVal = false;
			}
		}
		return retVal;
	}
	
	public void copyInventoryGCStats(IDuccWork dw, IDuccProcess inventoryProcess, IDuccProcess process) {
		String methodName = "copyInventoryGCStats";
		logger.trace(methodName, null, messages.fetch("enter"));
		DuccId jobId = dw.getDuccId();
		DuccId processId = process.getDuccId();
		ProcessGarbageCollectionStats newGCS = inventoryProcess.getGarbageCollectionStats();
		ProcessGarbageCollectionStats oldGCS = process.getGarbageCollectionStats();
		if(validateGCStats(jobId,processId,newGCS,oldGCS)) {
			process.setGarbageCollectionStats(newGCS);
			ProcessGarbageCollectionStats gcs = process.getGarbageCollectionStats();
			if(gcs != null) {
				logger.trace(methodName, jobId, processId, "GC Stats Count:"+gcs.getCollectionCount());
				logger.trace(methodName, jobId, processId, "GC Stats Time:"+gcs.getCollectionTime());
			}
		}
		logger.trace(methodName, null, messages.fetch("exit"));
		return;
	}
	
	public void copyInventoryCpuTime(IDuccWork dw, IDuccProcess inventoryProcess, IDuccProcess process) {
		String methodName = "copyInventoryCpuTime";
		logger.trace(methodName, null, messages.fetch("enter"));
		process.setCpuTime(inventoryProcess.getCpuTime());
		logger.trace(methodName, dw.getDuccId(), process.getDuccId(), "Cpu Time:"+process.getCpuTime());
		logger.trace(methodName, null, messages.fetch("exit"));
		return;
	}
	
	public void copyTimeInit(IDuccProcess inventoryProcess, IDuccProcess process) {
		String methodName = "copyTimeInit";
		logger.trace(methodName, null, messages.fetch("enter"));
		DuccId processId = inventoryProcess.getDuccId();
		DuccId jobId = getJobId(processId);
		ITimeWindow twInit = inventoryProcess.getTimeWindowInit();
		if(twInit != null) {
			if(!compare(twInit,process.getTimeWindowInit())) {
				process.setTimeWindowInit(twInit);
				String millis;
				String ts;
				millis = process.getTimeWindowInit().getStart();
				if(millis != null) {
					ts = TimeStamp.simpleFormat(millis);
					logger.info(methodName, jobId, processId, messages.fetchLabel("initialization start")+ts);
				}
				millis = process.getTimeWindowInit().getEnd();
				if(millis != null) {
					ts = TimeStamp.simpleFormat(millis);
					logger.info(methodName, jobId, processId, messages.fetchLabel("initialization end")+ts);
				}
			}
		}
		logger.trace(methodName, null, messages.fetch("exit"));
		return;
	}
	
	/*
	private void validate(IDuccProcess inventoryProcess) {
		String methodName = "validate";
		if(inventoryProcess != null) {
			ITimeWindow twInit = inventoryProcess.getTimeWindowInit();
			ITimeWindow twRun = inventoryProcess.getTimeWindowRun();
			if(twInit != null) {
				if(twRun != null) {
					long initEnd = twInit.getEndLong();
					long runStart = twRun.getStartLong();
					if(initEnd > runStart) {
						DuccId duccPid = inventoryProcess.getDuccId();
						logger.warn(methodName, null, duccPid, "initEnd:"+initEnd+" "+"runStart:"+runStart);
					}
				}
			}
		}
	}
	*/
	
	public void copyTimeRun(IDuccProcess inventoryProcess, IDuccProcess process) {
		String methodName = "copyTimeRun";
		logger.trace(methodName, null, messages.fetch("enter"));
		//validate(inventoryProcess);
		DuccId processId = inventoryProcess.getDuccId();
		DuccId jobId = getJobId(processId);
		ITimeWindow twRun = inventoryProcess.getTimeWindowRun();
		if(twRun != null) {
			if(!compare(twRun,process.getTimeWindowRun())) {
				process.setTimeWindowRun(twRun);
				String millis;
				String ts;
				millis = process.getTimeWindowRun().getStart();
				if(millis != null) {
					ts = TimeStamp.simpleFormat(millis);
					logger.info(methodName, jobId, processId, messages.fetchLabel("run start")+ts);
				}
				millis = process.getTimeWindowRun().getEnd();
				if(millis != null) {
					ts = TimeStamp.simpleFormat(millis);
					logger.info(methodName, jobId, processId, messages.fetchLabel("run end")+ts);
				}
			}
		}
		logger.trace(methodName, null, messages.fetch("exit"));
		return;
	}
	
	private void setResourceStateAndReason(IDuccWorkJob job, IDuccProcess inventoryProcess, IDuccProcess process) {
		String methodName = "setResourceStateAndReason";
		logger.trace(methodName, job.getDuccId(), messages.fetch("enter"));
		switch(inventoryProcess.getProcessState()) {
		case Stopped:
		case Failed:
		case FailedInitialization:
		case InitializationTimeout:
		case Killed:
			switch(process.getResourceState()) {
			case Allocated:
				process.setResourceState(ResourceState.Deallocated);
				logger.info(methodName, job.getDuccId(), process.getDuccId(), messages.fetchLabel("process state")+inventoryProcess.getProcessState()+" => "+messages.fetchLabel("resource state")+process.getResourceState());
				String reason = inventoryProcess.getReasonForStoppingProcess();
				switch(inventoryProcess.getProcessState()) {
				case Stopped:
					if(reason != null) {
						process.setReasonForStoppingProcess(reason);
					}
					process.setProcessDeallocationType(ProcessDeallocationType.AutonomousStop);
					break;
				case Failed:
					if(reason != null) {
						process.setReasonForStoppingProcess(reason);
					}
					process.setProcessDeallocationType(ProcessDeallocationType.Failed);
					break;
				case Killed:
					if(reason != null) {
						process.setReasonForStoppingProcess(reason);
					}
					process.setProcessDeallocationType(ProcessDeallocationType.Killed);
					break;
				}
				break;
			default:
				logger.debug(methodName, job.getDuccId(), process.getDuccId(), messages.fetchLabel("process state")+inventoryProcess.getProcessState()+" => "+messages.fetchLabel("resource state")+process.getResourceState());
				break;
			}
			break;
		default:
			logger.debug(methodName, job.getDuccId(), process.getDuccId(), messages.fetchLabel("process state")+inventoryProcess.getProcessState()+" => "+messages.fetchLabel("resource state")+process.getResourceState());
			break;
		}
		logger.trace(methodName, job.getDuccId(), messages.fetch("exit"));
	}
	
	public void copyInventoryProcessState(IDuccWorkJob job, IDuccProcess inventoryProcess, IDuccProcess process) {
		String methodName = "copyInventoryProcessState";
		logger.trace(methodName, job.getDuccId(), messages.fetch("enter"));
		if(!compare(inventoryProcess.getProcessState().toString(),process.getProcessState().toString())) {
			switch((JobState)job.getStateObject()) {
			//case Initializing:
			//	logger.info(methodName, jobId, processId, messages.fetchLabel("process state ignored")+inventoryProcess.getProcessState());
			//	break;
			default:
				process.advanceProcessState(inventoryProcess.getProcessState());
				logger.info(methodName, job.getDuccId(), process.getDuccId(), messages.fetchLabel("process state")+process.getProcessState());
				if ( inventoryProcess.getProcessJmxUrl() != null && process.getProcessJmxUrl() == null) {
					process.setProcessJmxUrl(inventoryProcess.getProcessJmxUrl());
				}
				process.setProcessExitCode(inventoryProcess.getProcessExitCode());
				logger.info(methodName, job.getDuccId(), process.getDuccId(), messages.fetchLabel("process exit code")+process.getProcessExitCode());
				break;
			}
		}
		logger.trace(methodName, job.getDuccId(), messages.fetch("exit"));
	}
	
	public void copyUimaPipelineComponentsState(IDuccWorkJob job, IDuccProcess inventoryProcess, IDuccProcess process) {
		String methodName = "copyUimaPipelineComponentsState";
		logger.trace(methodName, job.getDuccId(), messages.fetch("enter"));
		List<IUimaPipelineAEComponent> list = inventoryProcess.getUimaPipelineComponents();
		if(list != null) {
			if(!list.isEmpty()) {
				process.setUimaPipelineComponents(list);
			}
		}
		logger.trace(methodName, job.getDuccId(), messages.fetch("exit"));
	}
	
	private ITimeWindow makeTimeWindow(String ts) {
		ITimeWindow tw = new TimeWindow();
		tw.setStart(ts);
		tw.setEnd(ts);
		return tw;
	}
	
	private void initStop(IDuccWorkJob job, IDuccProcess process) {
		String ts = TimeStamp.getCurrentMillis();
		ITimeWindow twi = process.getTimeWindowInit();
		if(twi == null) {
			twi = makeTimeWindow(ts);
			process.setTimeWindowInit(twi);
		}
		else {
			long i0 = twi.getStartLong();
			long i1 = twi.getEndLong();
			if(i0 != i1) {
				if(i1 < i0) {
					twi.setEnd(ts);
				}
			}
		}
	}
	
	private void runStart(IDuccWorkJob job, IDuccProcess process) {
		ITimeWindow twi = process.getTimeWindowInit();
		ITimeWindow twr = makeTimeWindow(twi.getEnd());
		process.setTimeWindowRun(twr);
	}
	
	private void runStop(IDuccWorkJob job, IDuccProcess process) {
		String ts = TimeStamp.getCurrentMillis();
		ITimeWindow twi = process.getTimeWindowInit();
		if(twi == null) {
			twi = makeTimeWindow(ts);
			process.setTimeWindowRun(twi);
		}
		ITimeWindow twr = process.getTimeWindowRun();
		if(twr == null) {
			twr = makeTimeWindow(twi.getEnd());
			process.setTimeWindowRun(twr);
		}
		else {
			long r0 = twr.getStartLong();
			long r1 = twr.getEndLong();
			if(r0 != r1) {
				if(r1 < r0) {
					twr.setEnd(ts);
				}
			}
		}
		adjustWindows(job, process);
		adjustRunTime(process);
	}
	
	// <uima-3351>
	private void adjustRunTime(IDuccProcess process) {
		if(!process.isAssignedWork()) {
			ITimeWindow twr = process.getTimeWindowRun();
			if(twr == null) {
				twr = new TimeWindow();
				process.setTimeWindowRun(twr);
			}
			long time = 0;
			twr.setStartLong(time);
			twr.setEndLong(time);
		}
	}
	// </uima-3351>
	
	private void adjustWindows(IDuccWorkJob job, IDuccProcess process) {
		String methodName = "adjustWindows";
		ITimeWindow twi = process.getTimeWindowInit();
		long i0 = twi.getStartLong();
		long i1 = twi.getEndLong();
		ITimeWindow twr = process.getTimeWindowRun();
		long r0 = twr.getStartLong();
		long r1 = twr.getEndLong();
		if(i0 != i1) {
			if(r0 != r1) {
				if(r0 < i1) {
					logger.warn(methodName, job.getDuccId(), process.getDuccId(), "run-start: "+r0+" -> "+i1);
					r0 = i1;
					twr.setStartLong(r0);
					if(r1 < r0) {
						logger.warn(methodName, job.getDuccId(), process.getDuccId(), "run-end: "+r1+" -> "+r0);
						r1 = r0;
						twr.setEndLong(r1);
					}
				}
			}
		}
	}
	
	public void updateProcessTime(IDuccWorkJob job, IDuccProcess inventoryProcess, IDuccProcess process) {
		String methodName = "updateProcessTime";
		logger.trace(methodName, job.getDuccId(), messages.fetch("enter"));
		switch(inventoryProcess.getProcessState()) {
		case Starting:              // Process Manager sent request to start the Process
		case Initializing:			// Process Agent is initializing process	
			copyTimeInit(inventoryProcess, process);
			break;
		case Running:				// Process Agent is processing work items
			copyTimeInit(inventoryProcess, process);
			initStop(job, process);
			runStart(job, process);
			copyTimeRun(inventoryProcess, process);
			break;
		case Stopped:				// Process Agent reports process stopped
		case Failed:				// Process Agent reports process failed
		case FailedInitialization:	// Process Agent reports process failed initialization
		case InitializationTimeout: // Process Agent reports process initialization timeout
		case Killed:				// Agent forcefully killed the process
			copyTimeInit(inventoryProcess, process);
			copyTimeRun(inventoryProcess, process);
			initStop(job, process);
			runStop(job, process);
			break;
		case Undefined:
			break;
		}
		logger.trace(methodName, job.getDuccId(), messages.fetch("exit"));
	}
	
	public void setStatus(IDuccProcess inventoryProcess) {
		String methodName = "setStatus";
		logger.trace(methodName, null, messages.fetch("enter"));
		DuccId processId = inventoryProcess.getDuccId();
		logger.debug(methodName, null, processId, messages.fetchLabel("node")+inventoryProcess.getNodeIdentity().getName()+" "+messages.fetchLabel("PID")+inventoryProcess.getPID());
		long t0 = System.currentTimeMillis();
		synchronized(workMap) {
			if(processToJobMap.containsKey(processId)) {
				DuccId jobId = getJobId(processId);
				IDuccWork duccWork = workMap.findDuccWork(jobId);
				if(duccWork != null) {
					if(duccWork instanceof IDuccWorkExecutable) {
						IDuccWorkExecutable duccWorkExecutable = (IDuccWorkExecutable) duccWork;
						IDuccWorkJob job = null;
						if(duccWork instanceof IDuccWorkJob) { 
							job = (IDuccWorkJob)duccWork;
						}
						IDuccProcessMap processMap = duccWorkExecutable.getProcessMap();
						IDuccProcess process = processMap.get(processId);
						if(process == null) {
							if(job != null) { 
								process = job.getDriver().getProcessMap().get(processId);
							}
						}
						if(process != null) {
							// PID
							copyInventoryPID(job, inventoryProcess, process);
							// Scheduler State
							setResourceStateAndReason(job, inventoryProcess, process);
							// Process State
							copyInventoryProcessState(job, inventoryProcess, process);
							// Process Init & Run times
							updateProcessTime(job, inventoryProcess, process);
							// Process Initialization State
							switch(inventoryProcess.getProcessState()) {
							case Running:
								process.setInitialized();
								if(job != null) {
									switch(job.getDuccType()) {
									case Service:
										switch(job.getJobState()) {
										case Initializing:
											stateJobAccounting.stateChange(job, JobState.Running);
											break;
										}
										break;
									}
								}
							}
							// Process Pipeline Components State
							copyUimaPipelineComponentsState(job, inventoryProcess, process);
							// Process Swap Usage
							copyInventorySwapUsage(job, inventoryProcess, process);
							// Process Major Faults
							copyInventoryMajorFaults(job, inventoryProcess, process);
							// Process Rss
							copyInventoryRss(job, inventoryProcess, process);
							// Process GC Stats
							copyInventoryGCStats(job, inventoryProcess, process);
							// Process CPU Time
							copyInventoryCpuTime(job, inventoryProcess, process);
						}
						else {
							logger.warn(methodName, jobId, processId, messages.fetch("process not found job's process table"));
						}
					}
					else {
						logger.warn(methodName, jobId, processId, messages.fetch("not executable"));
					}
				}
				else {
					logger.warn(methodName, jobId, processId, messages.fetch("job ID not found"));
				}
			}
			else {
				logger.warn(methodName, null, processId, messages.fetch("ID not found in process map"));
			}
		}
		long t1 = System.currentTimeMillis();
		long elapsed = t1 - t0;
		if(elapsed > Constants.SYNC_LIMIT) {
			logger.debug(methodName, null, "elapsed msecs: "+elapsed);
		}
		logger.trace(methodName, null, messages.fetch("exit"));
		return;
	}
	
	public boolean setStatus(DriverStatusReport jdStatusReport, DuccWorkJob duccWorkJob) {
		String methodName = "setStatus";
		logger.trace(methodName, null, messages.fetch("enter"));
		boolean retVal = false;
		String jdTotalWorkItems = ""+jdStatusReport.getWorkItemsTotal();
		if(!compare(jdTotalWorkItems,duccWorkJob.getSchedulingInfo().getWorkItemsTotal())) {
			duccWorkJob.getSchedulingInfo().setWorkItemsTotal(jdTotalWorkItems);
			// <uima-3533> Limit max-shares to most that can be used (relevant only for fixed-shares)
			long total_work = jdStatusReport.getWorkItemsTotal();
			int tps = duccWorkJob.getSchedulingInfo().getIntThreadsPerShare();
			if (total_work > 0 && tps > 0) {
			    long max_usable = (total_work + tps - 1) / tps;
			    long max_shares = duccWorkJob.getSchedulingInfo().getLongSharesMax();
			    if (max_shares > max_usable) {
			        duccWorkJob.getSchedulingInfo().setLongSharesMax(max_usable);
			        logger.info(methodName, duccWorkJob.getDuccId(), "reduced max-shares", max_shares, "to max-usable", max_usable);
			    }
			}
			// </uima-3353>
		}
		String jdCompletedWorkItems = ""+jdStatusReport.getWorkItemsProcessingCompleted();
		if(!compare(jdCompletedWorkItems,duccWorkJob.getSchedulingInfo().getWorkItemsCompleted())) {
			duccWorkJob.getSchedulingInfo().setWorkItemsCompleted(jdCompletedWorkItems);
		}
		String jdDispatchedWorkItems = ""+jdStatusReport.getWorkItemsDispatched();
		if(!compare(jdDispatchedWorkItems,duccWorkJob.getSchedulingInfo().getWorkItemsDispatched())) {
			duccWorkJob.getSchedulingInfo().setWorkItemsDispatched(jdDispatchedWorkItems);
		}
		String jdErrorWorkItems = ""+jdStatusReport.getWorkItemsProcessingError();
		if(!compare(jdErrorWorkItems,duccWorkJob.getSchedulingInfo().getWorkItemsError())) {
			duccWorkJob.getSchedulingInfo().setWorkItemsError(jdErrorWorkItems);
		}
		String jdRetryWorkItems = ""+jdStatusReport.getWorkItemsRetry();
		if(!compare(jdRetryWorkItems,duccWorkJob.getSchedulingInfo().getWorkItemsRetry())) {
			duccWorkJob.getSchedulingInfo().setWorkItemsRetry(jdRetryWorkItems);
		}
		String jdLostWorkItems = ""+jdStatusReport.getWorkItemsLost();
		if(!compare(jdLostWorkItems,duccWorkJob.getSchedulingInfo().getWorkItemsLost())) {
			duccWorkJob.getSchedulingInfo().setWorkItemsLost(jdLostWorkItems);
		}
		String jdPreemptWorkItems = ""+jdStatusReport.getWorkItemsPreempted();
		if(!compare(jdPreemptWorkItems,duccWorkJob.getSchedulingInfo().getWorkItemsPreempt())) {
			duccWorkJob.getSchedulingInfo().setWorkItemsPreempt(jdPreemptWorkItems);
		}
		
		duccWorkJob.getSchedulingInfo().setCasQueuedMap(jdStatusReport.getCasQueuedMap());
		duccWorkJob.getSchedulingInfo().setLimboMap(jdStatusReport.getLimboMap());
		
		duccWorkJob.getSchedulingInfo().setMostRecentWorkItemStart(jdStatusReport.getMostRecentStart());
		
		duccWorkJob.getSchedulingInfo().setPerWorkItemStatistics(jdStatusReport.getPerWorkItemStatistics());

		logger.trace(methodName, null, messages.fetch("exit"));
		return retVal;
	}
	
	private void deallocate(IDuccWorkJob job, ProcessDeallocationType processDeallocationType, ProcessState processState, IDuccProcessMap processMap, String type) {
		String methodName = "deallocate";
		logger.trace(methodName, job.getDuccId(), messages.fetch("enter"));
		if(processMap != null) {
			Collection<IDuccProcess> processCollection = processMap.values();
			Iterator<IDuccProcess> iterator = processCollection.iterator();
			while(iterator.hasNext()) {
				IDuccProcess process = iterator.next();
				switch(process.getResourceState()) {
				case Allocated:
					process.setResourceState(ResourceState.Deallocated);
					process.setProcessDeallocationType(processDeallocationType);
					logger.info(methodName, job.getDuccId(), process.getDuccId(), type);
					if(processState != null) {
						logger.debug(methodName, job.getDuccId(), process.getProcessState()+" -> "+processState);
						process.advanceProcessState(processState);
					}
					break;
				case Deallocated:	
					if(processState != null) {
						logger.debug(methodName, job.getDuccId(), process.getProcessState()+" -> "+processState);
						process.advanceProcessState(processState);
					}
					break;
				}
			}
		}
		logger.trace(methodName, job.getDuccId(), messages.fetch("exit"));
		return;
	}
	
	private void deallocate(IDuccWorkJob job, ProcessDeallocationType processDeallocationType, ProcessState processState) {
		String methodName = "deallocate";
		logger.trace(methodName, job.getDuccId(), messages.fetch("enter"));
		deallocate(job,processDeallocationType,processState,job.getProcessMap(),"worker");
		switch(job.getDuccType()) {
		case Job:
			deallocate(job,processDeallocationType,processState,job.getDriver().getProcessMap(),"driver");
			break;
		case Service:
			break;
		}
		logger.trace(methodName, job.getDuccId(), messages.fetch("exit"));
		return;
	}
	
	public void deallocate(IDuccWorkJob job, ProcessDeallocationType processDeallocationType) {
		deallocate(job,processDeallocationType,null);
	}
	
	public void deallocateAndStop(IDuccWorkJob job, ProcessDeallocationType processDeallocationType) {
		deallocate(job,processDeallocationType,ProcessState.Stopped);
	}
}
