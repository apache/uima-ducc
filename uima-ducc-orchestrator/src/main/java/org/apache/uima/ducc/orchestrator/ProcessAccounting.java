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

import org.apache.uima.ducc.common.internationalization.Messages;
import org.apache.uima.ducc.common.node.metrics.ProcessGarbageCollectionStats;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.TimeStamp;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.orchestrator.user.UserLogging;
import org.apache.uima.ducc.orchestrator.utilities.TrackSync;
import org.apache.uima.ducc.transport.agent.IUimaPipelineAEComponent;
import org.apache.uima.ducc.transport.event.common.DuccWorkJob;
import org.apache.uima.ducc.transport.event.common.DuccWorkMap;
import org.apache.uima.ducc.transport.event.common.DuccWorkPopDriver;
import org.apache.uima.ducc.transport.event.common.IDuccProcess;
import org.apache.uima.ducc.transport.event.common.IDuccProcessMap;
import org.apache.uima.ducc.transport.event.common.IDuccSchedulingInfo;
import org.apache.uima.ducc.transport.event.common.IDuccState.JobState;
import org.apache.uima.ducc.transport.event.common.IDuccWork;
import org.apache.uima.ducc.transport.event.common.IDuccWorkExecutable;
import org.apache.uima.ducc.transport.event.common.IDuccWorkJob;
import org.apache.uima.ducc.transport.event.common.IProcessState.ProcessState;
import org.apache.uima.ducc.transport.event.common.IResourceState.ProcessDeallocationType;
import org.apache.uima.ducc.transport.event.common.IResourceState.ResourceState;
import org.apache.uima.ducc.transport.event.common.ITimeWindow;
import org.apache.uima.ducc.transport.event.common.TimeWindow;
import org.apache.uima.ducc.transport.event.jd.IDriverStatusReport;


public class ProcessAccounting {
	
	private static final DuccLogger logger = DuccLoggerComponents.getOrLogger(ProcessAccounting.class.getName());
	
	private OrchestratorCommonArea orchestratorCommonArea = OrchestratorCommonArea.getInstance();
	private Messages messages = orchestratorCommonArea.getSystemMessages();
	
	private ProcessToJobMap processToJobMap = ProcessToJobMap.getInstance();
	private StateJobAccounting stateJobAccounting = StateJobAccounting.getInstance();
	
	public ProcessAccounting() {
	}
	
	public DuccId getJobId(DuccId processId) {
		String methodName = "getJobId";
		DuccId retVal;
		DuccWorkMap workMap = orchestratorCommonArea.getWorkMap();
		TrackSync ts = TrackSync.await(workMap, this.getClass(), methodName);
		synchronized(workMap) {
			ts.using();
			retVal = processToJobMap.get(processId);
		}
		ts.ended();
		return retVal;
	}
	
	public int processCount() {
		String methodName = "processCount";
		int retVal;
		DuccWorkMap workMap = orchestratorCommonArea.getWorkMap();
		TrackSync ts = TrackSync.await(workMap, this.getClass(), methodName);
		synchronized(workMap) {
			ts.using();
			retVal = processToJobMap.size();
		}
		ts.ended();
		return retVal;
	}
	
	public boolean addProcess(DuccId processId, DuccId jobId) {
		String methodName = "addProcess";
		logger.trace(methodName, null, messages.fetch("enter"));
		boolean retVal = false;
		DuccWorkMap workMap = orchestratorCommonArea.getWorkMap();
		TrackSync ts = TrackSync.await(workMap, this.getClass(), methodName);
		ts.using();
		DuccId key = processToJobMap.put(processId, jobId);
		if(key == null) {
			retVal = true;
			logger.info(methodName, jobId, processId, messages.fetch("added"));
		}
		else {
			logger.warn(methodName, jobId, processId, messages.fetch("exists"));
		}
		ts.ended();
		logger.trace(methodName, null, messages.fetch("exit"));
		return retVal;
	}
	
	public boolean removeProcess(DuccId processId) {
		String methodName = "removeProcess";
		logger.trace(methodName, null, messages.fetch("enter"));
		boolean retVal = false;
		DuccWorkMap workMap = orchestratorCommonArea.getWorkMap();
		TrackSync ts = TrackSync.await(workMap, this.getClass(), methodName);
		synchronized(workMap) {
			ts.using();
			if(processToJobMap.containsKey(processId)) {
				DuccId jobId = processToJobMap.remove(processId);
				retVal = true;
				logger.info(methodName, jobId, processId, messages.fetch("removed"));
			}
			else {
				logger.warn(methodName, null, processId, messages.fetch("not found"));
			}
		}
		ts.ended();
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
	
	private void copyInventoryPID(IDuccWork dw, IDuccProcess inventoryProcess, IDuccProcess process) {
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
	
	private void copyInventorySwapUsage(IDuccWork dw, IDuccProcess inventoryProcess, IDuccProcess process) {
		String methodName = "copyInventorySwapUsage";
		logger.trace(methodName, null, messages.fetch("enter"));
		long value = inventoryProcess.getSwapUsage();
		logger.trace(methodName, dw.getDuccId(), inventoryProcess.getDuccId(), "PID:"+process.getPID()+" "+"swap:"+value);
		process.setSwapUsage(value);
		if(process.getSwapUsageMax() < process.getSwapUsage()) {
			process.setSwapUsageMax(process.getSwapUsage());
		}
		logger.trace(methodName, null, messages.fetch("exit"));
		return;
	}
	
	private void copyInventoryMajorFaults(IDuccWork dw, IDuccProcess inventoryProcess, IDuccProcess process) {
		String methodName = "copyInventoryMajorFaults";
		logger.trace(methodName, null, messages.fetch("enter"));
		process.setMajorFaults(inventoryProcess.getMajorFaults());
		logger.trace(methodName, null, messages.fetch("exit"));
		return;
	}
	
	private void copyInventoryRss(IDuccWork dw, IDuccProcess inventoryProcess, IDuccProcess process) {
		String methodName = "copyInventoryRss";
		logger.trace(methodName, null, messages.fetch("enter"));
		process.setResidentMemory(inventoryProcess.getResidentMemory());
		logger.trace(methodName, null, messages.fetch("exit"));
		return;
	}
	
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
	
	private void copyInventoryGCStats(IDuccWork dw, IDuccProcess inventoryProcess, IDuccProcess process) {
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
	
	private void copyInventoryCpuTime(IDuccWork dw, IDuccProcess inventoryProcess, IDuccProcess process) {
		String methodName = "copyInventoryCpuTime";
		logger.trace(methodName, null, messages.fetch("enter"));
		process.setCpuTime(inventoryProcess.getCpuTime());
		process.setCurrentCPU(inventoryProcess.getCurrentCPU());
		logger.trace(methodName, dw.getDuccId(), process.getDuccId(), "Cpu Time (overall):"+process.getCpuTime());
		logger.trace(methodName, dw.getDuccId(), process.getDuccId(), "Cpu Time (current):"+process.getCurrentCPU());
		logger.trace(methodName, null, messages.fetch("exit"));
		return;
	}
	
	private void copyTimeInit(IDuccProcess inventoryProcess, IDuccProcess process) {
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
					logger.trace(methodName, jobId, processId, messages.fetchLabel("initialization start")+ts);
				}
				millis = process.getTimeWindowInit().getEnd();
				if(millis != null) {
					ts = TimeStamp.simpleFormat(millis);
					logger.trace(methodName, jobId, processId, messages.fetchLabel("initialization end")+ts);
				}
			}
		}
		logger.trace(methodName, null, messages.fetch("exit"));
		return;
	}
	
	private void copyTimeRun(IDuccProcess inventoryProcess, IDuccProcess process) {
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
					logger.trace(methodName, jobId, processId, messages.fetchLabel("run start")+ts);
				}
				millis = process.getTimeWindowRun().getEnd();
				if(millis != null) {
					ts = TimeStamp.simpleFormat(millis);
					logger.trace(methodName, jobId, processId, messages.fetchLabel("run end")+ts);
				}
			}
			ITimeWindow tw = process.getTimeWindowRun();
			if(tw != null) {
				logger.trace(methodName, jobId, processId, "start:"+tw.getStart()+" "+"end:"+tw.getEnd());
			}
		}
		logger.trace(methodName, null, messages.fetch("exit"));
		return;
	}
	
	private void setResourceStateAndReason(IDuccWorkJob job, IDuccProcess inventoryProcess, IDuccProcess process) {
		String methodName = "setResourceStateAndReason";
		logger.trace(methodName, job.getDuccId(), messages.fetch("enter"));
		StringBuffer message;
		switch(inventoryProcess.getProcessState()) {
		case LaunchFailed:
		case Stopped:
		case Failed:
		case FailedInitialization:
		case InitializationTimeout:
		case Killed:
			switch(process.getResourceState()) {
			case Allocated:
				OrUtil.setResourceState(job, process, ResourceState.Deallocated);
				String reason = inventoryProcess.getReasonForStoppingProcess();
				String extendedReason = inventoryProcess.getExtendedReasonForStoppingProcess();
				message = new StringBuffer();
				message.append(messages.fetchLabel("process state")+inventoryProcess.getProcessState());
				message.append(" => "+messages.fetchLabel("resource state")+process.getResourceState());
				message.append(" : "+messages.fetchLabel("reason")+reason);
				if(extendedReason != null) {
					message.append(", "+extendedReason);
				}
				logger.info(methodName, job.getDuccId(), process.getDuccId(), message.toString());
				switch(inventoryProcess.getProcessState()) {
				case Stopped:
					if(reason != null) {
						process.setReasonForStoppingProcess(reason);
						process.setExtendedReasonForStoppingProcess(extendedReason);
						message = new StringBuffer();
						message.append(messages.fetchLabel("process state")+inventoryProcess.getProcessState());
						message.append(" => "+messages.fetchLabel("resource state")+process.getResourceState());
						message.append(" : "+messages.fetchLabel("reason")+reason);
						if(extendedReason != null) {
							message.append(", "+extendedReason);
						}
						logger.debug(methodName, job.getDuccId(), process.getDuccId(), message.toString());
					}
					process.setProcessDeallocationType(ProcessDeallocationType.AutonomousStop);
					break;
				case LaunchFailed:
				case Failed:
					if(reason != null) {
						process.setReasonForStoppingProcess(reason);
						process.setExtendedReasonForStoppingProcess(extendedReason);
						message = new StringBuffer();
						message.append(messages.fetchLabel("process state")+inventoryProcess.getProcessState());
						message.append(" => "+messages.fetchLabel("resource state")+process.getResourceState());
						message.append(" : "+messages.fetchLabel("reason")+reason);
						if(extendedReason != null) {
							message.append(", "+extendedReason);
						}
						logger.debug(methodName, job.getDuccId(), process.getDuccId(), message.toString());
					}
					process.setProcessDeallocationType(ProcessDeallocationType.Failed);
					break;
				/*
				case FailedInitialization:
					if(reason != null) {
						process.setReasonForStoppingProcess(reason);
						process.setExtendedReasonForStoppingProcess(extendedReason);
						message = new StringBuffer();
						message.append(messages.fetchLabel("process state")+inventoryProcess.getProcessState());
						message.append(" => "+messages.fetchLabel("resource state")+process.getResourceState());
						message.append(" : "+messages.fetchLabel("reason")+reason);
						if(extendedReason != null) {
							message.append(", "+extendedReason);
						}
						logger.debug(methodName, job.getDuccId(), process.getDuccId(), message.toString());
					}
					process.setProcessDeallocationType(ProcessDeallocationType.FailedInitialization);
					break;
				case InitializationTimeout:
					if(reason != null) {
						process.setReasonForStoppingProcess(reason);
						process.setExtendedReasonForStoppingProcess(extendedReason);
						message = new StringBuffer();
						message.append(messages.fetchLabel("process state")+inventoryProcess.getProcessState());
						message.append(" => "+messages.fetchLabel("resource state")+process.getResourceState());
						message.append(" : "+messages.fetchLabel("reason")+reason);
						if(extendedReason != null) {
							message.append(", "+extendedReason);
						}
						logger.debug(methodName, job.getDuccId(), process.getDuccId(), message.toString());
					}
					process.setProcessDeallocationType(ProcessDeallocationType.InitializationTimeout);
					break;
				*/
				case Killed:
					if(reason != null) {
						process.setReasonForStoppingProcess(reason);
						process.setExtendedReasonForStoppingProcess(extendedReason);
						message = new StringBuffer();
						message.append(messages.fetchLabel("process state")+inventoryProcess.getProcessState());
						message.append(" => "+messages.fetchLabel("resource state")+process.getResourceState());
						message.append(" : "+messages.fetchLabel("reason")+reason);
						if(extendedReason != null) {
							message.append(", "+extendedReason);
						}
						logger.debug(methodName, job.getDuccId(), process.getDuccId(), message.toString());
					}
					process.setProcessDeallocationType(ProcessDeallocationType.Killed);
					break;
				default:
					break;
				}
				break;
			default:
				logger.debug(methodName, job.getDuccId(), process.getDuccId(), messages.fetchLabel("process state")+inventoryProcess.getProcessState()+" => "+messages.fetchLabel("resource state")+process.getResourceState());
				break;
			}
			switch(job.getDuccType()) {
			case Service:
				IDuccWorkJob service = job;
				String userName = service.getStandardInfo().getUser();
				String userLogDir = service.getUserLogsDir();
				UserLogging.error(userName, userLogDir, "reason for stopping service instance["+service.getDuccId().getFriendly()+"]: "+process.getReasonForStoppingProcess());
				break;
			default:
				break;
			}
			break;
		default:
			logger.debug(methodName, job.getDuccId(), process.getDuccId(), messages.fetchLabel("process state")+inventoryProcess.getProcessState()+" => "+messages.fetchLabel("resource state")+process.getResourceState());
			break;
		}
		logger.trace(methodName, job.getDuccId(), messages.fetch("exit"));
	}
	
	private void copyInventoryProcessState(IDuccWorkJob job, IDuccProcess inventoryProcess, IDuccProcess process) {
		String methodName = "copyInventoryProcessState";
		logger.trace(methodName, job.getDuccId(), messages.fetch("enter"));
		
		if(!compare(inventoryProcess.getProcessState().toString(),process.getProcessState().toString())) {
			switch((JobState)job.getStateObject()) {
			//case Initializing:
			//	logger.info(methodName, jobId, processId, messages.fetchLabel("process state ignored")+inventoryProcess.getProcessState());
			//	break;
			default:
				process.advanceProcessState(inventoryProcess.getProcessState());
				logger.trace(methodName, job.getDuccId(), process.getDuccId(), messages.fetchLabel("process state")+process.getProcessState());
				if ( inventoryProcess.getProcessJmxUrl() != null && process.getProcessJmxUrl() == null) {
					process.setProcessJmxUrl(inventoryProcess.getProcessJmxUrl());
				}
				
				break;
			}
		}
		logger.trace(methodName, job.getDuccId(), messages.fetch("exit"));
	}
	
	private void copyReasonForStoppingProcess(IDuccWorkJob job, IDuccProcess inventoryProcess, IDuccProcess process) {
		String methodName = "copyReasonForStoppingProcess";
		logger.trace(methodName, job.getDuccId(), messages.fetch("enter"));
		switch(inventoryProcess.getProcessState()) {
		case LaunchFailed:
		case Stopped:
		case Failed:
		case FailedInitialization:
		case InitializationTimeout:
		case Killed:
			String reasonNew = inventoryProcess.getReasonForStoppingProcess();
			String reasonOld = process.getReasonForStoppingProcess();
			String extendedReasonNew = inventoryProcess.getExtendedReasonForStoppingProcess();
			if(reasonNew != null) {
				if(reasonOld == null) {
					process.setReasonForStoppingProcess(reasonNew);
					logger.info(methodName, job.getDuccId(), process.getDuccId(), messages.fetchLabel("process reason code")+process.getReasonForStoppingProcess());
					if(extendedReasonNew != null) {
						process.setExtendedReasonForStoppingProcess(extendedReasonNew);
						logger.info(methodName, job.getDuccId(), process.getDuccId(), messages.fetchLabel("process extended reason code")+process.getExtendedReasonForStoppingProcess());
					}
				}
				else if(!reasonNew.equals(reasonOld)) {
					process.setReasonForStoppingProcess(reasonNew);
					logger.info(methodName, job.getDuccId(), process.getDuccId(), messages.fetchLabel("process reason code")+process.getReasonForStoppingProcess());
					if(extendedReasonNew != null) {
						process.setExtendedReasonForStoppingProcess(extendedReasonNew);
						logger.info(methodName, job.getDuccId(), process.getDuccId(), messages.fetchLabel("process extended reason code")+process.getExtendedReasonForStoppingProcess());
					}
				}
			}
			
			break;
		default:
			break;
		}
		logger.trace(methodName, job.getDuccId(), messages.fetch("exit"));
	}	
	
	private void copyProcessExitCode(IDuccWorkJob job, IDuccProcess inventoryProcess, IDuccProcess process) {
		String methodName = "copyProcessExitCode";
		logger.trace(methodName, job.getDuccId(), messages.fetch("enter"));
		switch(inventoryProcess.getProcessState()) {
		case LaunchFailed:
		case Stopped:
		case Failed:
		case FailedInitialization:
		case InitializationTimeout:
		case Killed:
			int codeNew = inventoryProcess.getProcessExitCode();
			int codeOld = process.getProcessExitCode();
			if(codeNew != codeOld) {
				process.setProcessExitCode(codeNew);
				logger.info(methodName, job.getDuccId(), process.getDuccId(), messages.fetchLabel("process exit code")+process.getProcessExitCode());
			}
			break;
		default:
			break;
		}
		logger.trace(methodName, job.getDuccId(), messages.fetch("exit"));
	}	
	
	private void copyUimaPipelineComponentsState(IDuccWorkJob job, IDuccProcess inventoryProcess, IDuccProcess process) {
		String methodName = "copyUimaPipelineComponentsState";
		logger.trace(methodName, job.getDuccId(), messages.fetch("enter"));
		List<IUimaPipelineAEComponent> list = inventoryProcess.getUimaPipelineComponents();
		if(list != null) {
			logger.trace(methodName, job.getDuccId(), "size: "+list.size());
			process.setUimaPipelineComponents(list);
		}
		else {
			logger.trace(methodName, job.getDuccId(), "size: null");
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
		adjustRunTime(job, process);
	}
	
	// <uima-3351>
	private void adjustRunTime(IDuccWorkJob job, IDuccProcess process) {
		switch(job.getDuccType()) {
		case Job:
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
			break;
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
	
	private void updateProcessInitilization(IDuccWorkJob job, IDuccProcess inventoryProcess, IDuccProcess process) {
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
					default:
						break;
					}
					break;
				default:
					break;
				}
			}
		default:
			break;
		}
	}
	
	private void updateProcessTime(IDuccWorkJob job, IDuccProcess inventoryProcess, IDuccProcess process) {
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
		case LaunchFailed:			// Process Agent reports process launch failed
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
		default:
			break;
		}
		logger.trace(methodName, job.getDuccId(), messages.fetch("exit"));
	}

	private IDuccProcess getProcess(IDuccWorkExecutable dw, DuccId processId) {
		IDuccProcess process = null;
		if(dw != null) {
			if(processId != null) {
				IDuccProcessMap map = null;
				map = dw.getProcessMap();
				if(map != null) {
					process = map.get(processId);
					if(process == null) {
						if(dw instanceof IDuccWorkJob) {
							IDuccWorkJob job = (IDuccWorkJob) dw;
							DuccWorkPopDriver driver = job.getDriver();
							if(driver != null) {
								map = driver.getProcessMap();
								if(map != null) {
									process = map.get(processId);
								}
							}
						}
					}
				}
			}
		}
		return process;
	}
	
	public void setStatus(IDuccProcess inventoryProcess) {
		String methodName = "setStatus";
		logger.trace(methodName, null, messages.fetch("enter"));
		DuccWorkMap workMap = orchestratorCommonArea.getWorkMap();
		try {
			DuccId processId = inventoryProcess.getDuccId();
			logger.debug(methodName, null, processId, messages.fetchLabel("node")+inventoryProcess.getNodeIdentity().getName()+" "+messages.fetchLabel("PID")+inventoryProcess.getPID());
			TrackSync ts = TrackSync.await(workMap, this.getClass(), methodName);
			synchronized(workMap) {
				ts.using();
				if(processToJobMap.containsKey(processId)) {
					logger.trace(methodName, null, processId, "key found");
					DuccId jobId = getJobId(processId);
					logger.trace(methodName, jobId, processId, "jobId from process map");
					IDuccWork dw = WorkMapHelper.findDuccWork(workMap, jobId, this, methodName);
					if(dw != null) {
						logger.trace(methodName, dw.getDuccId(), processId, "entity found in work map");
						if(dw instanceof IDuccWorkExecutable) {
							IDuccWorkJob job = null;
							if(dw instanceof IDuccWorkJob) { 
								job = (IDuccWorkJob)dw;
								IDuccProcess process = getProcess(job, processId);
								if(process != null) {
									logger.trace(methodName, job.getDuccId(), processId, "process found");
									if(process.isComplete()) {
										logger.trace(methodName, jobId, process.getDuccId(), "finalized");
									}
									else {
										logger.trace(methodName, jobId, process.getDuccId(), "active");
										// PID
										copyInventoryPID(job, inventoryProcess, process);
										// Scheduler State
										setResourceStateAndReason(job, inventoryProcess, process);
										// Process State
										copyInventoryProcessState(job, inventoryProcess, process);
										// Process Reason
										copyReasonForStoppingProcess(job, inventoryProcess, process);
										// Process Exit code
										copyProcessExitCode(job, inventoryProcess, process);
										// Process Init & Run times
										updateProcessTime(job, inventoryProcess, process);
										// Process Initialization State
										updateProcessInitilization(job, inventoryProcess, process);
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
								}
								else {
									logger.warn(methodName, dw.getDuccId(), processId, messages.fetch("process not found in job's process table"));
								}
							}
							else {
								logger.warn(methodName, dw.getDuccId(), processId, "entity is not job");
							}
						}
						else {
							logger.warn(methodName, jobId, processId, messages.fetch("not executable"));
						}
					}
					else {
						logger.warn(methodName, jobId, processId, messages.fetch("ID not found"));
					}
				}
				else {
					logger.warn(methodName, null, processId, messages.fetch("ID not found in process map"));
				}
			}
			ts.ended();
		}
		catch(Throwable t) {
			logger.error(methodName, null, t);
		}
		logger.trace(methodName, null, messages.fetch("exit"));
		return;
	}
	
	public boolean setStatus(IDriverStatusReport jdStatusReport, DuccWorkJob duccWorkJob) {
		String methodName = "setStatus";
		logger.trace(methodName, null, messages.fetch("enter"));
		boolean retVal = false;
		String jdTotalWorkItems = ""+jdStatusReport.getWorkItemsTotal();
		if(!compare(jdTotalWorkItems,duccWorkJob.getSchedulingInfo().getWorkItemsTotal())) {
			duccWorkJob.getSchedulingInfo().setWorkItemsTotal(jdTotalWorkItems);
			// <uima-3533> Limit max-processes to most that can be used (relevant only for policy fixed)
			long total_work = jdStatusReport.getWorkItemsTotal();
			int tps = duccWorkJob.getSchedulingInfo().getIntThreadsPerProcess();
			if (total_work > 0 && tps > 0) {
			    long max_usable = (total_work + tps - 1) / tps;
			    long max_processes = duccWorkJob.getSchedulingInfo().getLongProcessesMax();
			    if (max_processes > max_usable) {
			        duccWorkJob.getSchedulingInfo().setLongProcessesMax(max_usable);
			        logger.info(methodName, duccWorkJob.getDuccId(), "reduced max-processes", max_processes, "to max-usable", max_usable);
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
		String jdPreemptWorkItems = ""+jdStatusReport.getWorkItemsPreempted();
		if(!compare(jdPreemptWorkItems,duccWorkJob.getSchedulingInfo().getWorkItemsPreempt())) {
			duccWorkJob.getSchedulingInfo().setWorkItemsPreempt(jdPreemptWorkItems);
		}
		
		IDuccSchedulingInfo si = duccWorkJob.getSchedulingInfo();
		
		si.setMostRecentWorkItemStart(jdStatusReport.getMostRecentStart());
		si.setPerWorkItemStatistics(jdStatusReport.getPerWorkItemStatistics());

		double avgTimeForWorkItemsSkewedByActive = jdStatusReport.getAvgTimeForWorkItemsSkewedByActive();
		si.setAvgTimeForWorkItemsSkewedByActive(avgTimeForWorkItemsSkewedByActive);
		
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
					OrUtil.setResourceState(job, process, ResourceState.Deallocated);
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
				default:
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
		default:
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
