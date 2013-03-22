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
package org.apache.uima.ducc.orchestrator.maintenance;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import org.apache.uima.ducc.common.internationalization.Messages;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.orchestrator.Constants;
import org.apache.uima.ducc.orchestrator.OrchestratorCheckpoint;
import org.apache.uima.ducc.orchestrator.OrchestratorCommonArea;
import org.apache.uima.ducc.orchestrator.StateManager;
import org.apache.uima.ducc.transport.event.common.DuccWorkMap;
import org.apache.uima.ducc.transport.event.common.DuccWorkPopDriver;
import org.apache.uima.ducc.transport.event.common.IDuccProcess;
import org.apache.uima.ducc.transport.event.common.IDuccProcessMap;
import org.apache.uima.ducc.transport.event.common.IDuccWorkJob;
import org.apache.uima.ducc.transport.event.common.IRationale;
import org.apache.uima.ducc.transport.event.common.Rationale;
import org.apache.uima.ducc.transport.event.common.IDuccCompletionType.JobCompletionType;
import org.apache.uima.ducc.transport.event.common.IProcessState.ProcessState;
import org.apache.uima.ducc.transport.event.common.IResourceState.ProcessDeallocationType;
import org.apache.uima.ducc.transport.event.common.IResourceState.ResourceState;


public class HealthMonitor {
	
	private static final DuccLogger logger = DuccLoggerComponents.getOrLogger(HealthMonitor.class.getName());
	
	private static HealthMonitor healthMonitor = new HealthMonitor();
	
	public static HealthMonitor getInstance() {
		return healthMonitor;
	}
	
	private OrchestratorCommonArea orchestratorCommonArea = OrchestratorCommonArea.getInstance();
	private Messages messages = orchestratorCommonArea.getSystemMessages();
	private DuccWorkMap workMap = orchestratorCommonArea.getWorkMap();

	private boolean isCancelJobExcessiveInitializationFailures(IDuccWorkJob job) {
		String methodName = "isCancelJobExcessiveInitializationFailures";
		logger.trace(methodName, null, messages.fetch("enter"));
		boolean ckpt = false;
		if(!job.isInitialized()) {
			long count = job.getProcessInitFailureCount();
			long limit = job.getProcessInitFailureLimit();
			if(count >= limit) {
				IRationale rationale = new Rationale("health monitor detected job initialization failures limit reached:"+limit);
				StateManager.getInstance().jobTerminate(job, JobCompletionType.ProcessInitializationFailure, rationale, ProcessDeallocationType.JobCanceled);
				logger.info(methodName, job.getDuccId(), JobCompletionType.ProcessInitializationFailure);
				ckpt = true;
			}
		}
		logger.trace(methodName, null, messages.fetch("exit"));
		return ckpt;
	}
	
	private boolean isCancelJobCappedWithNoJobProcesses(IDuccWorkJob job) {
		String methodName = "isCancelJobCappedWithNoJobProcesses";
		logger.trace(methodName, null, messages.fetch("enter"));
		boolean ckpt = false;
		long count = job.getProcessInitFailureCount();
		long cap = job.getProcessInitFailureCap();
		long procs = job.getAliveProcessCount();
		logger.debug(methodName, null, "fail.count:"+count+" "+"fail.cap:"+cap+" "+"alive.procs:"+procs);
		if(count >= cap) {
			if(job.getAliveProcessCount() == 0) {
				IRationale rationale = new Rationale("health monitor detected no resources assigned and job initialization failures cap reached:"+cap);
				StateManager.getInstance().jobTerminate(job, JobCompletionType.ProcessInitializationFailure, rationale, ProcessDeallocationType.JobCanceled);
				logger.info(methodName, job.getDuccId(), JobCompletionType.ProcessInitializationFailure);
				ckpt = true;
			}
		}
		logger.trace(methodName, null, messages.fetch("exit"));
		return ckpt;
	}
	
	private boolean isCancelJobExcessiveProcessFailures(IDuccWorkJob job) {
		String methodName = "isCancelJobExcessiveProcessFailures";
		logger.trace(methodName, null, messages.fetch("enter"));
		boolean ckpt = false;
		long processFailureCount = job.getProcessFailureCount();
		if(processFailureCount > 0) {
			long limit = job.getProcessFailureLimit();
			if(job.isInitialized()) {
				if(processFailureCount >= limit) {
					IRationale rationale = new Rationale("health monitor detected job process failures limit reached:"+limit);
					StateManager.getInstance().jobTerminate(job, JobCompletionType.ProcessFailure, rationale, ProcessDeallocationType.JobCanceled);
					logger.info(methodName, job.getDuccId(), JobCompletionType.ProcessFailure);
					ckpt = true;
				}
			}
			else {
				IRationale rationale = new Rationale("health monitor detected job process failure during initialization of first process");
				StateManager.getInstance().jobTerminate(job, JobCompletionType.ProcessInitializationFailure, rationale, ProcessDeallocationType.JobCanceled);
				logger.info(methodName, job.getDuccId(), JobCompletionType.ProcessInitializationFailure);
				ckpt = true;
			}
		}
		logger.trace(methodName, null, messages.fetch("exit"));
		return ckpt;
	}
	
	private boolean isCancelJobDriverProcessFailed(IDuccWorkJob job) {
		String methodName = "isCancelJobDriverProcessFailed";
		logger.trace(methodName, null, messages.fetch("enter"));
		boolean ckpt = false;
		if(!job.isFinished()) {
			DuccWorkPopDriver driver = job.getDriver();
			IDuccProcessMap processMap = driver.getProcessMap();
			if(processMap != null) {
				Collection<IDuccProcess> processCollection = processMap.values();
				Iterator<IDuccProcess> iterator = processCollection.iterator();
				while(iterator.hasNext()) {
					IDuccProcess process = iterator.next();
					if(process.isComplete()) {
						IRationale rationale = new Rationale("health monitor detected job driver failed unexpectedly");
						StateManager.getInstance().jobTerminate(job, JobCompletionType.DriverProcessFailed, rationale, ProcessDeallocationType.JobCanceled);
						logger.info(methodName, job.getDuccId(), JobCompletionType.DriverProcessFailed);
						ckpt = true;
						break;
					}
				}
			}
			if(job.getProcessMap().getAliveProcessCount() == 0) {
				job.getSchedulingInfo().setWorkItemsDispatched("0");
			}
		}
		else {
			if(!job.getSchedulingInfo().getWorkItemsDispatched().equals("0")) {
				job.getSchedulingInfo().setWorkItemsDispatched("0");
				logger.info(methodName, job.getDuccId(), "dispatched set to 0");
				ckpt = true;
			}
		}
		logger.trace(methodName, null, messages.fetch("exit"));
		return ckpt;
	}
	
	private boolean isDriverCompleted(IDuccWorkJob job) {
		String methodName = "isDriverCompleted";
		logger.trace(methodName, null, messages.fetch("enter"));
		boolean ckpt = false;
		if(job.isFinished()) {
			DuccWorkPopDriver driver = job.getDriver();
			IDuccProcessMap processMap = driver.getProcessMap();
			if(processMap != null) {
				Collection<IDuccProcess> processCollection = processMap.values();
				Iterator<IDuccProcess> iterator = processCollection.iterator();
				while(iterator.hasNext()) {
					IDuccProcess process = iterator.next();
					if(!process.isDeallocated()) {
						process.setResourceState(ResourceState.Deallocated);
						process.setProcessDeallocationType(ProcessDeallocationType.JobCompleted);
						logger.info(methodName, job.getDuccId(), process.getDuccId(), ProcessDeallocationType.JobCompleted);
						ckpt = true;
					}
					else {
						if(!process.isComplete()) {
							String nodeName = process.getNodeIdentity().getName();
							if(!NodeAccounting.getInstance().isAlive(nodeName)) {
								process.advanceProcessState(ProcessState.Stopped);
								logger.info(methodName, job.getDuccId(), process.getDuccId(), ProcessState.Stopped);
								ckpt = true;
							}
						}
					}
				}
			}
		}
		logger.trace(methodName, null, messages.fetch("exit"));
		return ckpt;
	}
	
	private void ajudicateJobs() {
		String methodName = "ajudicateJobs";
		logger.trace(methodName, null, messages.fetch("enter"));
		long t0 = System.currentTimeMillis();
		try {
			Set<DuccId> jobKeySet = workMap.getJobKeySet();
			boolean ckpt = false;
			for(DuccId jobId : jobKeySet) {
				try {
					IDuccWorkJob job = (IDuccWorkJob) workMap.findDuccWork(jobId);
					if(isDriverCompleted(job)) {
						ckpt = true;
					}
					if(isCancelJobExcessiveProcessFailures(job)) {
						ckpt = true;
					}
					else if(isCancelJobCappedWithNoJobProcesses(job)) {
						ckpt = true;
					}
					else if(isCancelJobDriverProcessFailed(job)) {
						ckpt = true;
					}
					long cap = job.getProcessInitFailureCap();
					// if an initialization cap was specified
					if(cap > 0) {
						long initFails = job.getProcessInitFailureCount();
						// if current number of initialization failures exceeds specified cap
						if(initFails > cap) {
							// set job's max shares to -1, indicating stop process expansion to RM
							job.getSchedulingInfo().setLongSharesMax(-1);
						}
					}
				}
				catch(Exception e) {
					logger.error(methodName, null, e);
				}
			}
			if(ckpt) {
				OrchestratorCheckpoint.getInstance().saveState();
			}
		}
		catch(Throwable t) {
			logger.error(methodName, null, t);
		}
		long t1 = System.currentTimeMillis();
		long elapsed = t1 - t0;
		if(elapsed > Constants.SYNC_LIMIT) {
			logger.debug(methodName, null, "elapsed msecs: "+elapsed);
		}		
		logger.trace(methodName, null, messages.fetch("exit"));
		return;
	}
	
	private void ajudicateServices() {
		String methodName = "ajudicateServices";
		logger.trace(methodName, null, messages.fetch("enter"));
		long t0 = System.currentTimeMillis();
		try {
			Set<DuccId> serviceKeySet = workMap.getServiceKeySet();
			boolean ckpt = false;
			for(DuccId serviceId : serviceKeySet) {
				try {
					IDuccWorkJob service = (IDuccWorkJob) workMap.findDuccWork(serviceId);
					if(isCancelJobExcessiveProcessFailures(service)) {
						ckpt = true;
					}
					else if(isCancelJobExcessiveInitializationFailures(service)) {
						ckpt = true;
					}
					long cap = service.getProcessInitFailureCap();
					// if an initialization cap was specified
					if(cap > 0) {
						long initFails = service.getProcessInitFailureCount();
						// if current number of initialization failures exceeds specified cap
						if(initFails > cap) {
							// set job's max shares to -1, indicating stop process expansion to RM
							service.getSchedulingInfo().setLongSharesMax(-1);
						}
					}
				}
				catch(Exception e) {
					logger.error(methodName, null, e);
				}
			}
			if(ckpt) {
				OrchestratorCheckpoint.getInstance().saveState();
			}
		}
		catch(Throwable t) {
			logger.error(methodName, null, t);
		}
		long t1 = System.currentTimeMillis();
		long elapsed = t1 - t0;
		if(elapsed > Constants.SYNC_LIMIT) {
			logger.debug(methodName, null, "elapsed msecs: "+elapsed);
		}		
		logger.trace(methodName, null, messages.fetch("exit"));
		return;
	}
	
	public void ajudicate() {
		ajudicateJobs();
		ajudicateServices();
	}
}
