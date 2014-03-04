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
package org.apache.uima.ducc.transport.event.jd;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.SerializationUtils;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.common.IDuccCompletionType.JobCompletionType;
import org.apache.uima.ducc.transport.event.common.IDuccPerWorkItemStatistics;
import org.apache.uima.ducc.transport.event.common.IDuccProcessWorkItems;
import org.apache.uima.ducc.transport.event.common.IDuccUimaDeploymentDescriptor;
import org.apache.uima.ducc.transport.event.common.IRationale;
import org.apache.uima.ducc.transport.event.common.Rationale;
import org.apache.uima.ducc.transport.event.common.Util;
import org.apache.uima.ducc.transport.event.jd.IDriverState.DriverState;


@SuppressWarnings("serial")
public class DriverStatusReport implements Serializable {
	
	private static DuccLogger duccOut = DuccLoggerComponents.getJdOut(DriverStatusReport.class.getName());
	private static DuccId jobid = null;
	
	private DuccId duccId = null;
	private String jdJmxUrl = null;
	
	private volatile DriverState driverState = DriverState.NotRunning;
	private JobCompletionType jobCompletionType = JobCompletionType.Undefined;
	private IRationale jobCompletionRationale = null;
	
	private long now = 0;
	private long clientInitStart = 0;
	private long clientInitEnd = 0;
	
	private AtomicBoolean terminateDriver = new AtomicBoolean(false);
	
	private AtomicBoolean atLeastOneService = new AtomicBoolean(false);
	
	private AtomicBoolean workItemsPending = new AtomicBoolean(true);
	
	private AtomicLong workItemsTotal = new AtomicLong(-1);
	
	private AtomicInteger workItemsFetched = new AtomicInteger(0);
	
	private AtomicInteger workItemsProcessingStarted = new AtomicInteger(0);
	private AtomicInteger workItemsProcessingCompleted = new AtomicInteger(0);
	private AtomicInteger workItemsProcessingError = new AtomicInteger(0);
	
	//private AtomicInteger workItemsQueued = new AtomicInteger(0);
	//private AtomicInteger workItemsDequeued = new AtomicInteger(0);
	
	private AtomicInteger workItemsLost = new AtomicInteger(0);
	private AtomicInteger workItemsRetry = new AtomicInteger(0);
	private AtomicInteger workItemsPreempted = new AtomicInteger(0);
	
	private long wiMillisMax = 0;
	private long wiMillisMin = 0;
	private long wiMillisAvg = 0;
	private long wiMillisOperatingLeast = 0;
	private long wiMillisCompletedMost = 0;
	
	private AtomicInteger threadCount = new AtomicInteger(0);
	
	private ConcurrentHashMap<String,DuccId> casQueuedMap = new ConcurrentHashMap<String,DuccId>();
	private ConcurrentHashMap<String,DuccId> casDequeuedPendingMap = new ConcurrentHashMap<String,DuccId>();
	
	private ConcurrentHashMap<String,HashMap<String,String>> casOperatingMap = new ConcurrentHashMap<String,HashMap<String,String>>();
	
	private ConcurrentHashMap<Integer,DuccId> limboMap = new ConcurrentHashMap<Integer,DuccId>();
	
	private ConcurrentHashMap<DuccId,String> killProcessMap = new ConcurrentHashMap<DuccId,String>();
	private AtomicBoolean killJob = new AtomicBoolean(false);
	
	private AtomicLong mostRecentWorkItemStart = new AtomicLong(0);
	
	private IDuccPerWorkItemStatistics perWorkItemStatistics = null;
	@Deprecated
	private PerformanceMetricsSummaryMap performanceMetricsSummaryMap = null;
	
	private ConcurrentHashMap<String,String> pendingProcessAssignmentMap = new ConcurrentHashMap<String,String>();
	
	private IDuccUimaDeploymentDescriptor uimaDeploymentDescriptor = null;
	
	private DuccProcessWorkItemsMap duccProcessWorkItemsMap = new DuccProcessWorkItemsMap();
	
	public DriverStatusReport(DuccId duccId, String jdJmxUrl) {
		setJdJmxUrl(jdJmxUrl);
		setDuccId(duccId);
	}
	
	/*
	 * DuccId
	 */
	public void setDuccId(DuccId duccId) {
		this.duccId = duccId;
	}
	
	public DuccId getDuccId() {
		return duccId;
	}
	
	/*
	 * Id
	 */
	public String getId() {
		return duccId.toString();
	}
	
	/*
	 * DuccProcessWorkItemsMap
	 */
	public void setDuccProcessWorkItemsMap(DuccProcessWorkItemsMap duccProcessWorkItemsMap) {
		this.duccProcessWorkItemsMap = duccProcessWorkItemsMap;
	}
	
	public DuccProcessWorkItemsMap getDuccProcessWorkItemsMap() {
		return duccProcessWorkItemsMap;
	}
	
	/*
	 * JdJmxUrl
	 */
	public void setJdJmxUrl(String jdJmxUrl) {
		this.jdJmxUrl = jdJmxUrl;
	}
	
	public String getJdJmxUrl() {
		return jdJmxUrl;
	}
	
	/*
	 * hashCode
	 */
	public int getHashCode() {
		return duccId.hashCode();
	}
	
	private void setDriverState(DriverState driverState) {
		String methodName = "setDriverState";
		synchronized(this) {
			String prev = getDriverState().toString();
			switch(this.driverState) {
			case Completed:
				break;
			default:
				this.driverState = driverState;
			}
			String curr = getDriverState().toString();
			duccOut.debug(methodName, duccId, "current:"+curr+" "+"previous:"+prev);
		}
	}
	
	public DriverState getDriverState() {
		String methodName = "getDriverState";
		synchronized(this) {
			String curr = driverState.toString();
			duccOut.debug(methodName, duccId, "current:"+curr);
			return driverState;
		}
	}
	
	public boolean isTerminateDriver() {
		return terminateDriver.get();
	}
	
	public void setTerminateDriver() {
		terminateDriver.set(true);
		calculateState();
	}
	

	private void setJobCompletion(JobCompletionType jobCompletionType, IRationale rationale) {
		this.jobCompletionType = jobCompletionType;
		this.jobCompletionRationale = rationale;
	}
	
	/*
	private void setJobCompletionType(JobCompletionType jobCompletionType) {
		this.jobCompletionType = jobCompletionType;
	}
	*/
	
	public JobCompletionType getJobCompletionType() {
		return jobCompletionType;
	}
	
	/*
	private void setJobCompletionRationale(IRationale rationale) {
		this.jobCompletionRationale = rationale;
	}
	*/
	
	public IRationale getJobCompletionRationale() {
		return jobCompletionRationale;
	}
	
	public void setClientInitStart(long time) {
		clientInitStart = time;
	}
	
	public long getClientInitStart() {
		return clientInitStart;
	}
	
	public void setClientInitEnd(long time) {
		clientInitEnd = time;
	}
	
	public long getClientInitEnd() {
		return clientInitEnd;
	}
	
	public void setNow() {
		now = System.currentTimeMillis();
	}
	
	public long getNow() {
		return now;
	}
	
	public void setInitializing() {
		setClientInitStart(System.currentTimeMillis());
		setClientInitEnd(0);
		setDriverState(DriverState.Initializing);
		logReport();
	}

	public void setInitializingCompleted() {
		setClientInitEnd(System.currentTimeMillis());
		setDriverState(DriverState.Idle);
		logReport();
	}

	public void setInitializingFailed(IRationale rationale) {
		setClientInitEnd(System.currentTimeMillis());
		setDriverState(DriverState.Completed);
		setJobCompletion(JobCompletionType.DriverInitializationFailure, rationale);
		logReport();
	}

	public void setExcessiveInitializationFailures(IRationale rationale) {
		setClientInitEnd(System.currentTimeMillis());
		setDriverState(DriverState.Completed);
		setJobCompletion(JobCompletionType.ProcessInitializationFailure, rationale);
		logReport();
	}
	
	public boolean getAtLeastOneService() {
		String methodName = "getAtLeastOneService";
		boolean retVal = atLeastOneService.get();
		duccOut.debug(methodName, jobid, retVal);
		return retVal;
	}
	
	public void setAtLeastOneService() {
		String methodName = "setAtLeastOneService";
		if(!atLeastOneService.get()) {
			atLeastOneService.set(true);
			duccOut.debug(methodName, jobid, atLeastOneService.get());
			calculateState();
			logReport();
		}
	}
	
	public void setWorkItemsPending() {
		String methodName = "setWorkItemsPending";
		if(!workItemsPending.getAndSet(true)) {
			duccOut.debug(methodName, jobid, true);
			calculateState();
			logReport();
		}
	}
	
	public void resetWorkItemsPending() {
		String methodName = "resetWorkItemsPending";
		if(workItemsPending.getAndSet(false)) {
			duccOut.debug(methodName, jobid, false);
			calculateState();
			logReport();
		}
	}
	
	public boolean isPending() {
		String methodName = "isPending";
		boolean retVal = workItemsPending.get();
		duccOut.debug(methodName, jobid, retVal);
		return retVal;
	}
	
	public void setWorkItemsTotal(long total) {
		workItemsTotal.set(total);
		logReport();
	}
	
	public long getWorkItemsTotal() {
		return workItemsTotal.get();
	}
	
	public void setWorkItemsFetched(int update) {
		int expect = workItemsFetched.get();
		while(expect < update) {
			workItemsFetched.compareAndSet(expect, update);
			expect = workItemsFetched.get();
		}
		logReport();
	}
	
	public int getWorkItemsFetched() {
		return workItemsFetched.get();
	}
	
	public void setMostRecentStart(long time) {
		mostRecentWorkItemStart.set(time);
	}
	
	public long getMostRecentStart() {
		return mostRecentWorkItemStart.get();
	}
	
	public void countWorkItemsProcessingStarted() {
		workItemsProcessingStarted.incrementAndGet();
		calculateState();
		logReport();
	}
	
	public int getWorkItemsProcessingStarted() {
		return workItemsProcessingStarted.get();
	}
	
	public void countWorkItemsProcessingCompleted() {
		workItemsProcessingCompleted.incrementAndGet();
		calculateState();
		logReport();
	}
	
	public int getWorkItemsProcessingCompleted() {
		return workItemsProcessingCompleted.get();
	}
	
	public int getWorkItemsOperating() {
		return casOperatingMap.size();
	}
	
	public void countWorkItemsProcessingError() {
		workItemsProcessingError.incrementAndGet();
		calculateState();
		logReport();
	}
	
	public int getWorkItemsProcessingError() {
		return workItemsProcessingError.get();
	}
	
	public void incrementWorkItemsLost() {
		workItemsLost.incrementAndGet();
		calculateState();
		logReport();
	}
	
	public void decrementWorkItemsLost() {
		workItemsLost.decrementAndGet();
		calculateState();
		logReport();
	}
	
	public void decrementWorkItemsLost(int value) {
		int delta = 0 - value;
		workItemsLost.addAndGet(delta);
		calculateState();
		logReport();
	}
	
	public int getWorkItemsLost() {
		return workItemsLost.get();
	}
	
	
	public boolean isWorkItemsLost() {
		return workItemsLost.get() > 0;
	}
	
	public void countWorkItemsRetry() {
		workItemsRetry.incrementAndGet();
		calculateState();
		logReport();
	}
	
	public void incrementWorkItemsRetry(int value) {
		workItemsRetry.addAndGet(value);
		calculateState();
		logReport();
	}
	
	public int getWorkItemsRetry() {
		return workItemsRetry.get();
	}
	
	public void countWorkItemsPreempted() {
		workItemsPreempted.incrementAndGet();
		calculateState();
		logReport();
	}
	
	public int getWorkItemsPreempted() {
		return workItemsPreempted.get();
	}
	
	public long getWorkItemsToDo() {
		long total = getWorkItemsTotal();
		long done = getWorkItemsProcessingCompleted();
		long error = getWorkItemsProcessingError();
		long lost = getWorkItemsLost();
		long todo = total - (done + error + lost);
		return todo;
	}
	
	public int getThreadCount() {
		return threadCount.get();
	}
	
	public void setThreadCount(int threadCount) {
		this.threadCount.set(threadCount);
	}
	
	public void killProcess(DuccId processId, String casId) {
		killProcessMap.put(processId, casId);
	}
	
	public boolean isKillProcess(DuccId processId) {
		boolean retVal = false;
		if(killProcessMap.containsKey(processId)) {
			retVal = true;
		}
		return retVal;
	}
	
	public Iterator<DuccId> getKillDuccIds() {
		return	killProcessMap.keySet().iterator();
	}
	
	public void killJob(JobCompletionType jobCompletionType, IRationale jobCompletionRationale) {
		killJob.getAndSet(true);
		setJobCompletion(jobCompletionType, jobCompletionRationale);
	}
	
	public boolean isKillJob() {
		return killJob.get();
	}
	
	public void limboAdd(int seqNo, DuccId pDuccId) {
		limboMap.put(new Integer(seqNo), pDuccId);
	}
	
	public void limboRemove(int seqNo, DuccId pDuccId) {
		limboMap.remove(new Integer(seqNo));
	}
	
	public ConcurrentHashMap<Integer,DuccId> getLimboMap() {
		return limboMap;
	}
	
	public ConcurrentHashMap<String,DuccId> getCasQueuedMap() {
		return casQueuedMap;
	}
	
	public int getWorkItemsQueued() {
		return casQueuedMap.size()+casDequeuedPendingMap.size();
	}
	
	public void workItemQueued(String casId, DuccId jobId) {
		String methodName = "workItemQueued";
		try {
			synchronized(casQueuedMap) {
				if(casQueuedMap.containsKey(casId)) {
					duccOut.debug(methodName, duccId, casId+" already queued");
				}
				if(casDequeuedPendingMap.containsKey(casId)) {
					duccOut.debug(methodName, duccId, casId+" already dequeued");
					casDequeuedPendingMap.remove(casId);
				}
				else {
					casQueuedMap.put(casId, jobId);
				}
			}
		}
		catch(Throwable t) {
			duccOut.debug(methodName, duccId, t);
		}
	}
	
	public void workItemDequeued(String casId) {
		String methodName = "workItemDequeued";
		try {
			synchronized(casQueuedMap) {
				if(!casQueuedMap.containsKey(casId)) {
					duccOut.debug(methodName, duccId, casId+" not found");
					casDequeuedPendingMap.put(casId, duccId);
				}
				else {
					casQueuedMap.remove(casId);
				}
			}
		}
		catch(Throwable t) {
			duccOut.debug(methodName, duccId, t);
		}
	}
	
	public int getWorkItemsDispatched() {
		String methodName = "getWorkItemsDispatched";
		//return getWorkItemsQueued()+getWorkItemsOperating();
		//<UIMA-3365>
		int retVal = 0;
		try {
			DuccProcessWorkItemsMap pwiMap = getDuccProcessWorkItemsMap();
			Iterator<DuccId> iterator = pwiMap.keySet().iterator();
			while(iterator.hasNext()) {
				DuccId processId = iterator.next();
				IDuccProcessWorkItems pwi = pwiMap.get(processId);
				retVal += pwi.getCountDispatch();
			}
		}
		catch(Throwable t) {
			duccOut.debug(methodName, duccId, t);
		}
		return retVal;
		//</UIMA-3365>
	}
	
	public void workItemPendingProcessAssignmentAdd(String casId) {
		pendingProcessAssignmentMap.put(casId, casId);
	}
	
	public void workItemPendingProcessAssignmentRemove(String casId) {
		pendingProcessAssignmentMap.remove(casId);
	}
	
	public int getWorkItemPendingProcessAssignmentCount() {
		int retVal = 0;
		retVal = pendingProcessAssignmentMap.size();
		return retVal;
	}
	
	public boolean isWorkItemPendingProcessAssignment() {
		boolean retVal = false;
		if(getWorkItemPendingProcessAssignmentCount() > 0) {
			retVal = true;
		}
		return retVal;
	}
	
	private static final String keyNodeIP = "nodeIP";
	private static final String keyPID = "PID";
	
	public void workItemOperatingStart(String casId, String nodeIP, String PID) {
		HashMap<String,String> operatingCAS = new HashMap<String,String>();
		operatingCAS.put(keyNodeIP, nodeIP);
		operatingCAS.put(keyPID, PID);
		casOperatingMap.put(casId, operatingCAS);
		workItemDequeued(casId);
		logReport();
	}
	
	public void workItemOperatingEnd(String casId) {
		casOperatingMap.remove(casId);
		logReport();
	}
	
	public boolean isOperating(String nodeIP, String PID) {
		boolean retVal = false;
		synchronized(this) {
			Iterator<String> iterator = casOperatingMap.keySet().iterator();
			while(iterator.hasNext()) {
				String casId = iterator.next();
				HashMap<String,String> casOperating = casOperatingMap.get(casId);
				if(Util.compare(nodeIP,casOperating.get(keyNodeIP))) {
					if(Util.compare(PID,casOperating.get(keyPID))) {
						retVal = true;
					}
				}
			}
		}
		return retVal;
	}
	
	public boolean isStarted() {
		int started = getWorkItemsProcessingStarted();
		return started > 0;
	}
	
	public boolean isProcessing() {
		String methodName = "isProcessing";
		synchronized(this) {
			int fetched = getWorkItemsFetched();
			int completed = getWorkItemsProcessingCompleted();
			int error = getWorkItemsProcessingError();
			int lost = getWorkItemsLost();
			boolean retVal = fetched != (completed + error + lost);
			duccOut.debug(methodName, jobid, "fetched:"+fetched+" "+"completed:"+completed+" "+"error:"+error+" "+"lost:"+lost);
			return retVal;
		}
	}
	
	public boolean isComplete() {
		boolean retVal = false;
		switch(getDriverState()) {
		case Completing:
		case Completed:
			retVal = true;
			break;
		}
		return retVal;
	}
	
	private void calculateState() {
		String methodName = "calculateState";
		switch(getDriverState()) {
		case Initializing:
			if(getAtLeastOneService()) {
				setDriverState(DriverState.Running);
			}
			break;
		case Idle:
			if(isProcessing()) {
				setDriverState(DriverState.Running);
			}
			else if(isPending()) {
				setDriverState(DriverState.Idle);
			}
			else if(isWorkItemsLost()) {
				setDriverState(DriverState.Idle);
			}
			else {
				setDriverState(DriverState.Completing);
			}
			break;
		case Running:
			if(!isProcessing()) {
				if(isPending()) {
					setDriverState(DriverState.Idle);
				}
				else if(isWorkItemsLost()) {
					setDriverState(DriverState.Idle);
				}
				else {
					setDriverState(DriverState.Completing);
				}
			}
			break;
		case Completing:
			if(isTerminateDriver()) {
				setDriverState(DriverState.Completed);
				if(getWorkItemsProcessingError() == 0) {
					if(getWorkItemsLost() == 0) {
						setJobCompletion(JobCompletionType.EndOfJob, new Rationale("job driver status reported as normal completion"));
					}
				}
			}
			break;
		case Completed:
			break;
		}
		duccOut.debug(methodName, duccId, "state:"+getDriverState());
	}

	public String getLogReport() {
		return "state:"+driverState+" "+"threads:"+getThreadCount()+" "
									   +"total:"+getWorkItemsTotal()+" "
									   +"fetched:"+getWorkItemsFetched()+" "
									   +"started:"+getWorkItemsProcessingStarted()+" "
									   +"completed:"+getWorkItemsProcessingCompleted()+" "
									   +"error:"+getWorkItemsProcessingError()+" "
									   +"lost:"+getWorkItemsLost()+" "
									   +"queued:"+getWorkItemsQueued()+" "
									   +"in-progress:"+casOperatingMap.size()+" "
									   +"pending:"+isPending()+" "
									   +"unassigned:"+getWorkItemPendingProcessAssignmentCount()+" "
									   +"retry:"+getWorkItemsRetry();
	}
	
	public void logReport() {
		String methodName = "logReport";
		duccOut.debug(methodName, duccId, getLogReport());
	}
	
	public DriverStatusReport deepCopy() {
		return (DriverStatusReport)SerializationUtils.clone(this);
	}
	
	public void setPerWorkItemStatistics(IDuccPerWorkItemStatistics perWorkItemStatistics) {
		this.perWorkItemStatistics = perWorkItemStatistics;
	}
	
	public IDuccPerWorkItemStatistics getPerWorkItemStatistics() {
		return perWorkItemStatistics;
	}
	
	@Deprecated
	public PerformanceMetricsSummaryMap getPerformanceMetricsSummaryMap() {
		return performanceMetricsSummaryMap;
	}
	
	public IDuccUimaDeploymentDescriptor getUimaDeploymentDescriptor() {
		return uimaDeploymentDescriptor;
	}
	
	public void setUimaDeploymentDescriptor(IDuccUimaDeploymentDescriptor uimaDeploymentDescriptor) {
		this.uimaDeploymentDescriptor = uimaDeploymentDescriptor;
	}
	
	public long getWiMillisMin() {
		return wiMillisMin;
	}

	public void setWiMillisMin(long value) {
		wiMillisMin = value;
	}

	public long getWiMillisMax() {
		return wiMillisMax;
	}

	public void setWiMillisMax(long value) {
		wiMillisMax = value;
	}

	public long getWiMillisAvg() {
		return wiMillisAvg;
	}

	public void setWiMillisAvg(long value) {
		wiMillisAvg = value;
	}

	public long getWiMillisOperatingLeast() {
		return wiMillisOperatingLeast;
	}

	public void setWiMillisOperatingLeast(long value) {
		wiMillisOperatingLeast = value;
	}

	public long getWiMillisCompletedMost() {
		return wiMillisCompletedMost;
	}

	public void setWiMillisCompletedMost(long value) {
		wiMillisCompletedMost = value;
	}
}
