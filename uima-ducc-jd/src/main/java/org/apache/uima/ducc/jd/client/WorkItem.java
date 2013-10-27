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
package org.apache.uima.ducc.jd.client;

import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.uima.aae.client.UimaAsynchronousEngine;
import org.apache.uima.aae.monitor.statistics.AnalysisEnginePerformanceMetrics;
import org.apache.uima.cas.CAS;
import org.apache.uima.ducc.common.internationalization.Messages;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.jd.JobDriverContext;
import org.apache.uima.ducc.transport.event.common.ITimeWindow;
import org.apache.uima.ducc.transport.event.common.TimeWindow;


/**
 * This Runnable class represents a unit of work. It uses a reference to a shared UIMA AS client and 
 * a CAS which are provided by the application. A thread from ThreadPoolExecutor thread pool will call
 * run() method which uses UIMA AS client synchronous sendAndReceive() to process a given CAS. When 
 * the CAS is returned this Runnable dies. This code needs to be enhanced with a callback to notify
 * the application that the CAS was processed successfully. Alternatively, instead of Runnable this
 * class could be Callable instead of Runnable and the driver should handle Future objects and wait 
 * for the Callable to finish. 
 */
public class WorkItem implements Runnable {
	
	private static DuccLogger duccOut = DuccLoggerComponents.getJdOut(WorkItem.class.getName());
	private static Messages duccMsg = JobDriverContext.getInstance().getSystemMessages();
	
	private UimaAsynchronousEngine client;
	private CasTuple casTuple = null;
	private DuccId jobId = null;
	private DuccId processId = null;
	private IWorkItemMonitor workItemMonitor = null;
	
	private ITimeWindow timeWindow = new TimeWindow();
	
	private ArrayList<AnalysisEnginePerformanceMetrics> analysisEnginePerformanceMetricsList = new ArrayList<AnalysisEnginePerformanceMetrics>();
	
	private CallbackState callbackState = new CallbackState();
	
	public AtomicBoolean isLost = new AtomicBoolean(false);
	
	public WorkItem(UimaAsynchronousEngine client, CasTuple casTuple, DuccId duccId, IWorkItemMonitor workItemMonitor) {
		init(client, casTuple, duccId, workItemMonitor);
	}
	
	private void init(UimaAsynchronousEngine client, CasTuple casTuple, DuccId jobId, IWorkItemMonitor workItemMonitor) {
		String methodName = "init";
		duccOut.trace(methodName, jobId, duccMsg.fetch("enter"));
		this.client = client;
		this.casTuple = casTuple;
		this.jobId = jobId;
		this.workItemMonitor = workItemMonitor;
		duccOut.debug(methodName, jobId, duccMsg.fetchLabel("seqNo")+casTuple.getSeqno());
		duccOut.trace(methodName, jobId, duccMsg.fetch("exit"));
	}
	
	public DuccId getJobId() {
		return jobId;
	}
	
	public DuccId getProcessId() {
		return processId;
	}
	
	public void setProcessId(DuccId processId) {
		this.processId = processId;
	}
	
	public CasTuple getCasTuple() {
		return this.casTuple;
	}
	
	public boolean isRetry() {
		return this.casTuple.isRetry();
	}
	
	public int getSeqNo() {
		return this.casTuple.getSeqno();
	}
	
	public CAS getCAS() {
		return this.casTuple.getCas();
	}
	
	public String getCasId() {
		return ""+getCAS().hashCode();
	}
	
	public String getCasDocumentText() {
		return ""+getCAS().getDocumentText();
	}
	
	public ITimeWindow getTimeWindow() {
		return timeWindow;
	}
	
	public ArrayList<AnalysisEnginePerformanceMetrics> getAnalysisEnginePerformanceMetricsList() {
		return analysisEnginePerformanceMetricsList;
	}
	
	public CallbackState getCallbackState() {
		return callbackState;
	}
	
	protected void injectRandomThrowable() throws Throwable {
		// < *** TEST ONLY!!! *** >
		final boolean test = false;
		if(test) {
			Random random = new Random();
			if(random.nextBoolean()) {
				throw new Throwable("just testing Throwable handler");
			}
		}
		// </ *** TEST ONLY!!! *** >
	}
	
	public void run() {
		String methodName = "run";
		duccOut.debug(methodName, jobId, duccMsg.fetch("enter"));
		try {
			try {
				start();
				CAS cas = this.casTuple.getCas();
				duccOut.debug(methodName, jobId, duccMsg.fetchLabel("CAS.size")+cas.size());
				callbackState.statePendingQueued();
				duccOut.debug(methodName, null, "seqNo:"+getSeqNo()+" "+callbackState.getState());
				client.sendAndReceiveCAS(cas, analysisEnginePerformanceMetricsList);
				duccOut.debug(methodName, null, "seqNo:"+getSeqNo()+" "+"send and receive returned");
				//injectRandomThrowable();
				if(!isLost.get()) {
					ended();
				}
				else {
					duccOut.debug(methodName, null, "seqNo:"+getSeqNo()+" "+"lost+ended");
				}
			} catch(Exception e) {
				if(!isLost.get()) {
					exception(e);
				}
				else {
					duccOut.debug(methodName, null, "seqNo:"+getSeqNo()+" "+"lost+exception");
					duccOut.debug(methodName, null, e);
				}
			}
		}
		catch(Throwable t) {
			error(t);
		}
		
		duccOut.debug(methodName, jobId, duccMsg.fetch("exit"));
	}
	
	private void start() {
		String methodName = "start";
		duccOut.debug(methodName, getJobId(), getProcessId(), "seqNo:"+getSeqNo()+" "+"casId:"+getCAS().hashCode());
		workItemMonitor.start(this);
	}
	
	private void ended() {
		String methodName = "ended";
		try {
			duccOut.debug(methodName, getJobId(), getProcessId(), "seqNo:"+getSeqNo()+" "+"casId:"+getCAS().hashCode());
			workItemMonitor.ended(this);
		}
		catch(Exception e) {
			duccOut.error(methodName, null, e);
		}
	}
	
	private void exception(Exception e) {
		String methodName = "exception";
		try {
			duccOut.debug(methodName, getJobId(), getProcessId(), "seqNo:"+getSeqNo()+" "+"casId:"+getCAS().hashCode(), e);
			workItemMonitor.exception(this,e);
		}
		catch(Exception exception) {
			duccOut.error(methodName, null, exception);
		}
	}
	
	private void error(Throwable t) {
		String methodName = "error";
		try {
			duccOut.debug(methodName, getJobId(), getProcessId(), "seqNo:"+getSeqNo()+" "+"casId:"+getCAS().hashCode(), t);
			workItemMonitor.error(this,t);
		}
		catch(Exception exception) {
			duccOut.error(methodName, null, exception);
		}
	}
	
	public void lost() {
		String methodName = "lost";
		try {
			isLost.set(true);
			duccOut.debug(methodName, getJobId(), getProcessId(), "seqNo:"+getSeqNo()+" "+"casId:"+getCAS().hashCode());
			workItemMonitor.lost(this);
		}
		catch(Exception exception) {
			duccOut.error(methodName, null, exception);
		}
	}
}
