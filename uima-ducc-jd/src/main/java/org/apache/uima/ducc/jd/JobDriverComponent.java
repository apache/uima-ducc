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
package org.apache.uima.ducc.jd;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.camel.CamelContext;
import org.apache.uima.ducc.common.NodeIdentity;
import org.apache.uima.ducc.common.component.AbstractDuccComponent;
import org.apache.uima.ducc.common.internationalization.Messages;
import org.apache.uima.ducc.common.jd.JdConstants;
import org.apache.uima.ducc.common.jd.files.WorkItemStateManager;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.JdStateDuccEvent;
import org.apache.uima.ducc.transport.event.OrchestratorAbbreviatedStateDuccEvent;
import org.apache.uima.ducc.transport.event.common.DuccWorkJob;
import org.apache.uima.ducc.transport.event.common.DuccWorkMap;
import org.apache.uima.ducc.transport.event.common.IDuccProcess;
import org.apache.uima.ducc.transport.event.common.Rationale;
import org.apache.uima.ducc.transport.event.common.IDuccTypes.DuccType;
import org.apache.uima.ducc.transport.event.jd.DriverStatusReport;
import org.apache.uima.ducc.transport.event.jd.PerformanceSummaryWriter;


public class JobDriverComponent extends AbstractDuccComponent 
implements IJobDriverComponent {
	
	private static DuccLogger duccOut = DuccLoggerComponents.getJdOut(JobDriverComponent.class.getName());
	private static Messages duccMsg = JobDriverContext.getInstance().getSystemMessages();
	
	public JobDriverComponent(CamelContext context, String jdBrokerUrl, String jdQueuePrefix, String localeLanguage, String localeCountry) {
		super("JobDriver",context);
		
		init(jdBrokerUrl,jdQueuePrefix);
		JobDriverContext.getInstance().initSystemMessages(localeLanguage,localeCountry);
		duccMsg = JobDriverContext.getInstance().getSystemMessages();
	}
	
	private DuccId duccId = null;
	private String jobId = String.valueOf(-1);
	protected JobDriver thread = null;
	private String jdBrokerUrl;
	private String jdQueue;
	
	private AtomicInteger publicationCounter = new AtomicInteger(0);
	
//	private int retryCount = 0;
//	private int retryLimit = 2;
	
	private void init(String jdBrokerUrl,String jdQueuePrefix) {
		String methodName = "init";
		duccOut.trace(methodName, null, duccMsg.fetch("enter"));
		String jobIdProperty = System.getProperty(JdConstants.key_duccJobId);
		jobId = DuccWorkMap.normalize(jobIdProperty);
		this.jdBrokerUrl = jdBrokerUrl;
		this.jdQueue = jdQueuePrefix+jobId;
		duccOut.debug(methodName, null, duccMsg.fetchLabel("job.broker")+this.jdBrokerUrl+" "+duccMsg.fetchLabel("job.queue")+this.jdQueue);
		duccOut.trace(methodName, null, duccMsg.fetch("exit"));
	}
	
	private void waitForPublication() {
		String methodName = "waitForPublication";
		int secs = 60;
		int pubC = publicationCounter.get();
		int pubT = pubC+3;
		int diff = pubT-pubC;
		int prev = 0;
		while(diff > 0) {
			if(diff != prev) {
				duccOut.debug(methodName, duccId, "pending publications:"+diff);
			}
			if(secs <= 0) {
				break;
			}
			secs--;
			try {
				Thread.sleep(1000);
			}
			catch(Exception e) {
			}
			pubC = publicationCounter.get();
			prev = diff;
			diff = pubT - pubC;
		}
		duccOut.debug(methodName, duccId, "pending publications:"+diff);
	}
	
	private void suicide() {
		String methodName = "suicide";
		waitForPublication();
		try {
			duccOut.debug(methodName, duccId, "..."+duccMsg.fetch("and away we go")+"!");
			super.stop();
		}
		catch(Exception e) {
			duccOut.error(methodName, duccId, e);
			System.exit(-1);
		}
	}
	
	private void dumpProcessMap(DuccWorkJob job) {
		String methodName = "pmap";
		if(job == null) {
			duccOut.debug(methodName, null, "job:"+job );
		}
		else {
			duccOut.debug(methodName, job.getDuccId(), "job:"+job.getId() );
			Map<DuccId, IDuccProcess> map = job.getProcessMap().getMap();
			for( Entry<DuccId, IDuccProcess> entry : map.entrySet() ) {
				IDuccProcess process = entry.getValue();
				process.getDuccId();
				NodeIdentity nodeIdentity = process.getNodeIdentity();
				String node = null;
				String ip = null;
				if(nodeIdentity != null) {
					node = nodeIdentity.getName();
					ip = nodeIdentity.getIp();
				}
				
				String pid = process.getPID();
				duccOut.debug(methodName, job.getDuccId(), process.getDuccId(), "node:"+node+" "+"ip:"+ip+" "+"pid:"+pid );
			}
		}
	}
	
	private boolean dumpProcessMapEnabled = false;
	
	protected void process(OrchestratorAbbreviatedStateDuccEvent duccEvent) {
		String methodName = "process";
		duccOut.trace(methodName, null, duccMsg.fetch("enter"));
		DuccWorkJob job = (DuccWorkJob) duccEvent.getWorkMap().findDuccWork(DuccType.Job, jobId);
		if(dumpProcessMapEnabled) {
			dumpProcessMap(job);
		}
		if(job != null) {
			if(duccId == null) {
				duccId = job.getDuccId();
			}
			duccOut.trace(methodName, duccId, "jd-cmd:"+job.getDriver().getCommandLine());
			duccOut.trace(methodName, duccId, "jp-cmd:"+job.getCommandLine());
			synchronized(jobId) {
				if(thread != null) {
					thread.setJob(job);
				}
				if(thread == null) {
					duccOut.debug(methodName, job.getDuccId(), job.getJobState());
					duccOut.trace(methodName, job.getDuccId(), duccMsg.fetch("creating driver thread"));
					try {
						thread = new JobDriver();
						thread.initialize(job, getProcessJmxUrl());
						thread.start();
					}
					catch(Exception e) {
						duccOut.error(methodName, job.getDuccId(), e.getMessage(), e);
					}
				}
			}
			/*
			if(job.isCompleted()) {
				int count = job.getProcessMap().getUsableProcessCount();
				if(count > 0) {
					duccOut.debug(methodName, job.getDuccId(), duccMsg.fetchLabel("processes active")+count);
				}
				else {
					duccOut.trace(methodName, job.getDuccId(), duccMsg.fetch("terminate driver thread"));
					thread.terminate();
					thread.interrupt();
					try {
						thread.join();
					} catch (InterruptedException e) {
						duccOut.debug(methodName, job.getDuccId(), e);
					}
					duccOut.trace(methodName, duccId, duccMsg.fetch("job completed"));
					suicide();
				}
			}
			*/
		}
		else {
			duccOut.debug(methodName, duccId, duccMsg.fetch("job not found"));
			if(thread != null) {
				thread.kill(new Rationale("job driver failed to locate job in map"));
				thread.interrupt();
				try {
					thread.join();
				} catch (InterruptedException e) {
					duccOut.debug(methodName, duccId, e);
				}
				duccOut.debug(methodName, duccId, duccMsg.fetch("thread killed"));
			}
			duccOut.debug(methodName, duccId, duccMsg.fetch("job killed"));
			suicide();
		}
		duccOut.trace(methodName, null, duccMsg.fetch("exit"));
	}
	
	public void publisher() {
		String methodName = "publisher";
		PerformanceSummaryWriter performanceSummaryWriter = thread.getPerformanceSummaryWriter();
		if(performanceSummaryWriter == null) {
			duccOut.debug(methodName, null, duccMsg.fetch("performanceSummaryWriter is null"));
		}
		else {
			performanceSummaryWriter.writeSummary();
		}
		WorkItemStateManager workItemStateManager = thread.getWorkItemStateManager();
		if(workItemStateManager == null) {
			duccOut.debug(methodName, null, duccMsg.fetch("workItemStateManager is null"));
		}
		else {
			try {
				workItemStateManager.exportData();
			}
			catch(Exception e) {
				duccOut.error(methodName, null, e);
			}
			
		}
		
	}
	
	public JdStateDuccEvent getState() {
		String methodName = "getState";
		duccOut.trace(methodName, null, duccMsg.fetch("enter"));
		JdStateDuccEvent jdStateDuccEvent = null;
		publicationCounter.addAndGet(1);
		try {
			duccOut.debug(methodName, null, duccMsg.fetch("publishing state"));
			jdStateDuccEvent = new JdStateDuccEvent();
			if(thread != null) {
				DriverStatusReport dsr = thread.getDriverStatusReportCopy();
				if(dsr == null) {
					duccOut.debug(methodName, null, duccMsg.fetch("dsr is null"));
				}
				else {
					duccOut.debug(methodName, null, "driverState:"+dsr.getDriverState());
					duccOut.debug(methodName, dsr.getDuccId(), dsr.getLogReport());
					jdStateDuccEvent.setState(dsr);
				}
				publisher();
			}
			else {
				duccOut.debug(methodName, null, duccMsg.fetch("thread is null"));
			}
		}
		catch(Exception e) {
			duccOut.error(methodName, null, e);
		}
		duccOut.trace(methodName, null, duccMsg.fetch("exit"));
		return jdStateDuccEvent;
	}
	
	@Override
	public void evaluateJobDriverConstraints(OrchestratorAbbreviatedStateDuccEvent duccEvent) {
		String methodName = "evaluateDispatchedJobConstraints";
		duccOut.trace(methodName, null, duccMsg.fetch("enter"));
		duccOut.debug(methodName, null, duccMsg.fetchLabel("received")+"OrchestratorStateEvent");
		process(duccEvent);
		duccOut.trace(methodName, null, duccMsg.fetch("exit"));
	}
	
}
