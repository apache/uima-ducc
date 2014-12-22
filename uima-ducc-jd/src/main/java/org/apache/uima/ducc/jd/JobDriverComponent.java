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
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.camel.CamelContext;
import org.apache.uima.ducc.common.NodeIdentity;
import org.apache.uima.ducc.common.component.AbstractDuccComponent;
import org.apache.uima.ducc.common.internationalization.Messages;
import org.apache.uima.ducc.common.jd.JdConstants;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.ExceptionHelper;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.JdStateDuccEvent;
import org.apache.uima.ducc.transport.event.OrchestratorAbbreviatedStateDuccEvent;
import org.apache.uima.ducc.transport.event.common.DuccWorkJob;
import org.apache.uima.ducc.transport.event.common.DuccWorkMap;
import org.apache.uima.ducc.transport.event.common.IDuccProcess;
import org.apache.uima.ducc.transport.event.common.IDuccTypes.DuccType;
import org.apache.uima.ducc.transport.event.common.Rationale;
import org.apache.uima.ducc.transport.event.jd.PerformanceSummaryWriter;
import org.apache.uima.ducc.transport.event.jd.v1.DriverStatusReportV1;
import org.apache.uima.ducc.transport.json.jp.JobProcessCollection;
import org.apache.uima.ducc.transport.json.jp.JobProcessData;


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
	protected DriverStatusReportV1 driverStatusReport = null;
	private String jdBrokerUrl;
	private String jdQueue;
	
	private AtomicInteger publicationCounter = new AtomicInteger(0);
	
	private JobProcessCollection jpc = null;
	
	private AtomicBoolean started = new AtomicBoolean(false);
	private AtomicBoolean active = new AtomicBoolean(true);

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
	public DuccLogger getLogger() {
	    return duccOut;
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
	
	protected String summarize(Exception e) {
		return ExceptionHelper.summarize(e);
	}
	
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
				if(!started.get()) {
					started.set(true);
					duccOut.debug(methodName, job.getDuccId(), job.getJobState());
					duccOut.trace(methodName, job.getDuccId(), duccMsg.fetch("creating driver thread"));
					try {
						thread = new JobDriver();
						duccOut.trace(methodName, job.getDuccId(), "thread:"+thread);
						driverStatusReport = new DriverStatusReportV1(job.getDuccId(),getProcessJmxUrl());
						thread.initialize(job, driverStatusReport);
						thread.start();
						jpc = new JobProcessCollection(job);
					}
					catch(Exception e) {
						duccOut.error(methodName, null, e);
						duccOut.error(methodName, job.getDuccId(), summarize(e), e);
					}
					catch(Throwable t) {
						duccOut.error(methodName, null, t);
					}
				}
				if(active.get()) {
					try {
						if(jpc != null) {
							ConcurrentSkipListMap<Long, JobProcessData> map = jpc.transform(job);
							jpc.exportData(map);
						}
					}
					catch(Exception e) {
						duccOut.error(methodName, job.getDuccId(), summarize(e), e);
					}
				}
			}
		}
		else {
			duccOut.debug(methodName, duccId, duccMsg.fetch("job not found"));
			if(active.get()) {
				active.set(false);
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
			}
		}
		duccOut.trace(methodName, null, duccMsg.fetch("exit"));
	}
	
	protected void publisher() {
		String methodName = "publisher";
		if(thread != null) {
			PerformanceSummaryWriter performanceSummaryWriter = thread.getPerformanceSummaryWriter();
			if(performanceSummaryWriter == null) {
				duccOut.debug(methodName, null, duccMsg.fetch("performanceSummaryWriter is null"));
			}
			else {
				performanceSummaryWriter.writeSummary();
			}
		}
		else {
			duccOut.debug(methodName, null, "thread is null");
		}
	}
	
	public JdStateDuccEvent getState() {
		String methodName = "getState";
		duccOut.trace(methodName, null, duccMsg.fetch("enter"));
		JdStateDuccEvent jdStateDuccEvent = new JdStateDuccEvent();
		if(thread != null) {
			if(active.get()) {
				publicationCounter.addAndGet(1);
				try {
					duccOut.debug(methodName, null, duccMsg.fetch("publishing state"));
					try {
						thread.rectifyStatus();
					}
					catch(Throwable t) {
						duccOut.warn(methodName, null, t);
					}
					DriverStatusReportV1 dsr = driverStatusReport;
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
				catch(Exception e) {
					duccOut.error(methodName, null, e);
				}
			}
		}
		else {
			duccOut.debug(methodName, null, "thread is null");
		}
		duccOut.trace(methodName, null, duccMsg.fetch("exit"));
		return jdStateDuccEvent;
	}
	
	public void evaluateJobDriverConstraints(OrchestratorAbbreviatedStateDuccEvent duccEvent) {
		String methodName = "evaluateDispatchedJobConstraints";
		duccOut.trace(methodName, null, duccMsg.fetch("enter"));
		duccOut.debug(methodName, null, duccMsg.fetchLabel("received")+"OrchestratorStateEvent");
		if(active.get()) {
			try {
				process(duccEvent);
			}
			catch(Throwable t) {
				duccOut.error(methodName, duccId, t);
			}
		}
		duccOut.trace(methodName, null, duccMsg.fetch("exit"));
	}
	
}
