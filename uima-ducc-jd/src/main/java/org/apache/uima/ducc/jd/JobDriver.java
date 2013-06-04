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

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.uima.aae.client.UimaAsynchronousEngine;
import org.apache.uima.adapter.jms.client.BaseUIMAAsynchronousEngine_impl;
import org.apache.uima.ducc.common.jd.files.WorkItemStateManager;
import org.apache.uima.ducc.common.jd.plugin.IJdProcessExceptionHandler;
import org.apache.uima.ducc.common.jd.plugin.IJdProcessExceptionHandler.Directive;
import org.apache.uima.ducc.common.jd.plugin.IJdProcessExceptionHandler.JdProperties;
import org.apache.uima.ducc.common.jd.plugin.JdProcessExceptionHandler;
import org.apache.uima.ducc.common.jd.plugin.JdProcessExceptionHandlerLoader;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.DuccPropertiesResolver;
import org.apache.uima.ducc.common.utils.ExceptionHelper;
import org.apache.uima.ducc.common.utils.TimeStamp;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.jd.client.CasDispatchMap;
import org.apache.uima.ducc.jd.client.CasSource;
import org.apache.uima.ducc.jd.client.CasTuple;
import org.apache.uima.ducc.jd.client.ClientThreadFactory;
import org.apache.uima.ducc.jd.client.DynamicThreadPoolExecutor;
import org.apache.uima.ducc.jd.client.ThreadLocation;
import org.apache.uima.ducc.jd.client.WorkItem;
import org.apache.uima.ducc.jd.client.WorkItemFactory;
import org.apache.uima.ducc.jd.client.WorkItemListener;
import org.apache.uima.ducc.transport.event.cli.JobRequestProperties;
import org.apache.uima.ducc.transport.event.common.DuccPerWorkItemStatistics;
import org.apache.uima.ducc.transport.event.common.DuccProcessMap;
import org.apache.uima.ducc.transport.event.common.DuccUimaDeploymentDescriptor;
import org.apache.uima.ducc.transport.event.common.IDuccCompletionType.JobCompletionType;
import org.apache.uima.ducc.transport.event.common.IDuccProcess;
import org.apache.uima.ducc.transport.event.common.IDuccProcessMap;
import org.apache.uima.ducc.transport.event.common.IDuccState.JobState;
import org.apache.uima.ducc.transport.event.common.DuccWorkPopDriver;
import org.apache.uima.ducc.transport.event.common.IDuccUimaDeployableConfiguration;
import org.apache.uima.ducc.transport.event.common.IDuccUimaDeploymentDescriptor;
import org.apache.uima.ducc.transport.event.common.IDuccWorkJob;
import org.apache.uima.ducc.transport.event.common.IRationale;
import org.apache.uima.ducc.transport.event.common.Rationale;
import org.apache.uima.ducc.transport.event.jd.DriverStatusReport;
import org.apache.uima.ducc.transport.event.jd.DuccProcessWorkItemsMap;
import org.apache.uima.ducc.transport.event.jd.PerformanceMetricsSummaryItem;
import org.apache.uima.ducc.transport.event.jd.PerformanceMetricsSummaryMap;
import org.apache.uima.ducc.transport.event.jd.PerformanceSummaryWriter;
import org.apache.uima.ducc.transport.uima.dd.generator.DeploymentDescriptorGenerator;
import org.apache.uima.util.Progress;


public class JobDriver extends Thread implements IJobDriver {

	private static DuccLogger duccOut = DuccLoggerComponents.getJdOut(JobDriver.class.getName());
	private static DuccLogger duccErr = DuccLoggerComponents.getJdErr(JobDriver.class.getName());
	
	private DuccId jobid = null;
	
	private IDuccWorkJob job = null;
	private String jdJmxUrl = null;
	
	private DriverStatusReport driverStatusReport = null;
	private WorkItemStateManager workItemStateManager = null;
	private PerformanceSummaryWriter performanceSummaryWriter = null;
	private SynchronizedStats synchronizedStats = null;
	
	private LinkedBlockingQueue<Runnable> queue = null;
	private DynamicThreadPoolExecutor executor = null;
	// Single instance of UIMA-AS client shared by multiple threads
	private UimaAsynchronousEngine client;
	private WorkItemListener workItemListener;
	
	private CasSource casSource;
	private CasDispatchMap casDispatchMap = new CasDispatchMap();
	
	private IJdProcessExceptionHandler jdProcessExceptionHandler = new JdProcessExceptionHandler();
	
	private String serverUri = null;
	private String endPoint = null;
	private int wiTimeout = 1;
	private int metaTimeout = 1;
	
	private WorkItemFactory workItemFactory;
	
	private AtomicInteger activeWorkItems = new AtomicInteger(0);
	
	private ConcurrentHashMap<String,WorkItem> casWorkItemMap = new ConcurrentHashMap<String,WorkItem>();
	
	public JobDriver() {
		super();
	}
	
	
	public void initialize(IDuccWorkJob job, String jdJmxUrl) throws JobDriverTerminateException {
		String location = "initialize";
		duccOut.info(location, jobid, "jd.step:"+location);
		try {
			setJobid(job.getDuccId());
			setDuccWorkJob(job);
			setJdJmxUrl(jdJmxUrl);
			driverStatusReport = new DriverStatusReport(job.getDuccId(),getJdJmxUrl());
			driverStatusReport.setInitializing();
			duccOut.debug(location, jobid, "driverState:"+driverStatusReport.getDriverState());
			// Handle UIMA deployment descriptor
			String directory = job.getLogDirectory()+job.getDuccId();
			DeploymentDescriptorGenerator ddg = new DeploymentDescriptorGenerator("JD",duccOut,directory);
			IDuccUimaDeployableConfiguration udc = job.getUimaDeployableConfiguration();
			String process_DD = ddg.generate(udc, job.getDuccId().toString());
			IDuccUimaDeploymentDescriptor uimaDeploymentDescriptor = new DuccUimaDeploymentDescriptor(process_DD);
			driverStatusReport.setUimaDeploymentDescriptor(uimaDeploymentDescriptor);
			// Prepare for gathering of UIMA performance statistics
			String logsjobdir = job.getLogDirectory()+job.getDuccId().getFriendly()+File.separator;
			performanceSummaryWriter = new PerformanceSummaryWriter(logsjobdir);
			workItemStateManager = new WorkItemStateManager(logsjobdir);
			synchronizedStats = new SynchronizedStats();
			// Prepare UIMA-AS client instance and multiple threads
			ClientThreadFactory factory = new ClientThreadFactory("UimaASClientThread");
			queue = new LinkedBlockingQueue<Runnable>();
			executor = new DynamicThreadPoolExecutor(1, 1, 10, TimeUnit.MICROSECONDS, queue, factory, null);
			client = new BaseUIMAAsynchronousEngine_impl();
			workItemListener = new WorkItemListener(this);
			client.addStatusCallbackListener(workItemListener);
			// Initialize CAS source
			duccOut.debug(location, jobid, "CAS source initializing...");
			String crxml = job.getDriver().getCR();
			String crcfg = job.getDriver().getCRConfig();
			casSource = new CasSource(this, crxml, crcfg, casDispatchMap);
			Progress progress = casSource.getProgress();
			if(progress != null) {
				long total = progress.getTotal();
				duccOut.info(location, jobid, "total: "+total);
				driverStatusReport.setWorkItemsTotal(total);
			}
			duccOut.debug(location, jobid, "CAS source initialized");
			// Initialize job process exception handler
			String jdProcessExceptionHandlerClassName = job.getDriver().getProcessExceptionHandler();
			if(jdProcessExceptionHandlerClassName != null) {
				try {
					jdProcessExceptionHandler = JdProcessExceptionHandlerLoader.load(job.getDriver().getProcessExceptionHandler());
					duccOut.info(location, jobid, "user specified handler = "+jdProcessExceptionHandlerClassName);
				}
				catch (Exception e) {
					duccOut.error(location, jobid, e);
					duccErr.error(location, jobid, e);
					driverStatusReport.setInitializingFailed(new Rationale("job driver exception occurred: "+summarize(e)));
					terminate();
					throw new JobDriverTerminateException("initialize failed", e);
				}
			}
			else {
				duccOut.info(location, jobid, "default handler = "+JdProcessExceptionHandler.class.getName());
			}
		}
		catch(JobDriverTerminateException e) {
			throw e;
		}
		catch(Exception e) {
			duccOut.error(location, jobid, e);
			duccErr.error(location, jobid, e);
			driverStatusReport.setInitializingFailed(new Rationale("job driver exception occurred: "+summarize(e)));
			terminate();
			throw new JobDriverTerminateException("initialize failed", e);
		}
	}
	
	public String summarize(Exception e) {
		return ExceptionHelper.summarize(e);
	}
	
	public void run() {
		try {
			process();
		} 
		catch (JobDriverTerminateException e) {
		}
	}
	
	private void setJobid(DuccId value) {
		jobid = value;
	}
	
	private void setDuccWorkJob(IDuccWorkJob value) {
		setJob(value);
	}
	
	private void setJdJmxUrl(String value) {
		jdJmxUrl = value;
	}
	
	private String getJdJmxUrl() {
		return jdJmxUrl;
	}
	
	private void process() throws JobDriverTerminateException {
		String location = "process";
		try {
			waitForEligibility();
			if(getJob().isRunnable()) {
				uimaAsClientInitialize();
				duccOut.info(location, jobid, "jd.step:"+location);
				executor.prestartAllCoreThreads();
				workItemFactory = new WorkItemFactory(client, jobid, this);
				queueCASes(1, queue, workItemFactory);
				boolean run = true;
				while(run) {
					boolean value;
					value = driverStatusReport.isTerminateDriver();
					if(value) {
						duccOut.info(location, jobid, "DriverTerminate:"+value);
						run = false;
						continue;
					}
					else {
						duccOut.debug(location, jobid, "pending job termination"+"...");
					}
					value = getJob().isFinished();
					if(value) {
						duccOut.info(location, jobid, "JobFinished:"+value+" "+"JobState:"+getJob().getJobState());
						run = false;
						continue;
					}
					else {
						duccOut.debug(location, jobid, "pending processes termination"+"...");
					}
					logState(getJob());
					interrupter();
					int threadCount = calculateThreadCount();
					driverStatusReport.setThreadCount(threadCount);
					if(threadCount > 0) {
						executor.changeCorePoolSize(threadCount);
					}
					int poolSize = executor.getCorePoolSize();
					duccOut.debug(location, jobid, "pool size:"+poolSize);
					while(isQueueDeficit(threadCount)) {
						if(!casSource.isExhaustedReader()) {
							duccOut.debug(location, jobid, "not exhausted reader");
							queueCASes(1,queue,workItemFactory);
							continue;
						}
						else {
							duccOut.trace(location, jobid, "exhausted reader");
						}
						if(!casSource.isLimboEmpty()) {
							if(casSource.hasLimboAvailable()) {
								duccOut.debug(location, jobid, "limbo available size:"+casSource.getLimboSize());
								queueCASes(1,queue,workItemFactory);
								continue;
							}
							else {
								duccOut.debug(location, jobid, "limbo unavailable size:"+casSource.getLimboSize());
							}
						}
						else {
							duccOut.debug(location, jobid, "limbo empty size:"+casSource.getLimboSize());
						}
						break;
					}
					value = driverStatusReport.isComplete();
					if(value) {
						duccOut.info(location, jobid, "DriverComplete:"+value+" "+"DriverState:"+driverStatusReport.getDriverState());
						run = false;
						continue;
					}
					try {
						Thread.sleep(10000);
					} 
					catch (InterruptedException e) {
					}
				}
			}
			else {
				duccOut.error(location, jobid, "not runnable");
				duccErr.error(location, jobid, "not runnable");
				driverStatusReport.setInitializingFailed(new Rationale("job driver not runnable"));
				terminate();
				throw new JobDriverTerminateException("not runnable");
			}
			int activeCount = getWorkItemActiveCount();
			if(getWorkItemActiveCount() > 0) {
				duccOut.debug(location, jobid, "pending active count="+activeCount+" work item completion"+"...");
				int max_secs = 120;
				while((activeCount > 0) && max_secs > 0) {
					try {
						Thread.sleep(1000);
						max_secs--;
					} 
					catch (InterruptedException e) {
					}
					activeCount = getWorkItemActiveCount();
				}
			}
			try {
				client.stop();
			}
			catch(Exception e) {
				duccOut.error(location, jobid, e);
			}
			statistics();
			terminate();
		}
		catch(JobDriverTerminateException e) {
			throw e;
		}
		catch(Exception e) {
			duccOut.error(location, jobid, e);
			terminate();
			throw new JobDriverTerminateException("process failed", e);
		}
	}
	
	/*
	 * initialize UIMA-AS shared client
	 */
	
	private void uimaAsClientInitialize() throws JobDriverTerminateException {
		String location = "uimaAsClientInitialize";
		duccOut.info(location, jobid, "jd.step:"+location);
		try {
			DuccWorkPopDriver popDriver = getJob().getDriver();
			serverUri = popDriver.getServerUri();
			duccOut.info(location, jobid, "broker"+":"+serverUri);
			endPoint = popDriver.getEndPoint();
			duccOut.info(location, jobid, "endpoint"+":"+endPoint);
			Map<String,Object> appCtx = new HashMap<String,Object>();
			try {
				metaTimeout = Integer.parseInt(getJob().getDriver().getMetaTimeout());
				duccOut.info(location, jobid, DuccPropertiesResolver.default_process_get_meta_time_max+":"+metaTimeout);
			}
			catch(Exception e) {
				duccOut.warn(location, jobid, DuccPropertiesResolver.default_process_get_meta_time_max+":"+metaTimeout);
			}
			try {
				wiTimeout = Integer.parseInt(getJob().getDriver().getWiTimeout());
				duccOut.info(location, jobid, JobRequestProperties.key_process_per_item_time_max+":"+wiTimeout);
			}
			catch(Exception e) {
				duccOut.warn(location, jobid, JobRequestProperties.key_process_per_item_time_max+":"+wiTimeout);
			}
			appCtx.put(UimaAsynchronousEngine.ServerUri, serverUri);
			appCtx.put(UimaAsynchronousEngine.Endpoint, endPoint);
			appCtx.put(UimaAsynchronousEngine.CasPoolSize, 1);
			appCtx.put(UimaAsynchronousEngine.GetMetaTimeout, metaTimeout*60*1000);
			appCtx.put(UimaAsynchronousEngine.Timeout, wiTimeout*60*1000);
		    appCtx.put(UimaAsynchronousEngine.CpcTimeout, wiTimeout*60*1000);
			// Initialize UIMA AS client. 
			// - create connection to a broker
			// - create a temp reply queue
			// - send GetMeta request to a service and
			// - wait for a reply
			// This method blocks until the reply for GetMeta comes back from the service.
			client.initialize(appCtx);
			driverStatusReport.setInitializingCompleted();
		}
		catch(Exception e) {
			duccOut.error(location, jobid, e);
			duccErr.error(location, jobid, e);
			driverStatusReport.setInitializingFailed(new Rationale("job driver exception occurred: "+summarize(e)));
			terminate();
			throw new JobDriverTerminateException("initialize failed", e);
		}
	}
	
	/*
	 * enqueue <count> CASes for distributed pipelines
	 */
	
	private boolean localKillJobMessageIssued = false;
	
	private void queueCASes(int count, LinkedBlockingQueue<Runnable> queue, WorkItemFactory workItemFactory) throws JobDriverTerminateException {
		String location = "queueCASes";
		try {
			if(driverStatusReport.isKillJob()) {
				if(!localKillJobMessageIssued) {
					duccOut.warn(location, jobid, "job killed - queue requests ignored.");
					localKillJobMessageIssued = true;
				}
				duccOut.trace(location, jobid, "return");
				return;
			}
			duccOut.trace(location, jobid, "continue...");
			for(int i=0; i<count; i++) {
				CasTuple casTuple = casSource.pop();
				driverStatusReport.setWorkItemsFetched(casSource.getSeqNo());
				if(casTuple == null) {
					if(casSource.isEmpty()) {
						driverStatusReport.resetWorkItemsPending();
					}
					break;
				}
				duccOut.debug(location, jobid, "queue:"+casTuple.getSeqno());
				ThreadLocation threadLocation = new ThreadLocation(""+casTuple.getSeqno());
				duccOut.debug(location, jobid, "action:ready "+threadLocation.getInfo());
				getCasDispatchMap().put(casTuple, threadLocation);
				Future<?> pendingWork = executor.submit(workItemFactory.create(casTuple));
				threadLocation.setPendingWork(pendingWork);
				workItemActive();
			}
		}
		catch(Exception e) {
			driverStatusReport.killJob(JobCompletionType.CanceledByDriver, new Rationale("job driver exception occurred: "+summarize(e)));
			driverStatusReport.countWorkItemsProcessingError();
			duccOut.error(location, jobid, "error fetching next CAS from CR",e);
			duccErr.error(location, jobid, "error fetching next CAS from CR",e);
			throw new JobDriverTerminateException("error fetching next CAS from CR", e);
		}
		duccOut.debug(location, jobid, "LinkedBlockingQueue.size"+":"+executor.getQueue().size());
		duccOut.debug(location, jobid, "CorePool.size"+":"+executor.getCorePoolSize());
		return;
	}
	
	private int getWorkItemActiveCount() {
		String location = "getWorkItemActiveCount";
		int active = activeWorkItems.get();
		duccOut.debug(location, jobid, "active work items:"+active);
		return active;
	}
	
	private void workItemActive() {
		String location = "workItemActive";
		int active = activeWorkItems.incrementAndGet();
		duccOut.debug(location, jobid, "active work items:"+active);
	}
	
	private void workItemInactive() {
		String location = "workItemInactive";
		int active = activeWorkItems.decrementAndGet();
		duccOut.debug(location, jobid, "active work items:"+active);
	}
	
	private void logState(IDuccWorkJob job) {
		String location = "logState";
		duccOut.debug(location, jobid, job.getJobState());
	}
	
	private void interrupter() {
		String location = "interrupter";
		CasDispatchMap casDispatchMap = getCasDispatchMap();
		IDuccProcessMap processMap = (IDuccProcessMap) getJob().getProcessMap().deepCopy();
		Iterator<DuccId> iterator = processMap.keySet().iterator();
		while(iterator.hasNext()) {
			DuccId duccId = iterator.next();
			IDuccProcess duccProcess = processMap.get(duccId);
			boolean statusComplete = duccProcess.isComplete();
			boolean statusDeallocated = duccProcess.isDeallocated();
			boolean statusProcessFailed = duccProcess.isFailed();
			if(statusComplete || statusDeallocated || statusProcessFailed) {
				duccOut.debug(location, jobid, duccProcess.getDuccId(), "isComplete:"+statusComplete+" "+"isDeallocated:"+statusDeallocated+" "+"isProcessFailed:"+statusProcessFailed);
				casDispatchMap.interrupt(getJob(), duccProcess);
			}
		}
	}
	
	private int calculateThreadCount() {
		String location = "calculateThreadCount";
		int threads_per_share = Integer.parseInt(getJob().getSchedulingInfo().getThreadsPerShare());
		int shares = getJob().getProcessMap().getUsableProcessCount();
		int threadCount = shares*threads_per_share;
		duccOut.debug(location, jobid, "shares"+":"+" "+"threads-per-share"+":"+threads_per_share);
		return threadCount;
	}
	
	private boolean isQueueDeficit(int threadCount) {
		String location = "isQueueDeficit";
		int active = getWorkItemActiveCount();
		boolean kill = driverStatusReport.isKillJob();
		duccOut.debug(location, jobid, "thread count:"+threadCount+" "+"active work items:"+active+" "+"kill:"+kill);
		boolean retVal = ((threadCount > active) && (!kill));
		return retVal;
	}
	
	private void statistics() {
		String location = "statistics";
		PerformanceMetricsSummaryMap map = performanceSummaryWriter.getSummaryMap();
		String stars = "********************";
		duccOut.info(location, jobid, stars);
		String sep = " / ";
		duccOut.info(location, jobid, "PerformanceMetricsSummaryMapSize:"+map.size());
		duccOut.info(location, jobid, "uniqueName"+sep+"name"+sep+"numProcessed"+sep+"analysisTime"+sep+"Avg"+sep+"Min"+sep+"Max");
		Set<Entry<String, PerformanceMetricsSummaryItem>> tset = map.entrySet();
		for (Entry<String, PerformanceMetricsSummaryItem> entry : tset) {
			String uniqueName = entry.getKey();
			PerformanceMetricsSummaryItem value = entry.getValue();
			String name = value.getName();
			long analysisTime = value.getAnalysisTime();
			long numProcessed = value.getNumProcessed();
			long analysisTimeMin = value.getAnalysisTimeMin();
			long analysisTimeMax = value.getAnalysisTimeMax();
			double analysisTimeAvg = 0;
			if(numProcessed > 0) {
				analysisTimeAvg = analysisTime/numProcessed;
			}
			duccOut.info(location, jobid, uniqueName+sep+name+sep+numProcessed+sep+analysisTime+sep+analysisTimeAvg+sep+analysisTimeMin+sep+analysisTimeMax);
		}
		duccOut.info(location, jobid, "casCount:"+performanceSummaryWriter.getSummaryMap().casCount());
		duccOut.info(location, jobid, stars);
		if(driverStatusReport.getPerWorkItemStatistics() != null) {
			double max = driverStatusReport.getPerWorkItemStatistics().getMax();
			double min = driverStatusReport.getPerWorkItemStatistics().getMin();
			double avg = driverStatusReport.getPerWorkItemStatistics().getMean();
			double dev = driverStatusReport.getPerWorkItemStatistics().getStandardDeviation();
			duccOut.info(location, jobid, "per work item processing times in seconds");
			duccOut.info(location, jobid, "max:"+Math.round(max)/1000.0);
			duccOut.info(location, jobid, "min:"+Math.round(min)/1000.0);
			duccOut.info(location, jobid, "avg:"+Math.round(avg)/1000.0);
			duccOut.info(location, jobid, "dev:"+Math.round(dev)/1000.0);
		}
		else {
			duccOut.info(location, jobid, "per work item statistics unavailable");
		}
		duccOut.info(location, jobid, stars);
	}
	
	private void terminate() {
		String location = "terminate";
		duccOut.info(location, jobid, "jd.step:"+location);
		try {
			driverStatusReport.setTerminateDriver();
		}
		catch(Exception e) {
			duccOut.error(location, jobid, e);
			System.exit(-1);
		}
	}
	
	public void kill(IRationale rationale) {
		String location = "kill";
		duccOut.error(location, jobid, "kill");
		getJob().setJobState(JobState.Completing);
		getJob().getStandardInfo().setDateOfCompletion(TimeStamp.getCurrentMillis());
		getJob().setCompletion(JobCompletionType.Error, rationale);
	}

	private void waitForEligibility() throws JobDriverTerminateException {
		String location = "waitForEligibility";
		duccOut.info(location, jobid, "jd.step:"+location);
		duccOut.debug(location, jobid, "begin");
		boolean run = true;
		while(run) {
			checkProcessesState();
			boolean value;
			value = driverStatusReport.isTerminateDriver();
			if(value) {
				duccOut.info(location, jobid, "DriverTerminate:"+value);
				run = false;
				continue;
			}
			value = getJob().isFinished();
			if(value) {
				duccOut.info(location, jobid, "JobFinished:"+value+" "+"JobState:"+getJob().getJobState());
				run = false;
				continue;
			}
			boolean v1 = getJob().isRunnable();
			boolean v2 = getJob().isProcessReady();
			value = v1 && v2;
			if(value) {
				duccOut.info(location, jobid, "JobRunnable:"+v1+" "+"JobState:"+getJob().getJobState()+" "+"ProcessReady:"+v2);
				run = false;
				continue;
			}
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				duccOut.debug(location, jobid, "interrupted");
			}
		}
		duccOut.debug(location, jobid, "end");
	}
	
	private int MAX_INIT_FAILURES = 1;
	
	private void checkProcessesState() throws JobDriverTerminateException {
		String location = "checkProcessesState";
		if(getJob().isProcessReady()) {
			driverStatusReport.setAtLeastOneService();
		}
		else {
			int failures = getJob().getFailedUnexpectedProcessCount();
			if(failures >= MAX_INIT_FAILURES) {
				driverStatusReport.setExcessiveInitializationFailures(new Rationale("job driver initialization failures limit reached:"+MAX_INIT_FAILURES));
				duccOut.error(location, jobid, "Initialization failures limit reached: "+MAX_INIT_FAILURES);
				terminate();
				throw new JobDriverTerminateException("excessive initialize failures");
			}
		}
	}
	
	// ==========
	
	private void remove(WorkItem workItem) {
		String location = "remove";
		ThreadLocation threadLocation = getThreadLocation(workItem);
		String nodeIP = "?";
		String PID = "?";
		if(threadLocation != null) {
			nodeIP = threadLocation.getNodeId();
			PID = threadLocation.getProcessId();
		}
		duccOut.debug(location, jobid, "seqNo:"+workItem.getSeqNo()+" "+"wiId:"+workItem.getCasDocumentText()+" "+"casId:"+workItem.getCasId()+" "+"node:"+nodeIP+" "+"PID:"+PID);
		casDispatchMap.remove(workItem.getCasId());
		casWorkItemMap.remove(workItem.getCasId());
	}
	
	private void retry(WorkItem workItem) {
		String location = "retry";
		duccOut.info(location, workItem.getJobId(), workItem.getProcessId(), "seqNo:"+workItem.getSeqNo()+" "+"wiId:"+workItem.getCasDocumentText());
		duccOut.debug(location, workItem.getJobId(), workItem.getProcessId(), "seqNo:"+workItem.getSeqNo()+" "+"wiId:"+workItem.getCasDocumentText()+" "+"casId:"+workItem.getCAS().hashCode());
		remove(workItem);
		duccOut.debug(location, workItem.getJobId(), workItem.getProcessId(), "size:"+casDispatchMap.size());
		CasTuple casTuple = workItem.getCasTuple();
		casTuple.setRetry();
		casTuple.setDuccId(workItem.getProcessId());
		casSource.push(casTuple);
		driverStatusReport.setWorkItemsPending();
		workItemStateManager.retry(workItem.getSeqNo());
		return;
	}
	// presume retry if registration info not found
	
	private boolean isRetry(WorkItem workItem) {
		String location = "isRetry";
		boolean retVal = false;
		String key = ""+workItem.getCAS().hashCode();
		if(casDispatchMap.containsKey(key)) {
			ThreadLocation threadLocation = casDispatchMap.get(key);
			duccOut.debug(location, workItem.getJobId(), workItem.getProcessId(), "threadLocation:"+threadLocation);
			if(threadLocation != null) {
				String nodeId = threadLocation.getNodeId();
				String extendedProcessId = threadLocation.getProcessId();
				if(nodeId == null) {
					retVal = true;
				}
				else if(extendedProcessId == null) {
					retVal = true;
				}
				else {
					String processId = extendedProcessId.split(":")[0];
					duccOut.debug(location, workItem.getJobId(), workItem.getProcessId(), "node:"+nodeId+" "+"processId:"+processId);
					DuccProcessMap duccProcessMap = (DuccProcessMap)getJob().getProcessMap();
					duccOut.debug(location, workItem.getJobId(), workItem.getProcessId(), "processMap:"+duccProcessMap);
					//IDuccProcess duccProcess = getJob().getProcessMap().findProcess(threadLocation.getNodeId(), threadLocation.getProcessId());
					IDuccProcess duccProcess = duccProcessMap.findProcess(duccOut, nodeId, processId);
					duccOut.debug(location, workItem.getJobId(), workItem.getProcessId(), "duccProcess:"+duccProcess);
					if(duccProcess == null) {
						retVal = true;
					}
					else if(duccProcess.isDeallocated()) {
						retVal = true;
					}
				}
			}
			else {
				retVal = true;
			}
		}
		else {
			retVal = true;
		}
		duccOut.debug(location, workItem.getJobId(), workItem.getProcessId(), "retVal:"+retVal);
		return retVal;
	}
	
	private boolean isError(WorkItem workItem, Exception e) {
		String location = "isError";
		boolean retVal = true;
		Directive directive = getDirective(workItem, e);
		String message = "seqNo:"+workItem.getSeqNo()+" "+"wiId:"+workItem.getCasDocumentText()+" "+"plugin exception handler "+"directive:"+directive+" "+"reason:"+directive.getReason();
		duccOut.debug(location, workItem.getJobId(), workItem.getProcessId(), message, e);
		switch(directive) {
		case ProcessContinue_CasNoRetry:
			duccOut.info(location, workItem.getJobId(), workItem.getProcessId(), message);
			break;
		case ProcessContinue_CasRetry:
			retVal = false;
			break;
		case ProcessStop_CasNoRetry:
			duccOut.info(location, workItem.getJobId(), workItem.getProcessId(), message);
			driverStatusReport.killProcess(workItem.getProcessId(),workItem.getCasId());
			break;
		case ProcessStop_CasRetry:
			driverStatusReport.killProcess(workItem.getProcessId(),workItem.getCasId());
			retVal = false;
			break;
		case JobStop:
			duccOut.info(location, workItem.getJobId(), workItem.getProcessId(), message);
			driverStatusReport.killJob(JobCompletionType.CanceledByDriver, new Rationale("job driver received JobStop from plugin error handler"));
			break;
		}
		duccOut.debug(location, workItem.getJobId(), workItem.getProcessId(), "retVal:"+retVal);
		return retVal;
	}
	
	private boolean isFailedProcess(WorkItem workItem) {
		String location = "isFailedProcess";
		boolean retVal = false;
		DuccProcessMap duccProcessMap = (DuccProcessMap)getJob().getProcessMap();
		IDuccProcess duccProcess = duccProcessMap.get(workItem.getProcessId());
		if(duccProcess != null) {
			retVal = duccProcess.isFailed();
		}
		duccOut.debug(location, workItem.getJobId(), workItem.getProcessId(), "retVal:"+retVal);
		return retVal;
	}
	
	private boolean isUnknownProcess(WorkItem workItem) {
		String location = "isFailedProcess";
		boolean retVal = false;
		if(workItem.getProcessId() == null) {
			retVal = true;
		}
		duccOut.debug(location, workItem.getJobId(), workItem.getProcessId(), "retVal:"+retVal);
		return retVal;
	}
	
	private Directive getDirective(WorkItem workItem, Exception e) {
		String location = "getDirective";
		duccOut.debug(location, workItem.getJobId(), workItem.getProcessId(), "seqNo:"+workItem.getSeqNo()+" "+"wiId:"+workItem.getCasDocumentText()+" "+"casId:"+workItem.getCAS().hashCode());
		String processKey = null;
		String threadLocation = getThreadLocationId(workItem);
		if(threadLocation != null) {
			String[] parts = threadLocation.split(":");
			if(parts.length == 3) {
				processKey = parts[0]+":"+parts[1];
			}
		}
		Properties properties = new Properties();
		properties.put(JdProperties.SequenceNumber, ""+workItem.getSeqNo());
		Directive directive = jdProcessExceptionHandler.handle(processKey, workItem.getCAS(), e, properties);
		duccOut.debug(location, workItem.getJobId(), workItem.getProcessId(), "seqNo:"+workItem.getSeqNo()+" "+"wiId:"+workItem.getCasDocumentText()+" "+"directive:"+directive+" "+"reason:"+directive.getReason());
		return directive;
	}
	
	// ==========
	
	
	public CasDispatchMap getCasDispatchMap() {
		return casDispatchMap;
	}

	
	public IDuccWorkJob getJob() {
		synchronized(job) {
			return job;
		}
	}
	
	
	public void setJob(IDuccWorkJob job) {
		String location = "setJob";
		if(job != null) {
			synchronized(job) {
				this.job = job;
			}
		}
		else {
			try {
				throw new RuntimeException();
			}
			catch(Exception e) {
				duccOut.error(location, null, "error?", e);
			}
		}
	}
	
	
	public DriverStatusReport getDriverStatusReportLive() {
		synchronized (driverStatusReport) {
			return driverStatusReport;
		}
	}
	
	
	public DriverStatusReport getDriverStatusReportCopy() {
		//synchronized (driverStatusReport) {
			return driverStatusReport.deepCopy();
		//}
	}
	
	
	public WorkItemStateManager getWorkItemStateManager() {
		return workItemStateManager;
	}
	
	
	public PerformanceSummaryWriter getPerformanceSummaryWriter() {
		return performanceSummaryWriter;
	}
	
	/**/
	
	private class NP {
		private String nodeIP = null;
		private String PID = null;;
		public NP() {
		}
		public NP(String nodeIP, String PID) {
			if(nodeIP != null) {
				this.nodeIP = nodeIP.trim();
			}
			if(PID != null) {
				this.PID = PID.trim();
			}
		}
		public String getNodeIP() {
			return nodeIP;
		}
		public String getPID() {
			return PID;
		}
	}
	
	private ConcurrentHashMap<String,NP> casLocationPendingMap = new ConcurrentHashMap<String,NP>();

	private void removeLocations(ArrayList<WorkItem> removalsList) {
		String location = "removeLocations";
		try {
			Iterator<WorkItem> iterator = removalsList.iterator();
			while(iterator.hasNext()) {
				WorkItem workItem = iterator.next();
				locationPendingMapRemove(""+workItem.getSeqNo(), workItem.getCasId());
				duccOut.debug(location, null, workItem.getCasId());
			}
		}
		catch(Exception e) {
			duccOut.error(location, null, "location error?", e);
		}
	}
	
	private void updateLocations(IJobDriver jobDriver) {
		String location = "updateLocations";
		try {
			IDuccWorkJob job = jobDriver.getJob();
			DuccId jobDuccId = job.getDuccId();
			ArrayList<WorkItem> removeList = new ArrayList<WorkItem>();
			duccOut.debug(location, jobDuccId, "pending map size:"+casLocationPendingMap.size());
			Iterator<Entry<String, NP>> iterator = casLocationPendingMap.entrySet().iterator();
			duccOut.debug(location, jobDuccId, iterator.hasNext());
			while(iterator.hasNext()) {
				Entry<String, NP> entry = iterator.next();
				String casId = entry.getKey();
				WorkItem workItem = casWorkItemMap.get(casId);
				String seqNo = null;
				if(workItem != null) {
					seqNo = ""+workItem.getSeqNo();
				}
				String nodeIP = entry.getValue().getNodeIP();
				if(nodeIP == null) {
					DuccId processDuccId = null;
					duccOut.debug(location, jobDuccId, processDuccId, "seqNo:"+seqNo+" "+"casId:"+casId);
					continue;
				}
				String PID = entry.getValue().getPID();
				if(PID == null) {
					DuccId processDuccId = null;
					duccOut.debug(location, jobDuccId, processDuccId, "seqNo:"+seqNo+" "+"casId:"+casId+" "+"node:"+nodeIP);
					continue;
				}
				DuccId processDuccId = null;
				IDuccProcess process = job.getProcessMap().findProcess(nodeIP, PID);
				if(process != null) {
					processDuccId = process.getDuccId();
					jobDriver.accountingWorkItemIsDispatch(processDuccId);
					workItem.setProcessId(processDuccId);
					removeList.add(workItem);
					duccOut.debug(location, jobDuccId, processDuccId, "seqNo:"+seqNo+" "+"casId:"+casId+" "+"node:"+nodeIP+" "+"PID:"+PID);
					continue;
				}
				duccOut.debug(location, jobDuccId, processDuccId, "seqNo:"+seqNo+" "+"casId:"+casId+" "+"node:"+nodeIP+" "+"PID:"+PID);
			}
			removeLocations(removeList);
		}
		catch(Exception e) {
			duccOut.error(location, null, "location error?", e);
		}
	}
	
	public void registerCasPendingLocation(IJobDriver jobDriver, String seqNo, String casId) {
		String location = "registerCasPendingLocation";
		try {
			IDuccWorkJob job = jobDriver.getJob();
			DuccId jobDuccId = job.getDuccId();
			DuccId processDuccId = null;
			NP casLocation = new NP();
			locationPendingMapPut(seqNo, casId, casLocation);
			duccOut.debug(location, jobDuccId, processDuccId, "seqNo:"+seqNo+" "+"casId:"+casId);
		}
		catch(Exception e) {
			duccOut.error(location, null, "location error?", e);
		}
		return;
	}
	
	public void waitForLocation(IJobDriver jobDriver, WorkItem workItem) {
		String location = "waitForLocation";
		try {
			String casId = workItem.getCasId();
			String seqNo = ""+workItem.getSeqNo();
			IDuccWorkJob job = jobDriver.getJob();
			DuccId jobDuccId = job.getDuccId();
			if(casLocationPendingMap.containsKey(casId)) {
				DuccId processDuccId = null;
				duccOut.debug(location, jobDuccId, processDuccId, "seqNo:"+seqNo+" "+"wiId:"+workItem.getCasDocumentText()+" "+"casId:"+casId+" location pending");
				while(casLocationPendingMap.containsKey(casId)) {
					try {
						Thread.sleep(1000);
						updateLocations(jobDriver);
					}
					catch(InterruptedException e) {
						duccOut.debug(location, jobDuccId, processDuccId, "interrupted");
					}
				}
				duccOut.debug(location, jobDuccId, processDuccId, "seqNo:"+seqNo+" "+"wiId:"+workItem.getCasDocumentText()+" "+"casId:"+casId+" location assigned");
			}
		}
		catch(Exception e) {
			duccOut.error(location, null, "location error?", e);
		}
	}
	
	/**/
	
	
	public void assignLocation(IJobDriver jobDriver, String casId, String nodeIP, String PID) {
		String location = "assignLocation";
		try {
			IDuccWorkJob job = jobDriver.getJob();
			DuccId jobDuccId = job.getDuccId();
			IDuccProcess process = job.getProcessMap().findProcess(nodeIP, PID);
			if(process != null) {
				DuccId processDuccId = process.getDuccId();
				jobDriver.accountingWorkItemIsDispatch(processDuccId);
				ThreadLocation threadLocation = jobDriver.getCasDispatchMap().get(casId);
				duccOut.debug(location, jobDuccId, processDuccId, "seqNo:"+threadLocation.getSeqNo()+" "+"casId:"+casId+" "+"node:"+nodeIP+" "+"PID:"+PID);
				WorkItem workItem = casWorkItemMap.get(casId);
				workItem.setProcessId(processDuccId);
				locationPendingMapRemove(threadLocation.getSeqNo(),casId);
			}
			else {
				NP casLocation = new NP(nodeIP, PID);
				ThreadLocation threadLocation = jobDriver.getCasDispatchMap().get(casId);
				locationPendingMapPut(threadLocation.getSeqNo(), casId, casLocation);
				DuccId processDuccId = null;
				duccOut.debug(location, jobDuccId, processDuccId, "seqNo:"+threadLocation.getSeqNo()+" "+"casId:"+casId+" "+"node:"+nodeIP+" "+"PID:"+PID);
			}
		}
		catch(Exception e) {
			duccOut.error(location, jobid, "location error?", e);
		}
	}
	
	private void locationPendingMapRemove(String seqNo, String casId) {
		String location = "locationPendingMapRemove";
		duccOut.debug(location, jobid, "seqNo:"+seqNo+" "+"casId:"+casId);
		casLocationPendingMap.remove(casId);
	}
	
	private void locationPendingMapPut(String seqNo, String casId, NP casLocation) {
		String location = "locationPendingMapPut";
		duccOut.debug(location, jobid, "seqNo:"+seqNo+" "+"casId:"+casId);
		casLocationPendingMap.put(casId, casLocation);
	}
	
	private DuccProcessWorkItemsMap getPwiMap() {
		String location = "getPwiMap";
		DuccProcessWorkItemsMap pwiMap = getDriverStatusReportLive().getDuccProcessWorkItemsMap();
		duccOut.debug(location, jobid, "DuccProcessWorkItemsMap size:"+pwiMap.size());
		return pwiMap;
	}
	
	
	public void accountingWorkItemIsDispatch(DuccId processId) {
		String location = "accountingWorkItemIsDispatch";
		try {
			getPwiMap().dispatch(processId);
		}
		catch(Exception e) {
			duccOut.error(location, jobid, "accounting error?", e);
		}
	}

	
	public void accountingWorkItemIsPreempt(DuccId processId) {
		String location = "accountingWorkItemIsPreempt";
		try {
			getPwiMap().preempt(processId);
		}
		catch(Exception e) {
			duccOut.error(location, jobid, "accounting error?", e);
		}
	}

	
	public void accountingWorkItemIsRetry(DuccId processId) {
		String location = "accountingWorkItemIsRetry";
		try {
			getPwiMap().retry(processId);
		}
		catch(Exception e) {
			duccOut.error(location, jobid, "accounting error?", e);
		}
	}
	
	public void accountingWorkItemIsError(DuccId processId) {
		String location = "accountingWorkItemIsError";
		try {
			getPwiMap().error(processId);
		}
		catch(Exception e) {
			duccOut.error(location, jobid, "accounting error?", e);
		}
	}
	
	public void accountingWorkItemIsDone(DuccId processId, long time) {
		String location = "accountingWorkItemIsDone";
		try {
			getPwiMap().done(processId, time);
		}
		catch(Exception e) {
			duccOut.error(location, jobid, "accounting error?", e);
		}
	}
	
	/**
	 * IWorkItemMonitor
	 */
	
	private ThreadLocation getThreadLocation(WorkItem workItem) {
		String location = "getThreadLocation";
		ThreadLocation threadLocation = null;
		try {
			threadLocation = getCasDispatchMap().get(workItem.getCasId());
		}
		catch(Exception e) {
			duccOut.warn(location, jobid, e);
		}
		return threadLocation;
	}
	
	private String getThreadLocationInfo(WorkItem workItem) {
		String location = "getThreadLocationInfo";
		String retVal;
		try {
			ThreadLocation threadLocation = getThreadLocation(workItem);
			retVal = threadLocation.getInfo();
		}
		catch(Exception e) {
			duccOut.warn(location, jobid, e);
			retVal = "unknown";
		}
		return retVal;
	}
	
	private String getThreadLocationId(WorkItem workItem) {
		String location = "getThreadLocationId";
		String retVal;
		try {
			ThreadLocation threadLocation = getThreadLocation(workItem);
			retVal = threadLocation.getLocationId();
		}
		catch(Exception e) {
			duccOut.warn(location, jobid, e);
			retVal = "unknown";
		}
		return retVal;
	}
	
	
	public WorkItem getWorkItem(String casId) {
		String location = "getWorkItem";
		WorkItem workItem = null;
		if(casId != null) {
			workItem = casWorkItemMap.get(casId);
			if(workItem == null) {
				duccOut.warn(location, jobid, casId);
			}
		}
		return workItem;
	}
	
	
	public void queued(WorkItem workItem) {
		String location = "queued";
		try {
			duccOut.info(location, workItem.getJobId(), workItem.getProcessId(), "seqNo:"+workItem.getSeqNo()+" "+"wiId:"+workItem.getCasDocumentText());
		}
		catch(Exception e) {
			duccOut.error(location, jobid, "processing error?", e);
		}
		return;
	}
	
	
	public void dequeued(WorkItem workItem, String node, String pid) {
		String location = "dequeued";
		try {
			duccOut.info(location, workItem.getJobId(), workItem.getProcessId(), "seqNo:"+workItem.getSeqNo()+" "+"wiId:"+workItem.getCasDocumentText()+" "+"node:"+node+" "+"pid:"+pid);
		}
		catch(Exception e) {
			duccOut.error(location, jobid, "processing error?", e);
		}
		return;
	}
	
	
	public void start(WorkItem workItem) {
		String location = "start";
		try {
			registerCasPendingLocation(this, ""+workItem.getSeqNo(), workItem.getCasId());
			casWorkItemMap.put(workItem.getCasId(), workItem);
			duccOut.info(location, workItem.getJobId(), workItem.getProcessId(), "seqNo:"+workItem.getSeqNo()+" "+"wiId:"+workItem.getCasDocumentText());
			workItemStateManager.start(workItem.getSeqNo(),workItem.getCasDocumentText());
			driverStatusReport.workItemPendingProcessAssignmentAdd(workItem.getCasId());
			if(!workItem.isRetry()) {
				driverStatusReport.countWorkItemsProcessingStarted();
			}
			workItem.getTimeWindow().setStart(TimeStamp.getCurrentMillis());
			driverStatusReport.setMostRecentStart(workItem.getTimeWindow().getStartLong());
		}
		catch(Exception e) {
			duccOut.error(location, jobid, "processing error?", e);
		}
		return;
	}

	
	public void ended(WorkItem workItem) {
		String location = "ended";
		try {
			waitForLocation(this, workItem);
			workItemInactive();
			duccOut.debug(location, jobid, "action:ended "+getThreadLocationInfo(workItem));
			driverStatusReport.workItemPendingProcessAssignmentRemove(workItem.getCasId());
			driverStatusReport.workItemOperatingEnd(workItem.getCasId());
			if(driverStatusReport.isKillJob()) {
				duccOut.debug(location, jobid, "action:kill-job "+getThreadLocationInfo(workItem));
				// killing job - don't add bother with retry
			}
			else if(driverStatusReport.isKillProcess(workItem.getProcessId())) {
				duccOut.debug(location, jobid, "action:kill-process "+getThreadLocationInfo(workItem));
				retry(workItem);
				// killing process - don't add another work item to queue
			}
			else if(isRetry(workItem)) {
				duccOut.debug(location, jobid, "action:shrink "+getThreadLocationInfo(workItem));
				retry(workItem);
				// must be shrinking - don't add another work item to queue
			}
			else {
				duccOut.info(location, workItem.getJobId(), workItem.getProcessId(), "seqNo:"+workItem.getSeqNo()+" "+"wiId:"+workItem.getCasDocumentText());
				duccOut.debug(location, jobid, "action:completed "+getThreadLocationInfo(workItem));
				workItemStateManager.ended(workItem.getSeqNo());
				driverStatusReport.countWorkItemsProcessingCompleted();
				workItem.getTimeWindow().setEnd(TimeStamp.getCurrentMillis());
				long time = workItem.getTimeWindow().getElapsedMillis();
				synchronizedStats.addValue(time);
				DuccPerWorkItemStatistics perWorkItemStatistics = new DuccPerWorkItemStatistics(
						synchronizedStats.getMax(),
						synchronizedStats.getMin(),
						synchronizedStats.getMean(),
						synchronizedStats.getStandardDeviation()
						);
				driverStatusReport.setPerWorkItemStatistics(perWorkItemStatistics);
				performanceSummaryWriter.getSummaryMap().update(duccOut, workItem.getAnalysisEnginePerformanceMetricsList());
				int casCount = performanceSummaryWriter.getSummaryMap().casCount();
				int endCount = driverStatusReport.getWorkItemsProcessingCompleted();
				String message = "casCount:"+casCount+" "+"endCount:"+endCount;
				duccOut.debug(location, jobid, message);
				remove(workItem);
				casSource.recycle(workItem.getCAS());
				accountingWorkItemIsDone(workItem.getProcessId(),time);
				queueCASes(1,queue,workItemFactory);
			}
		}
		catch(Exception e) {
			duccOut.error(location, jobid, "processing error?", e);
		}
		return;
	}

	private void employPluginExceptionHandler(WorkItem workItem, Exception e) {
		String location = "employPluginExceptionHandler";
		Directive directive = getDirective(workItem,e);
		String message = "seqNo:"+workItem.getSeqNo()+" "+"wiId:"+workItem.getCasDocumentText()+" "+"directive:"+directive+" "+"reason:"+directive.getReason();
		duccOut.debug(location, workItem.getJobId(), workItem.getProcessId(), message, e);
		switch(directive) {
		case ProcessContinue_CasNoRetry:
			duccOut.info(location, workItem.getJobId(), workItem.getProcessId(), message);
			workItemStateManager.error(workItem.getSeqNo());
			workItemError(workItem, e, directive);
			remove(workItem);
			casSource.recycle(workItem.getCAS());
			accountingWorkItemIsError(workItem.getProcessId());
			try {
				queueCASes(1,queue,workItemFactory);
			}
			catch(Exception exception) {
				duccOut.error(location, jobid, "processing error?", exception);
			}
			break;
		case ProcessContinue_CasRetry:
			retry(workItem);
			break;
		case ProcessStop_CasNoRetry:
			duccOut.info(location, workItem.getJobId(), workItem.getProcessId(), message);
			workItemStateManager.error(workItem.getSeqNo());
			workItemError(workItem, e, directive);
			remove(workItem);
			casSource.recycle(workItem.getCAS());
			accountingWorkItemIsError(workItem.getProcessId());
			break;
		case ProcessStop_CasRetry:
			retry(workItem);
			break;
		case JobStop:
			duccOut.info(location, workItem.getJobId(), workItem.getProcessId(), message);
			driverStatusReport.killJob(JobCompletionType.CanceledByDriver, new Rationale("job driver received JobStop from plugin error handler"));
			break;
		}
	}
	
	
	public void exception(WorkItem workItem, Exception e) {
		String location = "exception";
		try {
			duccOut.debug(location, workItem.getJobId(), workItem.getProcessId(), "seqNo:"+workItem.getSeqNo()+" "+"wiId:"+workItem.getCasDocumentText());
			duccOut.debug(location, jobid, "action:exception "+getThreadLocationInfo(workItem), e);
			boolean timeout = false;
			if(ExceptionClassifier.isTimeout(e)) {
				ArrayList<WorkItem> removalsList = new ArrayList<WorkItem>();
				removalsList.add(workItem);
				removeLocations(removalsList);
				timeout = true;
			}
			else {
				duccOut.debug(location, jobid, "action:location-wait "+getThreadLocationInfo(workItem), e);
				waitForLocation(this, workItem);
			}
			workItemInactive();
			driverStatusReport.workItemPendingProcessAssignmentRemove(workItem.getCasId());
			driverStatusReport.workItemOperatingEnd(workItem.getCasId());
			if(driverStatusReport.isKillJob()) {
				duccOut.debug(location, jobid, "action:kill-job "+getThreadLocationInfo(workItem), e);
				// killing job - don't add bother with retry
			}
			else if(timeout) {
				duccOut.debug(location, jobid, "action:timeout "+getThreadLocationInfo(workItem), e);
				employPluginExceptionHandler(workItem, e);
			}
			else if(isUnknownProcess(workItem)) {
				duccOut.debug(location, jobid, "action:unknown-process "+getThreadLocationInfo(workItem), e);
				retry(workItem);
				// unknown process (no callbacks) - don't add another work item to queue
			}
			else if(driverStatusReport.isKillProcess(workItem.getProcessId())) {
				duccOut.debug(location, jobid, "action:kill-process "+getThreadLocationInfo(workItem), e);
				retry(workItem);
				// killing process - don't add another work item to queue
			}
			else if(isFailedProcess(workItem)) {
				if(ExceptionClassifier.isInterrupted(e)) {
					duccOut.debug(location, jobid, "action:fail-process-retry "+getThreadLocationInfo(workItem), e);
					retry(workItem);
					// process failed, work item interrupted - don't add another work item to queue
				}
				else {
					duccOut.debug(location, jobid, "action:fail-process-handler "+getThreadLocationInfo(workItem), e);
					employPluginExceptionHandler(workItem, e);
				}
			}
			else if(isRetry(workItem)) {
				duccOut.debug(location, jobid, "action:shrink "+getThreadLocationInfo(workItem), e);
				retry(workItem);
				// must be shrinking - don't add another work item to queue
			}
			else if(isError(workItem, e)) {
				duccOut.info(location, workItem.getJobId(), workItem.getProcessId(), "seqNo:"+workItem.getSeqNo()+" "+"wiId:"+workItem.getCasDocumentText());
				duccOut.debug(location, jobid, "action:error "+getThreadLocationInfo(workItem), e);
				workItemStateManager.error(workItem.getSeqNo());
				workItemError(workItem, e);
				remove(workItem);
				casSource.recycle(workItem.getCAS());
				accountingWorkItemIsError(workItem.getProcessId());
				queueCASes(1,queue,workItemFactory);
			}
			else {
				duccOut.debug(location, jobid, "action:retry "+getThreadLocationInfo(workItem), e);
				retry(workItem);
				queueCASes(1,queue,workItemFactory);
			}
		}
		catch(Exception exception) {
			duccOut.error(location, jobid, "processing error?", exception);
		}
		return;
	}
	
	private void workItemError(WorkItem workItem, Exception e) {
		workItemError(workItem, e, null);
	}
	
	/*
	private void workItemError(WorkItem workItem, Directive directive) {
		workItemError(workItem, null, directive);
	}
	*/
	
	private void workItemError(WorkItem workItem, Exception e, Directive directive) {
		String location = " workItemError";
		driverStatusReport.countWorkItemsProcessingError();
		String nodeId = "?";
		String pid = "?";
		DuccId djid = workItem.getJobId();
		DuccId dpid = workItem.getProcessId();
		try {
			String key = ""+workItem.getCAS().hashCode();
			if(casDispatchMap.containsKey(key)) {
				ThreadLocation threadLocation = casDispatchMap.get(key);
				if(threadLocation != null) {
					nodeId = threadLocation.getNodeId();
					pid = threadLocation.getProcessId();
				}
			}
			String message = "seqNo:"+workItem.getSeqNo()+" "+"wiId:"+workItem.getCasDocumentText()+" "+"node:"+nodeId+" "+"PID:"+pid;
			if(directive != null) {
				message += " "+"directive:"+directive;
			}
			
			duccOut.error(location, djid, dpid, message);
			duccErr.error(location, djid, dpid, message);
			if(e != null) {
				duccOut.error(location, djid, dpid, e);
				duccErr.error(location, djid, dpid, e);
			}
		}
		catch(Exception exception) {
			duccOut.error(location, djid, dpid, exception);
			duccErr.error(location, djid, dpid, exception);
		}
	}

	public void rectifyStatus() {
		if(casSource != null) {
			casSource.rectifyStatus();
		}
	}
	
}
