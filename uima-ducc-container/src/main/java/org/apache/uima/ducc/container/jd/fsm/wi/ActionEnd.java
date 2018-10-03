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
package org.apache.uima.ducc.container.jd.fsm.wi;

import java.util.List;

import org.apache.uima.ducc.common.jd.files.workitem.IWorkItemStateKeeper;
import org.apache.uima.ducc.container.common.MessageBuffer;
import org.apache.uima.ducc.container.common.MetaCasHelper;
import org.apache.uima.ducc.container.common.Standardize;
import org.apache.uima.ducc.container.common.fsm.iface.IAction;
import org.apache.uima.ducc.container.common.logger.IComponent;
import org.apache.uima.ducc.container.common.logger.ILogger;
import org.apache.uima.ducc.container.common.logger.Logger;
import org.apache.uima.ducc.container.jd.JobDriver;
import org.apache.uima.ducc.container.jd.JobDriverHelper;
import org.apache.uima.ducc.container.jd.cas.CasManager;
import org.apache.uima.ducc.container.jd.log.LoggerHelper;
import org.apache.uima.ducc.container.jd.mh.RemoteWorkerProcess;
import org.apache.uima.ducc.container.jd.mh.iface.remote.IRemoteWorkerProcess;
import org.apache.uima.ducc.container.jd.timeout.TimeoutManager;
import org.apache.uima.ducc.container.jd.user.error.classload.ProxyUserErrorException;
import org.apache.uima.ducc.container.jd.user.error.classload.ProxyUserErrorStringify;
import org.apache.uima.ducc.container.jd.wi.IProcessStatistics;
import org.apache.uima.ducc.container.jd.wi.IWorkItem;
import org.apache.uima.ducc.container.jd.wi.IWorkItemStatistics;
import org.apache.uima.ducc.container.jd.wi.WiTracker;
import org.apache.uima.ducc.container.jd.wi.perf.IWorkItemPerformanceIndividualKeeper;
import org.apache.uima.ducc.container.jd.wi.perf.IWorkItemPerformanceSummaryKeeper;
import org.apache.uima.ducc.container.jd.wi.perf.WorkItemPerformanceIndividualKeeper;
import org.apache.uima.ducc.ps.net.iface.IMetaTask;
import org.apache.uima.ducc.ps.net.iface.IMetaTaskTransaction;
import org.apache.uima.ducc.ps.service.processor.IServiceResultSerializer;
import org.apache.uima.ducc.ps.service.processor.uima.utils.PerformanceMetrics;
import org.apache.uima.ducc.ps.service.processor.uima.utils.UimaResultDefaultSerializer;

public class ActionEnd extends ActionEndAbstract implements IAction {

	private static Logger logger = Logger.getLogger(ActionEnd.class, IComponent.Id.JD.name());
	
	private ProxyUserErrorStringify proxy = null;
	
	public ActionEnd() {
		super(logger);
		initialize();
	}
	
	private void initialize() {
		String location = "initialize";
		try {
			proxy = new ProxyUserErrorStringify();
		} 
		catch (ProxyUserErrorException e) {
			logger.error(location, ILogger.null_id, e);
		}
	}
	
	@Override
	public String getName() {
		return ActionEnd.class.getName();
	}
	
	private void successWorkItem(IActionData actionData, CasManager cm, IWorkItem wi) {
		String location = "successWorkItem";
		cm.getCasManagerStats().incEndSuccess();
		wi.setTodEnd();
		updateStatistics(actionData, wi);
		updatePerformanceMetrics(actionData, wi);
		MessageBuffer mb = LoggerHelper.getMessageBuffer(actionData);
		logger.debug(location, ILogger.null_id, mb.toString());
		ActionHelper.checkEnded(logger, actionData, cm);
	}
	
	private void updateStatistics(IActionData actionData, IWorkItem wi) {
		String location = "updateStatistics";
		IWorkItemStatistics wis = JobDriver.getInstance().getWorkItemStatistics();
		wis.ended(wi);
		MessageBuffer mb = LoggerHelper.getMessageBuffer(actionData);
		mb.append(Standardize.Label.avg.get()+wis.getMillisAvg());
		mb.append(Standardize.Label.max.get()+wis.getMillisMax());
		mb.append(Standardize.Label.min.get()+wis.getMillisMin());
		mb.append(Standardize.Label.stddev.get()+wis.getMillisStdDev());
		logger.debug(location, ILogger.null_id, mb.toString());
	}

	private String keyName = "name";
	private String keyUniqueName = "uniqueName";
	private String keyAnalysisTime = "analysisTime";
	private String keyAnalysisTasks = "analysisTasks";
	
	private boolean oldFormat = false;
	
	private String normalizeUniqueName(String uniqueName) {
		String retVal = uniqueName;
		if(oldFormat) {
			try {
				// expected format: <thread-number> Components /<annotators-path>
				retVal = uniqueName.trim().split("\\s++", 3)[2];
			}
			catch(Exception e) {
			}
		}
		return retVal;
	}
	
	private void updatePerformanceMetrics(IActionData actionData, IWorkItem wi) {
		String location = "updatePerformanceMetrics";
		if(wi != null) {
			IMetaTask metaCas = wi.getMetaCas();
			if(metaCas != null) {
				IServiceResultSerializer deserializer =
						new UimaResultDefaultSerializer();
				List<PerformanceMetrics> performanceMetrics = null; 
				String serializedPerformance = metaCas.getPerformanceMetrics();
				try {
					performanceMetrics = deserializer.deserialize(serializedPerformance);
					if(performanceMetrics == null) {
						logger.debug(location,  ILogger.null_id, "seqNo=", wi.getSeqNo(), "performanceMetrics=", performanceMetrics); // null
					}
					else if(performanceMetrics.isEmpty()) {
						logger.debug(location,  ILogger.null_id, "seqNo=", wi.getSeqNo(), "performanceMetrics=", performanceMetrics); // empty
					}
				} catch( Exception e) {
					logger.error(location, ILogger.null_id, e);
				}
				if(performanceMetrics != null && !performanceMetrics.isEmpty()) {
					int size = 0;
					size = performanceMetrics.size();
					logger.debug(location,  ILogger.null_id, "seqNo=", wi.getSeqNo(), "performanceMetrics.size=", size);
					JobDriver jd = JobDriver.getInstance();
					String logdir = jd.getLogDir();
					String seqNo = ""+wi.getSeqNo();
					IWorkItemPerformanceIndividualKeeper wipik = new WorkItemPerformanceIndividualKeeper(logdir, seqNo);
					IWorkItemPerformanceSummaryKeeper wipsk = jd.getWorkItemPerformanceSummaryKeeper();
					wipsk.count();
					long total_time = 0;
					long total_tasks = 0;
					//for(Properties properties : list) {
					for(PerformanceMetrics pm : performanceMetrics) {	
						String name = pm.getName();
						String uniqueName = normalizeUniqueName(pm.getUniqueName());
						long time = pm.getAnalysisTime();
						long tasks = pm.getNumberOfTasksProcessed();
						logger.debug(location, ILogger.null_id, "seqNo=", seqNo, "name=", name, "uniqueName=", uniqueName, "time=", time, "tasks=", tasks);
						if(time < 0) {
							String text = "seqNo="+seqNo+" "+"time="+time+" "+"analysisTime="+time+" "+"uniqueName="+uniqueName;
							logger.warn(location, ILogger.null_id, text);
						}
						wipik.dataAdd(name, uniqueName, time, tasks);
						wipsk.dataAdd(name, uniqueName, time, tasks);
						MessageBuffer mb = LoggerHelper.getMessageBuffer(actionData);
						mb.append(Standardize.Label.key.get()+keyName);
						mb.append(Standardize.Label.value.get()+name);				
						mb.append(Standardize.Label.key.get()+keyUniqueName);
						mb.append(Standardize.Label.value.get()+uniqueName);
						mb.append(Standardize.Label.key.get()+keyAnalysisTime);
						mb.append(Standardize.Label.value.get()+time);
						mb.append(Standardize.Label.key.get()+keyAnalysisTasks);
						mb.append(Standardize.Label.value.get()+tasks);
						logger.debug(location, ILogger.null_id, mb.toString());
						total_time += time;
						total_tasks += tasks;
					}
					wipik.publish();
					// Add the aggregate values as if a no-name delegate
					wipsk.dataAdd("TOTALS", "", total_time, total_tasks);
					MessageBuffer mb = LoggerHelper.getMessageBuffer(actionData);
					mb.append(Standardize.Label.size.get()+size);
					logger.debug(location, ILogger.null_id, mb.toString());
				}
			}
		}
	}
	
	@Override
	public void engage(Object objectData) {
		String location = "engage";
		logger.trace(location, ILogger.null_id, "");
		IActionData actionData = (IActionData) objectData;
		try {
			if(actionData != null) {
				IWorkItem wi = actionData.getWorkItem();
				IMetaTaskTransaction trans = actionData.getMetaCasTransaction();
				IRemoteWorkerProcess rwp = new RemoteWorkerProcess(trans);
				IMetaTask metaCas = wi.getMetaCas();
				JobDriver jd = JobDriver.getInstance();
				JobDriverHelper jdh = JobDriverHelper.getInstance();
				CasManager cm = jd.getCasManager();
				//
				IWorkItemStateKeeper wisk = jd.getWorkItemStateKeeper();
				MetaCasHelper metaCasHelper = new MetaCasHelper(metaCas);
				IProcessStatistics pStats = jdh.getProcessStatistics(rwp);
				//
				if(metaCas != null) {
					WiTracker.getInstance().unassign(wi);
					//
					TimeoutManager toMgr = TimeoutManager.getInstance();
					toMgr.receivedAck(actionData);
					toMgr.receivedEnd(actionData);
					//
					int seqNo = metaCasHelper.getSystemKey();
					Object exception = metaCas.getUserSpaceException();
					if(exception != null) {
						MessageBuffer mb = LoggerHelper.getMessageBuffer(actionData);
						mb.append("exception");
						logger.info(location, ILogger.null_id, mb.toString());
						Object userException = metaCas.getUserSpaceException();
						String printableException = null;
						try {
							printableException = proxy.convert(userException);
						}
						catch(Exception e) {
							MessageBuffer eb = LoggerHelper.getMessageBuffer(actionData);
							eb.append("unable to convert user job process exception into printable form");
							logger.error(location, ILogger.null_id, eb.toString());
						}
						handleException(actionData, ExceptionType.User, userException, printableException);
						displayProcessStatistics(logger, actionData, wi, pStats);
					}
					else {
						MessageBuffer mb = LoggerHelper.getMessageBuffer(actionData);
						mb.append("ended");
						logger.info(location, ILogger.null_id, mb.toString());
						wisk.ended(seqNo);
						successWorkItem(actionData, cm, wi);
						pStats.done(wi);
						displayProcessStatistics(logger, actionData, wi, pStats);
					}
					wi.reset();
				}
				else {
					MessageBuffer mb = LoggerHelper.getMessageBuffer(actionData);
					mb.append("No CAS found for processing");
					logger.info(location, ILogger.null_id, mb.toString());
				}
			}
			else {
				MessageBuffer mb = LoggerHelper.getMessageBuffer(actionData);
				mb.append("No action data found for processing");
				logger.warn(location, ILogger.null_id, mb.toString());
			}
		}
		catch(Exception e) {
			logger.error(location, ILogger.null_id, e);
		}
	}

}
