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
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.uima.ducc.common.jd.files.workitem.IWorkItemStateKeeper;
import org.apache.uima.ducc.container.common.MessageBuffer;
import org.apache.uima.ducc.container.common.MetaCasHelper;
import org.apache.uima.ducc.container.common.Standardize;
import org.apache.uima.ducc.container.common.fsm.iface.IAction;
import org.apache.uima.ducc.container.common.logger.IComponent;
import org.apache.uima.ducc.container.common.logger.ILogger;
import org.apache.uima.ducc.container.common.logger.Logger;
import org.apache.uima.ducc.container.jd.JobDriver;
import org.apache.uima.ducc.container.jd.JobDriverException;
import org.apache.uima.ducc.container.jd.JobDriverHelper;
import org.apache.uima.ducc.container.jd.cas.CasManager;
import org.apache.uima.ducc.container.jd.cas.CasManagerStats.RetryReason;
import org.apache.uima.ducc.container.jd.classload.ProxyJobDriverDirective;
import org.apache.uima.ducc.container.jd.classload.ProxyJobDriverErrorHandler;
import org.apache.uima.ducc.container.jd.log.ErrorLogger;
import org.apache.uima.ducc.container.jd.log.LoggerHelper;
import org.apache.uima.ducc.container.jd.mh.RemoteWorkerProcess;
import org.apache.uima.ducc.container.jd.mh.RemoteWorkerThread;
import org.apache.uima.ducc.container.jd.mh.iface.remote.IRemoteWorkerProcess;
import org.apache.uima.ducc.container.jd.mh.iface.remote.IRemoteWorkerThread;
import org.apache.uima.ducc.container.jd.timeout.TimeoutManager;
import org.apache.uima.ducc.container.jd.wi.IProcessStatistics;
import org.apache.uima.ducc.container.jd.wi.IWorkItem;
import org.apache.uima.ducc.container.jd.wi.IWorkItemStatistics;
import org.apache.uima.ducc.container.jd.wi.WiTracker;
import org.apache.uima.ducc.container.jd.wi.perf.IWorkItemPerformanceKeeper;
import org.apache.uima.ducc.container.net.iface.IMetaCas;
import org.apache.uima.ducc.container.net.iface.IMetaCasTransaction;
import org.apache.uima.ducc.container.net.iface.IMetaCasTransaction.JdState;
import org.apache.uima.ducc.container.net.iface.IPerformanceMetrics;

public class ActionEnd extends Action implements IAction {

	private static Logger logger = Logger.getLogger(ActionEnd.class, IComponent.Id.JD.name());
	
	@Override
	public String getName() {
		return ActionEnd.class.getName();
	}
	
	private void killJob(IActionData actionData, CasManager cm) {
		String location = "killJob";
		cm.getCasManagerStats().setKillJob();
		MessageBuffer mb = LoggerHelper.getMessageBuffer(actionData);
		logger.info(location, ILogger.null_id, mb.toString());
	}
	
	private void retryWorkItem(IActionData actionData, CasManager cm, IMetaCas metaCas) {
		String location = "retryWorkItem";
		cm.putMetaCas(metaCas, RetryReason.UserErrorRetry);
		cm.getCasManagerStats().incEndRetry();
		MessageBuffer mb = LoggerHelper.getMessageBuffer(actionData);
		logger.info(location, ILogger.null_id, mb.toString());
	}
	
	private void killWorkItem(IActionData actionData, CasManager cm) {
		String location = "killWorkItem";
		cm.getCasManagerStats().incEndFailure();
		MessageBuffer mb = LoggerHelper.getMessageBuffer(actionData);
		logger.info(location, ILogger.null_id, mb.toString());
		checkEnded(actionData, cm);
	}
	
	private void successWorkItem(IActionData actionData, CasManager cm, IWorkItem wi) {
		String location = "successWorkItem";
		cm.getCasManagerStats().incEndSuccess();
		wi.setTodEnd();
		updateStatistics(actionData, wi);
		updatePerformanceMetrics(actionData, wi);
		MessageBuffer mb = LoggerHelper.getMessageBuffer(actionData);
		logger.info(location, ILogger.null_id, mb.toString());
		checkEnded(actionData, cm);
	}
	
	private void updateStatistics(IActionData actionData, IWorkItem wi) {
		String location = "updateStatistics";
		IWorkItemStatistics wis = JobDriver.getInstance().getWorkItemStatistics();
		wis.ended(wi);
		MessageBuffer mb = LoggerHelper.getMessageBuffer(actionData);
		mb.append(Standardize.Label.avg.get()+wis.getMillisAvg());
		mb.append(Standardize.Label.max.get()+wis.getMillisMax());
		mb.append(Standardize.Label.min.get()+wis.getMillisMin());
		logger.debug(location, ILogger.null_id, mb.toString());
	}

	private String keyName = "name";
	private String keyUniqueName = "uniqueName";
	private String keyAnalysisTime = "analysisTime";
	
	private void updatePerformanceMetrics(IActionData actionData, IWorkItem wi) {
		String location = "updatePerformanceMetrics";
		IMetaCas metaCas = wi.getMetaCas();
		IPerformanceMetrics performanceMetrics = metaCas.getPerformanceMetrics();
		List<Properties> list = performanceMetrics.get();
		int size = 0;
		if(list !=  null) {
			size = list.size();
			JobDriver jd = JobDriver.getInstance();
			IWorkItemPerformanceKeeper wipk = jd.getWorkItemPerformanceKeeper();
			for(Properties properties : list) {
				String name = properties.getProperty(keyName);
				String uniqueName = properties.getProperty(keyUniqueName);
				String analysisTime = properties.getProperty(keyAnalysisTime);
				long time = 0;
				try {
					time = Long.parseLong(analysisTime);
				}
				catch(Exception e) {
					logger.error(location, ILogger.null_id, e);
				}
				wipk.dataAdd(name, uniqueName, time);
				for(Entry<Object, Object> entry : properties.entrySet()) {
					String key = (String) entry.getKey();
					String value = (String) entry.getValue();
					MessageBuffer mb = LoggerHelper.getMessageBuffer(actionData);
					mb.append(Standardize.Label.key.get()+key);
					mb.append(Standardize.Label.value.get()+value);
					logger.debug(location, ILogger.null_id, mb.toString());
				}
			}
		}
		MessageBuffer mb = LoggerHelper.getMessageBuffer(actionData);
		mb.append(Standardize.Label.size.get()+size);
		logger.debug(location, ILogger.null_id, mb.toString());
	}
	
	private void jdExhausted(IActionData actionData) {
		String location = "jdExhausted";
		JobDriver.getInstance().advanceJdState(JdState.Ended);
		MessageBuffer mb = LoggerHelper.getMessageBuffer(actionData);
		mb.append(Standardize.Label.jdState.get()+JobDriver.getInstance().getJdState());
		logger.info(location, ILogger.null_id, mb.toString());
	}
	
	private void checkEnded(IActionData actionData, CasManager cm) {
		String location = "checkEnded";
		int remainder = cm.getCasManagerStats().getUnfinishedWorkCount();
		MessageBuffer mb = LoggerHelper.getMessageBuffer(actionData);
		mb.append(Standardize.Label.remainder.get()+remainder);
		logger.debug(location, ILogger.null_id, mb.toString());
		if(remainder <= 0) {
			jdExhausted(actionData);
		}
	}
	
	private void toJdErrLog(String text) {
		ErrorLogger.record(text);
	}
	
	private void handleException(IActionData actionData) throws JobDriverException {
		String location = "handleException";
		IWorkItem wi = actionData.getWorkItem();
		IMetaCasTransaction trans = actionData.getMetaCasTransaction();
		IRemoteWorkerProcess rwp = new RemoteWorkerProcess(trans);
		IMetaCas metaCas = wi.getMetaCas();
		JobDriver jd = JobDriver.getInstance();
		JobDriverHelper jdh = JobDriverHelper.getInstance();
		CasManager cm = jd.getCasManager();
		//
		IWorkItemStateKeeper wisk = jd.getWorkItemStateKeeper();
		MetaCasHelper metaCasHelper = new MetaCasHelper(metaCas);
		IProcessStatistics pStats = jdh.getProcessStatistics(rwp);
		//
		int seqNo = metaCasHelper.getSystemKey();
		//String serializedException = (String) metaCas.getUserSpaceException();
		//TODO
		String serializedException = Standardize.Label.seqNo.get()+seqNo+" work-in-progress is to log actual exception here!";
		toJdErrLog(serializedException);
		//
		String serializedCas = (String) metaCas.getUserSpaceCas();
		ProxyJobDriverErrorHandler pjdeh = jd.getProxyJobDriverErrorHandler();
		ProxyJobDriverDirective pjdd = pjdeh.handle(serializedCas, serializedException);
		if(pjdd != null) {
			MessageBuffer mb = LoggerHelper.getMessageBuffer(actionData);
			mb.append(Standardize.Label.isKillJob.get()+pjdd.isKillJob());
			mb.append(Standardize.Label.isKillProcess.get()+pjdd.isKillProcess());
			mb.append(Standardize.Label.isKillWorkItem.get()+pjdd.isKillWorkItem());
			logger.info(location, ILogger.null_id, mb.toString());
			if(pjdd.isKillJob()) {
				wisk.error(seqNo);
				pStats.error(wi);
				killJob(actionData, cm);
				killWorkItem(actionData, cm);
			}
			else if(pjdd.isKillWorkItem()) {
				wisk.error(seqNo);
				pStats.error(wi);
				killWorkItem(actionData, cm);
			}
			else {
				wisk.retry(seqNo);
				pStats.retry(wi);
				retryWorkItem(actionData, cm, metaCas);
			}
		}
		else {
			wisk.error(seqNo);
			pStats.error(wi);
			killWorkItem(actionData, cm);
		}
	}
	
	@Override
	public void engage(Object objectData) {
		String location = "engage";
		logger.trace(location, ILogger.null_id, "");
		IActionData actionData = (IActionData) objectData;
		try {
			IWorkItem wi = actionData.getWorkItem();
			IMetaCasTransaction trans = actionData.getMetaCasTransaction();
			IRemoteWorkerThread rwt = new RemoteWorkerThread(trans);
			IRemoteWorkerProcess rwp = new RemoteWorkerProcess(trans);
			IMetaCas metaCas = wi.getMetaCas();
			JobDriver jd = JobDriver.getInstance();
			JobDriverHelper jdh = JobDriverHelper.getInstance();
			CasManager cm = jd.getCasManager();
			//
			IWorkItemStateKeeper wisk = jd.getWorkItemStateKeeper();
			MetaCasHelper metaCasHelper = new MetaCasHelper(metaCas);
			IProcessStatistics pStats = jdh.getProcessStatistics(rwp);
			//
			if(metaCas != null) {
				WiTracker.getInstance().unassign(rwt);
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
					handleException(actionData);
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
		catch(Exception e) {
			logger.error(location, ILogger.null_id, e);
		}
	}

}
