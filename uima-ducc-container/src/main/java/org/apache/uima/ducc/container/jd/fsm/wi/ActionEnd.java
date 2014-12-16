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
import org.apache.uima.ducc.container.jd.mh.RemoteWorkerProcess;
import org.apache.uima.ducc.container.jd.mh.RemoteWorkerThread;
import org.apache.uima.ducc.container.jd.mh.iface.remote.IRemoteWorkerProcess;
import org.apache.uima.ducc.container.jd.mh.iface.remote.IRemoteWorkerThread;
import org.apache.uima.ducc.container.jd.wi.IProcessStatistics;
import org.apache.uima.ducc.container.jd.wi.IWorkItem;
import org.apache.uima.ducc.container.jd.wi.IWorkItemStatistics;
import org.apache.uima.ducc.container.net.iface.IMetaCas;
import org.apache.uima.ducc.container.net.iface.IMetaCasTransaction;
import org.apache.uima.ducc.container.net.iface.IMetaCasTransaction.JdState;

public class ActionEnd implements IAction {

	private static Logger logger = Logger.getLogger(ActionEnd.class, IComponent.Id.JD.name());
	
	@Override
	public String getName() {
		return ActionEnd.class.getName();
	}
	
	private void killJob(CasManager cm, IWorkItem wi, IMetaCasTransaction trans, IMetaCas metaCas, IRemoteWorkerThread rwt) {
		String location = "killJob";
		cm.getCasManagerStats().setKillJob();
		MessageBuffer mb = new MessageBuffer();
		mb.append(Standardize.Label.transNo.get()+trans.getTransactionId().toString());
		mb.append(Standardize.Label.seqNo.get()+metaCas.getSystemKey());
		mb.append(Standardize.Label.remote.get()+rwt.toString());
		logger.info(location, ILogger.null_id, mb.toString());
	}
	
	private void retryWorkItem(CasManager cm, IWorkItem wi, IMetaCasTransaction trans, IMetaCas metaCas, IRemoteWorkerThread rwt) {
		String location = "retryWorkItem";
		cm.putMetaCas(metaCas, RetryReason.UserErrorRetry);
		cm.getCasManagerStats().incEndRetry();
		MessageBuffer mb = new MessageBuffer();
		mb.append(Standardize.Label.transNo.get()+trans.getTransactionId().toString());
		mb.append(Standardize.Label.seqNo.get()+metaCas.getSystemKey());
		mb.append(Standardize.Label.remote.get()+rwt.toString());
		logger.info(location, ILogger.null_id, mb.toString());
	}
	
	private void killWorkItem(CasManager cm, IWorkItem wi, IMetaCasTransaction trans, IMetaCas metaCas, IRemoteWorkerThread rwt) {
		String location = "killWorkItem";
		cm.getCasManagerStats().incEndFailure();
		MessageBuffer mb = new MessageBuffer();
		mb.append(Standardize.Label.transNo.get()+trans.getTransactionId().toString());
		mb.append(Standardize.Label.seqNo.get()+metaCas.getSystemKey());
		mb.append(Standardize.Label.remote.get()+rwt.toString());
		logger.info(location, ILogger.null_id, mb.toString());
		checkEnded(cm);
	}
	
	private void successWorkItem(CasManager cm, IWorkItem wi, IMetaCasTransaction trans, IMetaCas metaCas, IRemoteWorkerThread rwt) {
		String location = "successWorkItem";
		cm.getCasManagerStats().incEndSuccess();
		wi.setTodEnd();
		updateStatistics(wi);
		MessageBuffer mb = new MessageBuffer();
		mb.append(Standardize.Label.transNo.get()+trans.getTransactionId().toString());
		mb.append(Standardize.Label.seqNo.get()+metaCas.getSystemKey());
		mb.append(Standardize.Label.remote.get()+rwt.toString());
		logger.info(location, ILogger.null_id, mb.toString());
		checkEnded(cm);
	}
	
	private void updateStatistics(IWorkItem wi) {
		String location = "updateStatistics";
		IWorkItemStatistics wis = JobDriver.getInstance().getWorkItemStatistics();
		wis.ended(wi);
		MessageBuffer mb = new MessageBuffer();
		mb.append(Standardize.Label.seqNo.get()+wi.getMetaCas().getSystemKey());
		mb.append(Standardize.Label.avg.get()+wis.getMillisAvg());
		mb.append(Standardize.Label.max.get()+wis.getMillisMax());
		mb.append(Standardize.Label.min.get()+wis.getMillisMin());
		logger.debug(location, ILogger.null_id, mb.toString());
	}
	
	private void checkEnded(CasManager cm) {
		String location = "checkEnded";
		int remainder = cm.getCasManagerStats().getUnfinishedWorkCount();
		MessageBuffer mb1 = new MessageBuffer();
		mb1.append(Standardize.Label.remainder.get()+remainder);
		logger.debug(location, ILogger.null_id, mb1.toString());
		if(remainder <= 0) {
			JobDriver.getInstance().advanceJdState(JdState.Ended);
			MessageBuffer mb = new MessageBuffer();
			mb.append(Standardize.Label.jdState.get()+JobDriver.getInstance().getJdState());
			logger.info(location, ILogger.null_id, mb.toString());
		}
	}
	
	private void handleException(IActionData actionData) throws JobDriverException {
		String location = "handleException";
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
		int seqNo = metaCasHelper.getSystemKey();
		Object exception = metaCas.getUserSpaceException();
		//
		Object cas = metaCas.getUserSpaceCas();
		ProxyJobDriverErrorHandler pjdeh = jd.getProxyJobDriverErrorHandler();
		ProxyJobDriverDirective pjdd = pjdeh.handle(cas, exception);
		if(pjdd != null) {
			MessageBuffer mb = new MessageBuffer();
			mb.append(Standardize.Label.isKillJob.get()+pjdd.isKillJob());
			mb.append(Standardize.Label.isKillProcess.get()+pjdd.isKillProcess());
			mb.append(Standardize.Label.isKillWorkItem.get()+pjdd.isKillWorkItem());
			logger.info(location, ILogger.null_id, mb.toString());
			if(pjdd.isKillJob()) {
				wisk.error(seqNo);
				pStats.error(wi);
				killWorkItem(cm, wi, trans, metaCas, rwt);
				killJob(cm, wi, trans, metaCas, rwt);
			}
			else if(pjdd.isKillWorkItem()) {
				wisk.error(seqNo);
				pStats.error(wi);
				killWorkItem(cm, wi, trans, metaCas, rwt);
			}
			else {
				wisk.retry(seqNo);
				pStats.retry(wi);
				retryWorkItem(cm, wi, trans, metaCas, rwt);
			}
		}
		else {
			wisk.error(seqNo);
			pStats.error(wi);
			killWorkItem(cm, wi, trans, metaCas, rwt);
		}
	}
	
	private void displayProcessStatistics(IWorkItem wi, IProcessStatistics pStats) {
		String location = "displayProcessStatistics";
		MessageBuffer mb = new MessageBuffer();
		mb.append(Standardize.Label.seqNo.get()+wi.getMetaCas().getSystemKey());
		mb.append(Standardize.Label.avg.get()+pStats.getMillisAvg());
		mb.append(Standardize.Label.max.get()+pStats.getMillisMax());
		mb.append(Standardize.Label.min.get()+pStats.getMillisMin());
		logger.debug(location, ILogger.null_id, mb.toString());
	}
	
	@Override
	public void engage(Object objectData) {
		String location = "engage";
		logger.debug(location, ILogger.null_id, "");
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
				int seqNo = metaCasHelper.getSystemKey();
				Object exception = metaCas.getUserSpaceException();
				if(exception != null) {
					handleException(actionData);
				}
				else {
					wisk.ended(seqNo);
					successWorkItem(cm, wi, trans, metaCas, rwt);
					pStats.done(wi);
					displayProcessStatistics(wi, pStats);
				}
				wi.resetTods();
			}
			else {
				MessageBuffer mb = new MessageBuffer();
				mb.append("No CAS found for processing");
				logger.info(location, ILogger.null_id, mb.toString());
			}
		}
		catch(Exception e) {
			logger.error(location, ILogger.null_id, e);
		}
	}

}
