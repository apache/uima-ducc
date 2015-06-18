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
import org.apache.uima.ducc.container.jd.log.ErrorLogger;
import org.apache.uima.ducc.container.jd.log.LoggerHelper;
import org.apache.uima.ducc.container.jd.mh.RemoteWorkerProcess;
import org.apache.uima.ducc.container.jd.mh.iface.remote.IRemoteWorkerProcess;
import org.apache.uima.ducc.container.jd.timeout.TimeoutManager;
import org.apache.uima.ducc.container.jd.wi.IProcessStatistics;
import org.apache.uima.ducc.container.jd.wi.IWorkItem;
import org.apache.uima.ducc.container.net.iface.IMetaCas;
import org.apache.uima.ducc.container.net.iface.IMetaCasTransaction;
import org.apache.uima.ducc.container.net.iface.IMetaCasTransaction.JdState;

public abstract class ActionEndAbstract extends Action implements IAction {
	
	private Logger logger = Logger.getLogger(ActionEndAbstract.class, IComponent.Id.JD.name());
	
	protected ActionEndAbstract(Logger logger) {
		this.logger = logger;
	}
	
	private void jdExhausted(IActionData actionData) {
		String location = "jdExhausted";
		JobDriver jd = JobDriver.getInstance();
		switch(jd.getJdState()) {
		case Ended:
			break;
		default:
			jd.advanceJdState(JdState.Ended);
			MessageBuffer mb = LoggerHelper.getMessageBuffer(actionData);
			mb.append(Standardize.Label.jdState.get()+JobDriver.getInstance().getJdState());
			logger.info(location, ILogger.null_id, mb.toString());
			JobDriverHelper.getInstance().summarize();
			break;
		}
	}
	
	protected void checkEnded(IActionData actionData, CasManager cm) {
		String location = "checkEnded";
		int remainder = cm.getCasManagerStats().getUnfinishedWorkCount();
		MessageBuffer mb = LoggerHelper.getMessageBuffer(actionData);
		mb.append(Standardize.Label.remainder.get()+remainder);
		logger.debug(location, ILogger.null_id, mb.toString());
		if(remainder <= 0) {
			jdExhausted(actionData);
		}
	}
	
	private void retryWorkItem(IActionData actionData, CasManager cm, IMetaCas metaCas) {
		String location = "retryWorkItem";
		MessageBuffer mb = LoggerHelper.getMessageBuffer(actionData);
		logger.info(location, ILogger.null_id, mb.toString());
		TimeoutManager.getInstance().cancelTimer(actionData);
		cm.putMetaCas(metaCas, RetryReason.UserErrorRetry);
		cm.getCasManagerStats().incEndRetry();
	}
	
	private void killWorkItem(IActionData actionData, CasManager cm) {
		String location = "killWorkItem";
		MessageBuffer mb = LoggerHelper.getMessageBuffer(actionData);
		logger.info(location, ILogger.null_id, mb.toString());
		cm.getCasManagerStats().incEndFailure();
		checkEnded(actionData, cm);
	}
	
	// The job process is killed if either the job is killed (duh) or the 
	// work item is killed, so presently this method is not needed.  Someday
	// we may allow the plug-in error handler to not kill the process so that,
	// for example, very long running work items are not unnecessarily 
	// restarted from scratch.
	
	/*
	private void killProcess(IActionData actionData, CasManager cm, IMetaCas metaCas, IWorkItem wi, DeallocateReason deallocateReason) {
		String location = "killProcess";
		WiTracker tracker = WiTracker.getInstance();
		IRemoteWorkerProcess rwp = tracker.getRemoteWorkerProcess(wi);
		if(rwp == null) {
			MessageBuffer mb = LoggerHelper.getMessageBuffer(actionData);
			mb.append("remote worker process not found");
			logger.info(location, ILogger.null_id, mb.toString());
		}
		else {
			String nodeIp = rwp.getNodeAddress();
			String pid = ""+rwp.getPid();
			IRemoteLocation remoteLocation = new RemoteLocation(nodeIp,pid);
			JobDriver jd = JobDriver.getInstance();
			jd.killProcess(remoteLocation, deallocateReason);
			MessageBuffer mb = LoggerHelper.getMessageBuffer(actionData);
			mb.append(Standardize.Label.node.get()+nodeIp);
			mb.append(Standardize.Label.pid.get()+pid);
			logger.info(location, ILogger.null_id, mb.toString());
		}
	}
	*/
	
	private void killJob(IActionData actionData, CasManager cm) {
		String location = "killJob";
		cm.getCasManagerStats().setKillJob();
		MessageBuffer mb = LoggerHelper.getMessageBuffer(actionData);
		logger.info(location, ILogger.null_id, mb.toString());
	}
	
	private void toJdErrLog(String text) {
		ErrorLogger.record(text);
	}
	
	protected void handleException(IActionData actionData, Object userException, String printableException) throws JobDriverException {
		String location = "handleException";
		if(true) {
			MessageBuffer mb = LoggerHelper.getMessageBuffer(actionData);
			mb.append(Standardize.Label.enter+"");
			logger.debug(location, ILogger.null_id, mb.toString());
		}
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
		try {
			// Identify the timeout case in the header & record in one logger call as is multi-threadsd
			if (printableException != null) {
				toJdErrLog(Standardize.Label.seqNo.get()+seqNo+" ***** EXCEPTION *****\n"+printableException);
			} else {
				toJdErrLog(Standardize.Label.seqNo.get()+seqNo+" ***** TIMEOUT *****\n"+userException.toString()+"\n");
			}
		}
		catch(Exception e) {
			logger.error(location, ILogger.null_id, e);
		}
		//
		ProxyJobDriverDirective pjdd = null;
		try {
			String serializedCas = (String) metaCas.getUserSpaceCas();
			ProxyJobDriverErrorHandler pjdeh = jd.getProxyJobDriverErrorHandler();
			pjdd = pjdeh.handle(serializedCas, userException);
		}
		catch(Exception e) {
			logger.error(location, ILogger.null_id, e);
		}
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
		if(true) {
			MessageBuffer mb = LoggerHelper.getMessageBuffer(actionData);
			mb.append(Standardize.Label.exit+"");
			logger.debug(location, ILogger.null_id, mb.toString());
		}
	}
	
}
