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
import org.apache.uima.ducc.container.common.IJdConstants.DeallocateReason;
import org.apache.uima.ducc.container.common.fsm.iface.IAction;
import org.apache.uima.ducc.container.common.logger.IComponent;
import org.apache.uima.ducc.container.common.logger.ILogger;
import org.apache.uima.ducc.container.common.logger.Logger;
import org.apache.uima.ducc.container.jd.JobDriver;
import org.apache.uima.ducc.container.jd.JobDriverException;
import org.apache.uima.ducc.container.jd.JobDriverHelper;
import org.apache.uima.ducc.container.jd.cas.CasManager;
import org.apache.uima.ducc.container.jd.classload.ProxyJobDriverDirective;
import org.apache.uima.ducc.container.jd.classload.ProxyJobDriverErrorHandler;
import org.apache.uima.ducc.container.jd.log.LoggerHelper;
import org.apache.uima.ducc.container.jd.mh.RemoteWorkerProcess;
import org.apache.uima.ducc.container.jd.mh.iface.remote.IRemoteWorkerProcess;
import org.apache.uima.ducc.container.jd.wi.IProcessStatistics;
import org.apache.uima.ducc.container.jd.wi.IWorkItem;
import org.apache.uima.ducc.container.net.iface.IMetaCas;
import org.apache.uima.ducc.container.net.iface.IMetaCasTransaction;

public abstract class ActionEndAbstract extends Action implements IAction {
	
	private Logger logger = Logger.getLogger(ActionEndAbstract.class, IComponent.Id.JD.name());
	
	protected ActionEndAbstract(Logger logger) {
		this.logger = logger;
	}
	
	private DeallocateReason getDeallocateReason(ProxyJobDriverDirective pjdd) {
		// ToDo - determine reason (for now presume timeout)
		DeallocateReason deallocateReason = DeallocateReason.WorkItemTimeout;
		return deallocateReason;
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
				ActionHelper.toJdErrLog(Standardize.Label.seqNo.get()+seqNo+" ***** EXCEPTION *****\n"+printableException);
			} else {
				ActionHelper.toJdErrLog(Standardize.Label.seqNo.get()+seqNo+" ***** TIMEOUT *****\n"+userException.toString()+"\n");
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
				ActionHelper.killJob(logger, actionData, cm);
				ActionHelper.killWorkItem(logger, actionData, cm);
			}
			else if(pjdd.isKillProcess()) {
				wisk.error(seqNo);
				pStats.error(wi);
				DeallocateReason deallocateReason = getDeallocateReason(pjdd);
				ActionHelper.killProcess(logger, actionData, cm, metaCas, wi, deallocateReason);
				ActionHelper.killWorkItem(logger, actionData, cm);
			}
			else if(pjdd.isKillWorkItem()) {
				wisk.error(seqNo);
				pStats.error(wi);
				ActionHelper.killWorkItem(logger, actionData, cm);
			}
			else {
				wisk.retry(seqNo);
				pStats.retry(wi);
				ActionHelper.retryWorkItem(logger, actionData, cm, metaCas);
			}
		}
		else {
			wisk.error(seqNo);
			pStats.error(wi);
			ActionHelper.killWorkItem(logger, actionData, cm);
		}
		if(true) {
			MessageBuffer mb = LoggerHelper.getMessageBuffer(actionData);
			mb.append(Standardize.Label.exit+"");
			logger.debug(location, ILogger.null_id, mb.toString());
		}
	}
	
}
