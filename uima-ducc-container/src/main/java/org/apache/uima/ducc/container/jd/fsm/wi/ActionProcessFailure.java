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
import org.apache.uima.ducc.container.jd.JobDriverHelper;
import org.apache.uima.ducc.container.jd.cas.CasManager;
import org.apache.uima.ducc.container.jd.cas.CasManagerStats.RetryReason;
import org.apache.uima.ducc.container.jd.classload.ProxyJobDriverDirective;
import org.apache.uima.ducc.container.jd.classload.ProxyJobDriverErrorHandler;
import org.apache.uima.ducc.container.jd.log.LoggerHelper;
import org.apache.uima.ducc.container.jd.mh.iface.remote.IRemoteWorkerProcess;
import org.apache.uima.ducc.container.jd.timeout.TimeoutManager;
import org.apache.uima.ducc.container.jd.wi.IProcessStatistics;
import org.apache.uima.ducc.container.jd.wi.IWorkItem;
import org.apache.uima.ducc.container.net.iface.IMetaCas;

public class ActionProcessFailure extends Action implements IAction {

	private static Logger logger = Logger.getLogger(ActionProcessFailure.class, IComponent.Id.JD.name());
	
	public ActionProcessFailure() {
		super();
	}
	
	@Override
	public String getName() {
		return ActionProcessFailure.class.getName();
	}
	
	private void retryWorkItem(IActionData actionData, CasManager cm, IWorkItem wi, IMetaCas metaCas, IRemoteWorkerProcess rwp) {
		String location = "retryWorkItem";
		MessageBuffer mb = LoggerHelper.getMessageBuffer(actionData);
		logger.info(location, ILogger.null_id, mb.toString());
		//
		TimeoutManager.getInstance().cancelTimer(actionData);
		cm.putMetaCas(metaCas, RetryReason.ProcessDown);
		cm.getCasManagerStats().incEndRetry();
		JobDriver jd = JobDriver.getInstance();
		JobDriverHelper jdh = JobDriverHelper.getInstance();
		IWorkItemStateKeeper wisk = jd.getWorkItemStateKeeper();
		MetaCasHelper metaCasHelper = new MetaCasHelper(metaCas);
		IProcessStatistics pStats = jdh.getProcessStatistics(rwp);
		int seqNo = metaCasHelper.getSystemKey();
		wisk.retry(seqNo);
		pStats.retry(wi);
	}
	
	private void killWorkItem(CasManager cm, IWorkItem wi, IMetaCas metaCas, IRemoteWorkerProcess rwp) {
		//TODO
	}
	
	private void killJob(CasManager cm, IWorkItem wi, IMetaCas metaCas, IRemoteWorkerProcess rwp) {
		//TODO
	}
	
	@Override
	public void engage(Object objectData) {
		String location = "engage";
		logger.trace(location, ILogger.null_id, "");
		IActionData actionData = (IActionData) objectData;
		try {
			if(actionData != null) {
				IWorkItem wi = actionData.getWorkItem();
				IMetaCas metaCas = wi.getMetaCas();
				JobDriver jd = JobDriver.getInstance();
				CasManager cm = jd.getCasManager();
				JobDriverHelper jdh = JobDriverHelper.getInstance();
				IRemoteWorkerProcess rwp = jdh.getRemoteWorkerProcess(wi);
				if(rwp != null) {
					IProcessStatistics pStats = jdh.getProcessStatistics(rwp);
					if(metaCas != null) {
						String serializedCas = (String) metaCas.getUserSpaceCas();
						ProxyJobDriverErrorHandler pjdeh = jd.getProxyJobDriverErrorHandler();
						ProxyJobDriverDirective pjdd = pjdeh.handle(serializedCas);
						if(pjdd != null) {
							MessageBuffer mb = LoggerHelper.getMessageBuffer(actionData);
							mb.append(Standardize.Label.isKillJob.get()+pjdd.isKillJob());
							mb.append(Standardize.Label.isKillProcess.get()+pjdd.isKillProcess());
							mb.append(Standardize.Label.isKillWorkItem.get()+pjdd.isKillWorkItem());
							logger.info(location, ILogger.null_id, mb.toString());
							if(pjdd.isKillJob()) {
								killJob(cm, wi, metaCas, rwp);
							}
							else if(pjdd.isKillWorkItem()) {
								killWorkItem(cm, wi, metaCas, rwp);
							}
							else {
								retryWorkItem(actionData, cm, wi, metaCas, rwp);
							}
						}
						else {
							retryWorkItem(actionData, cm, wi, metaCas, rwp);
						}
						displayProcessStatistics(logger, actionData, wi, pStats);
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
					mb.append("No remote worker process entry found for processing");
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
