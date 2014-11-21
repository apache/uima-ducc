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

import org.apache.uima.ducc.container.common.MessageBuffer;
import org.apache.uima.ducc.container.common.Standardize;
import org.apache.uima.ducc.container.common.fsm.iface.IAction;
import org.apache.uima.ducc.container.common.logger.IComponent;
import org.apache.uima.ducc.container.common.logger.ILogger;
import org.apache.uima.ducc.container.common.logger.Logger;
import org.apache.uima.ducc.container.jd.JobDriver;
import org.apache.uima.ducc.container.jd.cas.CasManager;
import org.apache.uima.ducc.container.jd.cas.CasManagerStats.RetryReason;
import org.apache.uima.ducc.container.jd.classload.ProxyJobDriverDirective;
import org.apache.uima.ducc.container.jd.classload.ProxyJobDriverErrorHandler;
import org.apache.uima.ducc.container.jd.mh.RemoteWorkerIdentity;
import org.apache.uima.ducc.container.jd.mh.iface.remote.IRemoteWorkerIdentity;
import org.apache.uima.ducc.container.jd.wi.IWorkItem;
import org.apache.uima.ducc.container.jd.wi.IWorkItemStatistics;
import org.apache.uima.ducc.container.net.iface.IMetaCas;
import org.apache.uima.ducc.container.net.iface.IMetaCasTransaction;
import org.apache.uima.ducc.container.net.iface.IMetaCasTransaction.DriverState;

public class ActionEnd implements IAction {

	private static Logger logger = Logger.getLogger(ActionEnd.class, IComponent.Id.JD.name());
	
	@Override
	public String getName() {
		return ActionEnd.class.getName();
	}
	
	private void killJob(CasManager cm, IWorkItem wi, IMetaCasTransaction trans, IMetaCas metaCas, IRemoteWorkerIdentity rwi) {
		String location = "killJob";
		cm.getCasManagerStats().setKillJob();
		MessageBuffer mb = new MessageBuffer();
		mb.append(Standardize.Label.transNo.get()+trans.getTransactionId().toString());
		mb.append(Standardize.Label.seqNo.get()+metaCas.getSystemKey());
		mb.append(Standardize.Label.remote.get()+rwi.toString());
		logger.info(location, ILogger.null_id, mb.toString());
	}
	
	private void retryWorkItem(CasManager cm, IWorkItem wi, IMetaCasTransaction trans, IMetaCas metaCas, IRemoteWorkerIdentity rwi) {
		String location = "retryWorkItem";
		cm.putMetaCas(metaCas, RetryReason.UserErrorRetry);
		cm.getCasManagerStats().incEndRetry();
		MessageBuffer mb = new MessageBuffer();
		mb.append(Standardize.Label.transNo.get()+trans.getTransactionId().toString());
		mb.append(Standardize.Label.seqNo.get()+metaCas.getSystemKey());
		mb.append(Standardize.Label.remote.get()+rwi.toString());
		logger.info(location, ILogger.null_id, mb.toString());
	}
	
	private void killWorkItem(CasManager cm, IWorkItem wi, IMetaCasTransaction trans, IMetaCas metaCas, IRemoteWorkerIdentity rwi) {
		String location = "killWorkItem";
		cm.getCasManagerStats().incEndFailure();
		MessageBuffer mb = new MessageBuffer();
		mb.append(Standardize.Label.transNo.get()+trans.getTransactionId().toString());
		mb.append(Standardize.Label.seqNo.get()+metaCas.getSystemKey());
		mb.append(Standardize.Label.remote.get()+rwi.toString());
		logger.info(location, ILogger.null_id, mb.toString());
		checkEnded(cm);
	}
	
	private void successWorkItem(CasManager cm, IWorkItem wi, IMetaCasTransaction trans, IMetaCas metaCas, IRemoteWorkerIdentity rwi) {
		String location = "successWorkItem";
		cm.getCasManagerStats().incEndSuccess();
		wi.setTodEnd();
		updateStatistics(wi);
		MessageBuffer mb = new MessageBuffer();
		mb.append(Standardize.Label.transNo.get()+trans.getTransactionId().toString());
		mb.append(Standardize.Label.seqNo.get()+metaCas.getSystemKey());
		mb.append(Standardize.Label.remote.get()+rwi.toString());
		logger.info(location, ILogger.null_id, mb.toString());
		checkEnded(cm);
	}
	
	private void checkEnded(CasManager cm) {
		String location = "checkEnded";
		if(cm.getCasManagerStats().getUnfinishedWorkCount() <= 0) {
			JobDriver.getInstance().advanceDriverState(DriverState.Ended);
			MessageBuffer mb = new MessageBuffer();
			mb.append(Standardize.Label.driverState.get()+JobDriver.getInstance().getDriverState());
			logger.info(location, ILogger.null_id, mb.toString());
		}
	}
	
	@Override
	public void engage(Object objectData) {
		String location = "engage";
		logger.debug(location, ILogger.null_id, "");
		IActionData actionData = (IActionData) objectData;
		try {
			IWorkItem wi = actionData.getWorkItem();
			IMetaCasTransaction trans = actionData.getMetaCasTransaction();
			IRemoteWorkerIdentity rwi = new RemoteWorkerIdentity(trans);
			IMetaCas metaCas = wi.getMetaCas();
			JobDriver jd = JobDriver.getInstance();
			CasManager cm = jd.getCasManager();
			//
			if(metaCas != null) {
				Object exception = metaCas.getUserSpaceException();
				if(exception != null) {
					Object cas = metaCas.getUserSpaceCas();
					ProxyJobDriverErrorHandler pjdeh = jd.getProxyJobDriverErrorHandler();
					ProxyJobDriverDirective pjdd = pjdeh.handle(cas, exception);
					if(pjdd != null) {
						if(pjdd.isKillJob()) {
							killJob(cm, wi, trans, metaCas, rwi);
						}
						if(pjdd.isKillWorkItem()) {
							killWorkItem(cm, wi, trans, metaCas, rwi);
						}
						else {
							retryWorkItem(cm, wi, trans, metaCas, rwi);
						}
					}
					else {
						killWorkItem(cm, wi, trans, metaCas, rwi);
					}
				}
				else {
					successWorkItem(cm, wi, trans, metaCas, rwi);
				}
				wi.resetTods();
			}
			else {MessageBuffer mb = new MessageBuffer();
				mb.append("No CAS found for processing");
				logger.info(location, ILogger.null_id, mb.toString());
			}
		}
		catch(Exception e) {
			logger.error(location, ILogger.null_id, e);
		}
	}
	
	private void updateStatistics(IWorkItem wi) {
		IWorkItemStatistics wis = JobDriver.getInstance().getWorkItemStatistics();
		wis.ended(wi);
	}
}
