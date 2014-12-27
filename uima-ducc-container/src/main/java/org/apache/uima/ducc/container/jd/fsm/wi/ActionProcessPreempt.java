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
import org.apache.uima.ducc.container.jd.mh.iface.remote.IRemoteWorkerProcess;
import org.apache.uima.ducc.container.jd.wi.IProcessStatistics;
import org.apache.uima.ducc.container.jd.wi.IWorkItem;
import org.apache.uima.ducc.container.net.iface.IMetaCas;

public class ActionProcessPreempt extends Action implements IAction {

	private static Logger logger = Logger.getLogger(ActionProcessPreempt.class, IComponent.Id.JD.name());
	
	@Override
	public String getName() {
		return ActionProcessPreempt.class.getName();
	}
	
	private void preemptWorkItem(CasManager cm, IWorkItem wi, IMetaCas metaCas, IRemoteWorkerProcess rwp) {
		String location = "preemptWorkItem";
		cm.putMetaCas(metaCas, RetryReason.ProcessPreempt);
		cm.getCasManagerStats().incEndRetry();
		MessageBuffer mb = new MessageBuffer();
		mb.append(Standardize.Label.seqNo.get()+metaCas.getSystemKey());
		mb.append(Standardize.Label.remote.get()+rwp.toString());
		logger.info(location, ILogger.null_id, mb.toString());
	}
	
	@Override
	public void engage(Object objectData) {
		String location = "engage";
		logger.debug(location, ILogger.null_id, "");
		IActionData actionData = (IActionData) objectData;
		try {
			IWorkItem wi = actionData.getWorkItem();
			IMetaCas metaCas = wi.getMetaCas();
			JobDriver jd = JobDriver.getInstance();
			CasManager cm = jd.getCasManager();
			JobDriverHelper jdh = JobDriverHelper.getInstance();
			IRemoteWorkerProcess rwp = jdh.getRemoteWorkerProcess(wi);
			if(rwp != null) {
				if(metaCas != null) {
					preemptWorkItem(cm, wi, metaCas, rwp);
					IWorkItemStateKeeper wisk = jd.getWorkItemStateKeeper();
					MetaCasHelper metaCasHelper = new MetaCasHelper(metaCas);
					IProcessStatistics pStats = jdh.getProcessStatistics(rwp);
					int seqNo = metaCasHelper.getSystemKey();
					wisk.preempt(seqNo);
					pStats.preempt(wi);
					displayProcessStatistics(logger, wi, pStats);
					wi.reset();
				}
				else {
					MessageBuffer mb = new MessageBuffer();
					mb.append("No CAS found for processing");
					logger.info(location, ILogger.null_id, mb.toString());
				}
			}
			else {
				MessageBuffer mb = new MessageBuffer();
				mb.append("No remote worker process entry found for processing");
				logger.info(location, ILogger.null_id, mb.toString());
			}
		}
		catch(Exception e) {
			logger.error(location, ILogger.null_id, e);
		}
	}
}
