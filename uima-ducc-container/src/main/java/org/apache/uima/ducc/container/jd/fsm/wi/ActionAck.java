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
import org.apache.uima.ducc.container.common.fsm.iface.IAction;
import org.apache.uima.ducc.container.common.logger.IComponent;
import org.apache.uima.ducc.container.common.logger.ILogger;
import org.apache.uima.ducc.container.common.logger.Logger;
import org.apache.uima.ducc.container.jd.JobDriver;
import org.apache.uima.ducc.container.jd.fault.injector.FaultInjector;
import org.apache.uima.ducc.container.jd.log.LoggerHelper;
import org.apache.uima.ducc.container.jd.timeout.TimeoutManager;
import org.apache.uima.ducc.container.jd.wi.IWorkItem;
import org.apache.uima.ducc.container.net.iface.IMetaCas;

public class ActionAck implements IAction {

	private static Logger logger = Logger.getLogger(ActionAck.class, IComponent.Id.JD.name());
	
	@Override
	public String getName() {
		return ActionAck.class.getName();
	}

	@Override
	public void engage(Object objectData) {
		String location = "engage";
		logger.trace(location, ILogger.null_id, "");
		IActionData actionData = (IActionData) objectData;
		try {
			IWorkItem wi = actionData.getWorkItem();
			IMetaCas metaCas = wi.getMetaCas();
			JobDriver jd = JobDriver.getInstance();
			IWorkItemStateKeeper wisk = jd.getWorkItemStateKeeper();
			MetaCasHelper metaCasHelper = new MetaCasHelper(metaCas);
			if(metaCas != null) {
				if(FaultInjector.missingAck(actionData)) {
					return;
				}
				//
				TimeoutManager toMgr = TimeoutManager.getInstance();
				toMgr.receivedAck(actionData);
				toMgr.pendingEnd(actionData);
				//
				int seqNo = metaCasHelper.getSystemKey();
				wisk.operating(seqNo);
				//
				wi.setTodAck();
				MessageBuffer mb = LoggerHelper.getMessageBuffer(actionData);
				JobDriver.getInstance().getMessageHandler().incAcks();
				logger.info(location, ILogger.null_id, mb.toString());
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
