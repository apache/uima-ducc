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
import org.apache.uima.ducc.container.common.fsm.iface.IAction;
import org.apache.uima.ducc.container.common.fsm.iface.IEvent;
import org.apache.uima.ducc.container.common.fsm.iface.IFsm;
import org.apache.uima.ducc.container.common.logger.IComponent;
import org.apache.uima.ducc.container.common.logger.ILogger;
import org.apache.uima.ducc.container.common.logger.Logger;
import org.apache.uima.ducc.container.jd.log.LoggerHelper;
import org.apache.uima.ducc.container.jd.mh.iface.remote.IRemoteWorkerThread;
import org.apache.uima.ducc.container.jd.wi.IWorkItem;
import org.apache.uima.ducc.container.jd.wi.WiTracker;
import org.apache.uima.ducc.ps.net.iface.IMetaTask;

public class ActionGetRedux implements IAction {

	private static Logger logger = Logger.getLogger(ActionGetRedux.class, IComponent.Id.JD.name());
	
	@Override
	public String getName() {
		return ActionGetRedux.class.getName();
	}

	@Override
	public void engage(Object objectData) {
		String location = "engage";
		logger.trace(location, ILogger.null_id, "");
		IActionData actionData = (IActionData) objectData;
		try {
			if(actionData != null) {
				IRemoteWorkerThread rwt = actionData.getRemoteWorkerThread();
				WiTracker tracker = WiTracker.getInstance();
				IWorkItem wi = tracker.find(rwt);
				if(wi != null) {
					IEvent event = WiFsm.CAS_Unavailable;
					IMetaTask metaCas = wi.getMetaCas();
					if(metaCas != null) {
						event = WiFsm.CAS_Available;
						MessageBuffer mb = LoggerHelper.getMessageBuffer(actionData);
						logger.debug(location, ILogger.null_id, mb.toString());
						actionData.getWorkItem().setMetaCas(metaCas);
						actionData.getMetaCasTransaction().setMetaTask(metaCas);
					}
					else {
						MessageBuffer mb = LoggerHelper.getMessageBuffer(actionData);
						mb.append("No CAS found for processing");
						logger.info(location, ILogger.null_id, mb.toString());
					}
					IFsm fsm = wi.getFsm();
					fsm.transition(event, actionData);
				}
				else {
					MessageBuffer mb = LoggerHelper.getMessageBuffer(actionData);
					mb.append("No work item found for processing");
					logger.warn(location, ILogger.null_id, mb.toString());
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
