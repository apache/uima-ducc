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
import org.apache.uima.ducc.container.common.fsm.iface.IEvent;
import org.apache.uima.ducc.container.common.fsm.iface.IFsm;
import org.apache.uima.ducc.container.common.logger.IComponent;
import org.apache.uima.ducc.container.common.logger.ILogger;
import org.apache.uima.ducc.container.common.logger.Logger;
import org.apache.uima.ducc.container.jd.JobDriver;
import org.apache.uima.ducc.container.jd.cas.CasManager;
import org.apache.uima.ducc.container.jd.mh.RemoteWorkerThread;
import org.apache.uima.ducc.container.jd.mh.iface.remote.IRemoteWorkerThread;
import org.apache.uima.ducc.container.jd.wi.IWorkItem;
import org.apache.uima.ducc.container.net.iface.IMetaCas;
import org.apache.uima.ducc.container.net.iface.IMetaCasTransaction;

public class ActionInProgress implements IAction {

	private static Logger logger = Logger.getLogger(ActionInProgress.class, IComponent.Id.JD.name());
	
	@Override
	public String getName() {
		return ActionInProgress.class.getName();
	}

	@Override
	public void engage(Object objectData) {
		String location = "engage";
		logger.debug(location, ILogger.null_id, "");
		IActionData actionData = (IActionData) objectData;
		try {
			IWorkItem wi = actionData.getWorkItem();
			IFsm fsm = wi.getFsm();
			IMetaCasTransaction trans = actionData.getMetaCasTransaction();
			IRemoteWorkerThread rwt = new RemoteWorkerThread(trans);
			//
			CasManager cm = JobDriver.getInstance().getCasManager();
			IMetaCas metaCas = cm.getMetaCas();
			trans.setMetaCas(metaCas);
			//
			IEvent event = null;
			//
			if(metaCas != null) {
				wi.setTodGet();
				event = WiFsm.CAS_Available;
				MessageBuffer mb = new MessageBuffer();
				mb.append(Standardize.Label.transNo.get()+trans.getTransactionId().toString());
				mb.append(Standardize.Label.seqNo.get()+metaCas.getSystemKey());
				mb.append(Standardize.Label.remote.get()+rwt.toString());
				logger.info(location, ILogger.null_id, mb.toString());
			}
			else {
				event = WiFsm.CAS_Unavailable;
				MessageBuffer mb = new MessageBuffer();
				mb.append("No CAS found for processing");
				logger.info(location, ILogger.null_id, mb.toString());
			}
			//
			fsm.transition(event, actionData);
		}
		catch(Exception e) {
			logger.error(location, ILogger.null_id, e);
		}
		
	}

}
