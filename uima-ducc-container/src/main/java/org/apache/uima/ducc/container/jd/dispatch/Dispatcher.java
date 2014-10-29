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
package org.apache.uima.ducc.container.jd.dispatch;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.uima.ducc.container.common.ContainerLogger;
import org.apache.uima.ducc.container.common.IEntityId;
import org.apache.uima.ducc.container.common.IContainerLogger;
import org.apache.uima.ducc.container.common.MessageBuffer;
import org.apache.uima.ducc.container.common.Standardize;
import org.apache.uima.ducc.container.common.fsm.iface.IEvent;
import org.apache.uima.ducc.container.common.fsm.iface.IFsm;
import org.apache.uima.ducc.container.jd.JobDriverCommon;
import org.apache.uima.ducc.container.jd.fsm.wi.ActionData;
import org.apache.uima.ducc.container.jd.fsm.wi.WiFsm;
import org.apache.uima.ducc.container.net.iface.IMetaCas;
import org.apache.uima.ducc.container.net.iface.IMetaCasTransaction;
import org.apache.uima.ducc.container.net.iface.IMetaCasTransaction.Type;

public class Dispatcher {

	private static IContainerLogger logger = ContainerLogger.getLogger(Dispatcher.class, IContainerLogger.Component.JD.name());
	
	public Dispatcher() {
	}
	
	public void downNode(NodeInfo nodeInfo) {
		
	}
	
	public void downProcess(ProcessInfo processInfo) {
		
	}
	
	public void preemptProcess(ProcessInfo processInfo) {
		
	}
	
	public void handleMetaCasTransation(IMetaCasTransaction trans) {
		String location = "handleMetaCasTransation";
		try {
			RemoteWorkerIdentity rwi = new RemoteWorkerIdentity(trans);
			MessageBuffer mb = new MessageBuffer();
			mb.append(Standardize.Label.remote.get()+rwi.toString());
			mb.append(Standardize.Label.type.get()+trans.getType());
			logger.info(location, IEntityId.null_id, mb.toString());
			Type type = trans.getType();
			switch(type) {
			case Get:
				handleMetaCasTransationGet(trans, rwi);
				break;
			case Ack:
				handleMetaCasTransationAck(trans, rwi);
				break;
			case End:
				handleMetaCasTransationEnd(trans, rwi);
				break;
			default:
				break;
			}
		}
		catch(Exception e) {
			logger.error(location, IEntityId.null_id, e);
		}
	}
	
	private IWorkItem register(IRemoteWorkerIdentity rwi) {
		String location = "register";
		ConcurrentHashMap<IRemoteWorkerIdentity, IWorkItem> map = JobDriverCommon.getInstance().getMap();
		IWorkItem wi = map.get(rwi);
		while(wi == null) {
			IMetaCas metaCas = null;
			IFsm fsm = new WiFsm();
			map.putIfAbsent(rwi, new WorkItem(metaCas, fsm));
			wi = map.get(rwi);
			MessageBuffer mb = new MessageBuffer();
			mb.append(Standardize.Label.remote.get()+rwi.toString());
			logger.debug(location, IEntityId.null_id, mb.toString());
		}
		return wi;
	}
	
	private IWorkItem find(IRemoteWorkerIdentity rwi) {
		String location = "find";
		ConcurrentHashMap<IRemoteWorkerIdentity, IWorkItem> map = JobDriverCommon.getInstance().getMap();
		IWorkItem wi = map.get(rwi);
		if(wi != null) {
			MessageBuffer mb = new MessageBuffer();
			mb.append(Standardize.Label.remote.get()+rwi.toString());
			mb.append(Standardize.Label.seqNo.get()+wi.getMetaCas().getSystemKey());
			logger.debug(location, IEntityId.null_id, mb.toString());
		}
		else {
			MessageBuffer mb = new MessageBuffer();
			mb.append(Standardize.Label.remote.get()+rwi.toString());
			mb.append("has no work assigned presently");
			logger.debug(location, IEntityId.null_id, mb.toString());
		}
		return wi;
	}
	
	public void handleMetaCasTransationGet(IMetaCasTransaction trans, IRemoteWorkerIdentity rwi) {
		IWorkItem wi = register(rwi);
		IFsm fsm = wi.getFsm();
		IEvent event = WiFsm.Get_Request;
		Object actionData = new ActionData(wi, rwi, trans);
		fsm.transition(event, actionData);
		IMetaCas metaCas = trans.getMetaCas();
		if(metaCas != null) {
			wi.setMetaCas(metaCas);
			event = WiFsm.CAS_Available;
		}
		else {
			event = WiFsm.CAS_Unavailable;
		}
		fsm.transition(event, actionData);
	}
	
	public void handleMetaCasTransationAck(IMetaCasTransaction trans, IRemoteWorkerIdentity rwi) {
		String location = "handleMetaCasTransationAck";
		IWorkItem wi = find(rwi);
		if(wi == null) {
			MessageBuffer mb = new MessageBuffer();
			mb.append(Standardize.Label.remote.get()+rwi.toString());
			mb.append("has no work assigned presently");
			logger.debug(location, IEntityId.null_id, mb.toString());
		}
		else {
			trans.setMetaCas(wi.getMetaCas());
			IFsm fsm = wi.getFsm();
			IEvent event = WiFsm.Ack_Request;
			Object actionData = new ActionData(wi, rwi, trans);
			fsm.transition(event, actionData);
		}
	}
	
	public void handleMetaCasTransationEnd(IMetaCasTransaction trans, IRemoteWorkerIdentity rwi) {
		String location = "handleMetaCasTransationEnd";
		IWorkItem wi = find(rwi);
		if(wi == null) {
			MessageBuffer mb = new MessageBuffer();
			mb.append(Standardize.Label.remote.get()+rwi.toString());
			mb.append("has no work assigned presently");
			logger.debug(location, IEntityId.null_id, mb.toString());
		}
		else {
			trans.setMetaCas(wi.getMetaCas());
			IFsm fsm = wi.getFsm();
			IEvent event = WiFsm.End_Request;
			Object actionData = new ActionData(wi, rwi, trans);
			fsm.transition(event, actionData);
			MessageBuffer mb = new MessageBuffer();
			mb.append(Standardize.Label.AckMsecs.get()+(wi.getTodAck()-wi.getTodGet()));
			mb.append(Standardize.Label.EndMsecs.get()+(wi.getTodEnd()-wi.getTodAck()));
			logger.debug(location, IEntityId.null_id, mb.toString());
		}
	}
}
