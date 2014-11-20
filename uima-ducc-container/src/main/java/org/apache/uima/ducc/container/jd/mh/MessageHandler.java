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
package org.apache.uima.ducc.container.jd.mh;

import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.uima.ducc.container.common.MessageBuffer;
import org.apache.uima.ducc.container.common.Standardize;
import org.apache.uima.ducc.container.common.fsm.iface.IEvent;
import org.apache.uima.ducc.container.common.fsm.iface.IFsm;
import org.apache.uima.ducc.container.common.logger.IComponent;
import org.apache.uima.ducc.container.common.logger.ILogger;
import org.apache.uima.ducc.container.common.logger.Logger;
import org.apache.uima.ducc.container.common.logger.id.Id;
import org.apache.uima.ducc.container.common.logger.id.Transform;
import org.apache.uima.ducc.container.jd.JobDriver;
import org.apache.uima.ducc.container.jd.JobDriverHelper;
import org.apache.uima.ducc.container.jd.cas.CasManager;
import org.apache.uima.ducc.container.jd.cas.CasManagerStats;
import org.apache.uima.ducc.container.jd.fsm.wi.ActionData;
import org.apache.uima.ducc.container.jd.fsm.wi.WiFsm;
import org.apache.uima.ducc.container.jd.mh.iface.INodeInfo;
import org.apache.uima.ducc.container.jd.mh.iface.IOperatingInfo;
import org.apache.uima.ducc.container.jd.mh.iface.IProcessInfo;
import org.apache.uima.ducc.container.jd.mh.iface.remote.IRemoteWorkerIdentity;
import org.apache.uima.ducc.container.jd.mh.impl.OperatingInfo;
import org.apache.uima.ducc.container.jd.wi.IRunningWorkItemStatistics;
import org.apache.uima.ducc.container.jd.wi.IWorkItem;
import org.apache.uima.ducc.container.jd.wi.IWorkItemStatistics;
import org.apache.uima.ducc.container.jd.wi.RunningWorkItemStatistics;
import org.apache.uima.ducc.container.jd.wi.WorkItem;
import org.apache.uima.ducc.container.net.iface.IMetaCas;
import org.apache.uima.ducc.container.net.iface.IMetaCasTransaction;
import org.apache.uima.ducc.container.net.iface.IMetaCasTransaction.Type;

public class MessageHandler implements IMessageHandler {

	private static ILogger logger = Logger.getLogger(MessageHandler.class, IComponent.Id.JD.name());
	
	@Override
	public IOperatingInfo handleGetOperatingInfo() {
		String location = "handleGetOperatingInfo";
		IOperatingInfo retVal = null;
		JobDriver jd = JobDriver.getInstance();
		Id jobid = Transform.toId(jd.getJobId());
		try {
			IOperatingInfo oi = new OperatingInfo();
			JobDriverHelper jdh = JobDriverHelper.getInstance();
			CasManager cm = jd.getCasManager();
			CasManagerStats cms = cm.getCasManagerStats();
			IWorkItemStatistics wis = jd.getWorkItemStatistics();
			IRunningWorkItemStatistics rwis = RunningWorkItemStatistics.getCurrent();
			oi.setJobId(jd.getJobId());
			oi.setWorkItemCrTotal(cms.getCrTotal());
			oi.setWorkItemCrFetches(cms.getCrGets());
			oi.setWorkItemEndSuccesses(cms.getEndSuccess());
			oi.setWorkItemEndFailures(cms.getEndFailure());
			oi.setWorkItemEndRetrys(cms.getEndRetry());
			if(cms.isKillJob()) {
				oi.setKillJob();
			}
			oi.setWorkItemPreemptions(cms.getNumberOfPreemptions());
			oi.setWorkItemFinishedMillisMin(wis.getMillisMin());
			oi.setWorkItemFinishedMillisMax(wis.getMillisMax());
			oi.setWorkItemFinishedMillisAvg(wis.getMillisAvg());
			oi.setWorkItemRunningMillisMin(rwis.getMillisMin());
			oi.setWorkItemRunningMillisMax(rwis.getMillisMax());
			oi.setWorkItemTodMostRecentStart(rwis.getTodMostRecentStart());
			oi.setActiveWorkItemInfo(jdh.getActiveWotrkItemInfo());
			MessageBuffer mb = new MessageBuffer();
			mb.append(Standardize.Label.crTotal.get()+oi.getWorkItemCrTotal());
			mb.append(Standardize.Label.crFetches.get()+oi.getWorkItemCrFetches());
			mb.append(Standardize.Label.endSuccess.get()+oi.getWorkItemEndSuccesses());
			mb.append(Standardize.Label.endFailure.get()+oi.getWorkItemEndFailures());
			mb.append(Standardize.Label.killJob.get()+oi.isKillJob());
			mb.append(Standardize.Label.preemptions.get()+oi.getWorkItemPreemptions());
			mb.append(Standardize.Label.finishedMillisMin.get()+oi.getWorkItemFinishedMillisMin());
			mb.append(Standardize.Label.finishedMillisMax.get()+oi.getWorkItemFinishedMillisMax());
			mb.append(Standardize.Label.finishedMillisAvg.get()+oi.getWorkItemFinishedMillisAvg());
			mb.append(Standardize.Label.runningMillisMin.get()+oi.getWorkItemRunningMillisMin());
			mb.append(Standardize.Label.runningMillisMax.get()+oi.getWorkItemRunningMillisMax());
			mb.append(Standardize.Label.todMostRecentStart.get()+oi.getWorkItemTodMostRecentStart());
			logger.debug(location, jobid, mb.toString());
			retVal = oi;
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
		}
		return retVal;
	}

	@Override
	public void handleDownNode(INodeInfo nodeInfo) {
		String location = "handleDownNode";
		try {
			ConcurrentHashMap<IRemoteWorkerIdentity, IWorkItem> map = JobDriver.getInstance().getMap();
			//TODO
		}
		catch(Exception e) {
			logger.error(location, ILogger.null_id, e);
		}
	}
	
	@Override
	public void handleDownProcess(IProcessInfo processInfo) {
		String location = "handleDownProcess";
		try {
			ConcurrentHashMap<IRemoteWorkerIdentity, IWorkItem> map = JobDriver.getInstance().getMap();
			//TODO
		}
		catch(Exception e) {
			logger.error(location, ILogger.null_id, e);
		}
	}
	
	@Override
	public void handlePreemptProcess(IProcessInfo processInfo) {
		String location = "handlePreemptProcess";
		try {
			ConcurrentHashMap<IRemoteWorkerIdentity, IWorkItem> map = JobDriver.getInstance().getMap();
			for(Entry<IRemoteWorkerIdentity, IWorkItem> entry : map.entrySet()) {
				IRemoteWorkerIdentity rwi = entry.getKey();
				if(rwi.comprises(processInfo)) {
					MessageBuffer mb = new MessageBuffer();
					mb.append(Standardize.Label.remote.get()+rwi.toString());
					mb.append(Boolean.TRUE.toString());
					logger.debug(location, ILogger.null_id, mb.toString());
					IWorkItem wi = entry.getValue();
					IFsm fsm = wi.getFsm();
					IEvent event = WiFsm.Process_Preempt;
					Object actionData = new ActionData(wi, rwi, null);
					fsm.transition(event, actionData);
				}
				else {
					MessageBuffer mb = new MessageBuffer();
					mb.append(Standardize.Label.remote.get()+rwi.toString());
					mb.append(Boolean.FALSE.toString());
					logger.trace(location, ILogger.null_id, mb.toString());
				}
			}
		}
		catch(Exception e) {
			logger.error(location, ILogger.null_id, e);
		}
	}
	
	@Override
	public void handleMetaCasTransation(IMetaCasTransaction trans) {
		String location = "handleMetaCasTransation";
		try {
			RemoteWorkerIdentity rwi = new RemoteWorkerIdentity(trans);
			MessageBuffer mb = new MessageBuffer();
			mb.append(Standardize.Label.remote.get()+rwi.toString());
			mb.append(Standardize.Label.type.get()+trans.getType());
			logger.info(location, ILogger.null_id, mb.toString());
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
			logger.error(location, ILogger.null_id, e);
		}
	}
	
	private IWorkItem register(IRemoteWorkerIdentity rwi) {
		String location = "register";
		ConcurrentHashMap<IRemoteWorkerIdentity, IWorkItem> map = JobDriver.getInstance().getMap();
		IWorkItem wi = map.get(rwi);
		while(wi == null) {
			IMetaCas metaCas = null;
			IFsm fsm = new WiFsm();
			map.putIfAbsent(rwi, new WorkItem(metaCas, fsm));
			wi = map.get(rwi);
			MessageBuffer mb = new MessageBuffer();
			mb.append(Standardize.Label.remote.get()+rwi.toString());
			logger.debug(location, ILogger.null_id, mb.toString());
		}
		return wi;
	}
	
	private IWorkItem find(IRemoteWorkerIdentity rwi) {
		String location = "find";
		ConcurrentHashMap<IRemoteWorkerIdentity, IWorkItem> map = JobDriver.getInstance().getMap();
		IWorkItem wi = map.get(rwi);
		if(wi != null) {
			MessageBuffer mb = new MessageBuffer();
			mb.append(Standardize.Label.remote.get()+rwi.toString());
			mb.append(Standardize.Label.seqNo.get()+wi.getMetaCas().getSystemKey());
			logger.debug(location, ILogger.null_id, mb.toString());
		}
		else {
			MessageBuffer mb = new MessageBuffer();
			mb.append(Standardize.Label.remote.get()+rwi.toString());
			mb.append("has no work assigned presently");
			logger.debug(location, ILogger.null_id, mb.toString());
		}
		return wi;
	}
	
	private void handleMetaCasTransationGet(IMetaCasTransaction trans, IRemoteWorkerIdentity rwi) {
		IWorkItem wi = register(rwi);
		IFsm fsm = wi.getFsm();
		IEvent event = WiFsm.Get_Request;
		Object actionData = new ActionData(wi, rwi, trans);
		fsm.transition(event, actionData);
	}
	
	private void handleMetaCasTransationAck(IMetaCasTransaction trans, IRemoteWorkerIdentity rwi) {
		String location = "handleMetaCasTransationAck";
		IWorkItem wi = find(rwi);
		if(wi == null) {
			MessageBuffer mb = new MessageBuffer();
			mb.append(Standardize.Label.remote.get()+rwi.toString());
			mb.append("has no work assigned presently");
			logger.debug(location, ILogger.null_id, mb.toString());
		}
		else {
			trans.setMetaCas(wi.getMetaCas());
			IFsm fsm = wi.getFsm();
			IEvent event = WiFsm.Ack_Request;
			Object actionData = new ActionData(wi, rwi, trans);
			fsm.transition(event, actionData);
		}
	}
	
	private void handleMetaCasTransationEnd(IMetaCasTransaction trans, IRemoteWorkerIdentity rwi) {
		String location = "handleMetaCasTransationEnd";
		IWorkItem wi = find(rwi);
		if(wi == null) {
			MessageBuffer mb = new MessageBuffer();
			mb.append(Standardize.Label.remote.get()+rwi.toString());
			mb.append("has no work assigned presently");
			logger.debug(location, ILogger.null_id, mb.toString());
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
			logger.debug(location, ILogger.null_id, mb.toString());
		}
	}

}
