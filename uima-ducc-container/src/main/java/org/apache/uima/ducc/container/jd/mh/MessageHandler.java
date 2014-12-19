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
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.uima.ducc.container.common.MessageBuffer;
import org.apache.uima.ducc.container.common.Standardize;
import org.apache.uima.ducc.container.common.fsm.iface.IEvent;
import org.apache.uima.ducc.container.common.fsm.iface.IFsm;
import org.apache.uima.ducc.container.common.logger.IComponent;
import org.apache.uima.ducc.container.common.logger.ILogger;
import org.apache.uima.ducc.container.common.logger.Logger;
import org.apache.uima.ducc.container.common.logger.id.Id;
import org.apache.uima.ducc.container.common.logger.id.Transform;
import org.apache.uima.ducc.container.dgen.DgenManager;
import org.apache.uima.ducc.container.jd.JobDriver;
import org.apache.uima.ducc.container.jd.JobDriverHelper;
import org.apache.uima.ducc.container.jd.cas.CasManager;
import org.apache.uima.ducc.container.jd.cas.CasManagerStats;
import org.apache.uima.ducc.container.jd.fsm.wi.ActionData;
import org.apache.uima.ducc.container.jd.fsm.wi.WiFsm;
import org.apache.uima.ducc.container.jd.mh.iface.INodeInfo;
import org.apache.uima.ducc.container.jd.mh.iface.IOperatingInfo;
import org.apache.uima.ducc.container.jd.mh.iface.IProcessInfo;
import org.apache.uima.ducc.container.jd.mh.iface.remote.IRemoteWorkerThread;
import org.apache.uima.ducc.container.jd.mh.impl.OperatingInfo;
import org.apache.uima.ducc.container.jd.wi.IRunningWorkItemStatistics;
import org.apache.uima.ducc.container.jd.wi.IWorkItem;
import org.apache.uima.ducc.container.jd.wi.IWorkItemStatistics;
import org.apache.uima.ducc.container.jd.wi.RunningWorkItemStatistics;
import org.apache.uima.ducc.container.jd.wi.WorkItem;
import org.apache.uima.ducc.container.net.iface.IMetaCas;
import org.apache.uima.ducc.container.net.iface.IMetaCasTransaction;
import org.apache.uima.ducc.container.net.iface.IMetaCasTransaction.JdState;
import org.apache.uima.ducc.container.net.iface.IMetaCasTransaction.Type;

public class MessageHandler implements IMessageHandler {

	private static ILogger logger = Logger.getLogger(MessageHandler.class, IComponent.Id.JD.name());
	
	private AtomicInteger gets = new AtomicInteger(0);
	private AtomicInteger acks = new AtomicInteger(0);
	
	public MessageHandler() {
	}
	
	public void incGets() {
		gets.incrementAndGet();
	}
	
	public void incAcks() {
		acks.incrementAndGet();
	}
	
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
			DgenManager dgenManager = jd.getDdManager();
			oi.setJobId(jd.getJobId());
			oi.setJpAe(dgenManager.getAe());
			oi.setWorkItemCrTotal(cms.getCrTotal());
			oi.setWorkItemCrFetches(cms.getCrGets());
			oi.setWorkItemJpGets(gets.get());
			oi.setWorkItemJpAcks(acks.get());
			oi.setWorkItemEndSuccesses(cms.getEndSuccess());
			oi.setWorkItemEndFailures(cms.getEndFailure());
			oi.setWorkItemEndRetrys(cms.getEndRetry());
			if(cms.isKillJob()) {
				oi.setKillJob();
			}
			oi.setWorkItemDispatcheds(cms.getDispatched());
			oi.setWorkItemPreemptions(cms.getNumberOfPreemptions());
			oi.setWorkItemFinishedMillisMin(wis.getMillisMin());
			oi.setWorkItemFinishedMillisMax(wis.getMillisMax());
			oi.setWorkItemFinishedMillisAvg(wis.getMillisAvg());
			oi.setWorkItemRunningMillisMin(rwis.getMillisMin());
			oi.setWorkItemRunningMillisMax(rwis.getMillisMax());
			oi.setWorkItemTodMostRecentStart(rwis.getTodMostRecentStart());
			oi.setActiveWorkItemInfo(jdh.getActiveWorkItemInfo());
			oi.setProcessInfo(jdh.getProcessInfo());
			oi.setJdState(jd.getJdState().name());
			MessageBuffer mb = new MessageBuffer();
			mb.append(Standardize.Label.jdState.get()+oi.getJdState());
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
			ConcurrentHashMap<IRemoteWorkerThread, IWorkItem> map = JobDriver.getInstance().getRemoteThreadMap();
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
			MessageBuffer mb = new MessageBuffer();
			mb.append(Standardize.Label.node.get()+processInfo.getNodeName());
			mb.append(Standardize.Label.ip.get()+processInfo.getNodeAddress());
			mb.append(Standardize.Label.pid.get()+processInfo.getPid());
			logger.info(location, ILogger.null_id, mb.toString());
			ConcurrentHashMap<IRemoteWorkerThread, IWorkItem> map = JobDriver.getInstance().getRemoteThreadMap();
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
			MessageBuffer mb = new MessageBuffer();
			mb.append(Standardize.Label.node.get()+processInfo.getNodeName());
			mb.append(Standardize.Label.ip.get()+processInfo.getNodeAddress());
			mb.append(Standardize.Label.pid.get()+processInfo.getPid());
			logger.info(location, ILogger.null_id, mb.toString());
			ConcurrentHashMap<IRemoteWorkerThread, IWorkItem> map = JobDriver.getInstance().getRemoteThreadMap();
			for(Entry<IRemoteWorkerThread, IWorkItem> entry : map.entrySet()) {
				IRemoteWorkerThread rwt = entry.getKey();
				if(rwt.comprises(processInfo)) {
					MessageBuffer mb1 = new MessageBuffer();
					mb1.append(Standardize.Label.remote.get()+rwt.toString());
					mb1.append(Boolean.TRUE.toString());
					logger.info(location, ILogger.null_id, mb1.toString());
					IWorkItem wi = entry.getValue();
					IFsm fsm = wi.getFsm();
					IEvent event = WiFsm.Process_Preempt;
					Object actionData = new ActionData(wi, rwt, null);
					fsm.transition(event, actionData);
				}
				else {
					MessageBuffer mb2 = new MessageBuffer();
					mb2.append(Standardize.Label.remote.get()+rwt.toString());
					mb2.append(Boolean.FALSE.toString());
					logger.info(location, ILogger.null_id, mb2.toString());
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
			RemoteWorkerThread rwt = new RemoteWorkerThread(trans);
			MessageBuffer mb = new MessageBuffer();
			mb.append(Standardize.Label.remote.get()+rwt.toString());
			mb.append(Standardize.Label.type.get()+trans.getType());
			logger.info(location, ILogger.null_id, mb.toString());
			Type type = trans.getType();
			switch(type) {
			case Get:
				handleMetaCasTransationGet(trans, rwt);
				break;
			case Ack:
				handleMetaCasTransationAck(trans, rwt);
				break;
			case End:
				handleMetaCasTransationEnd(trans, rwt);
				break;
			default:
				break;
			}
			JdState jdState = JobDriver.getInstance().getJdState();
			trans.setJdState(jdState);
		}
		catch(Exception e) {
			logger.error(location, ILogger.null_id, e);
		}
	}
	
	private IWorkItem register(IRemoteWorkerThread rwi) {
		String location = "register";
		ConcurrentHashMap<IRemoteWorkerThread, IWorkItem> map = JobDriver.getInstance().getRemoteThreadMap();
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
	
	private IWorkItem find(IRemoteWorkerThread rwi) {
		String location = "find";
		ConcurrentHashMap<IRemoteWorkerThread, IWorkItem> map = JobDriver.getInstance().getRemoteThreadMap();
		IWorkItem wi = map.get(rwi);
		if(wi != null) {
			IMetaCas metaCas = wi.getMetaCas();
			if(metaCas != null) {
				MessageBuffer mb = new MessageBuffer();
				mb.append(Standardize.Label.remote.get()+rwi.toString());
				mb.append(Standardize.Label.seqNo.get()+metaCas.getSystemKey());
				logger.debug(location, ILogger.null_id, mb.toString());
			}
			else {
				MessageBuffer mb = new MessageBuffer();
				mb.append(Standardize.Label.remote.get()+rwi.toString());
				mb.append("has no work assigned presently");
				logger.debug(location, ILogger.null_id, mb.toString());
			}
		}
		else {
			MessageBuffer mb = new MessageBuffer();
			mb.append(Standardize.Label.remote.get()+rwi.toString());
			mb.append("has no work assigned presently");
			logger.debug(location, ILogger.null_id, mb.toString());
		}
		return wi;
	}
	
	private void update(IWorkItem wi, IMetaCas metaCas) {
		IMetaCas local = wi.getMetaCas();
		IMetaCas remote = metaCas;
		if(local != null) {
			if(remote != null) {
				local.setPerformanceMetrics(remote.getPerformanceMetrics());
				local.setUserSpaceException(remote.getUserSpaceException());
			}
		}
	}
	
	private void handleMetaCasTransationGet(IMetaCasTransaction trans, IRemoteWorkerThread rwt) {
		IWorkItem wi = register(rwt);
		IFsm fsm = wi.getFsm();
		IEvent event = WiFsm.Get_Request;
		Object actionData = new ActionData(wi, rwt, trans);
		fsm.transition(event, actionData);
	}
	
	private void handleMetaCasTransationAck(IMetaCasTransaction trans, IRemoteWorkerThread rwt) {
		String location = "handleMetaCasTransationAck";
		IWorkItem wi = find(rwt);
		if(wi == null) {
			MessageBuffer mb = new MessageBuffer();
			mb.append(Standardize.Label.remote.get()+rwt.toString());
			mb.append("has no work assigned presently");
			logger.debug(location, ILogger.null_id, mb.toString());
		}
		else {
			update(wi, trans.getMetaCas());
			IFsm fsm = wi.getFsm();
			IEvent event = WiFsm.Ack_Request;
			Object actionData = new ActionData(wi, rwt, trans);
			fsm.transition(event, actionData);
		}
	}
	
	private void handleMetaCasTransationEnd(IMetaCasTransaction trans, IRemoteWorkerThread rwt) {
		String location = "handleMetaCasTransationEnd";
		IWorkItem wi = find(rwt);
		if(wi == null) {
			MessageBuffer mb = new MessageBuffer();
			mb.append(Standardize.Label.remote.get()+rwt.toString());
			mb.append("has no work assigned presently");
			logger.debug(location, ILogger.null_id, mb.toString());
		}
		else {
			update(wi, trans.getMetaCas());
			IFsm fsm = wi.getFsm();
			IEvent event = WiFsm.End_Request;
			Object actionData = new ActionData(wi, rwt, trans);
			fsm.transition(event, actionData);
			MessageBuffer mb = new MessageBuffer();
			mb.append(Standardize.Label.AckMsecs.get()+(wi.getTodAck()-wi.getTodGet()));
			mb.append(Standardize.Label.EndMsecs.get()+(wi.getTodEnd()-wi.getTodAck()));
			logger.debug(location, ILogger.null_id, mb.toString());
		}
	}

}
