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

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
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
import org.apache.uima.ducc.container.jd.blacklist.JobProcessBlacklist;
import org.apache.uima.ducc.container.jd.cas.CasManager;
import org.apache.uima.ducc.container.jd.cas.CasManagerStats;
import org.apache.uima.ducc.container.jd.fsm.wi.ActionData;
import org.apache.uima.ducc.container.jd.fsm.wi.WiFsm;
import org.apache.uima.ducc.container.jd.mh.iface.INodeInfo;
import org.apache.uima.ducc.container.jd.mh.iface.IOperatingInfo;
import org.apache.uima.ducc.container.jd.mh.iface.IOperatingInfo.CompletionType;
import org.apache.uima.ducc.container.jd.mh.iface.IProcessInfo;
import org.apache.uima.ducc.container.jd.mh.iface.remote.IRemoteWorkerProcess;
import org.apache.uima.ducc.container.jd.mh.iface.remote.IRemoteWorkerThread;
import org.apache.uima.ducc.container.jd.mh.impl.OperatingInfo;
import org.apache.uima.ducc.container.jd.wi.IRunningWorkItemStatistics;
import org.apache.uima.ducc.container.jd.wi.IWorkItem;
import org.apache.uima.ducc.container.jd.wi.IWorkItemStatistics;
import org.apache.uima.ducc.container.jd.wi.RunningWorkItemStatistics;
import org.apache.uima.ducc.container.jd.wi.WiTracker;
import org.apache.uima.ducc.container.jd.wi.perf.IWorkItemPerformanceSummaryKeeper;
import org.apache.uima.ducc.container.net.iface.IMetaCas;
import org.apache.uima.ducc.container.net.iface.IMetaCasTransaction;
import org.apache.uima.ducc.container.net.iface.IMetaCasTransaction.Hint;
import org.apache.uima.ducc.container.net.iface.IMetaCasTransaction.JdState;
import org.apache.uima.ducc.container.net.iface.IMetaCasTransaction.Type;
import org.apache.uima.ducc.container.net.impl.MetaCas;
import org.apache.uima.ducc.container.net.impl.TransactionHelper;

public class MessageHandler implements IMessageHandler {

	private static ILogger logger = Logger.getLogger(MessageHandler.class, IComponent.Id.JD.name());
	
	private static final boolean reportTimes = logger.isDebug();  // Must be set for whole of job
	
	private AtomicInteger gets = new AtomicInteger(0);
	private AtomicInteger acks = new AtomicInteger(0);
	private AtomicInteger investmentResets = new AtomicInteger(0);
	
	private ConcurrentHashMap<String,String> failedInitializationMap = new ConcurrentHashMap<String,String>();
	
	private ConcurrentHashMap<IRemoteWorkerThread,IRemoteWorkerThread> wipMap = new ConcurrentHashMap<IRemoteWorkerThread,IRemoteWorkerThread>();
	
	private JobProcessBlacklist jobProcessBlacklist = JobProcessBlacklist.getInstance();
	
	public MessageHandler() {
	}
	
	@Override
	public void incGets() {
		gets.incrementAndGet();
	}
	
	@Override
	public void incAcks() {
		acks.incrementAndGet();
	}

	@Override
	public void incInvestmentResets() {
		investmentResets.incrementAndGet();
	}
	
	//
	
	private static AtomicBoolean piggybacking = new AtomicBoolean(true);
	
	public static void piggybackingDisable() {
		piggybacking.set(false);
	}
	
	public static void piggybackingEnable() {
		piggybacking.set(false);
	}
	
	//
	
	private void piggyback() {
		String location = "piggyback";
		if(piggybacking.get()) {
			try {
				JobDriver jd = JobDriver.getInstance();
				IWorkItemPerformanceSummaryKeeper wipsk = jd.getWorkItemPerformanceSummaryKeeper();
				wipsk.publish();
			}
			catch(Exception e) {
				logger.error(location, ILogger.null_id, e);
			}
		}
	}
	
	@Override
	public IOperatingInfo handleGetOperatingInfo() {
		String location = "handleGetOperatingInfo";
		piggyback();
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
			DgenManager dgenManager = DgenManager.getInstance();
			oi.setJobId(jd.getJobId());
			oi.setJpDeployable(dgenManager.getDeployable());
			oi.setWorkItemCrTotal(cms.getCrTotal());
			oi.setWorkItemCrFetches(cms.getCrGets());
			oi.setWorkItemJpGets(gets.get());
			oi.setWorkItemJpAcks(acks.get());
			oi.setWorkItemJpInvestmentResets(investmentResets.get());
			oi.setWorkItemEndSuccesses(cms.getEndSuccess());
			oi.setWorkItemEndFailures(cms.getEndFailure());
			oi.setWorkItemEndRetrys(cms.getEndRetry());
			if(cms.isKillJob()) {
				oi.setKillJob();
			}
			if(jd.isKillJob()) {
				oi.setKillJob();
				oi.setCompletionType(jd.getCompletionType());
				oi.setCompletionText(jd.getCompletionText());
			}
			oi.setWorkItemDispatcheds(cms.getDispatched());
			oi.setWorkItemRetrys(cms.getNumberOfRetrys());
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
			oi.setProcessKillMap(jd.getkillProcessMap());
			MessageBuffer mb = new MessageBuffer();
			mb.append(Standardize.Label.jdState.get()+oi.getJdState());
			mb.append(Standardize.Label.crTotal.get()+oi.getWorkItemCrTotal());
			mb.append(Standardize.Label.crFetches.get()+oi.getWorkItemCrFetches());
			mb.append(Standardize.Label.endSuccess.get()+oi.getWorkItemEndSuccesses());
			mb.append(Standardize.Label.endFailure.get()+oi.getWorkItemEndFailures());
			mb.append(Standardize.Label.killJob.get()+oi.isKillJob());
			mb.append(Standardize.Label.dispatched.get()+oi.getWorkItemDispatcheds());
			mb.append(Standardize.Label.retrys.get()+oi.getWorkItemRetrys());
			mb.append(Standardize.Label.preemptions.get()+oi.getWorkItemPreemptions());
			mb.append(Standardize.Label.finishedMillisMin.get()+oi.getWorkItemFinishedMillisMin());
			mb.append(Standardize.Label.finishedMillisMax.get()+oi.getWorkItemFinishedMillisMax());
			mb.append(Standardize.Label.finishedMillisAvg.get()+oi.getWorkItemFinishedMillisAvg());
			mb.append(Standardize.Label.runningMillisMin.get()+oi.getWorkItemRunningMillisMin());
			mb.append(Standardize.Label.runningMillisMax.get()+oi.getWorkItemRunningMillisMax());
			mb.append(Standardize.Label.todMostRecentStart.get()+oi.getWorkItemTodMostRecentStart());
			logger.debug(location, jobid, mb.toString());
			retVal = oi;
			totalWorkItems  = cms.getCrTotal();  // Used by accumlateTimes
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
		}
		return retVal;
	}

	@Override
	public void handleNodeDown(INodeInfo nodeInfo) {
		//TODO
		/*
		String location = "handleDownNode";
		try {
			ConcurrentHashMap<IRemoteWorkerThread, IWorkItem> map = JobDriver.getInstance().getRemoteThreadMap();
		}
		catch(Exception e) {
			logger.error(location, ILogger.null_id, e);
		}
		*/
	}
	
	private void processBlacklist(IProcessInfo processInfo, IRemoteWorkerProcess rwp) {
		String location = "processBlacklist";
		JobDriver jd = JobDriver.getInstance();
		if(!jd.isFinito()) {
			if(jobProcessBlacklist.includes(rwp)) {
				MessageBuffer mb1 = new MessageBuffer();
				mb1.append(Standardize.Label.remote.get()+rwp.toString());
				mb1.append(Standardize.Label.status.get()+"already kaput");
				logger.trace(location, ILogger.null_id, mb1.toString());
			}
			else {
				jobProcessBlacklist.add(rwp);
				MessageBuffer mb1 = new MessageBuffer();
				mb1.append(Standardize.Label.remote.get()+rwp.toString());
				mb1.append(Standardize.Label.status.get()+"transition to down");
				String reasonDeallocated = processInfo.getReasonDeallocated();
				if(reasonDeallocated != null) {
					mb1.append(Standardize.Label.deallocate.get()+reasonDeallocated);
				}
				logger.warn(location, ILogger.null_id, mb1.toString());
			}
		}
	}
	
	@Override
	public void handleProcessDown(IProcessInfo processInfo) {
		String location = "handleProcessDown";
		try {
			MessageBuffer mb = new MessageBuffer();
			mb.append(Standardize.Label.node.get()+processInfo.getNodeName());
			mb.append(Standardize.Label.ip.get()+processInfo.getNodeAddress());
			mb.append(Standardize.Label.pid.get()+processInfo.getPid());
			logger.trace(location, ILogger.null_id, mb.toString());
			ConcurrentHashMap<IRemoteWorkerThread, IWorkItem> map = WiTracker.getInstance().find(processInfo);
			for(Entry<IRemoteWorkerThread, IWorkItem> entry : map.entrySet()) {
				IRemoteWorkerThread rwt = entry.getKey();
				if(rwt.comprises(processInfo)) {
					RemoteWorkerProcess rwp = new RemoteWorkerProcess(rwt);
					processBlacklist(processInfo, rwp);
					IWorkItem wi = entry.getValue();
					IFsm fsm = wi.getFsm();
					IEvent event = WiFsm.Process_Failure;
					Object actionData = new ActionData(wi, rwt, null);
					fsm.transition(event, actionData);
				}
				else {
					MessageBuffer mb1 = new MessageBuffer();
					mb1.append(Standardize.Label.remote.get()+rwt.toString());
					mb1.append(Standardize.Label.status.get()+"unaffected");
					logger.trace(location, ILogger.null_id, mb1.toString());
				}
			}
		}
		catch(Exception e) {
			logger.error(location, ILogger.null_id, e);
		}
	}
	
	@Override
	public void handleProcessPreempt(IProcessInfo processInfo) {
		String location = "handleProcessPreempt";
		try {
			MessageBuffer mb = new MessageBuffer();
			mb.append(Standardize.Label.node.get()+processInfo.getNodeName());
			mb.append(Standardize.Label.ip.get()+processInfo.getNodeAddress());
			mb.append(Standardize.Label.pid.get()+processInfo.getPid());
			logger.trace(location, ILogger.null_id, mb.toString());
			ConcurrentHashMap<IRemoteWorkerThread, IWorkItem> map = WiTracker.getInstance().find(processInfo);
			for(Entry<IRemoteWorkerThread, IWorkItem> entry : map.entrySet()) {
				IRemoteWorkerThread rwt = entry.getKey();
				if(rwt.comprises(processInfo)) {
					RemoteWorkerProcess rwp = new RemoteWorkerProcess(rwt);
					processBlacklist(processInfo, rwp);
					IWorkItem wi = entry.getValue();
					IFsm fsm = wi.getFsm();
					IEvent event = WiFsm.Process_Preempt;
					Object actionData = new ActionData(wi, rwt, null);
					fsm.transition(event, actionData);
				}
				else {
					MessageBuffer mb1 = new MessageBuffer();
					mb1.append(Standardize.Label.remote.get()+rwt.toString());
					mb1.append(Standardize.Label.status.get()+"unaffected");
					logger.trace(location, ILogger.null_id, mb1.toString());
				}
			}
		}
		catch(Exception e) {
			logger.error(location, ILogger.null_id, e);
		}
	}
	
	@Override
	public void handleProcessVolunteered(IProcessInfo processInfo) {
		String location = "handleProcessVolunteered";
		try {
			MessageBuffer mb = new MessageBuffer();
			mb.append(Standardize.Label.node.get()+processInfo.getNodeName());
			mb.append(Standardize.Label.ip.get()+processInfo.getNodeAddress());
			mb.append(Standardize.Label.pid.get()+processInfo.getPid());
			logger.trace(location, ILogger.null_id, mb.toString());
			ConcurrentHashMap<IRemoteWorkerThread, IWorkItem> map = WiTracker.getInstance().find(processInfo);
			for(Entry<IRemoteWorkerThread, IWorkItem> entry : map.entrySet()) {
				IRemoteWorkerThread rwt = entry.getKey();
				if(rwt.comprises(processInfo)) {
					RemoteWorkerProcess rwp = new RemoteWorkerProcess(rwt);
					processBlacklist(processInfo, rwp);
					IWorkItem wi = entry.getValue();
					IFsm fsm = wi.getFsm();
					IEvent event = WiFsm.Process_Volunteered;
					Object actionData = new ActionData(wi, rwt, null);
					fsm.transition(event, actionData);
				}
				else {
					MessageBuffer mb1 = new MessageBuffer();
					mb1.append(Standardize.Label.remote.get()+rwt.toString());
					mb1.append(Standardize.Label.status.get()+"unaffected");
					logger.trace(location, ILogger.null_id, mb1.toString());
				}
			}
		}
		catch(Exception e) {
			logger.error(location, ILogger.null_id, e);
		}
	}
	
	private void block(IRemoteWorkerThread rwt) {
		String location = "block";
		if(rwt != null) {
			IRemoteWorkerThread result = wipMap.putIfAbsent(rwt, rwt);
			if(result != null) {
				MessageBuffer mb;
				mb = new MessageBuffer();
				mb.append(Standardize.Label.remote.get()+rwt.toString());
				mb.append(Standardize.Label.status.get()+"delayed");
				logger.warn(location, ILogger.null_id, mb.toString());
				while(result != null) {
					try {
						Thread.sleep(200);
					}
					catch(Exception e) {
					}
					result = wipMap.putIfAbsent(rwt, rwt);
				}
				mb = new MessageBuffer();
				mb.append(Standardize.Label.remote.get()+rwt.toString());
				mb.append(Standardize.Label.status.get()+"in-force");
				logger.warn(location, ILogger.null_id, mb.toString());
			}
		}
	}
	
	private void unblock(IRemoteWorkerThread rwt) {
		if(rwt != null) {
			wipMap.remove(rwt);
		}
	}
	
	@Override
	public void handleProcessFailedInitialization(IProcessInfo processInfo) {
		String location = "handleProcessFailedInitialization";
		try {
			MessageBuffer mb = new MessageBuffer();
			mb.append(Standardize.Label.node.get()+processInfo.getNodeName());
			mb.append(Standardize.Label.ip.get()+processInfo.getNodeAddress());
			mb.append(Standardize.Label.pid.get()+processInfo.getPid());
			logger.trace(location, ILogger.null_id, mb.toString());
			
			String nodeName = processInfo.getNodeName();
			String nodeAddress = processInfo.getNodeAddress();
			int pid = processInfo.getPid();
			
			StringBuffer sb = new StringBuffer();
			sb.append(nodeName);
			sb.append(File.pathSeparator);
			sb.append(nodeAddress);
			sb.append(pid);
			
			String jp = sb.toString();
			
			JobDriver jd = JobDriver.getInstance();
			boolean isKillJob = jd.isKillJob();
			
			boolean added = failedInitializationMap.putIfAbsent(jp, jp) == null;
			int failedCount = failedInitializationMap.size();
			int failedLimit = jd.getStartupInitializationErrorLimit();

			if(added) {
				mb = new MessageBuffer();
				mb.append(Standardize.Label.node.get()+nodeName);
				mb.append(Standardize.Label.ip.get()+nodeAddress);
				mb.append(Standardize.Label.pid.get()+pid);
				mb.append(Standardize.Label.count.get()+failedCount);
				mb.append(Standardize.Label.limit.get()+failedCount);
				mb.append(Standardize.Label.isKillJob.get()+isKillJob);
				logger.info(location, ILogger.null_id, mb.toString());
				if(!isKillJob) {
					if(failedCount >= failedLimit) {
						JdState jdState = jd.getJdState();
						switch(jdState) {
						case Initializing:
							String text = "startup initialization error limit exceeded";
							jd.killJob(CompletionType.Exception, text);
							mb = new MessageBuffer();
							mb.append(Standardize.Label.node.get()+nodeName);
							mb.append(Standardize.Label.ip.get()+nodeAddress);
							mb.append(Standardize.Label.pid.get()+pid);
							mb.append(Standardize.Label.isKillJob.get()+jd.isKillJob());
							mb.append(Standardize.Label.type.get()+jd.getCompletionType().toString());
							mb.append(Standardize.Label.reason.get()+jd.getCompletionText());
							logger.info(location, ILogger.null_id, mb.toString());
							break;
						default:
							break;
						}
					}
				}
			}
			else {
				mb = new MessageBuffer();
				mb.append(Standardize.Label.node.get()+nodeName);
				mb.append(Standardize.Label.ip.get()+nodeAddress);
				mb.append(Standardize.Label.pid.get()+pid);
				mb.append(Standardize.Label.count.get()+failedCount);
				logger.trace(location, ILogger.null_id, mb.toString());
			}
		}
		catch(Exception e) {
			logger.error(location, ILogger.null_id, e);
		}
	}
	
	@Override
	public void handleMetaCasTransation(IMetaCasTransaction trans) {
		String location = "handleMetaCasTransation";
		RemoteWorkerThread rwt = null;
		try {
			trans.setResponseHints(new ArrayList<Hint>());
			rwt = new RemoteWorkerThread(trans);
			block(rwt);
			MessageBuffer mb = new MessageBuffer();
			mb.append(Standardize.Label.remote.get()+rwt.toString());
			mb.append(Standardize.Label.type.get()+trans.getType());
			logger.debug(location, ILogger.null_id, mb.toString());
			Type type = trans.getType();
			long stime = System.nanoTime();  // Used when log level == debug
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
			case InvestmentReset:
				handleMetaCasTransationInvestmentReset(trans, rwt);
				break;
			default:
				break;
			}
			accumulateTimes(type.name(), stime);
			JdState jdState = JobDriver.getInstance().getJdState();
			trans.setJdState(jdState);
			IMetaCas metaCas = trans.getMetaCas();
			if(metaCas != null) {
				metaCas.setPerformanceMetrics(null);
				metaCas.setUserSpaceException(null);
			}
		}
		catch(Exception e) {
			logger.error(location, ILogger.null_id, e);
		}
		finally {
			unblock(rwt);
		}
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
		WiTracker tracker = WiTracker.getInstance();
		IWorkItem wi = tracker.link(rwt);
		IFsm fsm = wi.getFsm();
		IEvent event = WiFsm.Get_Request;
		Object actionData = new ActionData(wi, rwt, trans);
		fsm.transition(event, actionData);
	}

	private void handleMetaCasTransationAck(IMetaCasTransaction trans, IRemoteWorkerThread rwt) {
		String location = "handleMetaCasTransationAck";
		WiTracker tracker = WiTracker.getInstance();
		MetaCas metaCas = (MetaCas) trans.getMetaCas();
		if(tracker.isRecognized(rwt, metaCas)) {
			IWorkItem wi = tracker.find(rwt);
			update(wi, metaCas);
			IFsm fsm = wi.getFsm();
			IEvent event = WiFsm.Ack_Request;
			Object actionData = new ActionData(wi, rwt, trans);
			fsm.transition(event, actionData);
			MessageBuffer mb = new MessageBuffer();
			mb.append(Standardize.Label.AckMsecs.get()+(wi.getTodAck()-wi.getTodGet()));
			logger.debug(location, ILogger.null_id, mb.toString());
		}
		else {
			trans.setMetaCas(null);
			TransactionHelper.addResponseHint(trans, Hint.Rejected);
		}
	}
	
	private void handleMetaCasTransationEnd(IMetaCasTransaction trans, IRemoteWorkerThread rwt) {
		String location = "handleMetaCasTransationEnd";
		WiTracker tracker = WiTracker.getInstance();
		MetaCas metaCas = (MetaCas) trans.getMetaCas();
		if(tracker.isRecognized(rwt, metaCas)) {
			IWorkItem wi = tracker.find(rwt);
			update(wi, metaCas);
			IFsm fsm = wi.getFsm();
			IEvent event = WiFsm.End_Request;
			Object actionData = new ActionData(wi, rwt, trans);
			fsm.transition(event, actionData);
			MessageBuffer mb = new MessageBuffer();
			mb.append(Standardize.Label.EndMsecs.get()+(wi.getTodEnd()-wi.getTodAck()));
			logger.debug(location, ILogger.null_id, mb.toString());
		}
		else {
			trans.setMetaCas(null);
			TransactionHelper.addResponseHint(trans, Hint.Rejected);
		}
	}
	
	private void handleMetaCasTransationInvestmentReset(IMetaCasTransaction trans, IRemoteWorkerThread rwt) {
		String location = "handleMetaCasTransationInvestmentReset";
		WiTracker tracker = WiTracker.getInstance();
		MetaCas metaCas = (MetaCas) trans.getMetaCas();
		if(tracker.isRecognized(rwt, metaCas)) {
			IWorkItem wi = tracker.find(rwt);
			update(wi, metaCas);
			IFsm fsm = wi.getFsm();
			IEvent event = WiFsm.Investment_Reset;
			Object actionData = new ActionData(wi, rwt, trans);
			fsm.transition(event, actionData);
			MessageBuffer mb = new MessageBuffer();
			mb.append(Standardize.Label.remote.get()+rwt.toString());
			mb.append("investment reset");
			logger.debug(location, ILogger.null_id, mb.toString());
		}
		else {
			trans.setMetaCas(null);
			TransactionHelper.addResponseHint(trans, Hint.Rejected);
		}
	}

	/*
	 * When debugging report elapsed times for JD's handling of requests
	 * Check the initial value of the logging level ... cannot change mid-stream as are collecting statistics
	 */
	private static int totalWorkItems = -1;
	private static HashMap<String,long[]> timeMap = new HashMap<String,long[]>();
	
	public static void accumulateTimes(String name, long stime) {
		
		if (!reportTimes) return;
		
		long elapsed = System.nanoTime() - stime;

		synchronized (timeMap) {
			long[] nums = timeMap.get(name);
			if (nums == null) {
				nums = new long[]{0, 0, 10000000000L, 0};		// count total min max
				timeMap.put(name, nums);
			}
			++nums[0];
			nums[1] += elapsed;
			if (elapsed < nums[2])
				nums[2] = elapsed;
			else if (elapsed > nums[3])
				nums[3] = elapsed;
			
			// When processed last 'Get' log all times since winding down with empty Gets may distort avgs
			if (nums[0] == totalWorkItems && name.equals("Get")) {
				for (String nam : timeMap.keySet()) {
					nums = timeMap.get(nam);
					logger.debug("accumulateTimes", ILogger.null_id, "Elapsed times at last Get: " + nam + ": " + nums[0]
							+ " avg: " + 1E-6 * nums[1] / nums[0] 
							+ " min: " + nums[2] * 1E-6 
							+ " max: " + nums[3] * 1E-6 + " msec");
				}
			}
		}
	}
}
