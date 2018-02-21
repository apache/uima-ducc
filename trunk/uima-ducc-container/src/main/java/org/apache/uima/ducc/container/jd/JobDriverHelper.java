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
package org.apache.uima.ducc.container.jd;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.uima.ducc.container.common.MessageBuffer;
import org.apache.uima.ducc.container.common.Standardize;
import org.apache.uima.ducc.container.common.fsm.iface.IFsm;
import org.apache.uima.ducc.container.common.logger.IComponent;
import org.apache.uima.ducc.container.common.logger.ILogger;
import org.apache.uima.ducc.container.common.logger.Logger;
import org.apache.uima.ducc.container.jd.cas.CasManager;
import org.apache.uima.ducc.container.jd.cas.CasManagerStats;
import org.apache.uima.ducc.container.jd.mh.iface.IProcessInfo;
import org.apache.uima.ducc.container.jd.mh.iface.IWorkItemInfo;
import org.apache.uima.ducc.container.jd.mh.iface.remote.IRemotePid;
import org.apache.uima.ducc.container.jd.mh.iface.remote.IRemoteWorkerThread;
import org.apache.uima.ducc.container.jd.mh.impl.ProcessInfo;
import org.apache.uima.ducc.container.jd.mh.impl.WorkItemInfo;
import org.apache.uima.ducc.container.jd.wi.IProcessStatistics;
import org.apache.uima.ducc.container.jd.wi.IWorkItem;
import org.apache.uima.ducc.container.jd.wi.IWorkItemStatistics;
import org.apache.uima.ducc.container.jd.wi.ProcessStatistics;

public class JobDriverHelper {

	private static Logger logger = Logger.getLogger(JobDriverHelper.class, IComponent.Id.JD.name());
	
	private static JobDriverHelper instance = new JobDriverHelper();
	
	public static JobDriverHelper getInstance() {
		return instance;
	}
	
	public ArrayList<IWorkItemInfo> getActiveWorkItemInfo() {
		String location = "getActiveWorkItemInfo";
		ArrayList<IWorkItemInfo> list = new ArrayList<IWorkItemInfo>();
		try {
			JobDriver jd = JobDriver.getInstance();
			ConcurrentHashMap<IRemoteWorkerThread, IWorkItem> map = jd.getRemoteWorkerThreadMap();
			for(Entry<IRemoteWorkerThread, IWorkItem> entry : map.entrySet()) {
				IRemoteWorkerThread rwt = entry.getKey();
				IWorkItem wi = entry.getValue();
				IFsm fsm = wi.getFsm();
				IWorkItemInfo wii = new WorkItemInfo();
				wii.setNodeAddress(rwt.getNodeAddress());
				wii.setNodeName(rwt.getNodeName());
				wii.setPidName(rwt.getPidName());
				wii.setPid(rwt.getPid());
				wii.setTid(rwt.getTid());
				wii.setSeqNo(wi.getSeqNo());
				wii.setOperatingMillis(wi.getMillisOperating());
				wii.setInvestmentMillis(wi.getMillisInvestment());
				MessageBuffer mb = new MessageBuffer();
				mb.append(Standardize.Label.node.get()+wii.getNodeName());
				mb.append(Standardize.Label.pid.get()+wii.getPid());
				mb.append(Standardize.Label.tid.get()+wii.getTid());
				mb.append(Standardize.Label.state.get()+fsm.getStateCurrent().getStateName());
				mb.append(Standardize.Label.operatingMillis.get()+wii.getOperatingMillis());
				logger.debug(location, ILogger.null_id, mb);
				if(!fsm.isStateInitial()) {
					list.add(wii);
				}
			}
		}
		catch(Exception e) {
			logger.error(location, ILogger.null_id, e);
		}
		return list;
	}
	
	public ArrayList<IProcessInfo> getProcessInfo() {
		String location = "getProcessInfo";
		ArrayList<IProcessInfo> list = new ArrayList<IProcessInfo>();
		try {
			JobDriver jd = JobDriver.getInstance();
			ConcurrentHashMap<IRemotePid, IProcessStatistics> map = jd.getRemoteProcessMap();
			for(Entry<IRemotePid, IProcessStatistics> entry : map.entrySet()) {
				IRemotePid rwp = entry.getKey();
				IProcessStatistics pStats = entry.getValue();
				IProcessInfo processInfo = new ProcessInfo(rwp.getNodeName(), rwp.getNodeAddress(), rwp.getPidName(), rwp.getPid(), pStats);
				list.add(processInfo);
				MessageBuffer mb = new MessageBuffer();
				mb.append(Standardize.Label.node.get()+processInfo.getNodeName());
				mb.append(Standardize.Label.ip.get()+processInfo.getNodeAddress());
				mb.append(Standardize.Label.pidName.get()+processInfo.getPidName());
				mb.append(Standardize.Label.pid.get()+processInfo.getPid());
				mb.append(Standardize.Label.dispatch.get()+processInfo.getDispatch());
				mb.append(Standardize.Label.done.get()+processInfo.getDone());
				mb.append(Standardize.Label.error.get()+processInfo.getError());
				mb.append(Standardize.Label.preempt.get()+processInfo.getPreempt());
				mb.append(Standardize.Label.retry.get()+processInfo.getRetry());
				mb.append(Standardize.Label.avg.get()+processInfo.getAvg());
				mb.append(Standardize.Label.max.get()+processInfo.getMax());
				mb.append(Standardize.Label.min.get()+processInfo.getMin());
				logger.debug(location, ILogger.null_id, mb);
			}
		}
		catch(Exception e) {
			logger.error(location, ILogger.null_id, e);
		}
		return list;
	}
	
	public IProcessStatistics getProcessStatistics(IRemotePid remotePid) {
		String location = "getProcessStatistics";
		JobDriver jd = JobDriver.getInstance();
		ConcurrentHashMap<IRemotePid, IProcessStatistics> remoteprocessMap = jd.getRemoteProcessMap();
		IProcessStatistics processStatistics = remoteprocessMap.get(remotePid);
		boolean add = false;
		if(processStatistics == null) {
			add = true;
			remoteprocessMap.putIfAbsent(remotePid, new ProcessStatistics());
			processStatistics = remoteprocessMap.get(remotePid);
		}
		MessageBuffer mb = new MessageBuffer();
		mb.append(Standardize.Label.remote.get()+remotePid.toString());
		mb.append(Standardize.Label.add.get()+add);
		if(add) {
			logger.debug(location, ILogger.null_id, mb);
		}
		else {
			logger.trace(location, ILogger.null_id, mb);
		}
		return processStatistics;
	}
	
	public static double megabyte = 1.0*1024*1024;
	private static DecimalFormat df = new DecimalFormat("#.00");
	
	private String fmt100(double value) {
		String retVal = df.format(value);;
		return retVal;
	}
	
	private String fmtMB(long value) {
		return fmt100(value/megabyte);
	}
	
	private String fmtSec(long value) {
		return fmt100(value/1000.0);
	}
	
	public void summarize() {
		String location = "summarize";
		JobDriver jd = JobDriver.getInstance();
		MessageBuffer mb;
		mb = new MessageBuffer();
		Runtime.getRuntime().totalMemory();
		mb.append(Standardize.Label.memory.name()+" ");
		mb.append("[MB]"+" ");
		mb.append(Standardize.Label.total.get()+fmtMB(Runtime.getRuntime().totalMemory()));
		mb.append(Standardize.Label.free.get()+fmtMB(Runtime.getRuntime().freeMemory()));
		mb.append(Standardize.Label.max.get()+fmtMB(Runtime.getRuntime().maxMemory()));
		logger.info(location, ILogger.null_id, mb);
		//
		IWorkItemStatistics wis = jd.getWorkItemStatistics();
		mb = new MessageBuffer();
		mb.append(Standardize.Label.workitem.name()+" ");
		mb.append(Standardize.Label.statistics.name()+" ");
		mb.append("[sec]"+" ");
		mb.append(Standardize.Label.avg.get()+fmtSec(wis.getMillisAvg()));
		mb.append(Standardize.Label.min.get()+fmtSec(wis.getMillisMin()));
		mb.append(Standardize.Label.max.get()+fmtSec(wis.getMillisMax()));
		mb.append(Standardize.Label.stddev.get()+fmtSec(wis.getMillisStdDev()));
		logger.info(location, ILogger.null_id, mb);
		//
		CasManager cm = jd.getCasManager();
		CasManagerStats cms = cm.getCasManagerStats();
		mb = new MessageBuffer();
		mb.append(Standardize.Label.workitem.name()+" ");
		mb.append(Standardize.Label.count.name()+" ");
		mb.append(Standardize.Label.done.get()+cms.getEndSuccess());
		mb.append(Standardize.Label.error.get()+cms.getEndFailure());
		mb.append(Standardize.Label.retry.get()+cms.getNumberOfRetrys());
		mb.append(Standardize.Label.preempt.get()+cms.getNumberOfPreemptions());
		logger.info(location, ILogger.null_id, mb);
	}
	
}
