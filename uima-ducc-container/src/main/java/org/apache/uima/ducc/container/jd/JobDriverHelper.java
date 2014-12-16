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

import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.uima.ducc.container.common.MessageBuffer;
import org.apache.uima.ducc.container.common.Standardize;
import org.apache.uima.ducc.container.common.logger.IComponent;
import org.apache.uima.ducc.container.common.logger.ILogger;
import org.apache.uima.ducc.container.common.logger.Logger;
import org.apache.uima.ducc.container.jd.mh.RemoteWorkerProcess;
import org.apache.uima.ducc.container.jd.mh.iface.IProcessInfo;
import org.apache.uima.ducc.container.jd.mh.iface.IWorkItemInfo;
import org.apache.uima.ducc.container.jd.mh.iface.remote.IRemotePid;
import org.apache.uima.ducc.container.jd.mh.iface.remote.IRemoteWorkerProcess;
import org.apache.uima.ducc.container.jd.mh.iface.remote.IRemoteWorkerThread;
import org.apache.uima.ducc.container.jd.mh.impl.ProcessInfo;
import org.apache.uima.ducc.container.jd.mh.impl.WorkItemInfo;
import org.apache.uima.ducc.container.jd.wi.IProcessStatistics;
import org.apache.uima.ducc.container.jd.wi.IWorkItem;
import org.apache.uima.ducc.container.jd.wi.ProcessStatistics;
import org.apache.uima.ducc.container.net.iface.IMetaCas;

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
			ConcurrentHashMap<IRemoteWorkerThread, IWorkItem> map = jd.getRemoteThreadMap();
			for(Entry<IRemoteWorkerThread, IWorkItem> entry : map.entrySet()) {
				IRemoteWorkerThread rwt = entry.getKey();
				IWorkItem wi = entry.getValue();
				IWorkItemInfo wii = new WorkItemInfo();
				wii.setNodeAddress(rwt.getNodeAddress());
				wii.setNodeName(rwt.getNodeName());
				wii.setPid(rwt.getPid());
				wii.setTid(rwt.getTid());
				//TODO
				wii.setSeqNo(0);
				wii.setOperatingMillis(wi.getMillisOperating());
				list.add(wii);
				MessageBuffer mb = new MessageBuffer();
				mb.append(Standardize.Label.node.get()+wii.getNodeName());
				mb.append(Standardize.Label.pid.get()+wii.getPid());
				mb.append(Standardize.Label.tid.get()+wii.getTid());
				mb.append(Standardize.Label.operatingMillis.get()+wii.getOperatingMillis());
				logger.debug(location, ILogger.null_id, mb);
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
				IProcessInfo processInfo = new ProcessInfo(rwp.getNodeName(), rwp.getNodeAddress(), rwp.getPid(), pStats);
				list.add(processInfo);
				MessageBuffer mb = new MessageBuffer();
				mb.append(Standardize.Label.node.get()+processInfo.getNodeName());
				mb.append(Standardize.Label.ip.get()+processInfo.getNodeAddress());
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
		JobDriver jd = JobDriver.getInstance();
		ConcurrentHashMap<IRemotePid, IProcessStatistics> remoteprocessMap = jd.getRemoteProcessMap();
		IProcessStatistics processStatistics = remoteprocessMap.get(remotePid);
		if(processStatistics == null) {
			remoteprocessMap.putIfAbsent(remotePid, new ProcessStatistics());
			processStatistics = remoteprocessMap.get(remotePid);
		}
		return processStatistics;
	}
	
	public boolean isEqual(String a, String b) {
		boolean retVal = false;
		if(a != null) {
			if(b != null) {
				return a.equals(b);
			}
		}
		return retVal;
	}
	
	public boolean isEqual(IMetaCas a, IMetaCas b) {
		boolean retVal = false;
		if(a != null) {
			if(b != null) {
				return isEqual(a.getSystemKey(), b.getSystemKey());
			}
		}
		return retVal;
	}
	
	public boolean isEqual(IWorkItem a, IWorkItem b) {
		boolean retVal = false;
		if(a != null) {
			if(b != null) {
				return isEqual(a.getMetaCas(), b.getMetaCas());
			}
		}
		return retVal;
	}
	
	public IRemoteWorkerProcess getRemoteWorkerProcess(IWorkItem wi) {
		IRemoteWorkerProcess rwp = null;
		JobDriver jd = JobDriver.getInstance();
		ConcurrentHashMap<IRemoteWorkerThread, IWorkItem> map = jd.getRemoteThreadMap();
		for(Entry<IRemoteWorkerThread, IWorkItem> entry : map.entrySet()) {
			if(isEqual(entry.getValue(), wi)) {
				IRemoteWorkerThread rwt = entry.getKey();
				rwp = new RemoteWorkerProcess(rwt.getNodeName(),rwt.getNodeAddress(),rwt.getPid());
			}
		}
		return rwp;
	}
}
