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
package org.apache.uima.ducc.container.jd.wi;

import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.uima.ducc.container.common.MessageBuffer;
import org.apache.uima.ducc.container.common.MetaCasHelper;
import org.apache.uima.ducc.container.common.Standardize;
import org.apache.uima.ducc.container.common.fsm.iface.IFsm;
import org.apache.uima.ducc.container.common.logger.IComponent;
import org.apache.uima.ducc.container.common.logger.ILogger;
import org.apache.uima.ducc.container.common.logger.Logger;
import org.apache.uima.ducc.container.jd.JobDriver;
import org.apache.uima.ducc.container.jd.fsm.wi.WiFsm;
import org.apache.uima.ducc.container.jd.log.LoggerHelper;
import org.apache.uima.ducc.container.jd.mh.RemoteWorkerProcess;
import org.apache.uima.ducc.container.jd.mh.iface.IProcessInfo;
import org.apache.uima.ducc.container.jd.mh.iface.remote.IRemoteWorkerProcess;
import org.apache.uima.ducc.container.jd.mh.iface.remote.IRemoteWorkerThread;
import org.apache.uima.ducc.container.net.iface.IMetaCas;
import org.apache.uima.ducc.container.net.impl.MetaCas;

public class WiTracker {
	
	private static Logger logger = Logger.getLogger(WiTracker.class, IComponent.Id.JD.name());
	
	private static WiTracker instance = new WiTracker();
	
	private WiTracker() {
	}
	
	public static WiTracker getInstance() {
		return instance;
	}
	
	private ConcurrentHashMap<IRemoteWorkerThread, IWorkItem> getMap() {
		return JobDriver.getInstance().getRemoteWorkerThreadMap();
	}
	
	public IWorkItem link(IRemoteWorkerThread rwt) {
		ConcurrentHashMap<IRemoteWorkerThread, IWorkItem> map = getMap();
		IWorkItem wi = null;
		if(rwt != null) {
			wi = find(rwt);
			if(wi == null) {
				IMetaCas metaCas = null;
				IFsm fsm = new WiFsm();
				wi = new WorkItem(metaCas, fsm);
				map.put(rwt, wi);
			}
		}
		return wi;
	}
	
	public IWorkItem assign(IRemoteWorkerThread rwt) {
		String location = "assign";
		IWorkItem wi = null;
		if(rwt != null) {
			wi = find(rwt);
			IMetaCas metaCas = wi.getMetaCas();
			MetaCasHelper metaCasHelper = new MetaCasHelper(metaCas);
			int seqNo = metaCasHelper.getSystemKey();
			MessageBuffer mb = new MessageBuffer();
			mb.append(Standardize.Label.seqNo.get()+seqNo);
			mb.append(Standardize.Label.remote.get()+rwt.toString());
			logger.debug(location, ILogger.null_id, mb.toString());
		}
		report();
		return wi;
	}
	
	public void unassign(IWorkItem wi) {
		String location = "unassign";
		ConcurrentHashMap<IRemoteWorkerThread, IWorkItem> map = getMap();
		IRemoteWorkerThread rwt = find(wi);
		if(rwt != null) {
			wi = find(rwt);
			IMetaCas metaCas = wi.getMetaCas();
			MetaCasHelper metaCasHelper = new MetaCasHelper(metaCas);
			int seqNo = metaCasHelper.getSystemKey();
			MessageBuffer mb = new MessageBuffer();
			mb.append(Standardize.Label.seqNo.get()+seqNo);
			mb.append(Standardize.Label.remote.get()+rwt.toString());
			logger.debug(location, ILogger.null_id, mb.toString());
			map.remove(rwt);
		}
		report();
	}

	public IWorkItem find(IRemoteWorkerThread rwt) {
		String location = "find";
		ConcurrentHashMap<IRemoteWorkerThread, IWorkItem> map = getMap();
		IWorkItem wi = null;
		if(rwt != null) {
			wi = map.get(rwt);
		}
		if(wi != null) {
			IMetaCas metaCas = wi.getMetaCas();
			if(metaCas != null) {
				
			}
			else {
				MessageBuffer mb = new MessageBuffer();
				mb.append(Standardize.Label.remote.get()+rwt.toString());
				mb.append("has no work assigned presently");
				logger.debug(location, ILogger.null_id, mb.toString());
			}
		}
		return wi;
	}
	
	public IRemoteWorkerThread find(IWorkItem wi) {
		ConcurrentHashMap<IRemoteWorkerThread, IWorkItem> map = getMap();
		IRemoteWorkerThread rwt = null;
		if(wi != null) {
			for(Entry<IRemoteWorkerThread, IWorkItem> entry : map.entrySet()) {
				if(wi.getSeqNo() == entry.getValue().getSeqNo()) {
					rwt = entry.getKey();
					break;
				}
			}
		}
		return rwt;
	}

	public IRemoteWorkerProcess getRemoteWorkerProcess(IWorkItem wi) {
		String location = "getRemoteWorkerProcess";
		IRemoteWorkerProcess rwp = null;
		if(wi != null) {
			IRemoteWorkerThread rwt = find(wi);
			if(rwt != null) {
				rwp = new RemoteWorkerProcess(rwt.getNodeName(),rwt.getNodeAddress(),rwt.getPidName(),rwt.getPid());
			}
			else {
				MessageBuffer mb = new MessageBuffer();
				mb.append(Standardize.Label.seqNo.get()+wi.getSeqNo());
				mb.append("has no work assigned presently");
				logger.debug(location, ILogger.null_id, mb.toString());
			}
		}
		return rwp;
	}
	
	public ConcurrentHashMap<IRemoteWorkerThread, IWorkItem> find(IProcessInfo processInfo) {
		ConcurrentHashMap<IRemoteWorkerThread, IWorkItem> map  = getMap();
		ConcurrentHashMap<IRemoteWorkerThread, IWorkItem> submap = new ConcurrentHashMap<IRemoteWorkerThread, IWorkItem>();
		if(processInfo != null) {
			for(Entry<IRemoteWorkerThread, IWorkItem> entry : map.entrySet()) {
				IRemoteWorkerThread rwt = entry.getKey();
				if(rwt.comprises(processInfo)) {
					IWorkItem wi = entry.getValue();
					submap.put(rwt, wi);
				}
			}
		}
		return submap;
	}
	
	public boolean isRecognized(IRemoteWorkerThread rwt, MetaCas metaCas) {
		String location = "isRecognized";
		boolean retVal = true;
		IWorkItem wi = find(rwt);
		if(wi == null) {
			MessageBuffer mb = new MessageBuffer();
			mb.append(Standardize.Label.remote.get()+rwt.toString());
			mb.append("has no work assigned presently");
			logger.debug(location, ILogger.null_id, mb.toString());
			retVal = false;
		}
		else if(metaCas == null) {
			MessageBuffer mb = new MessageBuffer();
			mb.append(Standardize.Label.remote.get()+rwt.toString());
			mb.append("meta-cas not present");
			logger.debug(location, ILogger.null_id, mb.toString());
			retVal = false;
		}
		else if(wi.getSeqNo() != metaCas.getSeqNo()) {
			MessageBuffer mb = new MessageBuffer();
			mb.append(Standardize.Label.remote.get()+rwt.toString());
			mb.append(Standardize.Label.seqNo.get()+metaCas.getSeqNo());
			mb.append(Standardize.Label.seqNo.get()+wi.getSeqNo());
			mb.append("remote/local sequence number mis-match");
			logger.debug(location, ILogger.null_id, mb.toString());
			retVal = false;
		}
		return retVal;
	}
	
	public int getSize() {
		ConcurrentHashMap<IRemoteWorkerThread, IWorkItem> map = getMap();
		return map.size();
	}
	
	private void report() {
		String location = "report";
		ConcurrentHashMap<IRemoteWorkerThread, IWorkItem> map = getMap();
		MessageBuffer mb = new MessageBuffer();
		mb.append(Standardize.Label.size.get()+map.size());
		logger.trace(location, ILogger.null_id, mb.toString());
		for(Entry<IRemoteWorkerThread, IWorkItem> entry : map.entrySet()) {
			IWorkItem wi = entry.getValue();
			IRemoteWorkerThread rwt = entry.getKey();
			MessageBuffer mb1 = LoggerHelper.getMessageBuffer(wi, rwt);
			logger.trace(location, ILogger.null_id, mb1.toString());
		}
	}
}
