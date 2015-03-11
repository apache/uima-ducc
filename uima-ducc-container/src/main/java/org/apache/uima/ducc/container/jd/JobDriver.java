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

import java.util.concurrent.ConcurrentHashMap;

import org.apache.uima.ducc.common.jd.files.workitem.IWorkItemStateKeeper;
import org.apache.uima.ducc.common.jd.files.workitem.WorkItemStateKeeper;
import org.apache.uima.ducc.common.utils.DuccPropertiesResolver;
import org.apache.uima.ducc.container.common.FlagsExtendedHelper;
import org.apache.uima.ducc.container.common.MessageBuffer;
import org.apache.uima.ducc.container.common.Standardize;
import org.apache.uima.ducc.container.common.logger.IComponent;
import org.apache.uima.ducc.container.common.logger.ILogger;
import org.apache.uima.ducc.container.common.logger.Logger;
import org.apache.uima.ducc.container.dgen.DgenManager;
import org.apache.uima.ducc.container.jd.cas.CasManager;
import org.apache.uima.ducc.container.jd.classload.ProxyJobDriverErrorHandler;
import org.apache.uima.ducc.container.jd.mh.IMessageHandler;
import org.apache.uima.ducc.container.jd.mh.MessageHandler;
import org.apache.uima.ducc.container.jd.mh.iface.IOperatingInfo.CompletionType;
import org.apache.uima.ducc.container.jd.mh.iface.remote.IRemotePid;
import org.apache.uima.ducc.container.jd.mh.iface.remote.IRemoteWorkerThread;
import org.apache.uima.ducc.container.jd.wi.IProcessStatistics;
import org.apache.uima.ducc.container.jd.wi.IWorkItem;
import org.apache.uima.ducc.container.jd.wi.IWorkItemStatistics;
import org.apache.uima.ducc.container.jd.wi.WorkItemStatistics;
import org.apache.uima.ducc.container.jd.wi.perf.IWorkItemPerformanceKeeper;
import org.apache.uima.ducc.container.jd.wi.perf.WorkItemPerformanceKeeper;
import org.apache.uima.ducc.container.net.iface.IMetaCasTransaction.JdState;

public class JobDriver {

	private static Logger logger = Logger.getLogger(JobDriver.class, IComponent.Id.JD.name());
	
	private static JobDriver instance = null;
	
	public static JobDriver getInstance() {
		return instance;
	}
	
	public static JobDriver createInstance() throws JobDriverException {
		if(instance == null) {
			instance = new JobDriver();
		}
		return getInstance();
	}
	
	public static void destroyInstance() {
		instance = null;
	}
	
	private String jobId = null;
	private String logDir = null;
	private long workItemTimeoutMillis = 24*60*60*1000;
	private ConcurrentHashMap<IRemoteWorkerThread, IWorkItem> remoteThreadMap = null;
	private ConcurrentHashMap<IRemotePid, IProcessStatistics> remoteProcessMap = null;
	private IWorkItemStatistics wis = null;
	private CasManager cm = null;
	private ProxyJobDriverErrorHandler pjdeh = null;
	private IMessageHandler mh = null; // new MessageHandler();
	private DgenManager ddManager = null;
	
	private IWorkItemStateKeeper wisk = null;
	private IWorkItemPerformanceKeeper wipk = null;
	
	private JdState jdState = JdState.Prelaunch;
	
	private boolean killJob = false;
	private CompletionType completionType = CompletionType.Normal;
	private String completionText = null;

	private JobDriver() throws JobDriverException {
		initialize();
	}
	
	private void initialize() throws JobDriverException {
		String location = "initialize";
		try {
			advanceJdState(JdState.Initializing);
			FlagsExtendedHelper feh = FlagsExtendedHelper.getInstance();
			jobId = feh.getJobId();
			logDir = feh.getLogDirectory();
			setWorkItemTimeout();
			remoteThreadMap = new ConcurrentHashMap<IRemoteWorkerThread, IWorkItem>();
			remoteProcessMap = new ConcurrentHashMap<IRemotePid, IProcessStatistics>();
			wis = new WorkItemStatistics();
			wisk = new WorkItemStateKeeper(IComponent.Id.JD.name(), logDir);
			wipk = new WorkItemPerformanceKeeper(logDir);
			cm = new CasManager();
			pjdeh = new ProxyJobDriverErrorHandler();
			ddManager = new DgenManager();
			mh = new MessageHandler();
		}
		catch(Exception e) {
			logger.error(location, ILogger.null_id, e);
			throw new JobDriverException();
		}
	}
	
	private void setWorkItemTimeout() {
		String location = "setWorkItemTimeout";
		try {
			FlagsExtendedHelper feh = FlagsExtendedHelper.getInstance();
			String workItemTimeout = feh.getWorkItemTimeout();
			workItemTimeoutMillis = Long.parseLong(workItemTimeout)*(60*1000);
			MessageBuffer mb = new MessageBuffer();
			mb.append(Standardize.Label.value.get()+workItemTimeoutMillis);
			logger.trace(location, ILogger.null_id, mb.toString());
		}
		catch(Exception e) {
			logger.error(location, ILogger.null_id, e);
		}
	}
	
	public String getJobId() {
		return jobId;
	}
	
	public ConcurrentHashMap<IRemoteWorkerThread, IWorkItem> getRemoteThreadMap() {
		return remoteThreadMap;
	}

	public ConcurrentHashMap<IRemotePid, IProcessStatistics> getRemoteProcessMap() {
		return remoteProcessMap;
	}
	
	public IWorkItemStatistics getWorkItemStatistics() {
		return wis;
	}
	
	public CasManager getCasManager() {
		return cm;
	}
	
	public ProxyJobDriverErrorHandler getProxyJobDriverErrorHandler() {
		return pjdeh;
	}
	
	public IMessageHandler getMessageHandler() {
		return mh;
	}
	
	public DgenManager getDdManager() {
		return ddManager;
	}
	
	public long getWorkItemTimeoutMillis() {
		return workItemTimeoutMillis;
	}
	
	public IWorkItemStateKeeper getWorkItemStateKeeper() {
		return wisk;
	}
	
	public IWorkItemPerformanceKeeper getWorkItemPerformanceKeeper() {
		return wipk;
	}
	
	public JdState getJdState() {
		synchronized(jdState) {
			return jdState;
		}
	}
	
	public void advanceJdState(JdState value) {
		String location = "advanceJdState";
		String request = value.name();
		String current = null;
		String result = null;
		synchronized(jdState) {
			current = jdState.name();
			switch(jdState) {
			case Ended:
				break;
			case Active:
				switch(value) {
				case Ended:
					jdState = value;
					wipk.publish();
					break;
				}
				break;
			case Initializing:
				switch(value) {
				case Ended:
					jdState = value;
					break;
				case Active:
					jdState = value;
					break;
				}
				break;
			case Prelaunch:
				switch(value) {
				case Initializing:
					jdState = value;
					break;
				}
				break;
			}
			result = jdState.name();
		}
		MessageBuffer mb = new MessageBuffer();
		mb.append(Standardize.Label.current.get()+current);
		mb.append(Standardize.Label.request.get()+request);
		mb.append(Standardize.Label.result.get()+result);
		if(current.equals(result)) {
			logger.trace(location, ILogger.null_id, mb.toString());
		}
		else {
			logger.info(location, ILogger.null_id, mb.toString());
		}
	}
	
	public void killJob(CompletionType value) {
		killJob(value, null);
	}
	
	public void killJob(CompletionType value, String text) {
		if(!killJob) {
			killJob = true;
			completionType = value;
			completionText = text;
		}
	}
	
	public boolean isKillJob() {
		return killJob;
	}
	
	public CompletionType getCompletionType() {
		return completionType;
	}
	
	public String getCompletionText() {
		return completionText;
	}
	
	public int getStartupInitializationErrorLimit() {
		int startup_initialization_error_limit = DuccPropertiesResolver.get(DuccPropertiesResolver.ducc_jd_startup_initialization_error_limit, 1);
		return startup_initialization_error_limit;
	}
}
