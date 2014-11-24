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

import org.apache.uima.ducc.container.common.FlagsExtendedHelper;
import org.apache.uima.ducc.container.common.logger.IComponent;
import org.apache.uima.ducc.container.common.logger.ILogger;
import org.apache.uima.ducc.container.common.logger.Logger;
import org.apache.uima.ducc.container.jd.cas.CasManager;
import org.apache.uima.ducc.container.jd.classload.ProxyJobDriverErrorHandler;
import org.apache.uima.ducc.container.jd.mh.IMessageHandler;
import org.apache.uima.ducc.container.jd.mh.MessageHandler;
import org.apache.uima.ducc.container.jd.mh.iface.remote.IRemoteWorkerIdentity;
import org.apache.uima.ducc.container.jd.wi.IWorkItem;
import org.apache.uima.ducc.container.jd.wi.IWorkItemStatistics;
import org.apache.uima.ducc.container.jd.wi.WorkItemStatistics;
import org.apache.uima.ducc.container.net.iface.IMetaCasTransaction.JdState;

public class JobDriver {

	private static Logger logger = Logger.getLogger(JobDriver.class, IComponent.Id.JD.name());
	
	private static JobDriver instance = null;
	
	public static JobDriver getInstance() {
		return instance;
	}
	
	public static void createInstance() throws JobDriverException {
		if(instance != null) {
			throw new JobDriverException("already created");
		}
		else {
			instance = new JobDriver();
		}
	}
	
	public static void destroyInstance() {
		instance = null;
	}
	
	private String jobId = null;
	private ConcurrentHashMap<IRemoteWorkerIdentity, IWorkItem> map = null;
	private IWorkItemStatistics wis = null;
	private CasManager cm = null;
	private ProxyJobDriverErrorHandler pjdeh = null;
	private IMessageHandler mh = new MessageHandler();
	
	private JdState jdState = null;
	
	private JobDriver() throws JobDriverException {
		initialize();
	}
	
	private void initialize() throws JobDriverException {
		String location = "initialize";
		try {
			jdState = JdState.Initializing;
			FlagsExtendedHelper feh = FlagsExtendedHelper.getInstance();
			jobId = feh.getJobId();
			map = new ConcurrentHashMap<IRemoteWorkerIdentity, IWorkItem>();
			wis = new WorkItemStatistics();
			cm = new CasManager();
			pjdeh = new ProxyJobDriverErrorHandler();
			mh = new MessageHandler();
		}
		catch(Exception e) {
			logger.error(location, ILogger.null_id, e);
			throw new JobDriverException(e);
		}
	}
	
	public String getJobId() {
		return jobId;
	}
	
	public ConcurrentHashMap<IRemoteWorkerIdentity, IWorkItem> getMap() {
		return map;
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
	
	public JdState getJdState() {
		synchronized(jdState) {
			return jdState;
		}
	}
	
	public void advanceJdState(JdState value) {
		synchronized(jdState) {
			switch(jdState) {
			case Ended:
				break;
			case Active:
				switch(value) {
				case Ended:
					jdState = value;
					break;
				}
				break;
			case Initializing:
				switch(value) {
				case Ended:
				case Active:
					jdState = value;
				}
				break;
			}
			jdState = value;
		}
	}
}
