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

import org.apache.uima.ducc.container.common.ContainerLogger;
import org.apache.uima.ducc.container.common.IContainerLogger;
import org.apache.uima.ducc.container.common.IEntityId;
import org.apache.uima.ducc.container.jd.cas.CasManager;
import org.apache.uima.ducc.container.jd.classload.ProxyJobDriverErrorHandler;
import org.apache.uima.ducc.container.jd.mh.iface.remote.IRemoteWorkerIdentity;
import org.apache.uima.ducc.container.jd.wi.IWorkItem;
import org.apache.uima.ducc.container.jd.wi.IWorkItemStatistics;
import org.apache.uima.ducc.container.jd.wi.WorkItemStatistics;

public class JobDriver {

	private static IContainerLogger logger = ContainerLogger.getLogger(JobDriver.class, IContainerLogger.Component.JD.name());
	
	private static JobDriver instance = null;
	
	public static JobDriver getInstance() {
		if(instance == null) {
			instance = new JobDriver();
		}
		return instance;
	}
	
	public static void resetInstance() {
		instance = null;
	}
	
	private ConcurrentHashMap<IRemoteWorkerIdentity, IWorkItem> map = null;
	private IWorkItemStatistics wis = null;
	private CasManager cm = null;
	private ProxyJobDriverErrorHandler pjdeh = null;
	
	public JobDriver() {
		initialize();
	}
	
	public void initialize() {
		String location = "initialize";
		try {
			map = new ConcurrentHashMap<IRemoteWorkerIdentity, IWorkItem>();
			wis = new WorkItemStatistics();
			cm = new CasManager();
			pjdeh = new ProxyJobDriverErrorHandler();
		}
		catch(Exception e) {
			logger.error(location, IEntityId.null_id, e);
		}
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
	
}
