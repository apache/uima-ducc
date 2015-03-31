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
import org.apache.uima.ducc.container.common.Standardize;
import org.apache.uima.ducc.container.common.logger.IComponent;
import org.apache.uima.ducc.container.common.logger.ILogger;
import org.apache.uima.ducc.container.common.logger.Logger;
import org.apache.uima.ducc.container.jd.log.LoggerHelper;
import org.apache.uima.ducc.container.jd.mh.iface.remote.IRemoteWorkerThread;

public class WiTracker {

	private ConcurrentHashMap<IWorkItem, IRemoteWorkerThread> map = new ConcurrentHashMap<IWorkItem, IRemoteWorkerThread>();
	
	private static Logger logger = Logger.getLogger(WiTracker.class, IComponent.Id.JD.name());
	
	private static WiTracker instance = new WiTracker();
	
	public static WiTracker getInstance() {
		return instance;
	}
	
	public void assign(IWorkItem wi, IRemoteWorkerThread rwt) {
		String location = "assign";
		try {
			map.put(wi, rwt);
			report();
		}
		catch(Exception e) {
			logger.error(location, ILogger.null_id, e);
		}	
	}
	
	public void unassign(IWorkItem wi) {
		if(wi != null) {
			map.remove(wi);
			report();
		}
	}

	public int getSize() {
		return map.size();
	}
	
	private void report() {
		String location = "report";
		MessageBuffer mb = new MessageBuffer();
		mb.append(Standardize.Label.size.get()+map.size());
		logger.trace(location, ILogger.null_id, mb.toString());
		for(Entry<IWorkItem, IRemoteWorkerThread> entry : map.entrySet()) {
			IWorkItem wi = entry.getKey();
			IRemoteWorkerThread rwt = entry.getValue();
			MessageBuffer mb1 = LoggerHelper.getMessageBuffer(wi, rwt);
			logger.trace(location, ILogger.null_id, mb1.toString());
		}
	}
}
