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
package org.apache.uima.ducc.ws;

import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.uima.ducc.common.jd.files.WorkItemStateManager;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.common.DuccWorkMap;
import org.apache.uima.ducc.transport.event.common.IDuccWork;
import org.apache.uima.ducc.transport.event.common.IDuccWorkJob;

public class CacheManager {
	
	private static DuccLogger logger = DuccLoggerComponents.getWsLogger(CacheManager.class.getName());
	
	private static CacheManager instance = new CacheManager();
	
	private volatile HashMap<String,WorkItemStateManager> wisMap = null;
	
	private AtomicBoolean inProgress = new AtomicBoolean(false);
	
	public static CacheManager getInstance() {
		return instance;
	}
	
	public void update(DuccWorkMap map) {
		String location = "update";
		DuccId jobid = null;
		if(inProgress.compareAndSet(false, true)) {
			try {
				HashMap<String,WorkItemStateManager> updatedWisMap = new HashMap<String,WorkItemStateManager>();
				Iterator<DuccId> iterator = map.keySet().iterator();
				while(iterator.hasNext()) {
					jobid = iterator.next();
					IDuccWork duccWork = map.findDuccWork(jobid);
					switch(duccWork.getDuccType()) {
					case Job:
						IDuccWorkJob job = (IDuccWorkJob)duccWork;
						String jobNo = job.getId();
						String userId = job.getStandardInfo().getUser();
						String jobDir = job.getLogDirectory()+jobNo;
						WorkItemStateManager workItemStateManager = new WorkItemStateManager(jobDir);
						workItemStateManager.importData(userId);
						updatedWisMap.put(jobNo, workItemStateManager);
						break;
					}
				}
				wisMap = updatedWisMap;
				logger.debug(location, jobid, "size:"+wisMap.size());
			}
			catch(Exception e) {
				logger.error(location, jobid, e);
			}
		}
		else {
			logger.warn(location, jobid, "skipping: already in progress...");
		}
		inProgress.set(false);
	}
	
	public WorkItemStateManager getWorkItemStateManager(String jobNo) {
		WorkItemStateManager retVal = wisMap.get(jobNo);
		return retVal;
	}
	
}
