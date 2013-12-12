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
package org.apache.uima.ducc.ws.server;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.uima.ducc.common.jd.files.IWorkItemState;
import org.apache.uima.ducc.common.jd.files.WorkItemStateManager;
import org.apache.uima.ducc.common.jd.files.IWorkItemState.State;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.transport.event.common.IDuccWorkJob;
import org.apache.uima.ducc.ws.CacheManager;

public class WorkItemStateHelper {
	
	private static DuccLogger duccLogger = DuccLoggerComponents.getWsLogger(WorkItemStateHelper.class.getName());
	
	private WorkItemStateManager workItemStateManager = null;
	
	public WorkItemStateHelper(IDuccWorkJob job) throws IOException, ClassNotFoundException {
		String jobNo = job.getId();
		String userId = job.getStandardInfo().getUser();
		String jobDir = job.getLogDirectory()+jobNo;
		workItemStateManager = CacheManager.getInstance().getWorkItemStateManager(jobNo);
		if(workItemStateManager == null) {
			workItemStateManager = new WorkItemStateManager(jobDir);
			workItemStateManager.importData(userId);
		}
	}
	
	public double getLeastOperatingMillis(IDuccWorkJob job) {
		String methodName = "getLeastOperatingMillis";
		double retVal = 0;
		ConcurrentSkipListMap<Long,IWorkItemState> map = workItemStateManager.getMap();
	    if( (map == null) || (map.size() == 0) ) {
	    	// nada
	    }
	    else {
	    	double smallest = 0;
	    	new ConcurrentSkipListMap<IWorkItemState,IWorkItemState>();
			for (Entry<Long, IWorkItemState> entry : map.entrySet()) {
				IWorkItemState wis = entry.getValue();
				State state;
				double time;
				state = wis.getState();
				switch(state) {
				case operating:
					time = wis.getMillisProcessing();
					if(smallest == 0) {
						smallest = time;
						duccLogger.trace(methodName, job.getDuccId(), wis.getWiId()+" "+time+" "+time/1000);
					}
					else {
						if(time < smallest) {
							smallest = time;
							duccLogger.trace(methodName, job.getDuccId(), wis.getWiId()+" "+time+" "+time/1000);
						}
					}
				}
			}
			retVal = smallest;
	    }
		return retVal;
	}
	
	public double getMostCompletedMillis(IDuccWorkJob job) {
		String methodName = "getMostCompletedMillis";
		double retVal = 0;
		ConcurrentSkipListMap<Long,IWorkItemState> map = workItemStateManager.getMap();
	    if( (map == null) || (map.size() == 0) ) {
	    	// nada
	    }
	    else {
	    	double biggest = 0;
	    	new ConcurrentSkipListMap<IWorkItemState,IWorkItemState>();
			for (Entry<Long, IWorkItemState> entry : map.entrySet()) {
				IWorkItemState wis = entry.getValue();
				State state;
				double time;
				state = wis.getState();
				switch(state) {
				case ended:
					time = wis.getMillisProcessing();
					if(biggest == 0) {
						biggest = time;
						duccLogger.trace(methodName, job.getDuccId(), wis.getWiId()+" "+time+" "+time/1000);
					}
					else {
						if(time > biggest) {
							biggest = time;
							duccLogger.trace(methodName, job.getDuccId(), wis.getWiId()+" "+time+" "+time/1000);
						}
					}
				}
			}
			retVal = biggest;
	    }
		return retVal;
	}
	
}
