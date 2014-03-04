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
package org.apache.uima.ducc.common.jd.files.workitem.legacy;

import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.uima.ducc.common.jd.files.IWorkItemState;
@Deprecated
public class WorkItemStateManager {

	private ConcurrentSkipListMap<Long,IWorkItemState> map = new ConcurrentSkipListMap<Long,IWorkItemState>();
	
	private WorkItemStateSerializedObjects pSer;
	private WorkItemStateJson pJson;
	private WorkItemStateJsonGz pJsonGz;
	
	private String user = null;
	
	public WorkItemStateManager(String component, String dirname, String user) {
		setUser(user);
		pSer = new WorkItemStateSerializedObjects(dirname);
		pJson = new WorkItemStateJson(dirname);
		pJsonGz = new WorkItemStateJsonGz(dirname);
	}
	
	private void setUser(String value) {
		user = value;
	}
	
	public ConcurrentSkipListMap<Long,IWorkItemState> getMap() {
		importData();
		return map;
	}
	
	/*
	@SuppressWarnings("deprecation")
	public void exportData() throws IOException {
		try {
			pJsonGz.exportData(map);
			return;
		}
		catch(Exception e) {
		}
		try {
			pJson.exportData(map);
			return;
		}
		catch(Exception e) {
		}
		pSer.exportData(map);
	}
	*/
	
	private void importData() {
		try {
			map = pJsonGz.importData(user);
			return;
		}
		catch(Exception e) {
		}
		try {
			map = pJson.importData();
			return;
		}
		catch(Exception e) {
		}
		try {
			map = pSer.importData();
			return;
		}
		catch(Exception e) {
		}
	}
	
	public double getMin() {
		double retVal = -1;
		try {
			for (Entry<Long, IWorkItemState> entry : map.entrySet()) {
				IWorkItemState workItemState = entry.getValue();
				switch(workItemState.getState()) {
				case ended:
					long millis = workItemState.getMillisProcessing();
					if(millis < retVal) {
						retVal = millis;
					}
					else if(retVal < 0) {
						retVal = millis;
					}
					break;
				}
			}
		}
		catch(Throwable t) {
		}
		return retVal;
	}
	
	public double getMax() {
		double retVal = -1;
		try {
			for (Entry<Long, IWorkItemState> entry : map.entrySet()) {
				IWorkItemState workItemState = entry.getValue();
				switch(workItemState.getState()) {
				case ended:
					long millis = workItemState.getMillisProcessing();
					if(millis > retVal) {
						retVal = millis;
					}
					break;
				}
			}
		}
		catch(Throwable t) {
		}
		return retVal;
	}
	
	public double getAvg() {
		double retVal = 0;
		try {
			int count = 0;
			for (Entry<Long, IWorkItemState> entry : map.entrySet()) {
				IWorkItemState workItemState = entry.getValue();
				switch(workItemState.getState()) {
				case ended:
					retVal += workItemState.getMillisProcessing();
					count++;
					break;
				}
			}
			if(count > 0) {
				retVal = retVal / count;
			}
		}
		catch(Throwable t) {
		}
		return retVal;
	}
}
