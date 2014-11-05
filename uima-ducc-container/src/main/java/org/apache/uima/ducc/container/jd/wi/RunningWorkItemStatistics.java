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

import org.apache.uima.ducc.container.jd.JobDriverCommon;
import org.apache.uima.ducc.container.jd.mh.iface.remote.IRemoteWorkerIdentity;

public class RunningWorkItemStatistics implements IRunningWorkItemStatistics {

	private long millisMin = 0;
	private long millisMax = 0;
	
	private long todMostRecentStart = 0;
	
	public static RunningWorkItemStatistics getCurrent() {
		long min = Long.MAX_VALUE;
		long max = 0;
		long todMrs = 0;
		ConcurrentHashMap<IRemoteWorkerIdentity, IWorkItem> map = JobDriverCommon.getInstance().getMap();
		for(Entry<IRemoteWorkerIdentity, IWorkItem> entry : map.entrySet()) {
			IWorkItem wi = entry.getValue();
			long time = wi.getMillisOperating();
			if(time > 0) {
				if(time > max) {
					max = time;
				}
				if(time < min) {
					min = time;
				}
			}
			long tod = wi.getTodAck();
			if(tod > todMrs) {
				todMrs = tod;
			}
		}
		if(min > max) {
			min = max;
		}
		RunningWorkItemStatistics retVal = new RunningWorkItemStatistics(min,max,todMrs);
		return retVal;
	}
	
	public RunningWorkItemStatistics(long min, long max, long todMRS) {
		setMillisMin(min);
		setMillisMax(max);
		setTodMostRecentStart(todMRS);
	}
	
	@Override
	public void setMillisMin(long value) {
		millisMin = value;
	}
	
	@Override
	public long getMillisMin() {
		return millisMin;
	}
	
	@Override
	public void setMillisMax(long value) {
		millisMax = value;
	}
	
	@Override
	public long getMillisMax() {
		return millisMax;
	}

	@Override
	public void setTodMostRecentStart(long value) {
		todMostRecentStart = value;
	}

	@Override
	public long getTodMostRecentStart() {
		return todMostRecentStart;
	}
	
}
