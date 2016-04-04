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

import org.apache.uima.ducc.container.jd.JobDriver;
import org.apache.uima.ducc.container.jd.mh.iface.remote.IRemoteWorkerThread;

public class RunningWorkItemStatistics implements IRunningWorkItemStatistics {

	private long millisMin = 0;
	private long millisMax = 0;
	
	private long aboveAvgMillis = 0;
	private long aboveAvgCount = 0;
	
	private long todMostRecentStart = 0;
	
	public static RunningWorkItemStatistics getCurrent(long mean) {
		long min = Long.MAX_VALUE;
		long max = 0;
		long todMrs = 0;
		long mAbove = 0;
		long cAbove = 0;
		ConcurrentHashMap<IRemoteWorkerThread, IWorkItem> map = JobDriver.getInstance().getRemoteWorkerThreadMap();
		for(Entry<IRemoteWorkerThread, IWorkItem> entry : map.entrySet()) {
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
			if(time > mean) {
				mAbove += time;
				cAbove += 1;
			}
			long tod = wi.getTodAck();
			if(tod > todMrs) {
				todMrs = tod;
			}
		}
		if(min > max) {
			min = max;
		}
		RunningWorkItemStatistics retVal = new RunningWorkItemStatistics(min,max,todMrs,mAbove,cAbove);
		return retVal;
	}
	
	public RunningWorkItemStatistics(long min, long max, long todMRS, long aaMillis, long aaCount) {
		setMillisMin(min);
		setMillisMax(max);
		setTodMostRecentStart(todMRS);
		setAboveAvgMillis(aaMillis);
		setAboveAvgCount(aaCount);
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
	public void setAboveAvgMillis(long value) {
		aboveAvgMillis = value;
	}

	@Override
	public long getAboveAvgMillis() {
		return aboveAvgMillis;
	}

	@Override
	public void setAboveAvgCount(long value) {
		aboveAvgCount = value;
	}

	@Override
	public long getAboveAvgCount() {
		return aboveAvgCount;
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
