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
package org.apache.uima.ducc.transport.event.jd;

import java.io.Serializable;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.uima.aae.monitor.statistics.AnalysisEnginePerformanceMetrics;
import org.apache.uima.ducc.common.utils.DuccLogger;


public class PerformanceMetricsSummaryMap implements Serializable {

	/**
	 * please increment this sUID when removing or modifying a field 
	 */
	private static final long serialVersionUID = 1L;
	
	private ConcurrentHashMap<String,PerformanceMetricsSummaryItem> map = new ConcurrentHashMap<String,PerformanceMetricsSummaryItem>();

	private AtomicInteger casCount = new AtomicInteger(0);
	
	public String delim_old = "Components,";
	public String delim_new = " Components ";
	public String delim     = delim_new;
	
	private String getKey(AnalysisEnginePerformanceMetrics item) {
		String key = "?";
		try {
			String uniqueName = item.getUniqueName();
			if(uniqueName.contains(delim_old)) {
				key = uniqueName.split(delim_old,2)[1];
			}
			else if(uniqueName.contains(delim_new)) {
				key = uniqueName.split(delim_new,2)[1];
			}
			else {
				key = uniqueName;
			}
		}
		catch(Throwable t) {
		}
		return key;
	}
	
	private String getDisplayName(AnalysisEnginePerformanceMetrics item) {
		return getKey(item);
	}
	
	private void addEntry(String key, String displayName) {
		synchronized(map) {
			if(!map.containsKey(key)) {
				PerformanceMetricsSummaryItem summaryItem = new PerformanceMetricsSummaryItem(displayName,key);
				map.put(key, summaryItem);
			}
		}
	}
	
	/**
	 * For each unique name in completed work item's performance metrics list:
	 * 
	 * 1. accumulate analysis time
	 * 2. accumulate number processed
	 * 
	 * Also, accumulate number of (CR provided) CASes processed.
	 * 
	 */
	public void update(DuccLogger duccLogger, List<AnalysisEnginePerformanceMetrics> list) {
		String methodName = "update";
		int  count = casCount.addAndGet(1);
		for(AnalysisEnginePerformanceMetrics item : list ) {
			String key = getKey(item);
			String displayName = getDisplayName(item);
			addEntry(key,displayName);
			PerformanceMetricsSummaryItem summaryItem = map.get(key);
			synchronized(map) {
				long timeBefore = summaryItem.getAnalysisTime();
				long timeItem   = item.getAnalysisTime();
				long timeAfter  = summaryItem.addAndGetAnalysisTime(item.getAnalysisTime());
				long numbBefore = summaryItem.getNumProcessed();
				long numbItem   = item.getNumProcessed();
				long numbAfter  = summaryItem.addAndGetNumProcessed(item.getNumProcessed());
				if(duccLogger != null) {
					String t0 = "count:"+count;
					String t1 = "Numb before:"+numbBefore+" item:"+numbItem+" after:"+numbAfter;
					String t2 = "Time before:"+timeBefore+" item:"+timeItem+" after:"+timeAfter;
					String text = t0+" "+t1+" "+t2;
					duccLogger.debug(methodName, null, text);
				}
			}
		}
	}
	
	public void update(List<AnalysisEnginePerformanceMetrics> list) {
		update(null, list);
	}
	
	public Set<Entry<String, PerformanceMetricsSummaryItem>> entrySet() {
		return map.entrySet();
	}
	
	public int size() {
		return map.size();
	}
	
	public int casCount() {
		return casCount.get();
	}
	
	protected void putItem(String key, PerformanceMetricsSummaryItem value) {
		map.put(key, value);
	}
	
	protected void putCasCount(int value) {
		casCount.set(value);
	}
}
