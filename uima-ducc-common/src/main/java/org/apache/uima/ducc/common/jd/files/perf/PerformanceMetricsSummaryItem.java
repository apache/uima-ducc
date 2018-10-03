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
package org.apache.uima.ducc.common.jd.files.perf;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

public class PerformanceMetricsSummaryItem implements Serializable {
	/**
	 * please increment this sUID when removing or modifying a field 
	 */
	private static final long serialVersionUID = 1L;
	
	private String name;
	private String uniqueName;
	
	private AtomicLong analysisTime = new AtomicLong(0);
	private AtomicLong numProcessed = new AtomicLong(0);
	
	private AtomicLong analysisTimeMin = new AtomicLong(-1);
	private AtomicLong analysisTimeMax = new AtomicLong(-1);
	
	private AtomicLong analysisTasks = new AtomicLong(0);
	
	public PerformanceMetricsSummaryItem(String name, String uniqueName) {
		this.name = name;
		this.uniqueName = uniqueName;
	}
	
	public PerformanceMetricsSummaryItem(String name, 
										 String uniqueName,
										 long analysisTime,
										 long numProcessed,
										 long analysisTimeMin,
										 long analysisTimeMax,
										 long analysisTasks
										 ) 
	{
		this.name = name;
		this.uniqueName = uniqueName;
		this.analysisTime.set(analysisTime);
		this.numProcessed.set(numProcessed);
		this.analysisTimeMin.set(analysisTimeMin);
		this.analysisTimeMax.set(analysisTimeMax);
		this.analysisTasks.set(analysisTasks);
	}
	
	public String getName() {
		return name;
	}
	
	public static String delim_old = PerformanceMetricsSummaryMap.delim_old;
	public static String delim_new = PerformanceMetricsSummaryMap.delim_new;
	
	public String getDisplayName() {
		String itemName = getName();
		String displayName = itemName;
		try {
			if(itemName.contains(delim_old)) {
				displayName = itemName.split(delim_old,2)[1];
			}
			else if(itemName.contains(delim_new)) {
				displayName = itemName.split(delim_new,2)[1];
			}
			else {
				displayName = itemName;
			}
		}
		catch(Throwable t) {
		}
		return displayName;
	}
	
	public String getUniqueName() {
		return uniqueName;
	}
	
	public long getAnalysisTime() {
		return analysisTime.get();
	}
	
	public long getAnalysisTimeMin() {
		return analysisTimeMin.get();
	}
	
	public long getAnalysisTimeMax() {
		return analysisTimeMax.get();
	}
	
	public long getAnalysisTasks() {
		return analysisTasks.get();
	}
	
	public long getNumProcessed() {
		return numProcessed.get();
	}
	
	//
	
	private void updateAnalysisTimeMin(long delta) {
		long currentValue = analysisTimeMin.get();
		if(currentValue < 0) {
			analysisTimeMin.compareAndSet(currentValue, delta);
			currentValue = analysisTimeMin.get();
		}
		while(currentValue > delta) {
			analysisTimeMin.compareAndSet(currentValue, delta);
			currentValue = analysisTimeMin.get();
		}
	}
	
	private void updateAnalysisTimeMax(long delta) {
		long currentValue = analysisTimeMax.get();
		if(currentValue < 0) {
			analysisTimeMax.compareAndSet(currentValue, delta);
			currentValue = analysisTimeMax.get();
		}
		while(currentValue < delta) {
			analysisTimeMax.compareAndSet(currentValue, delta);
			currentValue = analysisTimeMax.get();
		}
	}
	
	public long addAndGetAnalysisTime(long delta) {
		updateAnalysisTimeMin(delta);
		updateAnalysisTimeMax(delta);
		return analysisTime.addAndGet(delta);
	}
	
	public long addAndGetAnalysisTasks(long delta) {
		return analysisTasks.addAndGet(delta);
	}
	
	public long addAndGetNumProcessed(long delta) {
		return numProcessed.addAndGet(delta);
	}
}
