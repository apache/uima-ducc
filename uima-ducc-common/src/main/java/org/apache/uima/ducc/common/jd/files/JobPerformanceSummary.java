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
package org.apache.uima.ducc.common.jd.files;

import java.util.concurrent.atomic.AtomicLong;

public class JobPerformanceSummary implements IJobPerformanceSummary {

	private String name;
	private String uniqueName;
	
	private AtomicLong analysisTime = new AtomicLong(0);
	private AtomicLong numProcessed = new AtomicLong(0);
	
	private AtomicLong analysisTimeMin = new AtomicLong(-1);
	private AtomicLong analysisTimeMax = new AtomicLong(-1);
	
	private AtomicLong analysisTasks = new AtomicLong(0);

	public String getName() {
		return name;
	}


	public void setName(String value) {
		name = value;
	}


	public String getUniqueName() {
		return uniqueName;
	}

	public void setUniqueName(String value) {
		uniqueName = value;
	}


	public long getAnalysisTime() {
		return analysisTime.get();
	}


	public void setAnalysisTime(long value) {
		analysisTime.set(value);
	}


	public long getAnalysisTimeMin() {
		return analysisTimeMin.get();
	}

	public void setAnalysisTimeMin(long value) {
		analysisTimeMin.set(value);
	}


	public long getAnalysisTimeMax() {
		return analysisTimeMax.get();
	}


	public void setAnalysisTimeMax(long value) {
		analysisTimeMax.set(value);
	}
	
	
	public long getAnalysisTasks() {
		return analysisTasks.get();
	}


	public void setAnalysisTasks(long value) {
		analysisTasks.set(value);
	}
	

	public long getNumProcessed() {
		return numProcessed.get();
	}


	public void setNumProcessed(long value) {
		numProcessed.set(value);
	}

}
