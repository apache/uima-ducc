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

	@Override
	public String getName() {
		return name;
	}

	@Override
	public void setName(String value) {
		name = value;
	}

	@Override
	public String getUniqueName() {
		return uniqueName;
	}

	@Override
	public void setUniqueName(String value) {
		uniqueName = value;
	}

	@Override
	public long getAnalysisTime() {
		return analysisTime.get();
	}

	@Override
	public void setAnalysisTime(long value) {
		analysisTime.set(value);
	}

	@Override
	public long getAnalysisTimeMin() {
		return analysisTimeMin.get();
	}

	@Override
	public void setAnalysisTimeMin(long value) {
		analysisTimeMin.set(value);
	}

	@Override
	public long getAnalysisTimeMax() {
		return analysisTimeMax.get();
	}

	@Override
	public void setAnalysisTimeMax(long value) {
		analysisTimeMax.set(value);
	}

	@Override
	public long getNumProcessed() {
		return numProcessed.get();
	}

	@Override
	public void setNumProcessed(long value) {
		numProcessed.set(value);
	}

}
