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
package org.apache.uima.ducc.container.jd.wi.perf;

public class WorkItemPerformanceSummaryInfo implements IWorkItemPerformanceSummaryInfo {

	String name = null;
	String uniqueName = null;
	double count = 0;
	double time = 0;
	double pctOfTime = 0;
	double avg = 0;
	double min = 0;
	double max = 0;
	
	public WorkItemPerformanceSummaryInfo(
			String name,
			String uniquename,
			double count,
			double time,
			double pctOfTime,
			double avg,
			double min,
			double max
			) 
	{
		setName(name);
		setUniqueName(uniquename);
		setCount(count);
		setTime(time);
		setPctOfTime(pctOfTime);
		setAvg(avg);
		setMin(min);
		setMax(max);
	}
	
	private void setName(String value) {
		name = value;
	}
	
	@Override
	public String getName() {
		return name;
	}
	
	private void setUniqueName(String value) {
		uniqueName = value;
	}
	
	@Override
	public String getUniqueName() {
		return uniqueName;
	}

	private void setCount(double value) {
		count = value;
	}
	
	@Override
	public double getCount() {
		return count;
	}
	
	private void setTime(double value) {
		time = value;
	}
	
	@Override
	public double getTime() {
		return time;
	}
	
	private void setPctOfTime(double value) {
		pctOfTime = value;
	}

	@Override
	public double getPctOfTime() {
		return pctOfTime;
	}

	private void setAvg(double value) {
		avg = value;
	}
	
	@Override
	public double getAvg() {
		return avg;
	}

	private void setMin(double value) {
		min = value;
	}
	
	@Override
	public double getMin() {
		return min;
	}
	
	private void setMax(double value) {
		max = value;
	}

	@Override
	public double getMax() {
		return max;
	}

}
