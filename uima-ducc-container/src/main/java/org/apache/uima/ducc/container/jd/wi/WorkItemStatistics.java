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

import org.apache.uima.ducc.container.common.Assertion;
import org.apache.uima.ducc.container.common.SynchronizedStats;

public class WorkItemStatistics implements IWorkItemStatistics {

	private SynchronizedStats stats = new SynchronizedStats();
	
	private long mintime = 0;
	
	@Override
	public void ended(IWorkItem wi) {
		long time = wi.getMillisOperating();
		Assertion.nonNegative(time);
		if(time < mintime) {
			time = mintime;
		}
		stats.addValue(time);
	}
	
	@Override
	public long getMillisMin() {
		double stat = stats.getMin();
		if(stat == Double.MAX_VALUE) {
			stat = 0;
		};
		long value = (long) stat;
		return value;
	}
	
	@Override
	public long getMillisMax() {
		double stat = stats.getMax();
		long value = (long) stat;
		return value;
	}
	
	@Override
	public long getMillisAvg() {
		double stat = stats.getMean();
		long value = (long) stat;
		return value;
	}
}
