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

import java.util.concurrent.ConcurrentSkipListMap;

public class JobPerformanceSummaryData {

	private ConcurrentSkipListMap<String, JobPerformanceSummary> map = new ConcurrentSkipListMap<String, JobPerformanceSummary>();
	private Integer casCount = new Integer(0);
	
	public JobPerformanceSummaryData() {
	}
	
	public JobPerformanceSummaryData(ConcurrentSkipListMap<String, JobPerformanceSummary> map, Integer casCount) {
		setMap(map);
		setCasCount(casCount);
	}
	
	public ConcurrentSkipListMap<String, JobPerformanceSummary> getMap() {
		return map;
	}

	public void setMap(ConcurrentSkipListMap<String, JobPerformanceSummary> value) {
		map = value;
	}

	public Integer getCasCount() {
		return casCount;
	}

	public void setCasCount(Integer value) {
		casCount = value;
	}

}
