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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.uima.ducc.common.jd.files.IJobPerformanceSummary;
import org.apache.uima.ducc.common.jd.files.JobPerformanceSummary;
import org.apache.uima.ducc.common.jd.files.JobPerformanceSummaryData;

public class PerformanceSummaryReader extends PerformanceSummaryBase {
	
	public PerformanceSummaryReader(String dirname) {
		super(dirname);
	}
	
	public PerformanceMetricsSummaryMap readJsonGz() throws IOException, ClassNotFoundException {
		PerformanceMetricsSummaryMap map = new PerformanceMetricsSummaryMap();
		JobPerformanceSummaryData data = jsonGz.importData();
		Integer casCount = data.getCasCount();
		map.putCasCount(casCount);
		ConcurrentSkipListMap<String, JobPerformanceSummary> gzMap = data.getMap();
		Set<Entry<String, JobPerformanceSummary>> entries = gzMap.entrySet();
		for(Entry<String, JobPerformanceSummary> entry : entries) {
			String key = entry.getKey();
			IJobPerformanceSummary jps = entry.getValue();
			PerformanceMetricsSummaryItem value = new PerformanceMetricsSummaryItem(jps.getName(),jps.getUniqueName(),jps.getAnalysisTime(),jps.getNumProcessed(),jps.getAnalysisTimeMin(),jps.getAnalysisTimeMax());
			map.putItem(key, value);
		}
		return map;
	}
	
	public PerformanceMetricsSummaryMap readJsonGz(String userId) throws IOException, ClassNotFoundException {
		PerformanceMetricsSummaryMap map = new PerformanceMetricsSummaryMap();
		JobPerformanceSummaryData data = null;
		if(data == null) {
			try {
				data = jsonGz.importData(userId);
			}
			catch(Exception e) {
			}
		}
		if(data == null) {
			try {
				data = jsonGz.importData();
			}
			catch(Exception e) {
			}
		}
		Integer casCount = data.getCasCount();
		map.putCasCount(casCount);
		ConcurrentSkipListMap<String, JobPerformanceSummary> gzMap = data.getMap();
		Set<Entry<String, JobPerformanceSummary>> entries = gzMap.entrySet();
		for(Entry<String, JobPerformanceSummary> entry : entries) {
			String key = entry.getKey();
			IJobPerformanceSummary jps = entry.getValue();
			PerformanceMetricsSummaryItem value = new PerformanceMetricsSummaryItem(jps.getName(),jps.getUniqueName(),jps.getAnalysisTime(),jps.getNumProcessed(),jps.getAnalysisTimeMin(),jps.getAnalysisTimeMax());
			map.putItem(key, value);
		}
		return map;
	}
	
	@Deprecated
	private boolean legacy = true;
	
	@Deprecated
	private PerformanceMetricsSummaryMap readSer() {
		PerformanceMetricsSummaryMap map = null;
		if(legacy) {
			try {
				FileInputStream fis = new FileInputStream(filename);
				ObjectInputStream in = new ObjectInputStream(fis);
				summaryMap = (PerformanceMetricsSummaryMap)in.readObject();
				in.close();
				map = getSummaryMap();
			}
			catch(Exception e) {
				System.err.println("PerformanceMetricsSummaryMap.readSer() could not read file: "+ filename);
			}
		}
		return map;
	}
	
	public PerformanceMetricsSummaryMap readSummary() {
		PerformanceMetricsSummaryMap map = null;
		try {
			map = readJsonGz();
			return map;
		}
		catch(Exception e) {
			if(!legacy) {
				e.printStackTrace();
			}
		}
		map = readSer();
		return map;
	}
	
	public PerformanceMetricsSummaryMap readSummary(String userId) {
		PerformanceMetricsSummaryMap map = null;
		try {
			map = readJsonGz(userId);
			return map;
		}
		catch(Exception e) {
			if(!legacy) {
				e.printStackTrace();
			}
		}
		map = readSer();
		return map;
	}
}
