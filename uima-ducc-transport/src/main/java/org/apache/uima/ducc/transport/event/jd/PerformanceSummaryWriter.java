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

import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.uima.ducc.common.jd.files.JobPerformanceSummary;
import org.apache.uima.ducc.common.jd.files.JobPerformanceSummaryData;

public class PerformanceSummaryWriter extends PerformanceSummaryReader {
	
	public PerformanceSummaryWriter(String dirname) {
		super(dirname);
	}
	
	private void writeJsonGz() {
		try {
			ConcurrentSkipListMap<String, JobPerformanceSummary> map = new ConcurrentSkipListMap<String, JobPerformanceSummary>();
			Set<Entry<String, PerformanceMetricsSummaryItem>> entries = summaryMap.entrySet();
			for(Entry<String, PerformanceMetricsSummaryItem> entry : entries) {
				PerformanceMetricsSummaryItem item = entry.getValue();
				JobPerformanceSummary jps = new JobPerformanceSummary();
				jps.setAnalysisTime(item.getAnalysisTime());
				jps.setAnalysisTimeMax(item.getAnalysisTimeMax());
				jps.setAnalysisTimeMin(item.getAnalysisTimeMin());
				jps.setNumProcessed(item.getNumProcessed());
				jps.setName(item.getName());
				jps.setUniqueName(item.getUniqueName());
				map.put(jps.getUniqueName(), jps);
			}
			
			Integer casCount = summaryMap.casCount();
			JobPerformanceSummaryData data = new JobPerformanceSummaryData(map,casCount);
			jsonGz.exportData(data);
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	@Deprecated
	private boolean legacy = false;
	
	@Deprecated
	private void writeSer(PerformanceMetricsSummaryMap map) {
		if(legacy) {
			try {
				FileOutputStream fos = new FileOutputStream(filename);
				ObjectOutputStream out = new ObjectOutputStream(fos);
				out.writeObject(map);
				out.close();
			}
			catch(Exception e) {
				System.err.println("PerformanceMetricsSummaryMap.writeSer() could not write file: "+ filename);
			}
		}
	}
	
	public void writeSummary() {
		writeSer(summaryMap);
		try {
			writeJsonGz();
			return;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
}
