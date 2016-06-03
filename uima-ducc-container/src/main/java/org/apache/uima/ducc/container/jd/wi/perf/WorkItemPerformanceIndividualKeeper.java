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

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.uima.ducc.common.utils.FormatHelper;
import org.apache.uima.ducc.common.utils.IOHelper;
import org.apache.uima.ducc.common.utils.FormatHelper.Precision;
import org.apache.uima.ducc.container.common.FlagsExtendedHelper;
import org.apache.uima.ducc.container.common.IJdConstants;
import org.apache.uima.ducc.container.common.MessageBuffer;
import org.apache.uima.ducc.container.common.Standardize;
import org.apache.uima.ducc.container.common.SynchronizedStats;
import org.apache.uima.ducc.container.common.logger.IComponent;
import org.apache.uima.ducc.container.common.logger.ILogger;
import org.apache.uima.ducc.container.common.logger.Logger;
import org.apache.uima.ducc.container.jd.wi.perf.files.JobPerformanceIndividual;
import org.apache.uima.ducc.container.jd.wi.perf.files.JobPerformanceIndividualData;
import org.apache.uima.ducc.container.jd.wi.perf.files.JobPerformanceIndividualJsonGz;
import org.apache.uima.ducc.container.jd.wi.perf.files.PerformanceMetricsIndividualItem;

public class WorkItemPerformanceIndividualKeeper implements IWorkItemPerformanceIndividualKeeper {
	
	private static Logger logger = Logger.getLogger(WorkItemPerformanceIndividualKeeper.class, IComponent.Id.JD.name());
	
	boolean isIndividualWorkItemPerformance = FlagsExtendedHelper.getInstance().isIndividualWorkItemPerformance();
	
	private String logFolder = null;
	private String wiNo = null;
	
	private ConcurrentHashMap<PerfKey, SynchronizedStats> map = new ConcurrentHashMap<PerfKey, SynchronizedStats>();

	public WorkItemPerformanceIndividualKeeper(String logDir, String wiNo) {
		if(isIndividualWorkItemPerformance) {
			String logFolder = IOHelper.marryDir2File(logDir,IJdConstants.folderNameWorkItemPerformance);
			IOHelper.mkdirs(logFolder);
			setLogFolder(logFolder);
			setWiNo(wiNo);
		}
	}
	
	private void setLogFolder(String value) {
		logFolder = value;
	}
	
	private void setWiNo(String value) {
		wiNo = value;
	}
	
	@Override
	public List<IWorkItemPerformanceIndividualInfo> dataGet() {
		List<IWorkItemPerformanceIndividualInfo> list = new ArrayList<IWorkItemPerformanceIndividualInfo>();
		if(isIndividualWorkItemPerformance) {
			for(Entry<PerfKey, SynchronizedStats> entry : map.entrySet()) {
				String name = entry.getKey().getName();
				String uniqueName = entry.getKey().getUniqueName();
				SynchronizedStats stats = entry.getValue();
				double time = stats.getSum();
				IWorkItemPerformanceIndividualInfo item = new WorkItemPerformanceIndividualInfo(
						name,
						uniqueName,
						time
						);
				list.add(item);
			}
		}
		return list;
	}
	
	@Override
	public void dataAdd(String name, String uniqueName, long time) {
		String location = "dataAdd";
		if(isIndividualWorkItemPerformance) {
			try {
				// name
				PerfKey perfKey = new PerfKey(name, uniqueName);
				if(!map.containsKey(perfKey)) {
					map.putIfAbsent(perfKey, new SynchronizedStats());
				}
				// stats
				SynchronizedStats stats = map.get(perfKey);
				stats.addValue(time);
				// sum
				long lTimeSum = (long)stats.getSum();
				String timeSum = FormatHelper.duration(lTimeSum,Precision.Tenths);
				// log
				MessageBuffer mb = new MessageBuffer();
				mb.append(Standardize.Label.name.get()+name);
				mb.append(Standardize.Label.sum.get()+timeSum);
				if(lTimeSum < 0) {
					logger.warn(location, ILogger.null_id, mb.toString());
				}
				else {
					logger.debug(location, ILogger.null_id, mb.toString());
				}
			}
			catch(Exception e) {
				logger.error(location, ILogger.null_id, e);
			}
		}
	}
	
	private PerformanceMetricsIndividualItem create(IWorkItemPerformanceIndividualInfo wipii) {
		PerformanceMetricsIndividualItem retVal = new PerformanceMetricsIndividualItem(
				wipii.getName(),
				wipii.getUniqueName(),
				(long)wipii.getTime()
				);
		return retVal;
	}
	
	public void publish() {	
		String location = "publish";
		if(isIndividualWorkItemPerformance) {
			try {
				List<IWorkItemPerformanceIndividualInfo> list = dataGet();
				ConcurrentSkipListMap<String, JobPerformanceIndividual> map = new ConcurrentSkipListMap<String, JobPerformanceIndividual>();
				for(IWorkItemPerformanceIndividualInfo wipii : list) {
					PerformanceMetricsIndividualItem item = create(wipii);
					JobPerformanceIndividual jps = new JobPerformanceIndividual();
					jps.setAnalysisTime(item.getAnalysisTime());
					jps.setName(item.getName());
					jps.setUniqueName(item.getUniqueName());
					map.put(jps.getUniqueName(), jps);
				}
				JobPerformanceIndividualData data = new JobPerformanceIndividualData(map);
				JobPerformanceIndividualJsonGz jsonGz = new JobPerformanceIndividualJsonGz(logFolder, wiNo);
				jsonGz.exportData(data);
			}
			catch(Exception e) {
				logger.error(location, ILogger.null_id, e);
			}
		}
	}
}
