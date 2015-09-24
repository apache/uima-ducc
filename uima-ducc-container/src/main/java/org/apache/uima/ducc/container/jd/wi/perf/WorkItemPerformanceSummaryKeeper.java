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
import java.util.concurrent.atomic.AtomicLong;

import org.apache.uima.ducc.common.jd.files.JobPerformanceSummary;
import org.apache.uima.ducc.common.jd.files.JobPerformanceSummaryData;
import org.apache.uima.ducc.common.jd.files.JobPerformanceSummaryJsonGz;
import org.apache.uima.ducc.common.jd.files.perf.PerformanceMetricsSummaryItem;
import org.apache.uima.ducc.common.utils.FormatHelper;
import org.apache.uima.ducc.common.utils.FormatHelper.Precision;
import org.apache.uima.ducc.container.common.MessageBuffer;
import org.apache.uima.ducc.container.common.Standardize;
import org.apache.uima.ducc.container.common.SynchronizedStats;
import org.apache.uima.ducc.container.common.logger.IComponent;
import org.apache.uima.ducc.container.common.logger.ILogger;
import org.apache.uima.ducc.container.common.logger.Logger;

public class WorkItemPerformanceSummaryKeeper implements IWorkItemPerformanceSummaryKeeper {
	
	private static Logger logger = Logger.getLogger(WorkItemPerformanceSummaryKeeper.class, IComponent.Id.JD.name());
	
	private String logDir = null;
	
	private AtomicLong count = new AtomicLong(0);
	private AtomicLong total = new AtomicLong(0);
	
	private ConcurrentHashMap<PerfKey, SynchronizedStats> map = new ConcurrentHashMap<PerfKey, SynchronizedStats>();

	public WorkItemPerformanceSummaryKeeper(String logDir) {
		setLogDir(logDir);
	}
	
	private void setLogDir(String value) {
		logDir = value;
	}
	
	@Override
	public List<IWorkItemPerformanceSummaryInfo> dataGet() {
		List<IWorkItemPerformanceSummaryInfo> list = new ArrayList<IWorkItemPerformanceSummaryInfo>();
		for(Entry<PerfKey, SynchronizedStats> entry : map.entrySet()) {
			String name = entry.getKey().getName();
			String uniqueName = entry.getKey().getUniqueName();
			SynchronizedStats stats = entry.getValue();
			double count = stats.getNum();
			double time = stats.getSum();
			double pctOfTime = 0;
			if(total.get()> 0) {
				pctOfTime = time*(100/total.get());
			}
			double avg = stats.getMean();
			double min = stats.getMin();
			double max = stats.getMax();
			IWorkItemPerformanceSummaryInfo item = new WorkItemPerformanceSummaryInfo(
					name,
					uniqueName,
					count,
					time,
					pctOfTime,
					avg,
					min,
					max
					);
			list.add(item);
		}
		return list;
	}
	
	@Override
	public void count() {
		count.addAndGet(1);
	}
	
	@Override
	public void dataAdd(String name, String uniqueName, long time) {
		String location = "dataAdd";
		try {
			// name
			PerfKey perfKey = new PerfKey(name, uniqueName);
			if(!map.containsKey(perfKey)) {
				map.putIfAbsent(perfKey, new SynchronizedStats());
			}
			// stats
			SynchronizedStats stats = map.get(perfKey);
			stats.addValue(time);
			total.addAndGet(time);
			// sum
			long lTimeSum = (long)stats.getSum();
			String timeSum = FormatHelper.duration(lTimeSum,Precision.Tenths);
			// avg
			long lTimeAvg = (long)stats.getMean();
			String timeAvg = FormatHelper.duration(lTimeAvg,Precision.Tenths);
			// min
			long lTimeMin = (long)stats.getMin();
			String timeMin = FormatHelper.duration(lTimeMin,Precision.Tenths);
			// max
			long lTimeMax = (long)stats.getMax();
			String timeMax = FormatHelper.duration(lTimeMax,Precision.Tenths);
			// log
			MessageBuffer mb = new MessageBuffer();
			mb.append(Standardize.Label.name.get()+name);
			mb.append(Standardize.Label.sum.get()+timeSum);
			mb.append(Standardize.Label.avg.get()+timeAvg);
			mb.append(Standardize.Label.min.get()+timeMin);
			mb.append(Standardize.Label.max.get()+timeMax);
			mb.append(Standardize.Label.count.get()+count.get());
			mb.append(Standardize.Label.total.get()+total.get());
			logger.debug(location, ILogger.null_id, mb.toString());
		}
		catch(Exception e) {
			logger.error(location, ILogger.null_id, e);
		}
	}
	
	private PerformanceMetricsSummaryItem create(IWorkItemPerformanceSummaryInfo wipsi) {
		PerformanceMetricsSummaryItem retVal = new PerformanceMetricsSummaryItem(
				wipsi.getName(),
				wipsi.getUniqueName(),
				(long)wipsi.getTime(),
				(long)wipsi.getCount(),
				(long)wipsi.getMin(),
				(long)wipsi.getMax()
				);
		return retVal;
	}
	
	public void publish() {	
		String location = "publish";
		try {
			List<IWorkItemPerformanceSummaryInfo> list = dataGet();
			ConcurrentSkipListMap<String, JobPerformanceSummary> map = new ConcurrentSkipListMap<String, JobPerformanceSummary>();
			for(IWorkItemPerformanceSummaryInfo wipsi : list) {
				PerformanceMetricsSummaryItem item = create(wipsi);
				JobPerformanceSummary jps = new JobPerformanceSummary();
				jps.setAnalysisTime(item.getAnalysisTime());
				jps.setAnalysisTimeMax(item.getAnalysisTimeMax());
				jps.setAnalysisTimeMin(item.getAnalysisTimeMin());
				jps.setNumProcessed(item.getNumProcessed());
				jps.setName(item.getName());
				jps.setUniqueName(item.getUniqueName());
				map.put(jps.getUniqueName(), jps);
			}
			Integer casCount = new Integer((int)count.get());
			JobPerformanceSummaryData data = new JobPerformanceSummaryData(map,casCount);
			JobPerformanceSummaryJsonGz jsonGz = new JobPerformanceSummaryJsonGz(logDir);
			jsonGz.exportData(data);
		}
		catch(Exception e) {
			logger.error(location, ILogger.null_id, e);
		}
	}
	
}
