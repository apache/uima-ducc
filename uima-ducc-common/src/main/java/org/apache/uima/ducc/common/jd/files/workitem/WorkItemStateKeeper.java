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
package org.apache.uima.ducc.common.jd.files.workitem;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.lang.reflect.Type;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.GZIPOutputStream;

import org.apache.uima.ducc.common.jd.files.IWorkItemState;
import org.apache.uima.ducc.common.jd.files.IWorkItemState.State;
import org.apache.uima.ducc.common.jd.files.WorkItemState;
import org.apache.uima.ducc.common.utils.DuccLogger;

import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonWriter;

public class WorkItemStateKeeper extends WorkItemStateAbstract {
	
	private DuccLogger logger = DuccLogger.getLogger(WorkItemStateKeeper.class, null);
	
	public WorkItemStateKeeper(String component, String directory) {
		logger = DuccLogger.getLogger(WorkItemStateKeeper.class, component);
		initialize(directory);
	}
	
	public ConcurrentHashMap<RemoteLocation, Long> getOperatingMillisMap() {
		DuccLogger logger = null;
		return getOperatingMillisMap(logger);
	}
	
	public ConcurrentHashMap<RemoteLocation, Long> getOperatingMillisMap(DuccLogger logger) {
		String location = "getOperatingMillisMap";
		ConcurrentHashMap<RemoteLocation, Long> map = new ConcurrentHashMap<RemoteLocation, Long>();
		if(logger!= null) {
			logger.trace(location, jobid, "size: "+activeMap.size());
		}
		for(Entry<Long, IWorkItemState> entry : activeMap.entrySet()) {
			IWorkItemState wis = entry.getValue();
			State state = wis.getState();
			String pid = wis.getPid();
			String node = wis.getNode();
			switch(state) {
			case operating:
				RemoteLocation key = new RemoteLocation(node, pid);
				if(key != null) {
					Long value = new Long(wis.getMillisProcessing());
					if(logger != null) {
						logger.trace(location, jobid, "node: "+node+" "+"pid: "+pid+" "+"time: "+value);
					}
					if(map.contains(key)) {
						value += map.get(key);
					}
					map.put(key,value);
				}
				break;
			}
			
		}
		if(logger != null) {
			for(Entry<RemoteLocation, Long> entry : map.entrySet()) {
				RemoteLocation key = entry.getKey();
				String nodeIP = key.getNodeIP();
				String pid = key.getPid();
				Long time = map.get(key);
				logger.trace(location, jobid, "nodeIP: "+nodeIP+" "+"pid: "+pid+" "+"time: "+time);
			}
		}
		return map;
	}
	
	public synchronized void zip() {
		String location = "zip";
		try {
			for(Entry<Long, IWorkItemState> entry : activeMap.entrySet()) {
				IWorkItemState wis = entry.getValue();
				recordFinal(wis);
				updateStatistics(wis);
			}
			deleteActive();
			deleteZip();
			transform();
			deleteJson();
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
		}
	}
	
	public void start(int seqNo, String wiId) {
		IWorkItemState wis = new WorkItemState(seqNo);
		Long key = new Long(seqNo);
		activeMap.put(key, wis);
		wis.setWiId(wiId);
		wis.stateStart();
		record(wis);
	}
	
	public void queued(String seqNo) {
		Long key = new Long(seqNo);
		queued(key.intValue());
	}
	
	public void queued(int seqNo) {
		Long key = new Long(seqNo);
		IWorkItemState wis = activeMap.get(key);
		wis.stateQueued();
		record(wis);
	}
	
	public void operating(String seqNo) {
		Long key = new Long(seqNo);
		operating(key.intValue());
	}
	
	public void operating(int seqNo) {
		Long key = new Long(seqNo);
		IWorkItemState wis = activeMap.get(key);
		wis.stateOperating();
		record(wis);
	}
	
	public void ended(int seqNo) {
		Long key = new Long(seqNo);
		IWorkItemState wis = activeMap.get(key);
		wis.stateEnded();
		record(wis);
	}
	
	public void error(int seqNo) {
		Long key = new Long(seqNo);
		IWorkItemState wis = activeMap.get(key);
		wis.stateError();
		record(wis);
	}
	
	public void lost(int seqNo) {
		Long key = new Long(seqNo);
		IWorkItemState wis = activeMap.get(key);
		wis.stateLost();
		record(wis);
	}
	
	public void retry(int seqNo) {
		Long key = new Long(seqNo);
		IWorkItemState wis = activeMap.get(key);
		wis.stateRetry();
		record(wis);
	}
	
	public void location(String seqNo, String node, String pid) {
		Long key = new Long(seqNo);
		location(key.intValue(), node, pid);
	}
	
	public void location(int seqNo, String node, String pid) {
		Long key = new Long(seqNo);
		IWorkItemState wis = activeMap.get(key);
		wis.setNode(node);
		wis.setPid(pid);
		record(wis);
	}
	
	private synchronized void record(IWorkItemState wis) {
		String location = "record";
		try {
			State state = wis.getState();
			String seqNo = wis.getSeqNo();
			Long key = Long.valueOf(seqNo);
			logger.info(location, jobid, "seqNo:"+seqNo+" "+state.name());
			switch(state) {
			case ended:
			case error:
				activeMap.remove(key);
				recordFinal(wis);
				updateStatistics(wis);
				break;
			default:
				activeMap.put(key, wis);
				break;
			}
			recordActive();
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
		}
	}
	
	private void transform() throws IOException {
		String location = "transform";
		InputStream fis = null;
		InputStreamReader isr = null;
		BufferedReader br = null;
		String line = null;
		File fileOut = null;
		FileOutputStream fos = null;
		GZIPOutputStream gos = null;
		OutputStreamWriter osw = null;
		JsonWriter jw = null;
		try {
			String targetFileIn = fnJson;
			String targetFileOut = fnJsonGz;
			fis = new FileInputStream(targetFileIn);
			isr = new InputStreamReader(fis);
			br = new BufferedReader(isr);
			fileOut = new File(targetFileOut);
			fos = new FileOutputStream(fileOut);
			gos = new GZIPOutputStream(fos);
			osw = new OutputStreamWriter(gos, encoding);
			jw = new JsonWriter(osw);
			jw.setIndent("  ");
	        jw.beginArray();
	        Type type = new TypeToken<WorkItemState>() { }.getType();
	        line = br.readLine();
			while(line != null) {
				logger.trace(location, jobid, line);
				StringReader sr = new StringReader(line.trim());
				WorkItemState wis = gson.fromJson(sr, type);
				gson.toJson(wis, WorkItemState.class, jw);
				line = br.readLine();
			}
	        jw.endArray();
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
		}
		finally {
			if(br != null) {
				br.close();
			}
			if(jw != null) {
				jw.close();
			}
		}
	}
	
	private void deleteZip() {
		String targetFile = fnJsonGz;
		File file = new File(targetFile);
		file.delete();
	}
	
	private void deleteActive() {
		String targetFile = fnActiveJson;
		File file = new File(targetFile);
		file.delete();
	}
	
	private void deleteJson() {
		String targetFile = fnJson;
		File file = new File(targetFile);
		file.delete();
	}
	
	private void recordActive() throws IOException {
		FileWriter fw = null;
		String targetFile = fnActiveJson;
		try {
			deleteActive();
			fw = new FileWriter(targetFile, !append);
			for(Entry<Long, IWorkItemState> entry : activeMap.entrySet()) {
				IWorkItemState wis = entry.getValue();
				String json = gson.toJson(wis)+linend;
				fw.write(json);
			}
		}
		finally {
			if(fw != null) {
				fw.close();
			}
		}
	}
	
	private void recordFinal(IWorkItemState wis) throws IOException {
		FileWriter fw = null;
		String targetFile = fnJson;
		try {
			fw = new FileWriter(targetFile, append);
			String json = gson.toJson(wis)+linend;
			fw.write(json);
		}
		finally {
			if(fw != null) {
				fw.close();
			}
		}
	}
	
	public WorkItemStatistics getStatistics() {
		return stats;
	}
	
	private void updateStatistics(IWorkItemState wis) throws IOException {
		long wiMillis = wis.getMillisProcessing();
		if(stats.count > 0) {
			if(wiMillis > stats.millisMax) {
				stats.millisMax = wiMillis;
			}
			if(wiMillis < stats.millisMin) {
				stats.millisMin = wiMillis;
			}
			long total = (stats.count * stats.millisAvg) + wiMillis;
			long count = stats.count + 1;
			long avg = (long) ((1.0 * total) / count);
			stats.count = count;
			stats.millisAvg = avg;
			switch(wis.getState()) {
			case ended:
			case error:
				if(wiMillis > stats.millisCompletedMost) {
					stats.millisCompletedMost = wiMillis;
				}
				break;
			default:
				long operatingLeast = wiMillis;
				for(Entry<Long, IWorkItemState> entry : activeMap.entrySet()) {
					long candidate = entry.getValue().getMillisProcessing();
					if(candidate < operatingLeast) {
						operatingLeast = candidate;
					}
				}
				stats.millisOperatingLeast = operatingLeast;
				break;
			}
		}
		else {
			stats.millisMax = wiMillis;
			stats.millisMin = wiMillis;
			stats.millisAvg = wiMillis;
			stats.count = 1;
			switch(wis.getState()) {
			case ended:
			case error:
				stats.millisCompletedMost = wiMillis;
				stats.millisOperatingLeast = 0;
				break;
			default:
				stats.millisCompletedMost = 0;
				stats.millisOperatingLeast = wiMillis;
				break;
			}
		}
	}
	
}
