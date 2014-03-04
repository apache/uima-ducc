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
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.lang.reflect.Type;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.zip.GZIPInputStream;

import org.apache.uima.ducc.common.jd.files.IWorkItemState;
import org.apache.uima.ducc.common.jd.files.WorkItemState;
import org.apache.uima.ducc.common.jd.files.workitem.legacy.WorkItemStateManager;
import org.apache.uima.ducc.common.utils.DuccLogger;

import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;

@SuppressWarnings("deprecation")
public class WorkItemStateReader extends WorkItemStateAbstract {
	
	protected DuccLogger logger = DuccLogger.getLogger(WorkItemStateReader.class, null);
	
	private long MaxRecords = 4096;
	
	private WorkItemStateManager wism = null;
	
	private String component = null;
	private String directory = null;
	private String user = null;
	private long wiVersion = 0;
	
	public WorkItemStateReader(String component, String directory, String user, long wiVersion) {
		logger = DuccLogger.getLogger(WorkItemStateKeeper.class, component);
		setComponent(component);
		setDirectory(directory);
		setUser(user);
		setWiVersion(wiVersion);
		initialize();
	}
	
	
	private void setComponent(String value) {
		component = value;
	}
	
	private void setDirectory(String value) {
		directory = value;
	}
	
	private void setUser(String value) {
		user = value;
	}
	
	private void setWiVersion(long value) {
		wiVersion = value;
	}
	
	private void initialize() {
		if(wiVersion == 0) {
			wism = new WorkItemStateManager(component, directory, user);
		}
		else {
			super.initialize(directory);
		}
		
	}
	
	public ConcurrentSkipListMap<Long,IWorkItemState> getMap() {
		long lastRecordNo = 0;
		long maxRecords = MaxRecords;
		return getMap(lastRecordNo, maxRecords);
	}
	
	public ConcurrentSkipListMap<Long,IWorkItemState> getMap(long lastRecordNo, long maxRecords) {
		String location = "getMap";
		ConcurrentSkipListMap<Long,IWorkItemState> map = new ConcurrentSkipListMap<Long, IWorkItemState>();;
		if(wiVersion == 0) {
			map = wism.getMap();
		}
		else {
			map = new ConcurrentSkipListMap<Long,IWorkItemState>();
			try {
				fetchZipped(map, lastRecordNo, maxRecords);
				if(map.isEmpty()) {
					fetchActive(map);
					fetchCompleted(map, lastRecordNo, maxRecords);
				}
			}
			catch(Exception e) {
				logger.error(location, jobid, e);
			}
		}
		return map;
	}
	
	private void fetchActive(ConcurrentSkipListMap<Long,IWorkItemState> map) throws IOException {
		String location = "fetchActive";
		InputStream fis = null;
		InputStreamReader isr = null;
		BufferedReader br = null;
		String line = null;
		try {
			String targetFile = fnActiveJson;
			fis = new FileInputStream(targetFile);
			isr = new InputStreamReader(fis);
			br = new BufferedReader(isr);
	        Type type = new TypeToken<WorkItemState>() { }.getType();
	        line = br.readLine();
			while(line != null) {
				logger.trace(location, jobid, line);
				StringReader sr = new StringReader(line.trim());
				WorkItemState wis = gson.fromJson(sr, type);
				Long key = Long.parseLong(wis.getSeqNo());
				IWorkItemState value = wis;
				map.put(key, value);
				line = br.readLine();
			}
		}
		finally {
			if(br != null) {
				br.close();
			}
		}
	}

	private void trim(IWorkItemState wis, ConcurrentSkipListMap<Long,IWorkItemState> map, long lastRecordNo, long maxRecords) throws IOException {
		String location = "trim";
		if(maxRecords > 0) {
			if(map.size() > maxRecords) {
				RemoveFirstCompleted: 
					for(Entry<Long, IWorkItemState> entry : map.entrySet()) {
						Long key = entry.getKey();
						switch(entry.getValue().getState()) {
						case ended:
						case error:
							map.remove(key);
							logger.debug(location, jobid, "seqNo:"+key);
							break RemoveFirstCompleted;
						}
					}
			}
		}
	}
	
	private void tryToAdd(IWorkItemState wis, ConcurrentSkipListMap<Long,IWorkItemState> map, long lastRecordNo, long maxRecords) throws IOException {
		String location = "tryToAdd";
		Long key = Long.parseLong(wis.getSeqNo());
		IWorkItemState value = wis;
		if(lastRecordNo > 0) {
			if(key <= lastRecordNo) {
				map.put(key, value);
				logger.debug(location, jobid, "seqNo:"+key);
			}
		}
		else {
			map.put(key, value);
		}
		trim(wis, map, lastRecordNo, maxRecords);
	}
	
	private void fetchCompleted(ConcurrentSkipListMap<Long,IWorkItemState> map, long lastRecordNo, long maxRecords) throws IOException {
		String location = "fetchCompleted";
		InputStream fis = null;
		InputStreamReader isr = null;
		BufferedReader br = null;
		String line = null;
		try {
			String targetFile = fnJson;
			fis = new FileInputStream(targetFile);
			isr = new InputStreamReader(fis);
			br = new BufferedReader(isr);
	        Type type = new TypeToken<WorkItemState>() { }.getType();
	        line = br.readLine();
			while(line != null) {
				logger.trace(location, jobid, line);
				StringReader sr = new StringReader(line.trim());
				WorkItemState wis = gson.fromJson(sr, type);
				tryToAdd(wis, map, lastRecordNo, maxRecords);
				if(lastRecordNo == 0) {
					if(maxRecords != 0) {
						if(map.size() >= maxRecords) {
							break;
						}
					}
				}
				line = br.readLine();
			}
		}
		finally {
			if(br != null) {
				br.close();
			}
		}
	}
	
	private void fetchZipped(ConcurrentSkipListMap<Long,IWorkItemState> map, long lastRecordNo, long maxRecords) throws IOException {
		String location = "fetchZipped";
		File file = null;
		FileInputStream fis = null;
		GZIPInputStream gis = null;
		InputStreamReader isr = null;
		JsonReader jr = null;
		try {
			String targetFile = fnJsonGz;
			file = new File(targetFile);
			fis = new FileInputStream(file);
			gis = new GZIPInputStream(fis);
			isr = new InputStreamReader(gis, encoding);
			jr = new JsonReader(isr);
			jr.beginArray();
			while(jr.hasNext()) {
				IWorkItemState wis = gson.fromJson(jr, WorkItemState.class);
				tryToAdd(wis, map, lastRecordNo, maxRecords);
				if(lastRecordNo == 0) {
					if(maxRecords != 0) {
						if(map.size() >= maxRecords) {
							break;
						}
					}
				}
			}
			jr.endArray();
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
		}
		finally {
			if(jr != null) {
				jr.close();
			}
		}
	}
	
	public double getMin() {
		double retVal = 0;
		if(wiVersion == 0) {
			retVal = wism.getMin();
		}
		return retVal;
	}
	
	public double getMax() {
		double retVal = 0;
		if(wiVersion == 0) {
			retVal = wism.getMax();
		}
		return retVal;
	}
	
	public double getAvg() {
		double retVal = 0;
		if(wiVersion == 0) {
			retVal = wism.getAvg();
		}
		return retVal;
	}
	
}
