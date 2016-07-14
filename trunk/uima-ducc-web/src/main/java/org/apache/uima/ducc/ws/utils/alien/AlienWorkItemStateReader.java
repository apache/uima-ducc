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
package org.apache.uima.ducc.ws.utils.alien;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.lang.reflect.Type;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.uima.ducc.common.jd.files.IWorkItemState;
import org.apache.uima.ducc.common.jd.files.WorkItemState;
import org.apache.uima.ducc.common.jd.files.workitem.WorkItemStateReader;
import org.apache.uima.ducc.common.utils.AlienFile;
import org.apache.uima.ducc.common.utils.DuccLogger;

import com.google.gson.reflect.TypeToken;

public class AlienWorkItemStateReader extends WorkItemStateReader {
	
	protected static DuccLogger logger = DuccLogger.getLogger(AlienWorkItemStateReader.class, null);
	
	private String user = null;
	
	private long MaxRecords = 4096;
	
	public AlienWorkItemStateReader(EffectiveUser eu, String component, String directory, long wiVersion) {
		super(component, directory, eu.get(), wiVersion);
		set_user(eu.get());
	}
	
	private void set_user(String value) {
		user = value;
	}

	@Override
	public ConcurrentSkipListMap<Long,IWorkItemState> getMap() {
		long lastRecordNo = 0;
		long maxRecords = MaxRecords;
		return getMap(lastRecordNo, maxRecords);
	}
	
	private ConcurrentSkipListMap<Long,IWorkItemState> getMap(long lastRecordNo, long maxRecords) {
		String location = "getMap";
		ConcurrentSkipListMap<Long,IWorkItemState> map = null;
		map = new ConcurrentSkipListMap<Long,IWorkItemState>();
		fetch(map,fnActiveJson);
		int sizeActive = map.size();
		fetch(map,fnJson);
		int sizeInactive = map.size() - sizeActive;
		logger.debug(location, jobid, "active:"+sizeActive+" "+"inactive:"+sizeInactive);
		return map;
	}
	
	private Type wisType = new TypeToken<WorkItemState>() { }.getType();
	
	private IWorkItemState convert(ConcurrentSkipListMap<Long,IWorkItemState> map, String line) {
		StringReader sr = new StringReader(line.trim());
		WorkItemState wis = gson.fromJson(sr, wisType);
		Long key = Long.parseLong(wis.getSeqNo());
		IWorkItemState value = wis;
		map.put(key, value);
		return wis;
	}
	
	private void fetch(ConcurrentSkipListMap<Long,IWorkItemState> map, String fn) {
		String location = "fetch";
		AlienFile alienFile = new AlienFile(user, fn);
		InputStreamReader isr = null;
		BufferedReader br = null;
		try {
			isr = alienFile.getInputStreamReader();
			br = new BufferedReader(isr);
			String line;
			while ((line = br.readLine()) != null)   {
				logger.debug(location, jobid, line);
				IWorkItemState wis = convert(map,line);;
				if(wis != null) {
					try {
						long key = Long.parseLong(wis.getSeqNo());
						map.put(key, wis);
					}
					catch(Exception e) {
						logger.error(location, jobid, wis.getSeqNo(), e);
					}
					
				}
			}
		}
		catch(FileNotFoundException e) {
			logger.debug(location, jobid, "File not found");
		}
		catch(IOException e) {
			logger.debug(location, jobid, "File error");
		}
		catch(Exception e) {
			logger.debug(location, jobid, e);
		}
	}
	
	@Override
	public double getMin() {
		double retVal = 0;
		return retVal;
	}
	
	@Override
	public double getMax() {
		double retVal = 0;
		return retVal;
	}
	
	@Override
	public double getAvg() {
		double retVal = 0;
		return retVal;
	}
	
}
