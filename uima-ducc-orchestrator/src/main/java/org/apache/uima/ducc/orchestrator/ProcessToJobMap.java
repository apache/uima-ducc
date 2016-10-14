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
package org.apache.uima.ducc.orchestrator;

import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.id.DuccId;

/**
 * Keep a map of processes-to-jobs to minimize searching job process
 * maps to discover which job a particular process belongs to.
 */

public class ProcessToJobMap {

	private static final DuccLogger logger = DuccLoggerComponents.getOrLogger(ProcessToJobMap.class.getName());
	private static final DuccId jobid = null;
	
	private static ProcessToJobMap instance = new ProcessToJobMap();
	
	public static ProcessToJobMap getInstance() {
		return instance;
	}
	
	private ConcurrentHashMap<DuccId,DuccId> processToJobMap = new ConcurrentHashMap<DuccId,DuccId>();

	public ConcurrentHashMap<DuccId,DuccId> getMap() {
		ConcurrentHashMap<DuccId,DuccId> retVal = new ConcurrentHashMap<DuccId,DuccId>();
		retVal.putAll(processToJobMap);
		return retVal;
	}
	
	public void putMap(ConcurrentHashMap<DuccId,DuccId> map) {
		String location = "putMap";
		if(map != null) {
			logger.debug(location, jobid, map.size());
			for(Entry<DuccId, DuccId> entry : map.entrySet()) {
				this.put(entry.getKey(),entry.getValue());
			}
		}
	}
	
	public boolean containsKey(DuccId key) {
		return processToJobMap.containsKey(key);
	}
	
	public DuccId put(DuccId processId, DuccId jobId) {
		String location = "put";
		DuccId retVal = processToJobMap.put(processId, jobId);
		logger.debug(location, jobId, processId, "size="+processToJobMap.size());
		return retVal;
	}
	
	public DuccId remove(DuccId processId) {
		String location = "remove";
		DuccId jobId = processToJobMap.get(processId);
		DuccId retVal = processToJobMap.remove(processId);
		logger.debug(location, jobId, processId, "size="+processToJobMap.size());
		return retVal;
	}
	
	public DuccId get(DuccId key) {
		String location = "get";
		DuccId retVal = processToJobMap.get(key);
		logger.debug(location, retVal, key, "size="+processToJobMap.size());
		return retVal;
	}
	
	public int size() {
		return processToJobMap.size();
	}
}
