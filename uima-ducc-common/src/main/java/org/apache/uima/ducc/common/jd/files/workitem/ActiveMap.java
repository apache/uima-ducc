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

import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.uima.ducc.common.jd.files.IWorkItemState;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.id.DuccId;

public class ActiveMap {
	
	private DuccLogger logger = DuccLogger.getLogger(ActiveMap.class, null);
	
	private DuccId jobid = null;
	
	protected ConcurrentSkipListMap<Long, IWorkItemState> activeMap = new ConcurrentSkipListMap<Long, IWorkItemState>();
	
	public ActiveMap(String component) {
		logger = DuccLogger.getLogger(ActiveMap.class, component);
	}
	
	public Set<Entry<Long, IWorkItemState>> entrySet() {
		return activeMap.entrySet();
	}
	
	public void remove(Long key) {
		String location = "remove";
		IWorkItemState value = get(key);
		if(value != null) {
			activeMap.remove(key);
			logger.info(location, jobid, "seqNo:"+value.getSeqNo()+" state:"+value.getState()+" size:"+activeMap.size());
		}
		getOperatingLeast();
	}
	
	public void put(Long key, IWorkItemState value) {
		String location = "put";
		if(value != null) {
			activeMap.put(key,  value);
			logger.info(location, jobid, "seqNo:"+value.getSeqNo()+" state:"+value.getState()+" size:"+activeMap.size());
		}
		getOperatingLeast();
	}
	
	public IWorkItemState get(Long key) {
		return activeMap.get(key);
	}
	
	public int size() {
		return activeMap.size();
	}
	
	public long getOperatingLeast() {
		String location = "getOperatingLeast";
		long operatingLeast = Long.MAX_VALUE;
		for(Entry<Long, IWorkItemState> entry : activeMap.entrySet()) {
			long candidate = entry.getValue().getMillisProcessing();
			if(candidate < operatingLeast) {
				operatingLeast = candidate;
			}
		}
		if(operatingLeast == Long.MAX_VALUE) {
			operatingLeast = 0;
		}
		logger.info(location, jobid, operatingLeast);
		return operatingLeast;
	}
	
	
}
