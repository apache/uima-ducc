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
package org.apache.uima.ducc.ws.db;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DbMachine implements IDbMachine{
	
	private Map<String, Object> map = new HashMap<String, Object>();
	
	private enum Key { 
		classes, 		// [weekly urgent background normal reserve JobDriver high debug low standalone fixed] 
		reservable,		// [true]
		share_order,	// [2]
		assignments,	// [0] 
		blacklisted,	// [false]
		memory,			// [30]
		online,			// [true]
		ip,				// [192.168.4.4]
		heartbeats,		// [0]
		nodepool,		// [--default--]
		shares_left,	// [2]
		quantum,		// [15]
		name,			// [bluejws67-4]
		responsive,		// [true]
		};
	
	public DbMachine(Map<String, Object> map) {
		initMap(map);
	}
	
	private void initMap(Map<String, Object> value) {
		if(value != null) {
			map.putAll(value);
		}
	}
	
	public List<String> getClasses() {
		List<String> retVal = new ArrayList<String>();
		String classes = (String) map.get(Key.classes.name());
		if(classes != null) {
			String[] array = classes.split("\\s+");
			if(array != null) {
				retVal = Arrays.asList(array);
			}
		}
		return retVal;
	}
	
	public Boolean getReservable() {
		Boolean retVal = (Boolean) map.get(Key.reservable.name());
		return retVal;
	}
	
	public Integer getShareOrder() {
		Integer retVal = (Integer) map.get(Key.share_order.name());
		return retVal;
	}
	
	public Integer getAssignments() {
		Integer retVal = (Integer) map.get(Key.assignments.name());
		return retVal;
	}
	
	public Boolean getBlacklisted() {
		Boolean retVal = (Boolean) map.get(Key.blacklisted.name());
		return retVal;
	}
	
	public Integer getMemory() {
		Integer retVal = (Integer) map.get(Key.memory.name());
		return retVal;
	}
	
	public Boolean getOnline() {
		Boolean retVal = (Boolean) map.get(Key.online.name());
		return retVal;
	}
	
	public String getIp() {
		String retVal = (String) map.get(Key.ip.name());
		return retVal;
	}
	
	public Integer getHeartbeats() {
		Integer retVal = (Integer) map.get(Key.heartbeats.name());
		return retVal;
	}
	
	public String getNodePool() {
		String retVal = (String) map.get(Key.nodepool.name());
		return retVal;
	}
	
	public Integer getSharesLeft() {
		Integer retVal = (Integer) map.get(Key.shares_left.name());
		return retVal;
	}
	
	public Integer getQuantum() {
		Integer retVal = (Integer) map.get(Key.quantum.name());
		return retVal;
	}
	
	public String getName() {
		String retVal = (String) map.get(Key.name.name());
		return retVal;
	}
	
	public Boolean getResponsive() {
		Boolean retVal = (Boolean) map.get(Key.responsive.name());
		return retVal;
	}

}
