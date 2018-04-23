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
package org.apache.uima.ducc.orchestrator.maintenance;

import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.uima.ducc.common.NodeIdentity;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccPropertiesResolver;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.common.IDuccProcess;


public class NodeAccounting {
	
	private static final DuccLogger logger = DuccLogger.getLogger(NodeAccounting.class);
	private static final DuccId jobid = null;
	
	private static NodeAccounting instance = new NodeAccounting();
	
	public static NodeAccounting getInstance() {
		return instance;
	}
	
	private ConcurrentHashMap<String, Long> timeMap = new ConcurrentHashMap<String, Long>();
	
	private long inventoryRate = 30 * 1000;
	private long inventorySkip = 0;
	
	private long heartbeatMissingTolerance = 3;
	
	private boolean inventoryRateMessage = false;
	private boolean inventorySkipMessage = false;
	
	private long getRate() {
		String methodName = "getRate";
		long retVal = inventoryRate;
		try {
			String property_inventory_rate = DuccPropertiesResolver.getInstance().getFileProperty(DuccPropertiesResolver.ducc_agent_node_inventory_publish_rate);
			if(property_inventory_rate == null) {
				property_inventory_rate = ""+inventoryRate;
			}
			long property_value = Long.parseLong(property_inventory_rate.trim());
			if(property_value != inventoryRate) {
				inventoryRate = property_value;
				logger.info(methodName, jobid, "rate:"+inventoryRate);
			}
			retVal = property_value;
		}
		catch(Throwable t) {
			if(!inventoryRateMessage) {
				inventoryRateMessage = true;
				logger.warn(methodName, jobid, t);
			}
		}
		return retVal;
	}
	
	private long getSkip() {
		String methodName = "getSkip";
		long retVal = inventorySkip;
		try {
			String property_inventory_skip = DuccPropertiesResolver.getInstance().getFileProperty(DuccPropertiesResolver.ducc_agent_node_inventory_publish_rate_skip);
			if(property_inventory_skip == null) {
				property_inventory_skip = ""+inventorySkip;
			}
			long property_value = Long.parseLong(property_inventory_skip.trim());
			if(property_value != inventorySkip) {
				inventorySkip = property_value;
				logger.info(methodName, jobid, "skip:"+inventorySkip);
			}
			retVal = property_value;
		}
		catch(Throwable t) {
			if(!inventorySkipMessage) {
				inventorySkipMessage = true;
				logger.warn(methodName, jobid, t);
			}
		}
		return retVal;
	}
	
	private long getNodeMissingTime() {
		String methodName = "getNodeMissingTime";
		long retVal = inventoryRate * heartbeatMissingTolerance;
		try {
			long rate = getRate();
			long skip = getSkip();
			if(skip > 0) {
				rate = rate * skip;
			}
			retVal = rate *  heartbeatMissingTolerance;
		}
		catch(Throwable t) {
			logger.error(methodName, jobid, t);
		}
		return retVal;
	}
	
	public void heartbeat(HashMap<DuccId,IDuccProcess> processMap) {
		String location = "heartbeat";
		try {
			Iterator<DuccId> iterator = processMap.keySet().iterator();
			while(iterator.hasNext()) {
				DuccId duccId = iterator.next();
				IDuccProcess process = processMap.get(duccId);
				NodeIdentity nodeIdentity = process.getNodeIdentity();
				String nodeName = nodeIdentity.getCanonicalName();
				heartbeat(nodeName);
				break;
			}
		}
		catch(Throwable t) {
			logger.error(location, jobid, "");
		}
	}
	
	public void heartbeat(String nodeName) {
		String location = "heartbeat";
		record(nodeName);
		logger.debug(location, jobid, nodeName);
	}
	
	private void record(String nodeName) {
		if(nodeName != null) {
			Long value = new Long(System.currentTimeMillis());
			timeMap.put(nodeName, value);
		}
	}
	
	public boolean isAlive(String nodeName) {
		String location = "isAlive";
		boolean retVal = true;
		try {
			if(!timeMap.containsKey(nodeName)) {
				record(nodeName);
			}
			long heartbeatTime = timeMap.get(nodeName);
			long currentTime = System.currentTimeMillis();
			long elapsed = currentTime - heartbeatTime;
			if( elapsed > getNodeMissingTime() ) {
				retVal = false;
				logger.info(location, jobid, "down:"+nodeName+" elapsed:"+elapsed);
			}
		}
		catch(Throwable t) {
			logger.error(location, jobid, nodeName);
		}
		return retVal;
	}
}
