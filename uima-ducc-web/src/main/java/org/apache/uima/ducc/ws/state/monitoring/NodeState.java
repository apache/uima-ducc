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
package org.apache.uima.ducc.ws.state.monitoring;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.uima.ducc.common.persistence.rm.IRmPersistence;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.id.DuccId;

/*
 * class to manage fetching of node status from datastore table, which is managed by RM
 */
public class NodeState implements INodeState {

	private static DuccLogger logger = DuccLogger.getLogger(NodeState.class);
	private static DuccId jobid = null;
	
	private static INodeState instance = null;
	
	public static INodeState getInstance() {
		if(instance == null) {
			instance = new NodeState();
		}
		return instance;
	}
	
	private Map<String, Map<String, Object>> map_empty = new HashMap<String, Map<String, Object>>();
	
	private Map<String, Map<String, Object>> map = map_empty;
	private Monitor monitor = null;
	
	private String key_online = "online";
	private String key_quiesced = "quiesced";
	
	private NodeState() {
		start();
	}
	
	// admin function to start monitoring thread
	@Override
	public void start() {
		if(monitor == null) {
			monitor = new Monitor();
			monitor.start();
			monitor.up();
		}
	}
	
	// admin function to stop monitoring thread
	@Override
	public void stop() {
		if(monitor != null) {
			monitor.down();
			monitor = null;
		}
	}
	
	// admin function to get status of monitoring thread
	@Override
	public boolean status() {
		boolean retVal = false;
		if(monitor != null) {
			monitor.status();
		}
		return retVal;
	}
	
	// general function to get online status for node
	@Override
	public String getOnline(String node, String otherwise) {
		String location = "getOnline";
		String retVal = otherwise;
		if(node != null) {
			if(map.size() > 0) {
				for(Entry<String, Map<String, Object>> entry : map.entrySet()) {
					String key = entry.getKey();
					if(key.equals(node)) {
						logger.debug(location, jobid, key+"=="+node);
						Map<String, Object> value = entry.getValue();
						Boolean value_online = (Boolean) value.get(key_online);
						retVal = ""+value_online;
						logger.debug(location, jobid, node+"=="+retVal);
						break;
					}
					else {
						logger.debug(location, jobid, key+"!="+node);
					}
				}
			}
			else {
				logger.warn(location, jobid, "size:"+0);
			}
		}
		else {
			logger.error(location, jobid, "node:"+node);
		}
		return retVal;
	}
	
	// general function to get quiesced status for node
	@Override
	public String getQuiesced(String node, String otherwise) {
		String location = "getQuiesced";
		String retVal = otherwise;
		if(node != null) {
			if(map.size() > 0) {
				for(Entry<String, Map<String, Object>> entry : map.entrySet()) {
					String key = entry.getKey();
					if(key.equals(node)) {
						logger.debug(location, jobid, key+"=="+node);
						Map<String, Object> value = entry.getValue();
						Boolean value_quiesced = (Boolean) value.get(key_quiesced);
						retVal = ""+value_quiesced;
						logger.debug(location, jobid, node+"=="+retVal);
						break;
					}
					else {
						logger.debug(location, jobid, key+"!="+node);
					}
				}
			}
			else {
				logger.warn(location, jobid, "size:"+0);
			}
		}
		else {
			logger.error(location, jobid, "node:"+node);
		}
		return retVal;
	}

	// monitoring thread
	private class Monitor extends Thread {
		
		private AtomicBoolean run_flag = new AtomicBoolean(true);
		private long sleep_seconds = 60;
		private long sleep_millis = sleep_seconds*1000;
		
		private IRmPersistence persistence = null;
		
		public Monitor() {
			initialize();
		}
		
		private void initialize() {
			String location = "initialize";
			try {
				up();
				persistence = configure();
			}
			catch(Exception e) {
				down();
				logger.error(location, jobid, e);
			}
		}
		
		// fetch new results from datastore every interval
		public void run() {
			String location = "";
			logger.info(location, jobid, "start");
			while(run_flag.get()) {
				try {
					map = fetch();
					Thread.sleep(sleep_millis);
				}
				catch(Exception e) {
					// ignore
				}
			}
			logger.info(location, jobid, "stop");
		}
		
		// stop monitoring thread
		public void up() {
			run_flag.set(true);
		}
		
		// start monitoring thread
		public void down() {
			run_flag.set(false);
		}
		
		// get status of monitoring thread
		public boolean status() {
			return run_flag.get();
		}
		
		// configure datastore access
		private IRmPersistence configure() throws Exception {
			String location = "configure";
			IRmPersistence retVal = null;
			String class_name = System.getProperty("ducc.rm.persistence.impl");
			logger.debug(location, jobid, class_name);
			@SuppressWarnings("unchecked")
			Class<IRmPersistence> iss = (Class<IRmPersistence>) Class.forName(class_name);
            retVal = (IRmPersistence) iss.newInstance();
            retVal.init(logger);
			return retVal;
		}
		
		// fetch node status map from datastore
		private Map<String, Map<String, Object>> fetch() throws Exception {
			String location = "fetch";
			Map<String, Map<String, Object>> retVal = map_empty;
			try {
				if(persistence != null) {
					retVal = persistence.getAllMachines();
				}
				logger.debug(location, jobid, "map:"+retVal.size());
			}
			catch(Exception e) {
				logger.error(location, jobid, e);
			}
			return retVal;
		}
	}

}
