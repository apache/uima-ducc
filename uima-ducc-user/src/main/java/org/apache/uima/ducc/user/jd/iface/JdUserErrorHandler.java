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
package org.apache.uima.ducc.user.jd.iface;

import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.uima.cas.CAS;

public class JdUserErrorHandler implements IJdUserErrorHandler {

	private AtomicInteger exceptionLimitPerJob = new AtomicInteger(15);
	private AtomicInteger exceptionLimitPerWorkItem = new AtomicInteger(0);
	
	private AtomicInteger exceptionCounter = new AtomicInteger();
	
	private ConcurrentHashMap<String,AtomicInteger> map = new ConcurrentHashMap<String,AtomicInteger>();
	
	public JdUserErrorHandler() {
	}
	
	public JdUserErrorHandler(Properties properties) {
		initialize(properties);
	}
	
	@Override
	public void initialize(Properties properties) {
		if(properties != null) {
			initializeLimitPerJob(properties);
			initializeLimitPerWorkItem(properties);
		}
	}
	
	private void initializeLimitPerJob(Properties properties) {
		try {
			String key = InitializeKey.killJobLimit.name();
			if(properties.containsKey(key)) {
				String value = properties.getProperty(key);
				int limit = Integer.parseInt(value);
				if(limit > 0) {
					exceptionLimitPerJob = new AtomicInteger(limit);
				}
			}
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	private void initializeLimitPerWorkItem(Properties properties) {
		try {
			String key = InitializeKey.killWorkItemLimit.name();
			if(properties.containsKey(key)) {
				String value = properties.getProperty(key);
				int limit = Integer.parseInt(value);
				if(limit > 0) {
					exceptionLimitPerWorkItem = new AtomicInteger(limit);
				}
			}
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public Properties handle(CAS cas, Exception exception) {
		Properties properties = new Properties();
		exceptionCounter.incrementAndGet();
		handleKillJob(properties, cas, exception);
		handleKillWorkItem(properties, cas, exception);
		return properties;
	}
	
	private void killJob(Properties properties, String reason) {
		String key;
		String value;
		key = HandleKey.killJobFlag.name();
		value = Boolean.TRUE.toString();
		properties.put(key, value);
		key = HandleKey.killJobReason.name();
		value = reason;
		properties.put(key, value);
	}
	
	private void killProcess(Properties properties, String reason) {
		String key;
		String value;
		key = HandleKey.killProcessFlag.name();
		value = Boolean.TRUE.toString();
		properties.put(key, value);
		key = HandleKey.killProcessReason.name();
		value = reason;
		properties.put(key, value);
	}
	
	private void killWorkItem(Properties properties, String reason) {
		String key;
		String value;
		key = HandleKey.killWorkItemFlag.name();
		value = Boolean.TRUE.toString();
		properties.put(key, value);
		key = HandleKey.killWorkItemReason.name();
		value = reason;
		properties.put(key, value);
	}
	
	private void handleKillJob(Properties properties, CAS cas, Exception exception) {
		try {
			int counter = exceptionCounter.get();
			int limit = exceptionLimitPerJob.get();
			if(counter > limit) {
				String reasonKJ = "errors="+counter+" "+"limit="+limit;
				killJob(properties, reasonKJ);
			}
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	private void handleKillWorkItem(Properties properties, CAS cas, Exception exception) {
		try {
			if(cas == null) {
				String reasonKJ = "cas=null";
				killJob(properties, reasonKJ);
				String reasonKP = "kill process (if possible!)";
				killProcess(properties, reasonKP);
			}
			else if(exception == null){
				String reasonKJ = "exception=null";
				killJob(properties, reasonKJ);
				String reasonKP = "kill process (if possible!)";
				killProcess(properties, reasonKP);
			}
			else {
				String mapKey = cas.getDocumentText();
				if(!map.containsKey(mapKey)) {
					map.putIfAbsent(mapKey, new AtomicInteger(0));
				}
				AtomicInteger mapValue = map.get(mapKey);
				int counter = mapValue.incrementAndGet();
				int limit = exceptionLimitPerWorkItem.get();
				if(counter > limit) {
					String reasonKW = "errors="+counter+" "+"limit="+limit;
					killWorkItem(properties, reasonKW);
				}
				String reasonKP = "kill process (if possible!)";
				killProcess(properties, reasonKP);
			}
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
}
