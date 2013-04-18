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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import javax.management.MalformedObjectNameException;

import org.apache.uima.ducc.common.exception.DuccConfigurationException;
import org.apache.uima.ducc.common.mq.MqHelper;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.DuccPropertiesHelper;
import org.apache.uima.ducc.common.utils.DuccPropertiesResolver;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.common.DuccWorkMap;

public class MqReaper {
	
	private static final DuccLogger logger = DuccLoggerComponents.getOrLogger(MqReaper.class.getName());

	private static DuccId duccId = null;

	private MqHelper mqHelper = null;
	
	private DuccPropertiesResolver duccPropertiesResolver = null;
	
	private String jd_queue_prefix = null;
	private String jd_queue_prefix_default = "ducc.jd.queue.";
	
	private static MqReaper instance = null;
	
	public static MqReaper getInstance() throws MalformedObjectNameException, DuccConfigurationException, IOException {
		if(instance == null) {
			instance = new MqReaper();
		}
		return instance;
	}
	
	public static void resetInstance() {
		instance = null;
	}
	
	public MqReaper() throws MalformedObjectNameException, DuccConfigurationException, IOException {
		resolve();
		init();
	}
	
	private void init() throws MalformedObjectNameException, DuccConfigurationException, IOException {
		String location = "init";
		try {
			mqHelper = new MqHelper();
		} catch (MalformedObjectNameException e) {
			logger.error(location,duccId,e);
			throw e;
		} catch (NullPointerException e) {
			logger.error(location,duccId,e);
			throw e;
		} catch (DuccConfigurationException e) {
			logger.error(location,duccId,e);
			throw e;
		} catch (IOException e) {
			logger.error(location,duccId,e);
			throw e;
		}
		logger.info(location,duccId,DuccPropertiesResolver.ducc_broker_hostname+":"+mqHelper.get_broker_hostname());
		logger.info(location,duccId,DuccPropertiesResolver.ducc_broker_jmx_port+":"+mqHelper.get_broker_jmx_port());
		logger.info(location,duccId,DuccPropertiesResolver.ducc_broker_name+":"+mqHelper.get_broker_name());
		logger.info(location,duccId,DuccPropertiesResolver.ducc_broker_url+":"+mqHelper.get_broker_url());
	}
	
	private void resolve() {
		String location = "resolve";
		jd_queue_prefix = getDuccProperty(DuccPropertiesResolver.ducc_jd_queue_prefix, jd_queue_prefix_default);
		logger.info(location,duccId,DuccPropertiesResolver.ducc_jd_queue_prefix+":"+jd_queue_prefix);
	}
	
	private void configure() {
		duccPropertiesResolver = DuccPropertiesHelper.configure();
	}
	
	private String getDuccProperty(String key, String defaultValue) {
		if(duccPropertiesResolver == null) {
			configure();
		}
		String value = duccPropertiesResolver.getFileProperty(key);
		if(value == null) {
			value = defaultValue;
		}
		else {
			value = value.trim();
			if(value.length() == 0) {
				value = defaultValue;
			}
		}
		return value;
	}
	
	private boolean isStartsWith(String a, String b) {
		boolean retVal = false;
		if(a != null) {
			if(b != null) {
				if(a.startsWith(b)) {
					retVal = true;
				}
			}
		}
		return retVal;
	}
	
	private ArrayList<String> getJdQueues() {
		String location = "getJdQueues";
		ArrayList<String> qList = mqHelper.getQueueList();
		ArrayList<String> jdQueueList = new ArrayList<String>();
		for(String qName : qList) {
			if(isStartsWith(qName,jd_queue_prefix)) {
				jdQueueList.add(qName);
			}
			else {
				logger.debug(location, duccId, "queue ignore:"+qName);
			}
		}
		return jdQueueList;
	}
	
	private String lastMessage = "";
	private long lastTime = System.currentTimeMillis();
	
	private void pruningStatus(int size, int removed) {
		String location = "pruningStatus";
		String message = "size:"+size+" "+"removed:"+removed;
		long time = System.currentTimeMillis();
		if(message.equals(lastMessage)) {
			long elapsed = time - lastTime;
			if(elapsed > 1000*60*60) {
				logger.info(location, duccId, message);
				lastTime = time;
			}
		}
		else {
			logger.info(location, duccId, message);
			lastMessage = message;
			lastTime = time;
		}
	}
	
	public int removeUnusedJdQueues(DuccWorkMap workMap) {
		String location = "removeUnusedJdQueues";
		int removed = 0;
		try {
			ArrayList<String> qList = getJdQueues();
			Iterator<DuccId> iterator = workMap.getJobKeySet().iterator();
			while( iterator.hasNext() ) {
				DuccId jobId = iterator.next();
				String jqKeep = jd_queue_prefix+jobId.getFriendly();
				if(qList.remove(jqKeep)) {
					logger.debug(location, duccId, "queue keep:"+jqKeep);
				}
				else {
					logger.trace(location, duccId, "queue not found:"+jqKeep);
				}
			}
			for( String qName : qList ) {
				logger.info(location, duccId, "queue discard:"+qName);
				try {
					mqHelper.removeQueue(qName);
					removed++;
				}
				catch(Throwable t) {
					logger.error(location, duccId, t);
					init();
				}
			}
			pruningStatus(qList.size(),removed);
		}
		catch(Throwable t) {
			logger.error(location, duccId, t);
		}
		return removed;
	}
	
}
