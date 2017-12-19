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
package org.apache.uima.ducc.ws;

import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.uima.ducc.common.boot.DuccDaemonRuntimeProperties.DaemonName;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.TimeStamp;
import org.apache.uima.ducc.transport.event.AbstractDuccEvent;
import org.apache.uima.ducc.transport.event.DuccEvent.EventType;
import org.apache.uima.ducc.ws.server.DuccWebProperties;

public class DuccDaemonsData {
	
	private static DuccLogger duccLogger = DuccLogger.getLogger(DuccDaemonsData.class);
	
	private static DuccDaemonsData duccDaemonsData = new DuccDaemonsData();
	private static ConcurrentHashMap<DaemonName,String> mapCurr = new ConcurrentHashMap<DaemonName,String>();
	private static ConcurrentHashMap<DaemonName,String> mapPrev = new ConcurrentHashMap<DaemonName,String>();
	private static ConcurrentHashMap<DaemonName,String> mapMax = new ConcurrentHashMap<DaemonName,String>();
	private static ConcurrentHashMap<DaemonName,String> mapMaxTOD = new ConcurrentHashMap<DaemonName,String>();
	private static ConcurrentHashMap<DaemonName,Long> eventSize = new ConcurrentHashMap<DaemonName,Long>();
	private static ConcurrentHashMap<DaemonName,Long> eventSizeMax = new ConcurrentHashMap<DaemonName,Long>();
	private static ConcurrentHashMap<DaemonName,String> eventSizeMaxTOD = new ConcurrentHashMap<DaemonName,String>();

	public static DuccDaemonsData getInstance() {
		return duccDaemonsData;
	}
	
	public void put(AbstractDuccEvent duccEvent) {
		EventType eventType = duccEvent.getEventType();
		DaemonName key;
		switch(eventType) {
		case ORCHESTRATOR_STATE:
			key = DaemonName.Orchestrator;
			putHeartbeat(key);
			putEventSize(key, duccEvent);
			break;
		case PM_STATE:
			key = DaemonName.ProcessManager;
			putHeartbeat(key);
			putEventSize(key, duccEvent);
			break;
		case RM_STATE:
			key = DaemonName.ResourceManager;
			putHeartbeat(key);
			putEventSize(key, duccEvent);
			break;
		case SM_STATE:
		case SM_HEARTBEAT:
			key = DaemonName.ServiceManager;
			putHeartbeat(key);
			putEventSize(key, duccEvent);
			break;
		case WEBSERVER_STATE:
			key = DaemonName.Webserver;
			putHeartbeat(key);
			putEventSize(key, duccEvent);
			break;	
		default:
			break;
		}
	}
	
	public void putEventSize(DaemonName key, AbstractDuccEvent duccEvent) {
		Long size = duccEvent.getEventSize();
		eventSize.put(key, size);
		Long prev;
		if(eventSizeMax.containsKey(key)) {
			prev = eventSizeMax.get(key);
		}
		else {
			prev = new Long(0);
		}
		if(size > prev) {
			eventSizeMax.put(key, size);
			String timestamp = TimeStamp.getCurrentMillis();
			eventSizeMaxTOD.put(key, timestamp);
		}
	}

	public Long getEventSize(DaemonName key) {
		Long retVal = new Long(0);
		if(eventSize.containsKey(key)) {
			retVal = eventSize.get(key);
		}
		return retVal;
	}
	
	public Long getEventSizeMax(DaemonName key) {
		Long retVal = new Long(0);
		if(eventSizeMax.containsKey(key)) {
			retVal = eventSizeMax.get(key);
		}
		return retVal;
	}
	
	public String getEventSizeMaxTOD(DaemonName key) {
		String retVal = "";
		if(mapMaxTOD.containsKey(key)) {
			retVal = eventSizeMaxTOD.get(key);
		}
		return retVal;
	}
	
	public void putHeartbeat(DaemonName key) {
		String timestamp = TimeStamp.getCurrentMillis();
		if(mapPrev.containsKey(key)) {
			String t0 = mapPrev.get(key);
			String t1 = timestamp;
			long millis = TimeStamp.diffMillis(t1, t0);
			if(mapMax.containsKey(key)) {
				long max = Long.parseLong(mapMax.get(key));
				if(millis > max) {
					mapMax.put(key, ""+millis);
					mapMaxTOD.put(key, t1);
				}
			}
			else {
				mapMax.put(key, ""+millis);
				mapMaxTOD.put(key, t1);
			}
		}
		if(mapCurr.containsKey(key)) {
			mapPrev.put(key, mapCurr.get(key));
		}
		mapCurr.put(key, timestamp);
	}
	
	public String getHeartbeat(DaemonName key) {
		String retVal = "";
		if(mapCurr.containsKey(key)) {
			String t1 = TimeStamp.getCurrentMillis();
			String t0 = mapCurr.get(key);
			long millis = TimeStamp.diffMillis(t1, t0);
			retVal = ""+millis/1000;
		}
		return retVal;
	}
	
	public String getMaxHeartbeat(DaemonName key) {
		String retVal = "";
		if(mapMax.containsKey(key)) {
			long max = Long.parseLong(mapMax.get(key));
			retVal = ""+(max/1000);
		}
		return retVal;
	}
	
	public String getMaxHeartbeatTOD(DaemonName key) {
		String retVal = "";
		if(mapMaxTOD.containsKey(key)) {
			retVal = mapMaxTOD.get(key);
		}
		return retVal;
	}
	
	/**
	 * @return number of millis since last receipt of self publication
	 */
	private long getWsTod() {
		long retVal = Long.MAX_VALUE;
		DaemonName key = DaemonName.Webserver;
		if(mapCurr.containsKey(key)) {
			String t1 = TimeStamp.getCurrentMillis();
			String t0 = mapCurr.get(key);
			long millis = TimeStamp.diffMillis(t1, t0);
			retVal = millis;
		}
		return retVal;
	}
	
	/**
	 * @return number of millis after which webserver publication is considered late
	 */
	private long getMillisMIA() {
		String methodName = "getMillisMIA";
		long secondsMIA = -1;
		Properties properties = DuccWebProperties.get();
		String ws_rate = properties.getProperty("ducc.ws.state.publish.rate");
		String ws_ratio = "1";
		try {
			long rate = Long.parseLong(ws_rate.trim());
			long ratio = Long.parseLong(ws_ratio .trim());
			secondsMIA = 3 * rate * ratio;
		}
		catch(Throwable t) {
			duccLogger.debug(methodName, null, t);
		}
		return secondsMIA;
	}
	
	/**
	 * 
	 * @return true if webserver self publications are on time, false otherwise
	 */
	public boolean isWsPublicationOntime() {
		boolean retVal = true;
		long expiry = getMillisMIA();
		if(getWsTod() > expiry) {
			retVal = false;
		}
		return retVal;
	}
	
}
