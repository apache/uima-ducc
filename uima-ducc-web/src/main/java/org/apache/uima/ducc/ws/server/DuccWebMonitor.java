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
package org.apache.uima.ducc.ws.server;

import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.uima.ducc.common.json.MonitorInfo;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.orchestrator.authentication.DuccWebAdministrators;
import org.apache.uima.ducc.transport.event.OrchestratorStateDuccEvent;
import org.apache.uima.ducc.transport.event.common.IDuccTypes.DuccType;
import org.apache.uima.ducc.ws.IListenerOrchestrator;

public class DuccWebMonitor implements IListenerOrchestrator, IWebMonitor {
	
	private static DuccLogger duccLogger = DuccLoggerComponents.getWsLogger(DuccWebMonitor.class.getName());
	private static DuccId jobid = null;
	
	public static DuccWebMonitor instance = new DuccWebMonitor();
	
	public String key_automatic_cancel_minutes = "ducc.ws.automatic.cancel.minutes";
	public String key_node = "ducc.ws.node";
	public String key_head = "ducc.head";
	public String key_port = "ducc.ws.port";
	
	private Properties properties = new Properties();
	
	private AtomicInteger updateCounter = new AtomicInteger(0);
	private AtomicBoolean operational = new AtomicBoolean(true);
	private AtomicBoolean statusMessageIssued = new AtomicBoolean(false);
	
	private String monitor_host = null;
	private String monitor_port = null;
	
	private String actual_host = null;
	private String actual_port = null;
	
	private long millisPerMinute = 60*1000;
	private long timeoutMinutes = 10;
	private long timeoutMillis = timeoutMinutes*millisPerMinute;
	
	private DuccWebMonitorJob duccWebMonitorJob = null;
	private DuccWebMonitorManagedReservation duccWebMonitorManagedReservation = null;

	public static DuccWebMonitor getInstance() {
		return instance;
	}
	
	public DuccWebMonitor() {
		super();
		initialize();
	}
	
	private void initialize() {
		String location = "initialize";
		properties = DuccWebProperties.get();
		String key = key_automatic_cancel_minutes;
		if(properties.containsKey(key)) {
			String value = properties.getProperty(key);
			try {
				timeoutMinutes = Long.parseLong(value);
				timeoutMillis = timeoutMinutes * millisPerMinute;
				duccLogger.info(location, jobid, "timeout minutes: "+timeoutMinutes);
			}
			catch(Exception e) {
				duccLogger.error(location, jobid, e);
			}
		}
		else {
			duccLogger.warn(location, jobid, "not found: "+key);
			duccLogger.info(location, jobid, "timeout minutes (default): "+timeoutMinutes);
		}
		String me = System.getProperty("user.name");
		if(!DuccWebAdministrators.getInstance().isAdministrator(me)) {
			duccLogger.warn(location, jobid, me+" is not an administrator");
		}
		DuccListeners.getInstance().register(this);
		//
		monitor_host = properties.getProperty(key_node);
		if(monitor_host == null) {
			monitor_host = properties.getProperty(key_head);
		}
		monitor_port = properties.getProperty(key_port);
		//
		duccWebMonitorJob = new DuccWebMonitorJob(timeoutMillis);
		duccWebMonitorManagedReservation = new DuccWebMonitorManagedReservation(timeoutMillis);
	}
	
	
	public void update(OrchestratorStateDuccEvent duccEvent) {
		String location = "update";
		duccLogger.trace(location, jobid, "enter");
		
		if(operational.get()) {
			updateCounter.incrementAndGet();
			monitor(duccEvent);
		}
		else {
			if(!statusMessageIssued.getAndSet(true)) {
				duccLogger.info(location, jobid, "auto-cancel monitor disabled");
			}
		}
		
		duccLogger.trace(location, jobid, "exit");
	}
	
	private void monitor(OrchestratorStateDuccEvent duccEvent) {
		String location = "monitor";
		duccLogger.trace(location, jobid, "enter");
		duccWebMonitorJob.monitor(duccEvent);
		duccWebMonitorManagedReservation.monitor(duccEvent);
		if(isAutoCancelEnabled()) {
			long nowMillis = System.currentTimeMillis();
			duccWebMonitorJob.canceler(nowMillis);
			duccWebMonitorManagedReservation.canceler(nowMillis);
		}
		else {
			duccLogger.debug(location, jobid, "auto-cancel monitor disabled");
		}
		duccLogger.trace(location, jobid, "exit");
	}
	
	
	public void register(String host, String port) {
		String location = "register";
		actual_host = host;
		actual_port = port;
		if(isAutoCancelEnabled()) {
			duccLogger.info(location, jobid, host+":"+port+" is cancel monitor "+monitor_host+":"+monitor_port);
		}
		else {
			duccLogger.warn(location, jobid, host+":"+port+" is *not* cancel monitor "+monitor_host+":"+monitor_port);
		}
	}

	
	public boolean isAutoCancelEnabled() {
		if(actual_host == null) {
			return false;
		}
		if(monitor_host == null) {
			return false;
		}
		if(!actual_host.equals(monitor_host)) {
			String actual_domainless_host = actual_host.split("\\.")[0];
			String monitor_domainless_host = monitor_host.split("\\.")[0];
			if(!actual_domainless_host.equals(monitor_domainless_host)) {
				return false;
			}
		}
		if(actual_port == null) {
			return false;
		}
		if(monitor_port == null) {
			return false;
		}
		if(!actual_port.equals(monitor_port)) {
			return false;
		}
		return true;
	}

	
	public MonitorInfo renew(DuccType duccType, String id) {
		MonitorInfo monitorInfo = new MonitorInfo();
		if(duccType != null) {
			if(id != null) {
				switch(duccType) {
				case Job:
					monitorInfo = duccWebMonitorJob.renew(id, updateCounter);
					break;
				case Reservation:
					monitorInfo = duccWebMonitorManagedReservation.renew(id, updateCounter);
					break;
				case Service:
					break;
				default:
					break;
				}
			}
		}
		return monitorInfo;
	}

	
	public Long getExpiry(DuccType duccType, DuccId duccId) {
		Long expiry = null;
		if(duccType != null) {
			if(duccId != null) {
				switch(duccType) {
				case Job:
					expiry = duccWebMonitorJob.getExpiry(duccId);
					break;
				case Reservation:
					expiry = duccWebMonitorManagedReservation.getExpiry(duccId);
					break;
				case Service:
					break;
				default:
					break;
				}
			}
		}
		return expiry;
	}

	
	public boolean isCanceled(DuccType duccType, DuccId duccId) {
		boolean flag = false;
		if(duccType != null) {
			if(duccId != null) {
				switch(duccType) {
				case Job:
					flag = duccWebMonitorJob.isCanceled(duccId);
					break;
				case Reservation:
					flag = duccWebMonitorManagedReservation.isCanceled(duccId);
					break;
				case Service:
					break;
				default:
					break;
				}
			}
		}
		return flag;
	}
	
	
	public ConcurrentHashMap<DuccId,Long> getExpiryMap(DuccType duccType) {
		ConcurrentHashMap<DuccId,Long> eMap = new ConcurrentHashMap<DuccId,Long>();
		if(duccType != null) {
			switch(duccType) {
			case Job:
				eMap = duccWebMonitorJob.getExpiryMap();
				break;
			case Reservation:
				eMap = duccWebMonitorManagedReservation.getExpiryMap();
				break;
			case Service:
				break;
			default:
				break;
			}
		}
		return eMap;
	}
	
}
