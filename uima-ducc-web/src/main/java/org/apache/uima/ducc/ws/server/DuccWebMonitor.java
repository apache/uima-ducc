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

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.uima.ducc.cli.DuccUiConstants;
import org.apache.uima.ducc.common.json.MonitorInfo;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.orchestrator.authentication.DuccWebAdministrators;
import org.apache.uima.ducc.transport.event.OrchestratorStateDuccEvent;
import org.apache.uima.ducc.transport.event.cli.JobRequestProperties;
import org.apache.uima.ducc.transport.event.cli.SpecificationProperties;
import org.apache.uima.ducc.transport.event.common.DuccWorkMap;
import org.apache.uima.ducc.transport.event.common.IDuccSchedulingInfo;
import org.apache.uima.ducc.transport.event.common.IDuccState.JobState;
import org.apache.uima.ducc.transport.event.common.IDuccWorkJob;
import org.apache.uima.ducc.ws.IListenerOrchestrator;

public class DuccWebMonitor implements IListenerOrchestrator {
	
	private static DuccLogger duccLogger = DuccLoggerComponents.getWsLogger(DuccWebMonitor.class.getName());
	private static DuccId jobid = null;
	
	public static DuccWebMonitor instance = new DuccWebMonitor();
	
	public static DuccWebMonitor getInstance() {
		return instance;
	}
	
	private long millisPerMinute = 60*1000;
	private long timeoutMinutes = 10;
	private long timeoutMillis = timeoutMinutes*millisPerMinute;
	
	private boolean disabledMessageAlreadyGiven = false;
	
	private Properties properties;
	
	private ConcurrentHashMap<DuccId,MonitorInfo> jMap = new ConcurrentHashMap<DuccId,MonitorInfo>();
	private ConcurrentHashMap<DuccId,TrackingInfo> tMap = new ConcurrentHashMap<DuccId,TrackingInfo>();
	
	private AtomicInteger updateCounter = new AtomicInteger(0);
	
	private ConcurrentHashMap<DuccId,Long> cMap = new ConcurrentHashMap<DuccId,Long>();
	
	private String monitor_host = null;
	private String monitor_port = null;
	
	private String actual_host = null;
	private String actual_port = null;
	
	public DuccWebMonitor() {
		super();
		initialize();
	}
	
	private void initialize() {
		String location = "initialize";
		properties = DuccWebProperties.get();
		String key = "ducc.ws.job.automatic.cancel.minutes";
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
			duccLogger.info(location, jobid, "timeout minutes (default): "+timeoutMinutes);
		}
		String me = System.getProperty("user.name");
		if(!DuccWebAdministrators.getInstance().isAdministrator(me)) {
			duccLogger.warn(location, jobid, me+" is not an administrator");
		}
		DuccListeners.getInstance().register(this);
		//
		monitor_host = properties.getProperty("ducc.ws.node");
		if(monitor_host == null) {
			monitor_host = properties.getProperty("ducc.head");
		}
		monitor_port = properties.getProperty("ducc.ws.port");
	}
	
	public void registerHostPort(String host, String port) {
		String location = "registerHostPort";
		actual_host = host;
		actual_port = port;
		if(isCanceler()) {
			duccLogger.info(location, jobid, host+":"+port+" is cancel monitor "+monitor_host+":"+monitor_port);
		}
		else {
			duccLogger.warn(location, jobid, host+":"+port+" is *not* cancel monitor "+monitor_host+":"+monitor_port);
		}
	}
	
	public boolean isCanceler() {
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
	
	private void cancel(DuccId duccId) {
		String location = "cancel";
		duccLogger.trace(location, jobid, "enter");
		
		String userId = System.getProperty("user.name");
		
		duccLogger.info(location, duccId, userId);
		
		String java = "/bin/java";
		String jhome = System.getProperty("java.home");
		String cp = System.getProperty("java.class.path");
		String jclass = "org.apache.uima.ducc.cli.DuccJobCancel";
		String arg1 = "--"+JobRequestProperties.key_id;
		String arg2 = ""+duccId;
		String arg3 = "--"+SpecificationProperties.key_reason;
		String arg4 = "\"submitter terminated, therefore job canceled automatically\"";
		String arg5 = "--"+SpecificationProperties.key_role_administrator;
		
		String[] arglistUser = { "-u", userId, "--", jhome+java, "-cp", cp, jclass, arg1, arg2, arg3, arg4, arg5 };
		String result = DuccAsUser.duckling(userId, arglistUser);
		duccLogger.warn(location, duccId, result);
		
		cMap.put(duccId, new Long(System.currentTimeMillis()));
		tMap.remove(duccId);

		duccLogger.trace(location, jobid, "exit");
	}
	
	private void canceler() {
		String location = "canceler";
		duccLogger.trace(location, jobid, "enter");
		
		long nowMillis = System.currentTimeMillis();
		
		Enumeration<DuccId> keys = tMap.keys();
		while(keys.hasMoreElements()) {
			DuccId duccId = keys.nextElement();
			TrackingInfo ti = tMap.get(duccId);
			long expiryMillis = ti.time;
			if(nowMillis > expiryMillis) {
				if(isCancelable(duccId)) {
					cancel(duccId);
				}
				else {
					duccLogger.debug(location, duccId, "not cancelable");
				}
			}
		}
		
		duccLogger.trace(location, jobid, "exit");
	}
	
	public boolean isCanceled(DuccId duccId) {
		return cMap.containsKey(duccId);
	}
	
	public void update(OrchestratorStateDuccEvent duccEvent) {
		String location = "update";
		duccLogger.trace(location, jobid, "enter");
		
		if(timeoutMillis > 0) {
			updateCounter.incrementAndGet();
			monitor(duccEvent);
		}
		else {
			if(!disabledMessageAlreadyGiven) {
				duccLogger.info(location, jobid, "Job monitor disabled");
				disabledMessageAlreadyGiven = true;
			}
		}
		
		duccLogger.trace(location, jobid, "exit");
	}
	
	private void monitor(OrchestratorStateDuccEvent duccEvent) {
		String location = "monitor";
		duccLogger.trace(location, jobid, "enter");
		
		DuccWorkMap dwm = duccEvent.getWorkMap();
		int size = dwm.getJobKeySet().size();
		duccLogger.debug(location, jobid, "jobs: "+size);
		
		Iterator<DuccId> iterator;
		ArrayList<DuccId> gone = new ArrayList<DuccId>();
		
		iterator = jMap.keySet().iterator();
		while( iterator.hasNext() ) {
			DuccId duccId = iterator.next();
			gone.add(duccId);
		}
		
		long expiryMillis = System.currentTimeMillis()+timeoutMillis+1;
		
		iterator = dwm.getJobKeySet().iterator();
		while( iterator.hasNext() ) {
			DuccId duccId = iterator.next();
			IDuccWorkJob dwj = (IDuccWorkJob)dwm.findDuccWork(duccId);
			gone.remove(duccId);
			if(!jMap.containsKey(duccId)) {
				MonitorInfo monitorInfo = new MonitorInfo();
				jMap.put(duccId, monitorInfo);
				duccLogger.info(location, duccId, "Job monitor start");
				if(!tMap.containsKey(duccId)) {
					try {
						Properties properties = DuccFile.getJobProperties(dwj);
						if(properties.containsKey(DuccUiConstants.name_monitor_cancel_job_on_interrupt)) {
							TrackingInfo ti = new TrackingInfo();
							ti.time = expiryMillis;
							ti.user = dwj.getStandardInfo().getUser();
							tMap.put(duccId,ti);
							duccLogger.info(location, duccId, "Job auto-cancel on");
						}
						else {
							duccLogger.info(location, duccId, "Job auto-cancel off");
						}
					}
					catch(Exception e) {
						duccLogger.info(location, duccId, e);
					}
				}
			}
			MonitorInfo monitorInfo = jMap.get(duccId);
			IDuccSchedulingInfo si = dwj.getSchedulingInfo();
			monitorInfo.total = si.getWorkItemsTotal();
			monitorInfo.done  = si.getWorkItemsCompleted();
			monitorInfo.error = si.getWorkItemsError();
			monitorInfo.retry = si.getWorkItemsRetry();
			monitorInfo.procs = ""+dwj.getProcessMap().getAliveProcessCount();
			
			ArrayList<String> stateSequence = monitorInfo.stateSequence;
			String state = dwj.getJobState().toString();
			if(!stateSequence.contains(state)) {
				duccLogger.info(location, duccId, "state: "+state);
				stateSequence.add(state);
			}
		}
		
		iterator = gone.iterator();
		while( iterator.hasNext() ) {
			DuccId duccId = iterator.next();
			jMap.remove(duccId);
			tMap.remove(duccId);
			duccLogger.info(location, duccId, "Job monitor stop");
		}
		
		if(isCanceler()) {
			canceler();
		}
		else {
			duccLogger.debug(location, jobid, "Canceler disabled");
		}
		
		duccLogger.trace(location, jobid, "exit");
	}
	
	private DuccId getKey(String jobId) {
		DuccId retVal = null;
		Enumeration<DuccId> keys = jMap.keys();
		while(keys.hasMoreElements()) {
			DuccId duccId = keys.nextElement();
			String mapId = ""+duccId.getFriendly();
			if(mapId.equals(jobId)) {
				retVal = duccId;
				break;
			}
		}
		return retVal;
	}
	
	public MonitorInfo renew(String jobId) {
		String location = "renew";
		duccLogger.trace(location, jobid, "enter");
		
		MonitorInfo monitorInfo = new MonitorInfo();
		
		int countAtArrival = updateCounter.get();
		int countAtPresent = countAtArrival;
		int sleepSecondsMax = 3*60;
		
		DuccId duccId = getKey(jobId);
		
		if(duccId == null) {
			int sleepSeconds = 0;
			duccLogger.info(location, duccId, "Waiting for update...");
			while(duccId == null) {
				try {
					duccLogger.debug(location, duccId, "Waiting continues...");
					Thread.sleep(1000);
					sleepSeconds += 1;
					if(sleepSeconds > sleepSecondsMax) {
						break;
					}
					countAtPresent = updateCounter.get();
					if((countAtPresent-countAtArrival) > 2) {
						break;
					}
					duccId = getKey(jobId);
				}
				catch(Exception e) {
				}
			}
			duccLogger.info(location, duccId, "Waiting complete.");
			duccId = getKey(jobId);
		}
		
		if(duccId != null) {
			monitorInfo = jMap.get(duccId);
			if(tMap.containsKey(duccId)) {
				long expiryMillis = System.currentTimeMillis()+timeoutMillis+1;
				TrackingInfo ti = tMap.get(duccId);
				ti.time = expiryMillis;
				duccLogger.info(location, duccId, "Job auto-cancel expiry extended");
			}
		}
		else {
			try {
				int iJobId = Integer.parseInt(jobId);
				duccId = new DuccId(iJobId);
				duccLogger.info(location, duccId, "Job not found");
			}
			catch(Exception e) {
				duccLogger.error(location, jobid, e);
			}
		}
		
		duccLogger.trace(location, jobid, "exit");
		
		return monitorInfo;
	}
	
	public ConcurrentHashMap<DuccId,Long> getExpiryMap() {
		String location = "getExpiryMap";
		duccLogger.trace(location, jobid, "enter");
		
		ConcurrentHashMap<DuccId,Long> eMap = new ConcurrentHashMap<DuccId,Long>();
		
		long nowMillis = System.currentTimeMillis();
		
		Enumeration<DuccId> keys = tMap.keys();
		while(keys.hasMoreElements()) {
			long minutesLeft = 0;
			DuccId duccId = keys.nextElement();
			TrackingInfo ti = tMap.get(duccId);
			long expiryMillis = ti.time;
			if(nowMillis < expiryMillis) {
				minutesLeft = (expiryMillis - nowMillis) / millisPerMinute;
			}
			eMap.put(duccId, minutesLeft);
		}
		
		duccLogger.trace(location, jobid, "exit");
		
		return eMap;
	}
	
	private boolean isCancelable(DuccId duccId) {
		String location = "isCancelable";
		duccLogger.trace(location, duccId, "enter");
		boolean retVal = false;
		if(!cMap.containsKey(duccId)) {
			MonitorInfo monitorInfo = jMap.get(duccId);
			if(monitorInfo != null) {
				ArrayList<String> stateSequence = monitorInfo.stateSequence;
				if(stateSequence != null) {
					if(stateSequence.contains(JobState.Completing.toString())) {
						duccLogger.debug(location, duccId, "state: <uncancelable> "+stateSequence);
					}
					else if(stateSequence.contains(JobState.Completed.toString())) {
						duccLogger.debug(location, duccId, "state: <uncancelable> "+stateSequence);
					}
					else {
						duccLogger.debug(location, duccId, "state: <cancelable> "+stateSequence);
						retVal = true;
					}
				}
				else {
					duccLogger.warn(location, duccId, "stateSequence: <null>");
				}
			}
			else {
				duccLogger.warn(location, duccId, "monitorInfo: <null>");
			}
		}
		else {
			duccLogger.debug(location, duccId, "already canceled");
		}
		duccLogger.trace(location, duccId, "exit");
		return retVal;
	}
	
	public boolean isCancelPending(DuccId duccId) {
		return cMap.containsKey(duccId);
	}
	
	public Long getExpiry(DuccId duccId) {
		String location = "getExpiry";
		duccLogger.trace(location, duccId, "enter");
		Long retVal = null;
		if(!isCanceled(duccId)) {
			if(isCancelable(duccId)) {
				ConcurrentHashMap<DuccId,Long> eMap = getExpiryMap();
				if(eMap.containsKey(duccId)) {
					retVal = eMap.get(duccId);
				}
			}
		}
		duccLogger.trace(location, duccId, "exit");
		return retVal;
	}
	
}
