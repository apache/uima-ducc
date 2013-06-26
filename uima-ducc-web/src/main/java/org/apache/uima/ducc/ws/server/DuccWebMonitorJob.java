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

import java.io.File;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.uima.ducc.common.json.MonitorInfo;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.OrchestratorStateDuccEvent;
import org.apache.uima.ducc.transport.event.cli.JobRequestProperties;
import org.apache.uima.ducc.transport.event.cli.SpecificationProperties;
import org.apache.uima.ducc.transport.event.common.DuccWorkJob;
import org.apache.uima.ducc.transport.event.common.DuccWorkMap;
import org.apache.uima.ducc.transport.event.common.IDuccSchedulingInfo;
import org.apache.uima.ducc.transport.event.common.IDuccState.JobState;
import org.apache.uima.ducc.transport.event.common.IDuccWork;
import org.apache.uima.ducc.transport.event.common.IRationale;

public class DuccWebMonitorJob {
	
	private static DuccLogger duccLogger = DuccLoggerComponents.getWsLogger(DuccWebMonitorJob.class.getName());
	private static DuccId jobid = null;
	
	private ConcurrentHashMap<DuccId,MonitorInfo> mMap = new ConcurrentHashMap<DuccId,MonitorInfo>();
	private ConcurrentHashMap<DuccId,TrackingInfo> tMap = new ConcurrentHashMap<DuccId,TrackingInfo>();
	private ConcurrentHashMap<DuccId,Long> cMap = new ConcurrentHashMap<DuccId,Long>();
	
	private long millisPerMinute = 60*1000;
	private long timeoutMillis;
	
	protected DuccWebMonitorJob(long timeoutMillis) {
		this.timeoutMillis = timeoutMillis;
	}
	
	protected void monitor(OrchestratorStateDuccEvent duccEvent) {
		String location = "monitor";
		duccLogger.trace(location, jobid, "enter");
		
		DuccWorkMap dwm = duccEvent.getWorkMap();
		int size = dwm.getJobKeySet().size();
		duccLogger.debug(location, jobid, "jobs: "+size);
		
		Iterator<DuccId> iterator;
		ArrayList<DuccId> gone = new ArrayList<DuccId>();
		
		iterator = mMap.keySet().iterator();
		while( iterator.hasNext() ) {
			DuccId duccId = iterator.next();
			gone.add(duccId);
		}
		
		long expiryMillis = System.currentTimeMillis()+timeoutMillis+1;
		
		iterator = dwm.getJobKeySet().iterator();
		while( iterator.hasNext() ) {
			DuccId duccId = iterator.next();
			IDuccWork dw = (IDuccWork)dwm.findDuccWork(duccId);
			gone.remove(duccId);
			if(!mMap.containsKey(duccId)) {
				MonitorInfo monitorInfo = new MonitorInfo();
				mMap.put(duccId, monitorInfo);
				duccLogger.info(location, duccId, "monitor start");
				if(!tMap.containsKey(duccId)) {
					if(dw.isCancelOnInterrupt()) {
						TrackingInfo ti = new TrackingInfo();
						ti.time = expiryMillis;
						ti.user = dw.getStandardInfo().getUser();
						tMap.put(duccId,ti);
						duccLogger.info(location, duccId, "auto-cancel on");
					}
					else {
						duccLogger.info(location, duccId, "auto-cancel off");
					}
				}
			}
			DuccWorkJob dwj = (DuccWorkJob) dw;
			MonitorInfo monitorInfo = mMap.get(duccId);
			IDuccSchedulingInfo si = dw.getSchedulingInfo();
			monitorInfo.total = si.getWorkItemsTotal();
			monitorInfo.done  = si.getWorkItemsCompleted();
			monitorInfo.error = ""+si.getIntWorkItemsError();
			monitorInfo.retry = si.getWorkItemsRetry();
			monitorInfo.lost = si.getWorkItemsLost();
			monitorInfo.procs = ""+dwj.getProcessMap().getAliveProcessCount();
			
			if(si.getIntWorkItemsError() > 0) {
				String logsjobdir = dwj.getUserLogsDir()+dwj.getDuccId().getFriendly()+File.separator;
				String logfile = "jd.err.log";
				ArrayList<String> errorLogs = new ArrayList<String>();
				errorLogs.add(logsjobdir+logfile);
				monitorInfo.errorLogs = errorLogs;
			}
			
			ArrayList<String> stateSequence = monitorInfo.stateSequence;
			String state = dwj.getJobState().toString();
			if(!stateSequence.contains(state)) {
				duccLogger.info(location, duccId, "state: "+state);
				stateSequence.add(state);
			}
			
			IRationale rationale = dwj.getCompletionRationale();
			if(rationale != null) {
				if(rationale.isSpecified()) {
					String text = rationale.getText();
					if(text != null) {
						monitorInfo.rationale = text;
					}
				}
			}
		}
		
		iterator = gone.iterator();
		while( iterator.hasNext() ) {
			DuccId duccId = iterator.next();
			mMap.remove(duccId);
			tMap.remove(duccId);
			duccLogger.info(location, duccId, "monitor stop");
		}
		
		duccLogger.trace(location, jobid, "exit");
	}
	
	protected DuccId getKey(String jobId) {
		DuccId retVal = null;
		Enumeration<DuccId> keys = mMap.keys();
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
	
	public MonitorInfo renew(String jobId, AtomicInteger updateCounter) {
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
			monitorInfo = mMap.get(duccId);
			if(tMap.containsKey(duccId)) {
				long expiryMillis = System.currentTimeMillis()+timeoutMillis+1;
				TrackingInfo ti = tMap.get(duccId);
				ti.time = expiryMillis;
				duccLogger.info(location, duccId, "auto-cancel expiry extended");
			}
		}
		else {
			try {
				int iJobId = Integer.parseInt(jobId);
				duccId = new DuccId(iJobId);
				duccLogger.info(location, duccId, "not found");
			}
			catch(Exception e) {
				duccLogger.error(location, jobid, e);
			}
		}
		
		duccLogger.trace(location, jobid, "exit");
		
		return monitorInfo;
	}
	
	protected Long getExpiry(DuccId duccId) {
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

	protected boolean isCanceled(DuccId duccId) {
		return cMap.containsKey(duccId);
	}
	
	private boolean isCancelable(DuccId duccId) {
		String location = "isCancelable";
		duccLogger.trace(location, duccId, "enter");
		boolean retVal = false;
		if(!cMap.containsKey(duccId)) {
			MonitorInfo monitorInfo = mMap.get(duccId);
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

	protected void cancel(DuccId duccId) {
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
		String arg4 = "\"submitter terminated, therefore canceled automatically\"";
		String arg5 = "--"+SpecificationProperties.key_role_administrator;
		
		String[] arglistUser = { "-u", userId, "--", jhome+java, "-cp", cp, jclass, arg1, arg2, arg3, arg4, arg5 };
		String result = DuccAsUser.duckling(userId, arglistUser);
		duccLogger.warn(location, duccId, result);
		
		cMap.put(duccId, new Long(System.currentTimeMillis()));
		tMap.remove(duccId);

		duccLogger.trace(location, jobid, "exit");
	}
	
	protected void canceler(long nowMillis) {
		String location = "canceler";
		duccLogger.trace(location, jobid, "enter");

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
	
}
