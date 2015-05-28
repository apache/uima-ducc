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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.uima.ducc.common.CancelReasons.CancelReason;
import org.apache.uima.ducc.common.json.MonitorInfo;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.OrchestratorStateDuccEvent;
import org.apache.uima.ducc.transport.event.cli.JobRequestProperties;
import org.apache.uima.ducc.transport.event.cli.SpecificationProperties;
import org.apache.uima.ducc.transport.event.common.DuccWorkJob;
import org.apache.uima.ducc.transport.event.common.IDuccProcess;
import org.apache.uima.ducc.transport.event.common.IDuccState.JobState;
import org.apache.uima.ducc.transport.event.common.IDuccWork;
import org.apache.uima.ducc.transport.event.common.IDuccWorkMap;
import org.apache.uima.ducc.transport.event.common.IRationale;
import org.apache.uima.ducc.ws.authentication.DuccAsUser;

public class DuccWebMonitorManagedReservation {
	
	private static DuccLogger duccLogger = DuccLoggerComponents.getWsLogger(DuccWebMonitorManagedReservation.class.getName());
	private static DuccId jobid = null;
	
	private ConcurrentHashMap<DuccId,MonitorInfo> mMap = new ConcurrentHashMap<DuccId,MonitorInfo>();
	private ConcurrentHashMap<DuccId,TrackingInfo> tMap = new ConcurrentHashMap<DuccId,TrackingInfo>();
	private ConcurrentHashMap<DuccId,Long> cMap = new ConcurrentHashMap<DuccId,Long>();
	
	private long millisPerMinute = 60*1000;
	private long timeoutMillis;
	
	protected DuccWebMonitorManagedReservation(long timeoutMillis) {
		this.timeoutMillis = timeoutMillis;
	}
	
	protected void monitor(OrchestratorStateDuccEvent duccEvent) {
		String location = "monitor";
		duccLogger.trace(location, jobid, "enter");
		
		IDuccWorkMap dwm = duccEvent.getWorkMap();
		int size = dwm.getManagedReservationKeySet().size();
		duccLogger.debug(location, jobid, "managed reservations: "+size);
		
		Iterator<DuccId> iterator;
		ArrayList<DuccId> gone = new ArrayList<DuccId>();
		
		iterator = mMap.keySet().iterator();
		while( iterator.hasNext() ) {
			DuccId duccId = iterator.next();
			gone.add(duccId);
		}
		
		long expiryMillis = System.currentTimeMillis()+timeoutMillis+1;
		
		iterator = dwm.getManagedReservationKeySet().iterator();
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
			DuccWorkJob dwr = (DuccWorkJob) dw;
			MonitorInfo monitorInfo = mMap.get(duccId);
			//IDuccSchedulingInfo si = dwr.getSchedulingInfo();
			//monitorInfo.total = si.getWorkItemsTotal();		// ignore for MR, default to 0
			//monitorInfo.done  = si.getWorkItemsCompleted();	// ignore for MR, default to 0
			//monitorInfo.error = si.getWorkItemsError();		// ignore for MR, default to 0
			//monitorInfo.retry = si.getWorkItemsRetry();		// ignore for MR, default to 0
			monitorInfo.procs = ""+dwr.getProcessMap().getAliveProcessCount();
			
			Map<DuccId, IDuccProcess> map = dwr.getProcessMap().getMap();
			monitorInfo.code = getCode(map);
			
			monitorInfo.remotePids = DuccWebUtil.getRemotePids(duccId, map);
			
			ArrayList<String> stateSequence = monitorInfo.stateSequence;
			String state = dwr.getJobState().toString();
			if(!stateSequence.contains(state)) {
				duccLogger.info(location, duccId, "state: "+state);
				stateSequence.add(state);
			}
			
			String text = null;
			
			String rmReason = dwr.getRmReason();
			if(rmReason != null) {
				 text = rmReason;
			}
			
	        IRationale rationale = dwr.getCompletionRationale();
	        if (rationale != null && rationale.isSpecified()) {
	            text = rationale.getText();
	        }
	        
	        if (text != null) {
	            monitorInfo.rationale = text;
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
	
	protected String getCode(Map<DuccId, IDuccProcess> map) {
		String code = "?";
		if(map != null) {
			Iterator<DuccId> iterator = map.keySet().iterator();
			while(iterator.hasNext()) {
				DuccId key = iterator.next();
				IDuccProcess process = map.get(key);
				code = ""+process.getProcessExitCode();
				break;
			}
		}
		return code;
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

	protected void cancel(DuccId duccId, String userId) {
		String location = "cancel";
		duccLogger.trace(location, jobid, "enter");
		
		duccLogger.info(location, duccId, userId);
		
		String java = "/bin/java";
		String jhome = System.getProperty("java.home");
		String cp = System.getProperty("java.class.path");
		String jclass = "org.apache.uima.ducc.cli.DuccManagedReservationCancel";
		String arg1 = "--"+JobRequestProperties.key_id;
		String arg2 = ""+duccId;
		String arg3 = "--"+SpecificationProperties.key_reason;
		String reason = CancelReason.MonitorPingOverdue.getText();
   		String arg4 = "\""+reason+"\"";
		
		String[] arglistUser = { "-u", userId, "--", jhome+java, "-cp", cp, jclass, arg1, arg2, arg3, arg4 };
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
					cancel(duccId, ti.user);
				}
				else {
					duccLogger.debug(location, duccId, "not cancelable");
				}
			}
		}
		
		duccLogger.trace(location, jobid, "exit");
	}
	
}
