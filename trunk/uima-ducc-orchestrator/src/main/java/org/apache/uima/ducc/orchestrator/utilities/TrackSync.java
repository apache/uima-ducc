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
package org.apache.uima.ducc.orchestrator.utilities;

import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.id.DuccId;

/**
 * Helper class to account for synchronization time (usually on DuccWorkMap) in or.log.
 * Introduced by Jira UIMA-3657.
 */

public class TrackSync {
	
	private static DuccLogger logger = DuccLoggerComponents.getOrLogger(TrackSync.class.getName());
	private static DuccId jobid = null;
	
	private static String sep = ".";
	
	private static ConcurrentSkipListMap<String,ConcurrentSkipListMap<String,AtomicLong>> map = new ConcurrentSkipListMap<String,ConcurrentSkipListMap<String,AtomicLong>>();
	
	private static ConcurrentSkipListMap<String,TrackSync> mapHeldBy = new ConcurrentSkipListMap<String,TrackSync>();
	
	private static long msPerSecond = 1000;
	private static long timeLimit = 10*msPerSecond;
	
	private String target = null;
	private String requester = null;
	
	private AtomicLong t0 = new AtomicLong(0);
	private AtomicLong t1 = new AtomicLong(0);
	private AtomicLong t2 = new AtomicLong(0);
	
	private static void addPending(String target, String requester) {
		String location = "addPending";
		try {
			ConcurrentSkipListMap<String,AtomicLong> tMap = new ConcurrentSkipListMap<String,AtomicLong>();
			map.putIfAbsent(target, tMap);
			tMap = map.get(target);
			AtomicLong rCount = new AtomicLong(0);
			tMap.putIfAbsent(requester, rCount);
			rCount = tMap.get(requester);
			rCount.getAndIncrement();
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
		}
	}
	
	private static void delPending(String target, String requester) {
		String location = "delPending";
		try {
			ConcurrentSkipListMap<String,AtomicLong> tMap = new ConcurrentSkipListMap<String,AtomicLong>();
			map.putIfAbsent(target, tMap);
			tMap = map.get(target);
			AtomicLong rCount = new AtomicLong(1);
			tMap.putIfAbsent(requester, rCount);
			rCount = tMap.get(requester);
			rCount.getAndDecrement();
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
		}
	}
	
	private static long getPending(String target) {
		String location = "getPending";
		long retVal = 0;
		try {
			ConcurrentSkipListMap<String, AtomicLong> oMap = map.get(target);
			if(oMap != null) {
				for(Entry<String, AtomicLong> entry : oMap.entrySet()) {
					retVal += entry.getValue().get();
				}
			}
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
		}
		return retVal;
	}
	
	private static void report(String target) {
		String location = "report";
		try {
			ConcurrentSkipListMap<String, AtomicLong> oMap = map.get(target);
			if(oMap != null) {
				for(Entry<String, AtomicLong> entry : oMap.entrySet()) {
					String requester = entry.getKey();
					long pending = entry.getValue().longValue();
					if(pending > 0) {
						logger.info(location, jobid, "target: "+target+" "+"requester: "+requester+" "+" pending: "+pending);
					}
				}
			}
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
		}
	}
	
	private static void overtime(TrackSync ts, String target) {
		String location = "overtime";
		try {
			if(ts != null) {
				long timeHeld = ts.getTimeHeld();
				if(timeHeld > timeLimit) {
					logger.info(location, jobid, "target: "+ts.target+" "+"requester: "+ts.requester+" "+"wait: "+ts.getTimeWait()+" "+"held: "+ts.getTimeHeld());
					report(target);
				}
			}
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
		}
	}
	
	private static void blocked(TrackSync ts, String target, String requester) {
		String location = "blocked";
		try {
			if(ts != null) {
				long timeHeld = ts.getTimeHeld();
				if(timeHeld > timeLimit) {
					logger.info(location, jobid, "target: "+ts.target+" "+"requester: "+ts.requester+" "+"time: "+ts.getTimeHeld()+" "+"blocking: "+requester);
				}
			}
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
		}
	}
	
	public static TrackSync await(Object targetObject, Class<?> requesterClass, String requesterLocation) {
		String location = "await";
		TrackSync ts = new TrackSync();
		try {
			ts.target = targetObject.getClass().getSimpleName();
			ts.requester = requesterClass.getSimpleName()+sep+requesterLocation;
			TrackSync tsHolder = mapHeldBy.get(ts.target);
			addPending(ts.target, ts.requester);
			blocked(tsHolder, ts.target, ts.requester);
			logger.trace(location, jobid, "target: "+ts.target+" "+"requester: "+ts.requester);
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
		}
		return ts;
	}
	
	public TrackSync() {
		setT0(this);
	}
	
	private void setT0(TrackSync ts) {
		if(ts != null) {
			if(ts.t0.get() == 0) {
				ts.t0.compareAndSet(0, System.currentTimeMillis());
			}
		}
	}
	
	private void setT1(TrackSync ts) {
		if(ts != null) {
			if(ts.t1.get() == 0) {
				ts.t1.compareAndSet(0, System.currentTimeMillis());
			}
		}
	}
	
	private void setT2(TrackSync ts) {
		if(ts != null) {
			if(ts.t2.get() == 0) {
				ts.t2.compareAndSet(0, System.currentTimeMillis());
			}
		}
	}
	
	public long getT0() {
		long value = t0.get();
		if(value == 0) {
			value = System.currentTimeMillis();
		}
		return value;
	}
	
	public long getT1() {
		long value = t1.get();
		if(value == 0) {
			value = System.currentTimeMillis();
		}
		return value;
	}
	
	public long getT2() {
		long value = t2.get();
		if(value == 0) {
			value = System.currentTimeMillis();
		}
		return value;
	}
	
	private String target() {
		return "target: "+target;
	}
	
	private String requester() {
		return "requester: "+requester;
	}
	
	private String timeWait() {
		return "wait: "+getTimeWait();
	}
	
	private String timeHeld() {
		return "held: "+getTimeHeld();
	}
	
	private String pending() {
		return "pending: "+getPending(target);
	}
	
	public void using() {
		String location = "using";
		try {
			TrackSync tsHolder = TrackSync.mapHeldBy.get(target); 
			setT2(tsHolder);
			overtime(tsHolder, target);
			setT1(this);
			TrackSync.delPending(target, requester);
			TrackSync.mapHeldBy.put(target, this);
			logger.trace(location, jobid, target()+" "+requester()+" "+timeWait());
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
		}
	}
	
	public void ended() {
		String location = "ended";
		try {
			setT2(this);
			logger.trace(location, jobid, target()+" "+requester()+" "+timeWait()+" "+timeHeld()+" "+pending());
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
		}
	}
	
	public long getTimeWait() {
		return getT1() - getT0();
	}
	
	public long getTimeHeld() {
		return getT2() - getT1();
	}
}
