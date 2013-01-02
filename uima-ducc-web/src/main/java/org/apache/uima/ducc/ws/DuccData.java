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

import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.uima.ducc.common.utils.TimeStamp;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.common.DuccWorkJob;
import org.apache.uima.ducc.transport.event.common.DuccWorkMap;
import org.apache.uima.ducc.transport.event.common.DuccWorkReservation;
import org.apache.uima.ducc.transport.event.common.IDuccWork;
import org.apache.uima.ducc.transport.event.common.history.HistoryPersistenceManager;
import org.apache.uima.ducc.transport.event.common.history.IHistoryPersistenceManager;


public class DuccData {

	private static DuccWorkMap duccWorkMap = new DuccWorkMap();
	private static DuccWorkMap duccWorkLive = new DuccWorkMap();
	
	private static ConcurrentSkipListMap<JobInfo,JobInfo> sortedJobs = new ConcurrentSkipListMap<JobInfo,JobInfo>();
	private static ConcurrentSkipListMap<ReservationInfo,ReservationInfo> sortedReservations = new ConcurrentSkipListMap<ReservationInfo,ReservationInfo>();
	private static ConcurrentSkipListMap<JobInfo,JobInfo> sortedServices = new ConcurrentSkipListMap<JobInfo,JobInfo>();

	private static ConcurrentSkipListMap<DuccId,Object> keyMap = new ConcurrentSkipListMap<DuccId,Object>();
	
	private static DuccData duccData = new DuccData();
	
	public static DuccData getInstance() {
		return duccData;
	}
	
	private volatile String published = null;
	
	private IHistoryPersistenceManager hpm = HistoryPersistenceManager.getInstance();
	
	public boolean isPublished() {
		return published != null;
	}
	
	public void setPublished() {
		published = TimeStamp.getCurrentMillis();
	}
	
	public String getPublished() {
		return published;
	}
	
	public void putIfNotPresent(IDuccWork duccWork) {
		synchronized(this) {
			DuccId duccId = duccWork.getDuccId();
			if(duccWorkMap.findDuccWork(duccId) == null) {
				duccWorkMap.addDuccWork(duccWork);
				updateSortedMaps(duccWork);
			}
		}
	}
	
	private void mergeHistory(DuccWorkMap map) {
		Iterator<DuccId> iterator = duccWorkLive.keySet().iterator();
		while(iterator.hasNext()) {
			DuccId duccId = iterator.next();
			IDuccWork duccWork = duccWorkLive.findDuccWork(duccId);
			IDuccWork history = null;
			switch(duccWork.getDuccType()) {
			case Job:
				history = hpm.jobRestore(duccId);
				break;
			case Reservation:
				history = hpm.reservationRestore(duccId);
				break;
			case Service:
				history = hpm.serviceRestore(duccId);
				break;
			}
			if(history != null) {
				map.put(duccId, history);
			}
		}
	}
	
	public void put(DuccWorkMap map) {
		synchronized(this) {
			DuccWorkMap mapCopy = map.deepCopy();
			mergeHistory(map);
			duccWorkLive = mapCopy;
			Iterator<DuccId> iterator = map.keySet().iterator();
			while(iterator.hasNext()) {
				DuccId duccId = iterator.next();
				IDuccWork duccWork = map.findDuccWork(duccId);
				duccWorkMap.addDuccWork(duccWork);
				updateSortedMaps(duccWork);
			}
		}
		setPublished();
	}
	
	public DuccWorkMap get() {
		return duccWorkMap;
	}
	
	public DuccWorkMap getLive() {
		return duccWorkLive;
	}
	
	private void cacheManager(IDuccWork duccWork, Object cacheKey) {
		DuccId duccId = duccWork.getDuccId();
		if(keyMap.containsKey(duccId)) {
			switch(duccWork.getDuccType()) {
			case Job:
				sortedJobs.remove(keyMap.get(duccId));
				break;
			case Reservation:
				sortedReservations.remove(keyMap.get(duccId));
				break;
			case Service:
				sortedServices.remove(keyMap.get(duccId));
				break;
			}
			keyMap.remove(duccId);
		}
		if(!duccWork.isCompleted()) {
			keyMap.put(duccId, cacheKey);
		}
	}
	
	private void updateSortedMaps(IDuccWork duccWork) {
		switch(duccWork.getDuccType()) {
			case Job:
				DuccWorkJob job = (DuccWorkJob)duccWork;
				JobInfo jobInfo = new JobInfo(job);
				cacheManager(job, jobInfo);
				sortedJobs.put(jobInfo, jobInfo);
				break;
			case Reservation:
				DuccWorkReservation reservation = (DuccWorkReservation)duccWork;
				ReservationInfo reservationInfo = new ReservationInfo(reservation);
				cacheManager(reservation, reservationInfo);
				sortedReservations.put(reservationInfo, reservationInfo);
				break;
			case Service:
				DuccWorkJob service = (DuccWorkJob)duccWork;
				JobInfo serviceInfo = new JobInfo(service);
				cacheManager(service, serviceInfo);
				sortedServices.put(serviceInfo, serviceInfo);
				break;
		}
	}
	
	public ConcurrentSkipListMap<JobInfo,JobInfo> getSortedJobs() {
		return sortedJobs;
	}
	
	public ConcurrentSkipListMap<ReservationInfo,ReservationInfo> getSortedReservations() {
		return sortedReservations;
	}
	
	public ConcurrentSkipListMap<JobInfo,JobInfo> getSortedServices() {
		return sortedServices;
	}
	
	public boolean isLive(DuccId duccId) {
		return duccWorkLive.containsKey(duccId);
	}
	
}
