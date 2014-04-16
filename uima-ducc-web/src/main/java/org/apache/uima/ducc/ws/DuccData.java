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
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.TimeStamp;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.common.DuccWorkJob;
import org.apache.uima.ducc.transport.event.common.DuccWorkMap;
import org.apache.uima.ducc.transport.event.common.DuccWorkReservation;
import org.apache.uima.ducc.transport.event.common.IDuccTypes.DuccType;
import org.apache.uima.ducc.transport.event.common.IDuccWork;
import org.apache.uima.ducc.transport.event.common.IDuccWorkJob;
import org.apache.uima.ducc.transport.event.common.IDuccWorkService.ServiceDeploymentType;
import org.apache.uima.ducc.transport.event.common.history.HistoryPersistenceManager;
import org.apache.uima.ducc.transport.event.common.history.IHistoryPersistenceManager;


public class DuccData {

	private static DuccLogger logger = DuccLoggerComponents.getWsLogger(DuccData.class.getName());
	private static DuccId jobid = null;
	
	private static DuccWorkMap duccWorkMap = new DuccWorkMap();
	private static DuccWorkMap duccWorkLive = new DuccWorkMap();
	
	private static ConcurrentSkipListMap<JobInfo,JobInfo> sortedJobs = new ConcurrentSkipListMap<JobInfo,JobInfo>();
	private static ConcurrentSkipListMap<DuccId,JobInfo> keyMapJobs = new ConcurrentSkipListMap<DuccId,JobInfo>();
	
	private static ConcurrentSkipListMap<ReservationInfo,ReservationInfo> sortedReservations = new ConcurrentSkipListMap<ReservationInfo,ReservationInfo>();
	private static ConcurrentSkipListMap<DuccId,ReservationInfo> keyMapReservations = new ConcurrentSkipListMap<DuccId,ReservationInfo>();
	
	private static ConcurrentSkipListMap<JobInfo,JobInfo> sortedServices = new ConcurrentSkipListMap<JobInfo,JobInfo>();
	private static ConcurrentSkipListMap<DuccId,JobInfo> keyMapServices = new ConcurrentSkipListMap<DuccId,JobInfo>();
	
	private static ConcurrentSkipListMap<Info,Info> sortedCombinedReservations = new ConcurrentSkipListMap<Info,Info>();
	private static ConcurrentSkipListMap<DuccId,Info> keyMapCombinedReservations = new ConcurrentSkipListMap<DuccId,Info>();
	
	private static PagingObserver pagingObserver = PagingObserver.getInstance();
	
	private static DuccData duccData = new DuccData();
	
	private static long slack = 100;
	
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
		String location = "put";
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
				pagingObserver.put(duccWork);
				PagingInfo pi;
				long dataTotal = 0;
				long diffTotal = 0;
				pi = pagingObserver.getData(duccId);
				if(pi != null) {
					dataTotal = pi.total;
				}
				pi = pagingObserver.getDiff(duccId);
				if(pi != null) {
					diffTotal = pi.total;
				}
				logger.debug(location, duccId, "dataTotal:"+dataTotal+" "+"diffTotal:"+diffTotal);
			}
		}
		prune();
		setPublished();
	}

	private int pruneJobs() {
		String location = "pruneJobs";
		int pruned = 0;
		if(sortedJobs.size() > (DuccBoot.maxJobs + slack)) {
			int count = 0;
			for(JobInfo jobInfo : sortedJobs.keySet()) {
				if(!jobInfo.isOperational()) {
					count++;
					if(count > DuccBoot.maxJobs) {
						DuccId duccId = jobInfo.getJob().getDuccId();
						sortedJobs.remove(jobInfo);
						keyMapJobs.remove(duccId);
						pagingObserver.remove(duccId);
						logger.debug(location, duccId, "size: "+sortedJobs.size());
						pruned++;
					}
				}
			}
			logger.debug(location, jobid, "pruned: "+pruned);
		}
		return pruned;
	}
	
	private int pruneReservations() {
		String location = "pruneReservations";
		int pruned = 0;
		if(sortedReservations.size() > (DuccBoot.maxReservations + slack)) {
			int count = 0;
			for(ReservationInfo reservationInfo : sortedReservations.keySet()) {
				if(!reservationInfo.isOperational()) {
					count++;
					if(count > DuccBoot.maxReservations) {
						DuccId duccId = reservationInfo.getReservation().getDuccId();
						sortedReservations.remove(reservationInfo);
						keyMapReservations.remove(duccId);
						logger.debug(location, duccId, "size: "+sortedReservations.size());
						pruned++;
					}
				}
			}
			logger.debug(location, jobid, "pruned: "+pruned);
		}
		return pruned;
	}
	
	private int pruneServices() {
		String location = "pruneServices";
		int pruned = 0;
		if(sortedServices.size() > (DuccBoot.maxServices + slack)) {
			int count = 0;
			for(JobInfo jobInfo : sortedServices.keySet()) {
				if(!jobInfo.isOperational()) {
					count++;
					if(count > DuccBoot.maxServices) {
						DuccId duccId = jobInfo.getJob().getDuccId();
						sortedServices.remove(jobInfo);
						keyMapServices.remove(duccId);
						logger.debug(location, duccId, "size: "+sortedServices.size());
						pruned++;
					}
				}
			}
			logger.debug(location, jobid, "pruned: "+pruned);
		}
		return pruned;
	}
	
	private int pruneCombinedReservations() {
		String location = "pruneCombinedReservations";
		int pruned = 0;
		if(sortedCombinedReservations.size() > (DuccBoot.maxReservations + slack)) {
			int count = 0;
			for(Info info : sortedCombinedReservations.keySet()) {
				if(!info.isOperational()) {
					count++;
					if(count > DuccBoot.maxReservations) {
						DuccId duccId = info.getDuccWork().getDuccId();
						sortedCombinedReservations.remove(info);
						keyMapCombinedReservations.remove(duccId);
						logger.debug(location, duccId, "size: "+sortedCombinedReservations.size());
						pruned++;
					}
				}
			}
			logger.debug(location, jobid, "pruned: "+pruned);
		}
		return pruned;
	}
	
	public void report() {
		String location = "report";
		int jc = sortedJobs.size();
		int rc = sortedReservations.size();
		int sc = sortedServices.size();
		int cc = sortedCombinedReservations.size();
		logger.info(location, jobid, ""+jc+":"+rc+":"+sc+":"+cc);
	}
	
	private void prune() {
		String location = "prune";
		int jc = pruneJobs();
		int rc = pruneReservations();
		int sc = pruneServices();
		int cc = pruneCombinedReservations();
		logger.debug(location, jobid, ""+jc+":"+rc+":"+sc+":"+cc);
	}
	
	public DuccWorkMap get() {
		return duccWorkMap;
	}
	
	public DuccWorkMap getLive() {
		return duccWorkLive;
	}
	
	public int getJobDriverNodes() {
		String location = "getJobDriverNodes";
		int retVal = 0;
		try {
			retVal = duccWorkLive.getJobCount();
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
		}
		return retVal;
	}
	
	private void updateJobs(IDuccWork duccWork) {
		String location = "updateJobs";
		DuccId duccId = duccWork.getDuccId();
		DuccWorkJob job = (DuccWorkJob)duccWork;
		if(keyMapJobs.containsKey(duccId)) {
			sortedJobs.remove(keyMapJobs.get(duccId));
			keyMapJobs.remove(duccId);
		}
		JobInfo jobInfo = new JobInfo(job);
		sortedJobs.put(jobInfo, jobInfo);
		if(!duccWork.isCompleted()) {
			keyMapJobs.put(duccId, jobInfo);
			logger.debug(location, duccId, "put job");
		}
	}
	
	private void updateReservations(IDuccWork duccWork) {
		String location = "updateReservations";
		DuccId duccId = duccWork.getDuccId();
		DuccWorkReservation reservation = (DuccWorkReservation)duccWork;
		if(keyMapReservations.containsKey(duccId)) {
			sortedReservations.remove(keyMapReservations.get(duccId));
			keyMapReservations.remove(duccId);
		}
		ReservationInfo reservationInfo = new ReservationInfo(reservation);
		sortedReservations.put(reservationInfo, reservationInfo);
		if(!duccWork.isCompleted()) {
			keyMapReservations.put(duccId, reservationInfo);
			logger.debug(location, duccId, "put reservation");
		}
		//
		if(keyMapCombinedReservations.containsKey(duccId)) {
			sortedCombinedReservations.remove(keyMapCombinedReservations.get(duccId));
			keyMapCombinedReservations.remove(duccId);
		}
		Info rInfo = new Info(reservation);
		sortedCombinedReservations.put(rInfo, rInfo);
		if(!duccWork.isCompleted()) {
			keyMapCombinedReservations.put(duccId, rInfo);
			logger.debug(location, duccId, "put combined");
		}
	}
	
	private void updateServices(IDuccWork duccWork) {
		String location = "updateServices";
		DuccId duccId = duccWork.getDuccId();
		DuccWorkJob service = (DuccWorkJob)duccWork;
		if(keyMapServices.containsKey(duccId)) {
			sortedServices.remove(keyMapServices.get(duccId));
			keyMapServices.remove(duccId);
		}
		JobInfo serviceInfo = new JobInfo(service);
		sortedServices.put(serviceInfo, serviceInfo);
		if(!duccWork.isCompleted()) {
			keyMapServices.put(duccId, serviceInfo);
			logger.debug(location, duccId, "put service");
		}
		//
		ServiceDeploymentType sdt = service.getServiceDeploymentType();
		if(sdt != null) {
			switch(sdt) {
			case other:
				if(keyMapCombinedReservations.containsKey(duccId)) {
					sortedCombinedReservations.remove(keyMapCombinedReservations.get(duccId));
					keyMapCombinedReservations.remove(duccId);
				}
				Info sInfo = new Info(service);
				sortedCombinedReservations.put(sInfo, sInfo);
				if(!duccWork.isCompleted()) {
					keyMapCombinedReservations.put(duccId, sInfo);
					logger.debug(location, duccId, "put combined");
				}
				break;
			default:
				break;
			}
		}
	}
	
	private void updateSortedMaps(IDuccWork duccWork) {
		if(duccWork != null) {
			DuccType duccType = duccWork.getDuccType();
			if(duccType != null) {
				switch(duccWork.getDuccType()) {
				case Job:
					updateJobs(duccWork);
					break;
				case Reservation:
					updateReservations(duccWork);
					break;
				case Service:
					updateServices(duccWork);
					break;
				default:
					break;
				}
			}
		}
	}
	
	public IDuccWorkJob getJob(DuccId duccId) {
		IDuccWorkJob retVal = null;
		ConcurrentSkipListMap<JobInfo,JobInfo> sortedJobs = getSortedJobs();
		if(sortedJobs.size()> 0) {
			Iterator<Entry<JobInfo, JobInfo>> iterator = sortedJobs.entrySet().iterator();
			while(iterator.hasNext()) {
				JobInfo jobInfo = iterator.next().getValue();
				DuccWorkJob job = jobInfo.getJob();
				if(job.getDuccId().getFriendly() == duccId.getFriendly()) {
					retVal = job;
					break;
				}
			}
		}
		return retVal;
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
	
	public ConcurrentSkipListMap<Info,Info> getSortedCombinedReservations() {
		return sortedCombinedReservations;
	}
	
	public boolean isLive(DuccId duccId) {
		return duccWorkLive.containsKey(duccId);
	}
	
}
