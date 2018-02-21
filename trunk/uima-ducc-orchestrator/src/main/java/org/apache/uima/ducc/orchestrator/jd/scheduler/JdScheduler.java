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
package org.apache.uima.ducc.orchestrator.jd.scheduler;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.uima.ducc.common.NodeIdentity;
import org.apache.uima.ducc.common.SizeBytes;
import org.apache.uima.ducc.common.db.DbQuery;
import org.apache.uima.ducc.common.db.IDbMachine;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.TimeStamp;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.orchestrator.OrchestratorCheckpoint;
import org.apache.uima.ducc.orchestrator.OrchestratorCommonArea;
import org.apache.uima.ducc.orchestrator.ReservationFactory;
import org.apache.uima.ducc.orchestrator.StateManager;
import org.apache.uima.ducc.orchestrator.WorkMapHelper;
import org.apache.uima.ducc.transport.event.cli.ReservationRequestProperties;
import org.apache.uima.ducc.transport.event.cli.ReservationSpecificationProperties;
import org.apache.uima.ducc.transport.event.common.DuccWorkJob;
import org.apache.uima.ducc.transport.event.common.DuccWorkMap;
import org.apache.uima.ducc.transport.event.common.DuccWorkReservation;
import org.apache.uima.ducc.transport.event.common.IDuccCompletionType.JobCompletionType;
import org.apache.uima.ducc.transport.event.common.IDuccCompletionType.ReservationCompletionType;
import org.apache.uima.ducc.transport.event.common.IDuccSchedulingInfo;
import org.apache.uima.ducc.transport.event.common.IDuccState.ReservationState;
import org.apache.uima.ducc.transport.event.common.IDuccTypes.DuccType;
import org.apache.uima.ducc.transport.event.common.IDuccWork;
import org.apache.uima.ducc.transport.event.common.IDuccWorkMap;
import org.apache.uima.ducc.transport.event.common.IDuccWorkReservation;
import org.apache.uima.ducc.transport.event.common.IRationale;
import org.apache.uima.ducc.transport.event.common.IResourceState.ProcessDeallocationType;
import org.apache.uima.ducc.transport.event.common.JdReservationBean;
import org.apache.uima.ducc.transport.event.common.Rationale;

public class JdScheduler {
	
	private static DuccLogger logger = new DuccLogger(JdScheduler.class);
	private static DuccId jobid = null;
	
	private static ReservationFactory reservationFactory = ReservationFactory.getInstance();
	
	private static JdScheduler instance = new JdScheduler();
	
	private static OrchestratorCommonArea orchestratorCommonArea = OrchestratorCommonArea.getInstance();
	
	public static JdScheduler getInstance() {
		return instance;
	}
	
	private ConcurrentHashMap<DuccId,JdReservation> map = new ConcurrentHashMap<DuccId,JdReservation>();
	
	private AtomicBoolean autoManage = new AtomicBoolean(true);
	private AtomicBoolean requestPending = new AtomicBoolean(false);
	private AtomicBoolean changes = new AtomicBoolean(false);
	
	// Manage the allocation of JD slices, which comprises making reservations,
	// subdividing said reservations into slices, one per JD, and returning 
	// unused reservations.
	
	public JdScheduler() {	
	}
	
	// Auto-manage true is nominal.
	
	public void setAutomanage() {
		autoManage.set(true);
	}
	
	// Auto-manage false is for testing only.
	
	public void resetAutomanage() {
		autoManage.set(false);
	}
	
	// Save current slice allocations within each Reservation comprising
	// the Orchestrator's checkpoint map.
	
	public void ckpt() {
		String location = "ckpt";
		if(changes.get()) {
			changes.set(false);
			try {
				IDuccWorkMap dwm = OrchestratorCommonArea.getInstance().getWorkMap();
				for(Entry<DuccId, JdReservation> entry : map.entrySet()) {
					DuccId reservationIdentity = entry.getKey();
					IDuccWork dw = dwm.findDuccWork(reservationIdentity);
					if(dw instanceof IDuccWorkReservation) {
						IDuccWorkReservation dwr = (IDuccWorkReservation) dw;
						List<JdReservationBean> jdReservationBeanList = getJdReservationBeanList(reservationIdentity);
						dwr.setJdReservationBeanList(jdReservationBeanList);
						if(jdReservationBeanList != null) {
							logger.debug(location, reservationIdentity, "size: "+jdReservationBeanList.size());
						}
						else {
							logger.debug(location, reservationIdentity, "size: "+null);
						}
					}
				}
			}
			catch(Exception e) {
				logger.error(location, jobid, e);
			}
			OrchestratorCheckpoint.getInstance().saveState();
		}
	}
	
	// Restore current slice allocations within each Reservation comprising
	// the Orchestrator's checkpoint map.
	
	public void restore() {
		String location = "restore";
		try {
			IDuccWorkMap dwm = OrchestratorCommonArea.getInstance().getWorkMap();
			for(Entry<DuccId, IDuccWork> entry : dwm.getMap().entrySet()) {
				IDuccWork dw = entry.getValue();
				if(dw instanceof IDuccWorkReservation) {
					IDuccWorkReservation dwr = (IDuccWorkReservation) dw;
					List<JdReservationBean> jdReservationBeanList = dwr.getJdReservationBeanList();
					if(jdReservationBeanList != null) {
						setJdReservationBeanList(jdReservationBeanList);
					}
				}
			}
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
		}
	}
	
	public boolean isMinimalAllocateRequirementMet() {
		String location = "isMinimalAllocateRequirementMet";
		boolean retVal = false;
		StringBuffer sb = new StringBuffer();
		long minSlices = getReservationSlicesMinimum();
		long resCount = getReservationCount();
		sb.append("minSlices="+minSlices);
		sb.append(" ");
		sb.append("resCount="+resCount);
		String text = sb.toString();
		logger.debug(location, jobid, text);
		if(minSlices == 0) {
			retVal = true;
		}
		else if(resCount > 0) {
			retVal = true;
		}
		return retVal;
	}
	
	// Return the number of Reservation Slices minimum needed for JDs.
	
	private long getReservationSlicesMinimum() {
		JdHostProperties jdHostProperties = new JdHostProperties();
		return getSlicesReserveDesired(jdHostProperties);
	} 
	
	// Return the number of Reservations allocated for JDs.
	
	private int getReservationCount() {
		int count = 0;
		for(Entry<DuccId, JdReservation> entry : map.entrySet()) {
			JdReservation jdReservation = entry.getValue();
			switch(jdReservation.getReservationState()) {
			case Assigned:
				count += 1;
				break;
			default:
				break;
			}
		}
		return count;
	} 
	
	// Fetch Reservations on given node (name)
	
	private List<JdReservation> getJdReservationsByName(String name) {
		String location = "getJdReservationByName";
		List<JdReservation> retVal = new ArrayList<JdReservation>();
		try {
			if(name != null) {
				for(Entry<DuccId, JdReservation> entry : map.entrySet()) {
					String host = entry.getValue().getHost();
					if(host.equals(name)) {
						retVal.add(entry.getValue());
						break;
					}
				}
			}
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
		}
		return retVal;
	}
	
	// Cancel Reservation
	
	private void cancelReservation(JdReservation jdReservation, String reason) {
		String location = "cancelReservation";
		try {
			DuccId reservationIdentity = jdReservation.getDuccId();
			DuccWorkMap workMap = orchestratorCommonArea.getWorkMap();
			String id = ""+reservationIdentity.getFriendly();
			DuccWorkReservation duccWorkReservation = (DuccWorkReservation) WorkMapHelper.findDuccWork(workMap, DuccType.Reservation, id, this, location);
			switch(duccWorkReservation.getReservationState()) {
			case Completed:
				break;
			default:
				JdHostProperties jdHostProperties = new JdHostProperties();
				duccWorkReservation.getStandardInfo().setCancelUser(jdHostProperties.getHostUser());
				duccWorkReservation.getStandardInfo().setDateOfCompletion(TimeStamp.getCurrentMillis());
				duccWorkReservation.stateChange(ReservationState.Completed);
				duccWorkReservation.complete(ReservationCompletionType.CanceledBySystem);
				IRationale rationale = new Rationale(reason);
				duccWorkReservation.setCompletionRationale(rationale);
				OrchestratorCheckpoint.getInstance().saveState();
				logger.warn(location, reservationIdentity, reason);
				break;
			}
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
		}
	}
	
	// Cancel Jobs
	
	private void cancelJobs(JdReservation jdReservation, String reason) {
		String location = "cancelJobs";
		try {
			Map<DuccId, SizeBytes> map = jdReservation.getMap();
			for(Entry<DuccId, SizeBytes> entry : map.entrySet()) {
				DuccId jobIdentity = entry.getKey();
				DuccWorkMap workMap = orchestratorCommonArea.getWorkMap();
				String id = ""+jobIdentity.getFriendly();
				DuccWorkJob duccWorkJob = (DuccWorkJob) WorkMapHelper.findDuccWork(workMap, DuccType.Job, id, this, location);
				IRationale rationale = new Rationale(reason.toString());
				JobCompletionType jobCompletionType = JobCompletionType.CanceledBySystem;
				
				StateManager stateManager = StateManager.getInstance();
				stateManager.jobTerminate(duccWorkJob, jobCompletionType, rationale, ProcessDeallocationType.JobCanceled);
				
				OrchestratorCheckpoint.getInstance().saveState();
				logger.warn(location, jobIdentity, reason);
			}
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
		}
	}
	
	// Handle down JD node
	
	private void handleDownNode(JdReservation jdReservation) {
		String location = "handleDownNode";
		try {
			if(jdReservation != null) {
				String host = jdReservation.getHost();
				String text = "job driver node down: "+host;
				cancelReservation(jdReservation, text);
				cancelJobs(jdReservation, text);
			}
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
		}
	}
	
	// Monitor for down JD nodes
	
	private void monitor() {
		String location = "monitor";
		try {
			Map<String, IDbMachine> qMap = DbQuery.getInstance().getMapMachines();
			for(Entry<String, IDbMachine> entry : qMap.entrySet()) {
				Boolean responsive = entry.getValue().getResponsive();
				if(!responsive.booleanValue()) {
					String name = entry.getValue().getName();
					List<JdReservation> list = getJdReservationsByName(name);
					if(list != null) {
						for(JdReservation jdReservation : list) {
							handleDownNode(jdReservation);
						}
					}
				}
			}
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
		}
	}
	
	// Process an OR publication.
	
	public void handle(IDuccWorkMap dwm) {
		String location = "handle";
		try {
			monitor();
			if(dwm != null) {
				JdHostProperties jdHostProperties = new JdHostProperties();
				resourceAccounting(dwm, jdHostProperties);
				resourceAdjustment(dwm, jdHostProperties);
				ckpt();
			}
			else {
				logger.debug(location, jobid, "dwm: null");
			}
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
		}
	}
	
	// Account for state changes for JD Reservations.
	
	private void resourceAccounting(IDuccWorkMap dwm, JdHostProperties jdHostProperties) {
		String location = "resourceAccounting";
		String jdHostClass = jdHostProperties.getHostClass();
		boolean pendingFlag = false;
		if(jdHostClass != null) {
			Set<DuccId> known = new HashSet<DuccId>();
			known.addAll(map.keySet());
			for(Entry<DuccId, IDuccWork> entry : dwm.getMap().entrySet()) {
				IDuccWork dw = entry.getValue();
				if(dw instanceof IDuccWorkReservation) {
					IDuccWorkReservation dwr = (IDuccWorkReservation) dw;
					IDuccSchedulingInfo schedulingInfo = dwr.getSchedulingInfo();
					if(schedulingInfo != null) {
						String schedulingClass = schedulingInfo.getSchedulingClass();
						if(schedulingClass != null) {
							if(schedulingClass.equals(jdHostClass)) {
								ReservationState reservationState = dwr.getReservationState();
								switch(reservationState) {
								case Assigned:
									reservationUp(dwr, jdHostProperties);
									break;
								case Completed:
									reservationDown(dwr);
									break;
								case Received:	
								case Undefined:
									reservationOther(dwr, jdHostProperties);
									break;
								case WaitingForResources:
									pendingFlag = true;
									break;
								default:
									reservationOther(dwr, jdHostProperties);
									break;
								}
							}
						}
					}
					known.remove(entry.getKey());
				}
			}
			for(DuccId reservationIdentity : known) {
				reservationVanished(reservationIdentity);
			}
		}
		if(pendingFlag) {
			requestPending.set(true);
		}
		else {
			requestPending.set(false);
		}
		logger.trace(location, jobid, "total: "+countReservationsTotal()+" "+"up: "+countReservationsUp());
	}
	
	private long getSlicesReserveDesired(JdHostProperties jdHostProperties) {
		long slicesReserveDesired = JdHelper.getSlicesReserve(jdHostProperties);
		return slicesReserveDesired;
	}
	
	private long getSlicesReserveActual() {
		long slicesReserveActual = countSlicesAvailable();
		return slicesReserveActual;
	}
	
	// Determine if at least one JD Reservation can be unreserved.
	
	private boolean isReservationDivestable(IDuccWorkMap dwm, JdHostProperties jdHostProperties) {
		boolean retVal = false;
		long slicesReserveDesired = getSlicesReserveDesired(jdHostProperties);
		long slicesReserveActual = getSlicesReserveActual();
		if(map.size() > 1) {
			synchronized(this) {
				for(Entry<DuccId, JdReservation> entry : map.entrySet()) {
					JdReservation jdReservation = entry.getValue();
					if(jdReservation.isEmpty()) {
						long slicesToRelease = jdReservation.getSlicesTotal();
						long slicesReserveAfterRelease = slicesReserveActual - slicesToRelease;
						if(slicesReserveAfterRelease > slicesReserveDesired) {
							retVal = true;
							break;
						}
					}
				}
			}
		}
		return retVal;
	}
	
	// Acquire or divest JD Reservations based upon need.
	
	private void resourceAdjustment(IDuccWorkMap dwm, JdHostProperties jdHostProperties) {
		String location = "resourceAdjustment";
		if(autoManage.get()) {
			long slicesReserveDesired = getSlicesReserveDesired(jdHostProperties);
			long slicesReserveActual = getSlicesReserveActual();
			logger.debug(location, jobid, "actual: "+slicesReserveActual+" "+"desired: "+slicesReserveDesired);
			if(slicesReserveActual < slicesReserveDesired) {
				if(requestPending.get()) {
					reservationPending(dwm, jdHostProperties);
				}
				else {
					reservationAcquire(dwm, jdHostProperties);
				}
			}
			else if(isReservationDivestable(dwm, jdHostProperties)) {
				while(isReservationDivestable(dwm, jdHostProperties)) {
					reservationDivest(dwm, jdHostProperties);
				}
			}
			else {
				reservationNoChange(dwm, jdHostProperties);
			}
		}
		else {
			logger.debug(location, jobid, "automanage: "+autoManage.get());
		}
	}
	
	private void reservationPending(IDuccWorkMap dwm, JdHostProperties jdHostProperties) {
		String location = "reservationPending";
		long slicesReserveDesired = getSlicesReserveDesired(jdHostProperties);
		long slicesReserveActual = getSlicesReserveActual();
		logger.debug(location, jobid, "actual: "+slicesReserveActual+" "+"desired: "+slicesReserveDesired);
	}
	
	// Request a new JD Reservation.
	
	private void reservationAcquire(IDuccWorkMap dwm, JdHostProperties jdHostProperties) {
		String location = "reservationAcquire";
		ReservationRequestProperties reservationRequestProperties = new ReservationRequestProperties();
		//
		String key;
		String value;
		//
		key = ReservationSpecificationProperties.key_scheduling_class;
		value = jdHostProperties.getHostClass();
		reservationRequestProperties.setProperty(key, value);
		//
		key = ReservationSpecificationProperties.key_memory_size;
		value = jdHostProperties.getHostMemorySize();
		reservationRequestProperties.setProperty(key, value);
		//
		key = ReservationSpecificationProperties.key_user;
		value = jdHostProperties.getHostUser();
		reservationRequestProperties.setProperty(key, value);
		//
		key = ReservationSpecificationProperties.key_description;
		value = jdHostProperties.getHostDescription();
		reservationRequestProperties.setProperty(key, value);
		//
		DuccWorkReservation dwr = reservationFactory.create(reservationRequestProperties);
		dwr.setJdReservation();
		//
		DuccWorkMap workMap = (DuccWorkMap) dwm;
		WorkMapHelper.addDuccWork(workMap, dwr, this, location);
		// state: Received
		dwr.stateChange(ReservationState.Received);
		OrchestratorCheckpoint.getInstance().saveState();
		// state: WaitingForResources
		dwr.stateChange(ReservationState.WaitingForResources);
		OrchestratorCheckpoint.getInstance().saveState();
		//
		long slicesReserveDesired = getSlicesReserveDesired(jdHostProperties);
		long slicesReserveActual = getSlicesReserveActual();
		DuccId reservationIdentity = dwr.getDuccId();
		logger.info(location, reservationIdentity, "actual: "+slicesReserveActual+" "+"desired: "+slicesReserveDesired);
	}
	
	// Return an unused JD Reservation.
	
	private void reservationDivest(IDuccWorkMap dwm, JdHostProperties jdHostProperties) {
		String location = "reservationDivest";
		DuccId reservationIdentity = null;
		synchronized(this) {
			for(Entry<DuccId, JdReservation> entry : map.entrySet()) {
				JdReservation jdReservation = entry.getValue();
				if(jdReservation.isEmpty()) {
					reservationIdentity = entry.getKey();
					map.remove(reservationIdentity);
					break;
				}
				jdReservation = null;
			}
		}
		if(reservationIdentity != null) {
			IDuccWork dw = dwm.findDuccWork(reservationIdentity);
			if(dw != null) {
				IDuccWorkReservation dwr = (IDuccWorkReservation) dw;
				// state: Completed
				dwr.stateChange(ReservationState.Completed);
				dwr.setCompletionType(ReservationCompletionType.CanceledBySystem);
				dwr.setCompletionRationale( new Rationale("excess capacity"));
				OrchestratorCheckpoint.getInstance().saveState();
			}
		}
		long slicesReserveDesired = getSlicesReserveDesired(jdHostProperties);
		long slicesReserveActual = getSlicesReserveActual();
		logger.info(location, reservationIdentity, "actual: "+slicesReserveActual+" "+"desired: "+slicesReserveDesired);
	}
	
	// Nothing to do.
	
	private void reservationNoChange(IDuccWorkMap dwm, JdHostProperties jdHostProperties) {
		String location = "reservationNoChange";
		long slicesReserveDesired = getSlicesReserveDesired(jdHostProperties);
		long slicesReserveActual = getSlicesReserveActual();
		logger.trace(location, jobid, "actual: "+slicesReserveActual+" "+"desired: "+slicesReserveDesired);
	}
	
	// Update a list of JDs (DuccId's) allocated on a JD Reservation.
	
	private void setJdReservationBeanList(List<JdReservationBean> jdReservationBeanList) {
		if(jdReservationBeanList != null) {
			for(JdReservationBean entry : jdReservationBeanList) {
				JdReservation jdReservation = (JdReservation) entry;
				DuccId reservationIdentity = jdReservation.getDuccId();
				map.put(reservationIdentity, jdReservation);
			}
		}
	}
	
	// Return a list of JDs (DuccId's) allocated on a JD Reservation.
	
	public List<JdReservationBean> getJdReservationBeanList(DuccId reservationIdentity) {
		String location = "getJdReservationBeanList";
		List<JdReservationBean> jdReservationBeanList = new ArrayList<JdReservationBean>();
		for(Entry<DuccId, JdReservation> entry : map.entrySet()) {
			JdReservation jdReservation = entry.getValue();
			if(reservationIdentity.equals(jdReservation.getDuccId())) {
				jdReservationBeanList.add(jdReservation);
				logger.trace(location, reservationIdentity, jdReservationBeanList.size());
			}
		}
		return jdReservationBeanList;
	}
	
	// Return the number of JD Reservations.
	
	public int countReservationsTotal() {
		int count = map.size();
		return count;
	}
	
	// Return the number of JD Reservations that are "up" (e.g. in Assigned state).
	
	public int countReservationsUp() {
		int count = 0;
		for(Entry<DuccId, JdReservation> entry : map.entrySet()) {
			JdReservation jdReservation = entry.getValue();
			if(jdReservation.isUp()) {
				count +=1;
			}
		}
		return count;
	}
	
	// Handle a JD Reservation that has become available.
	
	private void reservationUp(IDuccWorkReservation dwr, JdHostProperties jdHostProperties) {
		String location = "reservationUp";
		DuccId reservationIdentity = dwr.getDuccId();
		JdReservation jdReservation = null;
		SizeBytes reservationSize = JdHelper.getReservationSize(dwr);
		SizeBytes sliceSize = JdHelper.getSliceSize(jdHostProperties);
		jdReservation = map.get(reservationIdentity);
		if(jdReservation == null) {
			jdReservation = new JdReservation(dwr, reservationSize, sliceSize);
			map.putIfAbsent(reservationIdentity, jdReservation);
		}
		else if(!jdReservation.isUp()) {
			jdReservation = new JdReservation(dwr, reservationSize, sliceSize);
			map.putIfAbsent(reservationIdentity, jdReservation);
		}
		jdReservation = map.get(reservationIdentity);
		if(jdReservation != null) {
			logger.debug(location, reservationIdentity, "host: "+jdReservation.getHost());
		}
	}
	
	// Handle a JD Reservation that has become unavailable.
	
	private void reservationDown(IDuccWorkReservation dwr) {
		String location = "reservationDown";
		DuccId reservationIdentity = dwr.getDuccId();
		JdReservation jdReservation = null;
		List<JdReservation> list = new ArrayList<JdReservation>();
		synchronized(this) {
			jdReservation = map.get(reservationIdentity);
			if(jdReservation != null) {
				map.remove(reservationIdentity);
				list.add(jdReservation);
			}
		}
		if(list.size() > 0) {
			logger.info(location, reservationIdentity, list.size());
			defunct(list);
		}
	}
	
	// Handle unexpected state for a JD Reservation.
	
	private void reservationOther(IDuccWorkReservation dwr, JdHostProperties jdHostProperties) {
		String location = " reservationOther";
		DuccId reservationIdentity = dwr.getDuccId();
		JdReservation jdReservation = null;
		SizeBytes reservationSize = JdHelper.getReservationSize(dwr);
		SizeBytes sliceSize = JdHelper.getSliceSize(jdHostProperties);
		jdReservation = map.get(reservationIdentity);
		if(jdReservation == null) {
			jdReservation = new JdReservation(dwr, reservationSize, sliceSize);
			map.putIfAbsent(reservationIdentity, jdReservation);
		}
		jdReservation = map.get(reservationIdentity);
		logger.trace(location, reservationIdentity, "total: "+countReservationsTotal()+" "+"up: "+countReservationsUp());
	}
	
	// Handle a JD Reservation that has disappeared for the Orchestrator publication.
	
	private void reservationVanished(DuccId reservationIdentity) {
		String location = "reservationVanished";
		List<JdReservation> list = new ArrayList<JdReservation>();
		synchronized(this) {
			JdReservation jdReservation = map.get(reservationIdentity);
			if(jdReservation != null) {
				jdReservation = map.remove(reservationIdentity);
				list.add(jdReservation);
			}
		}
		if(list.size() > 0) {
			DuccId duccId = (DuccId) reservationIdentity;
			logger.info(location, duccId, list.size());
			defunct(list);
		}
	}
	
	// Handle a list of JD Reservations that are no longer viable.
	
	private void defunct(List<JdReservation> list) {
		if(list != null) {
			if(!list.isEmpty()) {
				for(JdReservation jdReservation : list) {
					defunct(jdReservation);
				}
			}
		}
	}
	
	// Handle an individual JD Reservation that is no longer viable.
	
	private void defunct(JdReservation jdReservation) {
		String location = "defunct";
		//TODO phase I  = kill Job
		//TODO phase II = start new JD
		if(jdReservation != null) {
			DuccId reservationIdentity = (DuccId) jdReservation.getDuccId();
			logger.debug(location, reservationIdentity, "host: "+jdReservation.getHost());
		}
	}
	
	// Get a slice, if one is available.
	
	public NodeIdentity allocate(DuccId jobIdentity, DuccId driverIdentity) {
		String location = "allocate";
		NodeIdentity nodeIdentity = null;
		if(jobIdentity != null) {
			String host = null;
			synchronized(this) {
				for(Entry<DuccId, JdReservation> entry : map.entrySet()) {
					JdReservation jdReservation = entry.getValue();
					host = jdReservation.getHost();
					logger.debug(location, jobIdentity, "host: "+host+" "+"job: "+jobIdentity);
					nodeIdentity = jdReservation.allocate(jobIdentity, driverIdentity);
					if(nodeIdentity != null) {
						host = nodeIdentity.getName();
						changes.set(true);
						break;
					}
				}
			} 
			if(nodeIdentity != null) {
				logger.debug(location, jobIdentity, "host: "+host+" "+"job: "+jobIdentity);
			}
		}
		return nodeIdentity;
	}
	
	// Return a slice.
	
	public void deallocate(DuccId jobIdentity, DuccId driverIdentity) {
		String location = "deallocate";
		NodeIdentity nodeIdentity = null;
		if(jobIdentity != null) {
			String host = null;
			logger.debug(location, jobIdentity, "map size: "+map.size());
			synchronized(this) {
				for(Entry<DuccId, JdReservation> entry : map.entrySet()) {
					JdReservation jdReservation = entry.getValue();
					host = jdReservation.getHost();
					logger.debug(location, jobIdentity, "host: "+host+" "+"job: "+jobIdentity);
					nodeIdentity = jdReservation.deallocate(jobIdentity, driverIdentity);
					if(nodeIdentity != null) {
						host = nodeIdentity.getName();
						changes.set(true);
						break;
					}
				}
			}
			if(nodeIdentity != null) {
				logger.debug(location, jobIdentity, "host: "+host+" "+"job: "+jobIdentity);
			}
		}
	}
	
	// Return the number of slices total for the specified JD Reservation.
	
	public int countSlicesTotal(DuccId reservationIdentity) {
		String location = "countSlicesTotal";
		int count = 0;
		JdReservation jdReservation = map.get(reservationIdentity);
		if(jdReservation != null) {
			count += jdReservation.getSlicesTotal();
		}
		logger.trace(location, reservationIdentity, count);
		return count;
	}
	
	// Return the number of slices inuse for the specified JD Reservation.
	
	public int countSlicesInuse(DuccId reservationIdentity) {
		String location = "countSlicesInuse";
		int count = 0;
		JdReservation jdReservation = map.get(reservationIdentity);
		if(jdReservation != null) {
			count += jdReservation.getSlicesInuse();
		}
		logger.trace(location, reservationIdentity, count);
		return count;
	}
	
	// Return the number of slices available for the specified JD Reservation.
	
	public int countSlicesAvailable(DuccId reservationIdentity) {
		String location = "countSlicesAvailable";
		int count = 0;
		JdReservation jdReservation = map.get(reservationIdentity);
		if(jdReservation != null) {
			count += jdReservation.getSlicesAvailable();
		}
		logger.trace(location, reservationIdentity, count);
		return count;
	}
	
	// Return the number of slices total (for all JD Reservations).
	
	public int countSlicesTotal() {
		String location = "countSlicesTotal";
		int count = 0;
		for(Entry<DuccId, JdReservation> entry : map.entrySet()) {
			JdReservation jdReservation = entry.getValue();
			count += jdReservation.getSlicesTotal();
		}
		logger.trace(location, jobid, count);
		return count;
	}
	
	// Return the number of slices inuse (for all JD Reservations).
	
	public int countSlicesInuse() {
		String location = "countSlicesInuse";
		int count = 0;
		for(Entry<DuccId, JdReservation> entry : map.entrySet()) {
			JdReservation jdReservation = entry.getValue();
			count += jdReservation.getSlicesInuse();
		}
		logger.trace(location, jobid, count);
		return count;
	}
	
	// Return the number of slices available (for all JD Reservations).
	
	public int countSlicesAvailable() {
		String location = "countSlicesAvailable";
		int count = 0;
		for(Entry<DuccId, JdReservation> entry : map.entrySet()) {
			JdReservation jdReservation = entry.getValue();
			count += jdReservation.getSlicesAvailable();
		}
		logger.trace(location, jobid, count);
		return count;
	}

}
