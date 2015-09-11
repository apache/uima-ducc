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
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.uima.ducc.common.NodeIdentity;
import org.apache.uima.ducc.common.SizeBytes;
import org.apache.uima.ducc.common.config.CommonConfiguration;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.orchestrator.OrchestratorCheckpoint;
import org.apache.uima.ducc.orchestrator.OrchestratorCommonArea;
import org.apache.uima.ducc.orchestrator.ReservationFactory;
import org.apache.uima.ducc.orchestrator.WorkMapHelper;
import org.apache.uima.ducc.transport.event.cli.ReservationRequestProperties;
import org.apache.uima.ducc.transport.event.cli.ReservationSpecificationProperties;
import org.apache.uima.ducc.transport.event.common.DuccWorkMap;
import org.apache.uima.ducc.transport.event.common.DuccWorkReservation;
import org.apache.uima.ducc.transport.event.common.IDuccCompletionType.ReservationCompletionType;
import org.apache.uima.ducc.transport.event.common.IDuccSchedulingInfo;
import org.apache.uima.ducc.transport.event.common.IDuccState.ReservationState;
import org.apache.uima.ducc.transport.event.common.IDuccWork;
import org.apache.uima.ducc.transport.event.common.IDuccWorkMap;
import org.apache.uima.ducc.transport.event.common.IDuccWorkReservation;
import org.apache.uima.ducc.transport.event.common.JdReservationBean;
import org.apache.uima.ducc.transport.event.common.Rationale;

public class JdScheduler {
	
	private static DuccLogger logger = new DuccLogger(JdScheduler.class);
	private static DuccId jobid = null;
	
	private static ReservationFactory reservationFactory = ReservationFactory.getInstance();
	
	private static JdScheduler instance = new JdScheduler();
	
	public static JdScheduler getInstance() {
		return instance;
	}
	
	private ConcurrentHashMap<DuccId,JdReservation> map = new ConcurrentHashMap<DuccId,JdReservation>();
	
	private AtomicBoolean autoManage = new AtomicBoolean(true);
	private AtomicBoolean requestPending = new AtomicBoolean(false);
	private AtomicBoolean changes = new AtomicBoolean(false);
	
	public JdScheduler() {	
	}
	
	public void setAutomanage() {
		autoManage.set(true);
	}
	
	public void resetAutomanage() {
		autoManage.set(false);
	}
	
	public void ckpt() {
		String location = "ckpt";
		if(changes.get()) {
			changes.set(false);
			try {
				IDuccWorkMap dwm = OrchestratorCommonArea.getInstance().getWorkMap();
				for(Entry<DuccId, JdReservation> entry : map.entrySet()) {
					DuccId jdReservationDuccId = entry.getKey();
					DuccId duccId = (DuccId) jdReservationDuccId;
					IDuccWork dw = dwm.findDuccWork(duccId);
					if(dw instanceof IDuccWorkReservation) {
						IDuccWorkReservation dwr = (IDuccWorkReservation) dw;
						List<JdReservationBean> jdReservationBeanList = getJdReservationBeanList(jdReservationDuccId);
						dwr.setJdReservationBeanList(jdReservationBeanList);
						if(jdReservationBeanList != null) {
							logger.debug(location, duccId, "size: "+jdReservationBeanList.size());
						}
						else {
							logger.debug(location, duccId, "size: "+null);
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
	
	public int getReservationCount() {
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
	
	public void handle(IDuccWorkMap dwm) {
		String location = "handle";
		try {
			if(dwm != null) {
				logger.debug(location, jobid, "dwm size: "+dwm.size());
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
			for(DuccId jdReservationDuccId : known) {
				reservationVanished(jdReservationDuccId);
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
	
	private boolean isReservationDivestable(IDuccWorkMap dwm, JdHostProperties jdHostProperties) {
		boolean retVal = false;
		long slicesReserveDesired = getSlicesReserveDesired(jdHostProperties);
		long slicesReserveActual = getSlicesReserveActual();
		if(map.size() > 1) {
			if(slicesReserveActual > (2 * slicesReserveDesired)) {
				synchronized(map) {
					for(Entry<DuccId, JdReservation> entry : map.entrySet()) {
						JdReservation jdReservation = entry.getValue();
						if(jdReservation.isEmpty()) {
							retVal = true;
							break;
						}
					}
				}
			}
		}
		return retVal;
	}
	
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
	
	private void reservationAcquire(IDuccWorkMap dwm, JdHostProperties jdHostProperties) {
		String location = "reservationAcquire";
		CommonConfiguration common = null;
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
		DuccWorkReservation dwr = reservationFactory.create(common, reservationRequestProperties);
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
		DuccId duccId = dwr.getDuccId();
		logger.debug(location, duccId, "actual: "+slicesReserveActual+" "+"desired: "+slicesReserveDesired);
	}
	
	private void reservationDivest(IDuccWorkMap dwm, JdHostProperties jdHostProperties) {
		String location = "reservationDivest";
		DuccId jdReservationDuccId = null;
		synchronized(map) {
			for(Entry<DuccId, JdReservation> entry : map.entrySet()) {
				JdReservation jdReservation = entry.getValue();
				if(jdReservation.isEmpty()) {
					jdReservationDuccId = entry.getKey();
					map.remove(jdReservationDuccId);
					break;
				}
				jdReservation = null;
			}
		}
		if(jdReservationDuccId != null) {
			DuccId duccId = (DuccId) jdReservationDuccId;
			IDuccWork dw = dwm.findDuccWork(duccId);
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
		logger.debug(location, jobid, "actual: "+slicesReserveActual+" "+"desired: "+slicesReserveDesired);
	}
	
	private void reservationNoChange(IDuccWorkMap dwm, JdHostProperties jdHostProperties) {
		String location = "reservationNoChange";
		long slicesReserveDesired = getSlicesReserveDesired(jdHostProperties);
		long slicesReserveActual = getSlicesReserveActual();
		logger.trace(location, jobid, "actual: "+slicesReserveActual+" "+"desired: "+slicesReserveDesired);
	}
	
	private void setJdReservationBeanList(List<JdReservationBean> jdReservationBeanList) {
		if(jdReservationBeanList != null) {
			for(JdReservationBean entry : jdReservationBeanList) {
				JdReservation jdReservation = (JdReservation) entry;
				DuccId jdReservationDuccId = jdReservation.getDuccId();
				map.put(jdReservationDuccId, jdReservation);
			}
		}
	}
	
	public List<JdReservationBean> getJdReservationBeanList(DuccId jdReservationDuccId) {
		String location = "getJdReservationBeanList";
		List<JdReservationBean> jdReservationBeanList = new ArrayList<JdReservationBean>();
		for(Entry<DuccId, JdReservation> entry : map.entrySet()) {
			JdReservation jdReservation = entry.getValue();
			if(jdReservationDuccId.equals(jdReservation.getDuccId())) {
				jdReservationBeanList.add(jdReservation);
				DuccId duccId = (DuccId) jdReservationDuccId;
				logger.trace(location, duccId, jdReservationBeanList.size());
			}
		}
		return jdReservationBeanList;
	}
	
	public int countReservationsTotal() {
		int count = map.size();
		return count;
	}
	
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
	
	private void reservationUp(IDuccWorkReservation dwr, JdHostProperties jdHostProperties) {
		String location = "reservationUp";
		DuccId duccId = dwr.getDuccId();
		DuccId jdReservationDuccId = (DuccId) duccId;
		JdReservation jdReservation = null;
		SizeBytes reservationSize = JdHelper.getReservationSize(dwr);
		SizeBytes sliceSize = JdHelper.getSliceSize(jdHostProperties);
		synchronized(map) {
			jdReservation = map.get(jdReservationDuccId);
			if(jdReservation == null) {
				jdReservation = new JdReservation(dwr, reservationSize, sliceSize);
				map.put(jdReservationDuccId, jdReservation);
			}
			else if(!jdReservation.isUp()) {
				jdReservation = new JdReservation(dwr, reservationSize, sliceSize);
				map.put(jdReservationDuccId, jdReservation);
			}
			else {
				jdReservation = null;
			}
		}
		if(jdReservation != null) {
			logger.debug(location, duccId, "host: "+jdReservation.getHost());
		}
	}
	
	private void reservationDown(IDuccWorkReservation dwr) {
		String location = "reservationDown";
		DuccId duccId = dwr.getDuccId();
		DuccId jdReservationDuccId = (DuccId) duccId;
		JdReservation jdReservation = null;
		List<JdReservation> list = new ArrayList<JdReservation>();
		synchronized(map) {
			jdReservation = map.get(jdReservationDuccId);
			if(jdReservation != null) {
				map.remove(jdReservationDuccId);
				list.add(jdReservation);
			}
		}
		if(list.size() > 0) {
			logger.info(location, duccId, list.size());
			defunct(list);
		}
	}
	
	private void reservationOther(IDuccWorkReservation dwr, JdHostProperties jdHostProperties) {
		String location = " reservationOther";
		DuccId duccId = dwr.getDuccId();
		DuccId jdReservationDuccId = (DuccId) duccId;
		JdReservation jdReservation = null;
		SizeBytes reservationSize = JdHelper.getReservationSize(dwr);
		SizeBytes sliceSize = JdHelper.getSliceSize(jdHostProperties);
		synchronized(map) {
			jdReservation = map.get(jdReservationDuccId);
			if(jdReservation == null) {
				jdReservation = new JdReservation(dwr, reservationSize, sliceSize);
				map.put(jdReservationDuccId, jdReservation);
			}
		}
		logger.trace(location, duccId, "total: "+countReservationsTotal()+" "+"up: "+countReservationsUp());
	}
	
	private void reservationVanished(DuccId jdReservationDuccId) {
		String location = "reservationVanished";
		List<JdReservation> list = new ArrayList<JdReservation>();
		synchronized(map) {
			JdReservation jdReservation = map.get(jdReservationDuccId);
			if(jdReservation != null) {
				jdReservation = map.remove(jdReservationDuccId);
				list.add(jdReservation);
			}
		}
		if(list.size() > 0) {
			DuccId duccId = (DuccId) jdReservationDuccId;
			logger.info(location, duccId, list.size());
			defunct(list);
		}
	}
	
	private void defunct(List<JdReservation> list) {
		if(list != null) {
			if(!list.isEmpty()) {
				for(JdReservation jdReservation : list) {
					defunct(jdReservation);
				}
			}
		}
	}
	
	private void defunct(JdReservation jdReservation) {
		String location = "defunct";
		//TODO phase I  = kill Job
		//TODO phase II = start new JD
		if(jdReservation != null) {
			DuccId duccId = (DuccId) jdReservation.getDuccId();
			logger.debug(location, duccId, "host: "+jdReservation.getHost());
		}
	}
	
	public NodeIdentity allocate(DuccId jdId, DuccId jobId) {
		String location = "allocate";
		NodeIdentity nodeIdentity = null;
		if(jdId != null) {
			String host = null;
			synchronized(map) {
				for(Entry<DuccId, JdReservation> entry : map.entrySet()) {
					JdReservation jdReservation = entry.getValue();
					nodeIdentity = jdReservation.allocate(jdId, jobId);
					if(nodeIdentity != null) {
						host = nodeIdentity.getName();
						changes.set(true);
						break;
					}
				}
			} 
			if(nodeIdentity != null) {
				logger.debug(location, jobId, "jdId:"+jdId+" "+"host: "+host);
			}
		}
		return nodeIdentity;
	}
	
	public void deallocate(DuccId jdId, DuccId jobId) {
		String location = "deallocate";
		NodeIdentity nodeIdentity = null;
		if(jdId != null) {
			String host = null;
			logger.debug(location, jobId, "map size: "+map.size());
			synchronized(map) {
				for(Entry<DuccId, JdReservation> entry : map.entrySet()) {
					JdReservation jdReservation = entry.getValue();
					logger.debug(location, jobId, "get host: "+jdReservation.getHost());
					logger.debug(location, jobId, "jdId: "+jdId);
					nodeIdentity = jdReservation.deallocate(jdId, jobId);
					if(nodeIdentity != null) {
						host = nodeIdentity.getName();
						changes.set(true);
						break;
					}
				}
			}
			if(nodeIdentity != null) {
				logger.debug(location, jobId, "jdId:"+jdId+" "+"host: "+host);
			}
		}
	}
	
	public int countSlicesTotal(DuccId duccId) {
		String location = "countSlicesTotal";
		int count = 0;
		JdReservation jdReservation = map.get(duccId);
		if(jdReservation != null) {
			count += jdReservation.getSlicesTotal();
		}
		logger.trace(location, duccId, count);
		return count;
	}
	
	public int countSlicesInuse(DuccId duccId) {
		String location = "countSlicesInuse";
		int count = 0;
		JdReservation jdReservation = map.get(duccId);
		if(jdReservation != null) {
			count += jdReservation.getSlicesInuse();
		}
		logger.trace(location, duccId, count);
		return count;
	}
	
	public int countSlicesAvailable(DuccId duccId) {
		String location = "countSlicesAvailable";
		int count = 0;
		JdReservation jdReservation = map.get(duccId);
		if(jdReservation != null) {
			count += jdReservation.getSlicesAvailable();
		}
		logger.trace(location, duccId, count);
		return count;
	}
	
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
