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

import java.util.concurrent.ConcurrentHashMap;

import org.apache.uima.ducc.common.NodeIdentity;
import org.apache.uima.ducc.common.SizeBytes;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.common.IDuccState.ReservationState;
import org.apache.uima.ducc.transport.event.common.IDuccWorkReservation;
import org.apache.uima.ducc.transport.event.common.JdReservationBean;

public class JdReservation extends JdReservationBean implements IJdReservation {

	private static final long serialVersionUID = 1L;
	
	private static DuccLogger logger = new DuccLogger(JdReservation.class);
	private static DuccId jobid = null;
	
	// Each instance of JdReservation represents an individual DUCC Reservation
	// that once Assigned is partitioned into smaller equal sized slices, where 
	// each individual slice is used for a single JD.
	
	public JdReservation(IDuccWorkReservation dwr, SizeBytes sizeOfReservation, SizeBytes sizeOfSlice) {
		initialize(dwr, sizeOfReservation, sizeOfSlice);
	}
	
	private void initialize(IDuccWorkReservation dwr, SizeBytes sizeOfReservation, SizeBytes sizeOfSlice) {
		if(dwr != null) {
			DuccId jdReservationId = (DuccId) dwr.getDuccId();
			setJdReservationId(jdReservationId);
			setNodeIdentity(JdHelper.getNodeIdentity(dwr));
			setReservationState(dwr.getReservationState());
			if(sizeOfReservation != null) {
				setSizeOfReservation(sizeOfReservation);
			}
			if(sizeOfSlice != null) {
				setSizeOfSlice(sizeOfSlice);
			}
		}
	}
	
	// Return the Host for this JdReservation.
	
	public String getHost() {
		String retVal = null;
		NodeIdentity nodeIdentity= getNodeIdentity();
		if(nodeIdentity != null) {
			retVal = nodeIdentity.getName();
		}
		return retVal;
	}
	
	// Return true if JdReservation is usable.
	
	public boolean isUp() {
		boolean retVal = false;
		ReservationState reservationState = getReservationState();
		if(reservationState != null) {
			switch(reservationState) {
			case Assigned:
				retVal = true;
				break;
			default:
				break;
			}
		}
		return retVal;
	}
	
	// Return the number of slices (capacity) for this JdReservation.
	
	public Long getSlicesTotal() {
		String location = "getSlicesTotal";
		SizeBytes sizeOfReservation = getSizeOfReservation();
		SizeBytes sizeOfSlice = getSizeOfSlice();
		Long retVal = (long) (sizeOfReservation.getBytes() / (1.0 * sizeOfSlice.getBytes()));
		logger.trace(location, jobid, retVal);
		return retVal;
	}
	
	// Return the number of slices inuse for this JdReservation.
	
	public Long getSlicesInuse() {
		String location = "getSlicesInuse";
		ConcurrentHashMap<DuccId, SizeBytes> map = getMap();
		long retVal = new Long(map.size());
		logger.trace(location, jobid, retVal);
		return retVal;
	}
	
	// Return the number of slices not inuse for this JdReservation.
	
	public Long getSlicesAvailable() {
		String location = "getSlicesAvailable";
		Long retVal = getSlicesTotal() - getSlicesInuse();
		logger.trace(location, jobid, retVal);
		return retVal;
	}
	
	// Return true if all slices for this JdReservation are inuse.
	
	public boolean isFull() {
		boolean retVal = (getSlicesTotal() <= getSlicesInuse());
		return retVal;
	}
	
	// Return true if all slices for this JdReservation are not inuse.
	
	public boolean isEmpty() {
		boolean retVal = (getSlicesInuse() == 0);
		return retVal;
	}
	
	protected NodeIdentity allocate(DuccId jobIdentity, DuccId driverIdentity) {
		NodeIdentity retVal = allocate(jobIdentity, driverIdentity, getSizeOfSlice());
		return retVal;
	}
	
	protected NodeIdentity allocate(DuccId jobIdentity, DuccId driverIdentity, SizeBytes size) {
		String location = "allocate";
		NodeIdentity retVal = null;
		ConcurrentHashMap<DuccId, SizeBytes> map = getMap();
		if(jobIdentity != null) {
			synchronized(this) {
				if(!map.containsKey(jobIdentity)) {
					if(!isFull()) {
						SizeBytes previous = map.putIfAbsent(jobIdentity, size);
						if(previous == null) {
							retVal = getNodeIdentity();
						}
					}
				}
			}
			if(retVal != null) {
				logger.info(location, jobIdentity, "driverId:"+driverIdentity+" "+"host: "+retVal.getName()+" "+"size: "+map.size());
			}
		}
		return retVal;
	}
	
	protected NodeIdentity deallocate(DuccId jobIdentity, DuccId driverIdentity) {
		String location = "deallocate";
		NodeIdentity retVal = null;
		ConcurrentHashMap<DuccId, SizeBytes> map = getMap();
		if(jobIdentity != null) {
			synchronized(this) {
				if(map.containsKey(jobIdentity)) {
					map.remove(jobIdentity);
					retVal = getNodeIdentity();
				}
			}
			if(retVal != null) {
				logger.info(location, jobIdentity, "driverId:"+driverIdentity+" "+"host: "+retVal.getName()+" "+"size: "+map.size());
			}
		}
		return retVal;
	}
	
}
