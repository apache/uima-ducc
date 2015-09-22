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
	
	public String getHost() {
		String retVal = null;
		NodeIdentity nodeIdentity= getNodeIdentity();
		if(nodeIdentity != null) {
			retVal = nodeIdentity.getName();
		}
		return retVal;
	}
	
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
	
	public Long getSlicesTotal() {
		String location = "getSlicesTotal";
		SizeBytes sizeOfReservation = getSizeOfReservation();
		SizeBytes sizeOfSlice = getSizeOfSlice();
		Long retVal = (long) (sizeOfReservation.getBytes() / (1.0 * sizeOfSlice.getBytes()));
		logger.trace(location, jobid, retVal);
		return retVal;
	}
	
	public Long getSlicesInuse() {
		String location = "getSlicesInuse";
		ConcurrentHashMap<DuccId, SizeBytes> map = getMap();
		long retVal = new Long(map.size());
		logger.trace(location, jobid, retVal);
		return retVal;
	}
	
	public Long getSlicesAvailable() {
		String location = "getSlicesAvailable";
		Long retVal = getSlicesTotal() - getSlicesInuse();
		logger.trace(location, jobid, retVal);
		return retVal;
	}
	
	public boolean isFull() {
		boolean retVal = (getSlicesTotal() <= getSlicesInuse());
		return retVal;
	}
	
	public boolean isEmpty() {
		boolean retVal = (getSlicesInuse() == 0);
		return retVal;
	}
	
	protected NodeIdentity allocate(DuccId jdId, DuccId jobId) {
		NodeIdentity retVal = allocate(jdId, jobId, getSizeOfSlice());
		return retVal;
	}
	
	protected NodeIdentity allocate(DuccId jdId, DuccId jobId, SizeBytes size) {
		String location = "allocate";
		NodeIdentity retVal = null;
		ConcurrentHashMap<DuccId, SizeBytes> map = getMap();
		if(jdId != null) {
			synchronized(map) {
				if(!map.containsKey(jdId)) {
					if(!isFull()) {
						map.put(jdId, size);
						retVal = getNodeIdentity();;
					}
				}
			}
			if(retVal != null) {
				logger.info(location, jobId, "jdId:"+jdId+" "+"host: "+retVal.getName()+" "+"size: "+map.size());
			}
		}
		return retVal;
	}
	
	protected NodeIdentity deallocate(DuccId jdId, DuccId jobId) {
		String location = "deallocate";
		NodeIdentity retVal = null;
		ConcurrentHashMap<DuccId, SizeBytes> map = getMap();
		if(jdId != null) {
			synchronized(map) {
				if(map.containsKey(jdId)) {
					map.remove(jdId);
					retVal = getNodeIdentity();
				}
			}
			if(retVal != null) {
				logger.info(location, jobId, "jdId:"+jdId+" "+"host: "+retVal.getName()+" "+"size: "+map.size());
			}
		}
		return retVal;
	}
	
}
