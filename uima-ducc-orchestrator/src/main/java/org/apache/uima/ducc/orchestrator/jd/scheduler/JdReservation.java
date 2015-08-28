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

import java.text.DecimalFormat;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.uima.ducc.common.NodeIdentity;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.common.IDuccState.ReservationState;
import org.apache.uima.ducc.transport.event.common.IDuccUnits.MemoryUnits;
import org.apache.uima.ducc.transport.event.common.IDuccWorkReservation;
import org.apache.uima.ducc.transport.event.common.IJdReservation;

public class JdReservation implements IJdReservation {

	private static final long serialVersionUID = 1L;
	
	private static DuccLogger logger = new DuccLogger(JdReservation.class);
	
	private DuccId jdReservationDuccId = null;
	private NodeIdentity nodeIdentity;
	private ReservationState reservationState = null;
	private Long shareCount = new Long(1);
	private Long shareSize = new Long(30*JdHelper.GB);
	private Long sliceSize = new Long(300*JdHelper.MB);
	
	private ConcurrentHashMap<DuccId, Long> map = new ConcurrentHashMap<DuccId, Long>();
	
	public JdReservation(IDuccWorkReservation dwr, Long shareSizeMB, Long sliceSizeMB) {
		initialize(dwr, shareSizeMB, sliceSizeMB);
	}
	
	private String getProcessMemorySize(DuccId id, String type, String size, MemoryUnits units) {
		String methodName = "getProcessMemorySize";
		String retVal = "?";
		double multiplier = 1;
		switch(units) {
		case KB:
			multiplier = Math.pow(10, -6);
			break;
		case MB:
			multiplier = Math.pow(10, -3);
			break;
		case GB:
			multiplier = Math.pow(10, 0);
			break;
		case TB:
			multiplier = Math.pow(10, 3);
			break;
		}
		try {
			double dSize = Double.parseDouble(size) * multiplier;
			DecimalFormat formatter = new DecimalFormat("###0");
			retVal = formatter.format(dSize);
		}
		catch(Exception e) {
			logger.trace(methodName, id, "type"+type+" "+"size"+size, e);
		}
		return retVal;	
	}
	
	private Long calculateShares(IDuccWorkReservation dwr) {
		Long retVal = new Long(1);
		try {
			String type="Reservation";
			String size = dwr.getSchedulingInfo().getShareMemorySize();
			MemoryUnits units = dwr.getSchedulingInfo().getShareMemoryUnits();
			String ms = getProcessMemorySize(dwr.getDuccId(),type,size,units);
			retVal = Long.parseLong(ms);
		}
		catch(Exception e) {
		}
		return retVal;
	}
	
	private void initialize(IDuccWorkReservation dwr, Long shareSize, Long sliceSize) {
		if(dwr != null) {
			DuccId jdReservationId = (DuccId) dwr.getDuccId();
			setJdReservationId(jdReservationId);
			setNodeIdentity(JdHelper.getNodeIdentity(dwr));
			setReservationState(dwr.getReservationState());
			if(shareSize != null) {
				setShareSize(shareSize);
			}
			if(sliceSize != null) {
				setSliceSize(sliceSize);
			}
			setShareCount(calculateShares(dwr));
		}
	}
	
	private void setJdReservationId(DuccId value) {
		jdReservationDuccId = value;
	}
	
	public DuccId getDuccId() {
		return jdReservationDuccId;
	}
	
	private void setNodeIdentity(NodeIdentity value) {
		nodeIdentity = value;
	}
	
	public String getHost() {
		String retVal = null;
		if(nodeIdentity != null) {
			retVal = nodeIdentity.getName();
		}
		return retVal;
	}
	private void setReservationState(ReservationState value) {
		reservationState = value;
	}
	
	public ReservationState getReservationState() {
		return reservationState;
	}
	
	public boolean isUp() {
		boolean retVal = false;
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
	
	private void setShareCount(Long value) {
		shareCount = value;
	}
	
	private Long getShareCount() {
		return shareCount;
	}
	
	private void setShareSize(Long value) {
		shareSize = value;
	}
	
	private Long getShareSize() {
		return shareSize;
	}
	
	private void setSliceSize(Long value) {
		sliceSize = value;
	}
	
	private Long getSliceSize() {
		return sliceSize;
	}
	
	public Long getSlicesTotal() {
		Long shareCount = getShareCount();
		Long shareSize = getShareSize();
		Long sliceSize = getSliceSize();
		Long retVal = (shareSize * shareCount) / sliceSize;
		return retVal;
	}
	
	public Long getSlicesInuse() {
		long retVal = new Long(map.size());
		return retVal;
	}
	
	public Long getSlicesAvailable() {
		Long retVal = getSlicesTotal() - getSlicesInuse();
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
		NodeIdentity retVal = allocate(jdId, jobId, getSliceSize());
		return retVal;
	}
	
	protected NodeIdentity allocate(DuccId jdId, DuccId jobId, Long size) {
		String location = "allocate";
		NodeIdentity retVal = null;
		if(jdId != null) {
			synchronized(map) {
				if(!map.containsKey(jdId)) {
					if(!isFull()) {
						map.put(jdId, size);
						retVal = nodeIdentity;
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
		if(jdId != null) {
			synchronized(map) {
				if(map.containsKey(jdId)) {
					map.remove(jdId);
					retVal = nodeIdentity;
				}
			}
			if(retVal != null) {
				logger.info(location, jobId, "jdId:"+jdId+" "+"host: "+retVal.getName()+" "+"size: "+map.size());
			}
		}
		return retVal;
	}
	
}
