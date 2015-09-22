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
package org.apache.uima.ducc.transport.event.common;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.uima.ducc.common.NodeIdentity;
import org.apache.uima.ducc.common.SizeBytes;
import org.apache.uima.ducc.common.SizeBytes.Type;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.common.IDuccState.ReservationState;

public class JdReservationBean implements Serializable {

	private static final long serialVersionUID = 1L;
	
	private DuccId jdReservationDuccId = null;
	private NodeIdentity nodeIdentity;
	private ReservationState reservationState = null;
	private SizeBytes sizeOfReservation = new SizeBytes(SizeBytes.Type.GBytes,30);
	private SizeBytes sizeOfSlice = new SizeBytes(SizeBytes.Type.MBytes,300);
	
	@Deprecated
	private Long reservationSize = new Long(0);
	@Deprecated
	private Long sliceSize = new Long(0);
	
	private ConcurrentHashMap<DuccId, SizeBytes> map = new ConcurrentHashMap<DuccId, SizeBytes>();
	
	public void setMap(ConcurrentHashMap<DuccId, SizeBytes> value) {
		map = value;
	}
	
	public ConcurrentHashMap<DuccId, SizeBytes> getMap() {
		return map;
	}
	
	public void setJdReservationId(DuccId value) {
		jdReservationDuccId = value;
	}
	
	public DuccId getDuccId() {
		return jdReservationDuccId;
	}
	
	public void setNodeIdentity(NodeIdentity value) {
		nodeIdentity = value;
	}
	
	public NodeIdentity getNodeIdentity() {
		return nodeIdentity;
	}
	
	public void setReservationState(ReservationState value) {
		reservationState = value;
	}
	
	public ReservationState getReservationState() {
		return reservationState;
	}
	
	public void setSizeOfReservation(SizeBytes value) {
		reservationSize = new Long(0);
		sizeOfReservation = value;
	}
	
	public SizeBytes getSizeOfReservation() {
		if(reservationSize > 0) {
			sizeOfReservation = new SizeBytes(Type.Bytes, reservationSize);
			reservationSize = new Long(0);
		}
		return sizeOfReservation;
	}
	
	public void setSizeOfSlice(SizeBytes value) {
		sliceSize = new Long(0);
		sizeOfSlice = value;
	}
	
	public SizeBytes getSizeOfSlice() {
		if(sliceSize > 0) {
			sizeOfSlice = new SizeBytes(Type.Bytes, sliceSize);
			sliceSize = new Long(0);
		}
		return sizeOfSlice;
	}
}
