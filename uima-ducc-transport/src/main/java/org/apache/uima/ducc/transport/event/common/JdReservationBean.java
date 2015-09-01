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
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.common.IDuccState.ReservationState;

public class JdReservationBean implements Serializable {

	private static final long serialVersionUID = 1L;
	public static long KB = 1024;
	public static long MB = 1024*KB;
	public static long GB = 1024*MB;
	public static long TB = 1024*GB;
	
	private DuccId jdReservationDuccId = null;
	private NodeIdentity nodeIdentity;
	private ReservationState reservationState = null;
	private Long shareCount = new Long(1);
	private Long shareSize = new Long(30*GB);
	private Long sliceSize = new Long(300*MB);
	
	private ConcurrentHashMap<DuccId, Long> map = new ConcurrentHashMap<DuccId, Long>();
	
	public void setMap(ConcurrentHashMap<DuccId, Long> value) {
		map = value;
	}
	
	public ConcurrentHashMap<DuccId, Long> getMap() {
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
	
	public void setShareCount(Long value) {
		shareCount = value;
	}
	
	public Long getShareCount() {
		return shareCount;
	}
	
	public void setShareSize(Long value) {
		shareSize = value;
	}
	
	public Long getShareSize() {
		return shareSize;
	}
	
	public void setSliceSize(Long value) {
		sliceSize = value;
	}
	
	public Long getSliceSize() {
		return sliceSize;
	}
	
}
