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

import org.apache.uima.ducc.transport.event.common.DuccWorkReservation;

public class ReservationInfo implements Comparable<ReservationInfo> {

	private DuccWorkReservation _reservation;
	
	public ReservationInfo(DuccWorkReservation reservation) {
		_reservation = reservation;
	}

	public DuccWorkReservation getReservation() {
		return _reservation;
	}
	
	public boolean isOperational() {
		return _reservation.isOperational();
	}
	
	@Override
	public int compareTo(ReservationInfo reservation) {
		int retVal = 0;
		ReservationInfo r1 = this;
		ReservationInfo r2 = reservation;
		long f1 = r1.getReservation().getDuccId().getFriendly();
		long f2 = r2.getReservation().getDuccId().getFriendly();
		if(f1 != f2) {
			if(!r1.isOperational() && r2.isOperational()) {
				retVal = 1;
			}
			else if(r1.isOperational() && !r2.isOperational()) {
				retVal = -1;
			}
			else if(f1 > f2) {
				retVal = -1;
			}
			else if(f1 < f2) {
				retVal = 1;
			}
		}
		return retVal;
	}
	
	@Override 
	public boolean equals(Object object) {
		boolean retVal = false;
		try {
			ReservationInfo i1 = this;
			ReservationInfo i2 = (ReservationInfo)object;
			DuccWorkReservation j1 = i1.getReservation();
			DuccWorkReservation j2 = i2.getReservation();
			String s1 = j1.getDuccId().toString();
			String s2 = j2.getDuccId().toString();
			retVal = s1.equals(s2);
		}
		catch(Throwable t) {	
		}
		return retVal;
	}
	
	@Override 
	public int hashCode() {
		ReservationInfo i1 = this;
		DuccWorkReservation j1 = i1.getReservation();
		String s1 = j1.getDuccId().toString();
		return s1.hashCode();
	}
}
