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

import org.apache.uima.ducc.transport.event.common.DuccWorkJob;
import org.apache.uima.ducc.transport.event.common.DuccWorkReservation;
import org.apache.uima.ducc.transport.event.common.IDuccTypes.DuccType;
import org.apache.uima.ducc.transport.event.common.IDuccWork;

public class Info implements Comparable<Info> {

	private IDuccWork _dw;
	
	public Info(DuccWorkJob job) {
		_dw = job;
	}
	
	public Info(DuccWorkReservation reservation) {
		_dw = reservation;
	}
	
	public IDuccWork getDuccWork() {
		return _dw;
	}
	
	public DuccWorkJob getJob() {
		DuccWorkJob retVal = null;
		if(_dw instanceof DuccWorkJob) {
			retVal = (DuccWorkJob) _dw;
		}
		return retVal;
	}
	
	public DuccWorkReservation getReservation() {
		DuccWorkReservation retVal = null;
		if(_dw instanceof DuccWorkReservation) {
			retVal = (DuccWorkReservation) _dw;
		}
		return retVal;
	}
	
	public boolean isOperational() {
		return _dw.isOperational();
	}
	
	@Override
	public int compareTo(Info info) {
		int retVal = 0;
		IDuccWork dw1 = this._dw;
		IDuccWork dw2 = info._dw;
		long f1 = dw1.getDuccId().getFriendly();
		long f2 = dw2.getDuccId().getFriendly();
		if(f1 != f2) {
			if(!dw1.isOperational() && dw2.isOperational()) {
				retVal = 1;
			}
			else if(dw1.isOperational() && !dw2.isOperational()) {
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
			IDuccWork dw1 = this._dw;
			IDuccWork dw2 = (IDuccWork)object;
			String s1 = dw1.getDuccId().toString();
			String s2 = dw2.getDuccId().toString();
			DuccType dt1 = dw1.getDuccType();
			DuccType dt2 = dw2.getDuccType();
			if(dt1 == dt2) {
				retVal = s1.equals(s2);
			}
		}
		catch(Throwable t) {	
		}
		return retVal;
	}
	
	@Override 
	public int hashCode() {
		IDuccWork dw = this._dw;
		String s1 = dw.getDuccId().toString();
		return s1.hashCode();
	}

}
