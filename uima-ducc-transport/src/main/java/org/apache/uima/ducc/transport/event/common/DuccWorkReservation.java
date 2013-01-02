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

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.common.IDuccCompletionType.ReservationCompletionType;
import org.apache.uima.ducc.transport.event.common.IDuccState.ReservationState;
import org.apache.uima.ducc.transport.event.common.IDuccTypes.DuccType;


public class DuccWorkReservation extends ADuccWork implements IDuccWorkReservation {
	/**
	 * please increment this sUID when removing or modifying a field 
	 */
	private static final long serialVersionUID = 1L;

	private static final DuccLogger logger = DuccLoggerComponents.getOrLogger(DuccWorkReservation.class.getName());
	
	private IDuccReservationMap duccReservationMap = new DuccReservationMap();
	
	private IRationale completionRationale = null;
	
	public DuccWorkReservation() {
		init(null);
	}
	
	public DuccWorkReservation(DuccId duccId) {
		init(duccId);
	}
	
	private void init(DuccId duccId) {
		setDuccType(DuccType.Reservation);
		setDuccId(duccId);
		setStateObject(IDuccState.ReservationState.Undefined);
		setCompletionTypeObject(IDuccCompletionType.ReservationCompletionType.Undefined);
	}

	@Override
	public IDuccReservationMap getReservationMap() {
		return duccReservationMap;
	}

	@Override
	public void setReservationMap(IDuccReservationMap reservationMap) {
		this.duccReservationMap = reservationMap;
	}
	
	@Override
	public ReservationState getReservationState() {
		return (ReservationState)getStateObject();
	}

	@Override
	public void setReservationState(ReservationState reservationState) {
		setStateObject(reservationState);
	}

	@Override
	public void setCompletion(ReservationCompletionType completionType, IRationale completionRationale) {
		setCompletionType(completionType);
		setCompletionRationale(completionRationale);
	}
	
	@Override
	public ReservationCompletionType getCompletionType() {
		return (ReservationCompletionType)getCompletionTypeObject();
	}

	@Override
	public void setCompletionType(ReservationCompletionType completionType) {
		setCompletionTypeObject(completionType);
	}
	
	@Override
	public IRationale getCompletionRationale() {
		IRationale retVal = null;
		try {
			if(this.completionRationale != null) {
				retVal = this.completionRationale;
			}
			else {
				retVal = new Rationale();
			}
		}
		catch(Exception e) {
			retVal = new Rationale();
		}
		return retVal;
	}
	
	@Override
	public void setCompletionRationale(IRationale completionRationale) {
		this.completionRationale = completionRationale;
	}
	
	
	public boolean isActive() {
		boolean retVal = false;
		switch(getReservationState()) {
		case WaitingForResources:
		case Assigned:
			retVal = true;	
			break;
		}
		return retVal;
	}
	
	public boolean isPending() {
		boolean retVal = false;
		switch(getReservationState()) {
		case WaitingForResources:
			retVal = true;	
			break;
		}
		return retVal;
	}
	
	public boolean isSchedulable() {
		boolean retVal = false;
		switch(getReservationState()) {
		case WaitingForResources:
		case Assigned:
			retVal = true;	
			break;
		}
		return retVal;
	}
	
	public boolean isDispatchable() {
		boolean retVal = false;
		switch(getReservationState()) {
		case Assigned:
			retVal = true;	
			break;
		}
		return retVal;
	}
	
	public boolean isCompleted() {
		boolean retVal = false;
		switch(getReservationState()) {
		case Completed:
			retVal = true;	
			break;
		}
		return retVal;
	}
	
	@Override
	public boolean isOperational() {
		boolean retVal = true;
		switch(getReservationState()) {
		case Completed:
			retVal = false;	
			break;
		}
		return retVal;
	}
	
	private boolean stateChangeFromAssigned(ReservationState prev, ReservationState next) {
		boolean retVal = false;
		switch(next) {
		case Assigned:								break;
		case Completed:				retVal = true;	break;
		case Received:								break;
		case Undefined:								break;
		case WaitingForResources:					break;
		}
		return retVal;
	}
	
	private boolean stateChangeFromCompleted(ReservationState prev, ReservationState next) {
		boolean retVal = false;
		switch(next) {
		case Assigned:								break;
		case Completed:								break;
		case Received:								break;
		case Undefined:								break;
		case WaitingForResources:					break;
		}
		return retVal;
	}
	
	private boolean stateChangeFromReceived(ReservationState prev, ReservationState next) {
		boolean retVal = false;
		switch(next) {
		case Assigned:								break;
		case Completed:				retVal = true;	break;
		case Received:								break;
		case Undefined:								break;
		case WaitingForResources:	retVal = true;	break;
		}
		return retVal;
	}
	
	private boolean stateChangeFromUndefined(ReservationState prev, ReservationState next) {
		boolean retVal = false;
		switch(next) {
		case Assigned:								break;
		case Completed:								break;
		case Received:				retVal = true;	break;
		case Undefined:								break;
		case WaitingForResources:					break;
		}
		return retVal;
	}
	
	private boolean stateChangeFromWaitingForResources(ReservationState prev, ReservationState next) {
		boolean retVal = false;
		switch(next) {
		case Assigned:				retVal = true;	break;
		case Completed:				retVal = true;	break;
		case Received:								break;
		case Undefined:								break;
		case WaitingForResources:					break;
		}
		return retVal;
	}
	
	public boolean stateChange(ReservationState state) {
		String methodName = "stateChange";
		boolean retVal = false;
		ReservationState prev = getReservationState();
		ReservationState next = state;
		switch(prev) {
		case Assigned:
			retVal = stateChangeFromAssigned(prev, next);
			break;
		case Completed:
			retVal = stateChangeFromCompleted(prev, next);
			break;
		case Received:
			retVal = stateChangeFromReceived(prev, next);
			break;
		case Undefined:
			retVal = stateChangeFromUndefined(prev, next);
			break;
		case WaitingForResources:
			retVal = stateChangeFromWaitingForResources(prev, next);
			break;	
		}
		if(retVal) {
			setReservationState(state);
			logger.info(methodName, getDuccId(),"current["+next+"] previous["+prev+"]");
		}
		else {
			logger.error(methodName, getDuccId(),"current["+prev+"] requested["+next+"]"+" ignored");
		}
		return retVal;
	}
	
	public boolean complete(ReservationCompletionType completionType) {
		String methodName = "complete";
		boolean retVal = false;
		switch(getCompletionType()) {
		case Undefined:
			retVal = true;
			break;
		}
		if(retVal) {
			setCompletionType(completionType);
			logger.info(methodName, getDuccId(), completionType);
		}
		else {
			logger.info(methodName, getDuccId(), completionType+" "+"ignored");
		}
		return retVal;
	}
	
	public void logState() {
		String methodName = "logState";
		logger.info(methodName, getDuccId(), getReservationState());
	}
	
	// **********
	
	@Override
	public int hashCode() {
		return super.hashCode();
	}
	
	public boolean equals(Object obj) {
		return super.equals(obj);
	}

}
