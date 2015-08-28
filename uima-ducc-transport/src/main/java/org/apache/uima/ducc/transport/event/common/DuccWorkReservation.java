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

import java.util.ArrayList;
import java.util.List;

import org.apache.uima.ducc.common.main.DuccService;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.common.IDuccCompletionType.ReservationCompletionType;
import org.apache.uima.ducc.transport.event.common.IDuccState.ReservationState;
import org.apache.uima.ducc.transport.event.common.IDuccTypes.DuccType;


public class DuccWorkReservation extends ADuccWork implements IDuccWorkReservation {
	/**
	 * please increment this sUID when removing or modifying a field 
	 */
	private static final long serialVersionUID = 1L;

	private static final DuccLogger logger = DuccService.getDuccLogger(DuccWorkReservation.class.getName());
	private IDuccReservationMap duccReservationMap = new DuccReservationMap();
	private IRationale completionRationale = null;
	private boolean waitForAssignment = false;
	
	// for JD Reservations only
	private List<IJdReservation> jdReservationList = null;
	
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

	public void setJdReservationList(List<IJdReservation> value) {
		jdReservationList = value;
	}
	
	public List<IJdReservation> getJdReservationList() {
		return jdReservationList;
	}
	
	public IDuccReservationMap getReservationMap() {
		return duccReservationMap;
	}

	
	public void setReservationMap(IDuccReservationMap reservationMap) {
		this.duccReservationMap = reservationMap;
	}
	
	
	public ReservationState getReservationState() {
		return (ReservationState)getStateObject();
	}

	
	public void setReservationState(ReservationState reservationState) {
		setStateObject(reservationState);
	}

	
	public void setCompletion(ReservationCompletionType completionType, IRationale completionRationale) {
		setCompletionType(completionType);
		setCompletionRationale(completionRationale);
	}
	
	
	public ReservationCompletionType getCompletionType() {
		return (ReservationCompletionType)getCompletionTypeObject();
	}

	
	public void setCompletionType(ReservationCompletionType completionType) {
		setCompletionTypeObject(completionType);
	}
	
	
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
	
	
	public void setCompletionRationale(IRationale completionRationale) {
		this.completionRationale = completionRationale;
	}
	
	
	public boolean isWaitForAssignment() {
		return waitForAssignment;
	}
	
	
	public void setWaitForAssignment() {
		waitForAssignment = true;
	}
	
	
	public void resetWaitForAssignment() {
		waitForAssignment = false;
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
	
	public boolean isFinished() {
		return isCompleted();
	}
	
	
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
	
	public List<String> getNodes(boolean unique) {
		ArrayList<String> list = new ArrayList<String>();
		if(!getReservationMap().isEmpty()) {
			IDuccReservationMap map = getReservationMap();
			for (DuccId key : map.keySet()) { 
				IDuccReservation value = getReservationMap().get(key);
				String node = value.getNodeIdentity().getName();
				if(unique) {
					if(!list.contains(node)) {
						list.add(node);
					}
				}
				else {
					list.add(node);
				}
			}
		}
		return list;
	}
	
	public List<String> getNodes() {
		return getNodes(false);
	}
	
	public List<String> getUniqueNodes() {
		return getNodes(true);
	}
	
	// **********
	
	
	public int hashCode() {
		return super.hashCode();
	}
	
	public boolean equals(Object obj) {
		return super.equals(obj);
	}

}
