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
import java.util.List;

import org.apache.uima.ducc.transport.event.common.IDuccCompletionType.ReservationCompletionType;
import org.apache.uima.ducc.transport.event.common.IDuccState.ReservationState;


public interface IDuccWorkReservation extends IDuccWork, Serializable {

	public IDuccReservationMap getReservationMap();
	public void setReservationMap(IDuccReservationMap reservationMap);

	public ReservationState getReservationState();
	public void setReservationState(ReservationState reservationState);
	
	public void setCompletion(ReservationCompletionType completionType, IRationale completionRationale);
	
	public ReservationCompletionType getCompletionType();
	public void setCompletionType(ReservationCompletionType completionType);
	
	public IRationale getCompletionRationale();
	public void setCompletionRationale(IRationale completionRationale);
	
	public boolean isActive();
	public boolean isSchedulable();
	public boolean isDispatchable();
	public boolean isCompleted();
	public boolean isFinished();
	public boolean stateChange(ReservationState state);
	
	public boolean complete(ReservationCompletionType completionType);
	
	public void logState();
	
	public List<String> getUniqueNodes();
}
