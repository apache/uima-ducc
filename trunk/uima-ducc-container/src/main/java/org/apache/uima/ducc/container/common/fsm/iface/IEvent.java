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
package org.apache.uima.ducc.container.common.fsm.iface;

public interface IEvent extends Comparable<Object> {

	public enum EventType {
		Get_Request,
		CAS_Available,
		CAS_Unavailable,
		Ack_Request,
		Send_Failure,
		Ack_Timer_Pop,
		End_Request,
		End_Timer_Pop,
		Host_Failure,
		Process_Failure,
		Process_Premept,
		Process_Volunteered,
		Investment_Reset,
		;
	}
	
	public String getEventName();
	public EventType getEventType();
}
