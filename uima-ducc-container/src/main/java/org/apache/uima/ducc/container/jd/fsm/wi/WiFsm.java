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
package org.apache.uima.ducc.container.jd.fsm.wi;

import org.apache.uima.ducc.container.common.fsm.Event;
import org.apache.uima.ducc.container.common.fsm.Fsm;
import org.apache.uima.ducc.container.common.fsm.FsmException;
import org.apache.uima.ducc.container.common.fsm.State;
import org.apache.uima.ducc.container.common.fsm.iface.IAction;
import org.apache.uima.ducc.container.common.fsm.iface.IEvent;
import org.apache.uima.ducc.container.common.fsm.iface.IState;

public class WiFsm extends Fsm {
	
	public static IState Start 				= new State("Start");
	public static IState Get_Pending 		= new State("Get_Pending");
	public static IState CAS_Send 			= new State("CAS_Send");
	public static IState CAS_Active 		= new State("CAS_Active");
	
	public static IEvent Get_Request 		= new Event("Get_Request");
	public static IEvent CAS_Available		= new Event("CAS_Available");
	public static IEvent CAS_Unavailable	= new Event("CAS_Unavailable");
	public static IEvent Ack_Request 		= new Event("Ack_Request");
	public static IEvent Send_Failure 		= new Event("Send_Failure");
	public static IEvent Ack_Timer_Pop		= new Event("Ack_Timer_Pop");
	public static IEvent End_Request 		= new Event("End_Request");
	public static IEvent End_Timer_Pop		= new Event("End_Timer_Pop");
	public static IEvent Host_Failure		= new Event("Host_Failure");
	public static IEvent Process_Failure	= new Event("Process_Failure");
	public static IEvent Process_Preempt	= new Event("Process_Premept");
	
	public IAction ActionGetCAS		= new ActionGetCAS();
	public IAction ActionSendCAS	= new ActionSendCAS();
	public IAction ActionAckCAS		= new ActionAckCAS();
	public IAction ActionEndCAS		= new ActionEndCAS();
	
	public IAction ActionIgnore 	= new ActionIgnore();
	
	public WiFsm() throws FsmException {
		super();
		initialize();
	}
	
	private void initialize() throws FsmException {
		
		addInitial(Start, Get_Request, ActionGetCAS, Get_Pending);
		
		add(Get_Pending, CAS_Available, ActionSendCAS, CAS_Send);
		add(Get_Pending, CAS_Unavailable, ActionSendCAS, Start);
		
		add(CAS_Send, Ack_Request, ActionAckCAS, CAS_Active);
		
		add(CAS_Active, End_Request, ActionEndCAS, Start);
	}
}
