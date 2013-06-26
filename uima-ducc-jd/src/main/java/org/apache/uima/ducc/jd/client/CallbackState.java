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
package org.apache.uima.ducc.jd.client;

public class CallbackState {
	
	public static enum State { PendingQueued, PendingAssigned, NotPending };
	
	public State state = State.NotPending;
	
	private void setState(State value) {
		synchronized(state) {
			state = value;
		}
	}
	
	public void statePendingQueued() {
		setState(State.PendingQueued);
	}
	
	public void statePendingAssigned() {
		setState(State.PendingAssigned);
	}
	
	public void stateNotPending() {
		setState(State.NotPending);
	}
	
	public State getState() {
		return state;
	}
	
	public boolean isPendingQueued() {
		switch(state) {
		case PendingQueued:
			return true;
		default:
			return false;
		}
	}
	
	public boolean isPendingAssigned() {
		switch(state) {
		case PendingAssigned:
			return true;
		default:
			return false;
		}
	}
	
	public boolean isPendingCallback() {
		switch(state) {
		case PendingQueued:
		case PendingAssigned:
			return true;
		default:
			return false;
		}
	}
}
