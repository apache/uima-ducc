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
package org.apache.uima.ducc.container.common.fsm;

import org.apache.uima.ducc.container.common.fsm.iface.IEvent;
import org.apache.uima.ducc.container.common.fsm.iface.IState;
import org.apache.uima.ducc.container.common.fsm.iface.IStateEventKey;

public class StateEventKey implements IStateEventKey {

	private IState state = null;
	private IEvent event = null;
	
	public StateEventKey(IState state, IEvent event) throws FsmException {
		if(state == null) {
			throw new FsmException("state object is null");
		}
		if(event == null) {
			throw new FsmException("event object is null");
		}
		setState(state);
		setEvent(event);
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((state == null) ? 0 : state.hashCode());
		result = prime * result + ((event == null) ? 0 : event.hashCode());
		return result;
	}
	
	@Override 
	public boolean equals(Object o) {
		boolean retVal = false;
		if(o != null) {
			if(o instanceof IStateEventKey) {
				IStateEventKey that = (IStateEventKey) o;
				IState thatState = that.getState();
				IEvent thatEvent = that.getEvent();
				if(thatState != null) {
					if(thatEvent != null) {
						if(thatState.equals(state)) {
							if(thatEvent.equals(event)) {
								retVal = true;
							}
						}
					}
				}
			}
		}
		return retVal;
	}
	
	@Override
	public int compareTo(Object o) {
		int retVal = 0;
		if(state != null) {
			if(event != null) {
				if(o != null) {
					StateEventKey that = (StateEventKey)o;
					retVal = this.state.compareTo(that.state);
					if(retVal == 0) {
						retVal = this.event.compareTo(that.event);
					}
				}
			}
		}
		return retVal;
	}

	private void setState(IState value) {
		state = value;
	}

	@Override
	public IState getState() {
		return state;
	}
	
	private void setEvent(IEvent value) {
		event = value;
	}

	@Override
	public IEvent getEvent() {
		return event;
	}

}
