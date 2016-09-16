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

public class Event implements IEvent {

	private EventType eventType = null;
	
	public Event(EventType eventType) throws FsmException {
		if(eventType == null) {
			throw new FsmException("event type is null");
		}
		setEventType(eventType);
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		String name = getEventName();
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		return result;
	}
	
	@Override 
	public boolean equals(Object o) {
		boolean retVal = false;
		if(o != null) {
			if(o instanceof IEvent) {
				IEvent that = (IEvent)o;
				String thisName = this.getEventName();
				String thatName = that.getEventName();
				retVal = thisName.compareTo(thatName) == 0;
			}
		}
		return retVal;
	}
	
	@Override
	public int compareTo(Object o) {
		int retVal = 0;
		if(eventType != null) {
			if(o != null) {
				if(o instanceof IEvent) {
					IEvent that = (IEvent)o;
					String thisName = this.getEventName();
					String thatName = that.getEventName();
					retVal = thisName.compareTo(thatName);
				}
			}
		}
		return retVal;
	}

	@Override
	public String getEventName() {
		return eventType.name();
	}
	
	@Override
	public EventType getEventType() {
		return eventType;
	}
	
	private void setEventType(EventType value) {
		eventType = value;
	}
}
