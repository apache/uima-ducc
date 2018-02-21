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

import org.apache.uima.ducc.container.common.fsm.iface.IState;

public class State implements IState {

	private StateType stateType = null;
	
	public State(StateType stateType) throws FsmException {
		if(stateType == null) {
			throw new FsmException("state type is null");
		}
		setStateType(stateType);
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		String name = getStateName();
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		return result;
	}
	
	@Override 
	public boolean equals(Object o) {
		boolean retVal = false;
		if(o != null) {
			if(o instanceof IState) {
				IState that = (IState)o;
				String thisName = this.getStateName();
				String thatName = that.getStateName();
				retVal = thisName.compareTo(thatName) == 0;
			}
		}
		return retVal;
	}
	
	@Override
	public int compareTo(Object o) {
		int retVal = 0;
		if(stateType != null) {
			if(o != null) {
				if(o instanceof IState) {
					IState that = (IState)o;
					String thisName = this.getStateName();
					String thatName = that.getStateName();
					retVal = thisName.compareTo(thatName);
				}
			}
		}
		return retVal;
	}
	
	@Override
	public String getStateName() {
		return stateType.name();
	}
	
	@Override
	public StateType getStateType() {
		return stateType;
	}
	
	private void setStateType(StateType value) {
		stateType = value;
	}
}
