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

	private String name = null;
	
	public State(String name) throws FsmException {
		if(name == null) {
			throw new FsmException("state name is null");
		}
		setName(name);
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		return result;
	}
	
	@Override 
	public boolean equals(Object o) {
		boolean retVal = false;
		if(o != null) {
			if(o instanceof IState) {
				IState that = (IState)o;
				String thisName = this.getName();
				String thatName = that.getName();
				retVal = thisName.compareTo(thatName) == 0;
			}
		}
		return retVal;
	}
	
	@Override
	public int compareTo(Object o) {
		int retVal = 0;
		if(name != null) {
			if(o != null) {
				if(o instanceof IState) {
					IState that = (IState)o;
					String thisName = this.getName();
					String thatName = that.getName();
					retVal = thisName.compareTo(thatName);
				}
			}
		}
		return retVal;
	}
	
	@Override
	public String getName() {
		return name;
	}
	
	private void setName(String value) {
		name = value;
	}
}
