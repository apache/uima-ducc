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

import org.apache.uima.ducc.container.common.fsm.iface.IAction;
import org.apache.uima.ducc.container.common.fsm.iface.IState;
import org.apache.uima.ducc.container.common.fsm.iface.IStateEventValue;

public class StateEventValue implements IStateEventValue {

	private IAction action = null;
	private IState state = null;
	
	public StateEventValue(IAction action, IState state) throws FsmException {
		if(action == null) {
			throw new FsmException("action object is null");
		}
		if(state == null) {
			throw new FsmException("state object is null");
		}
		setAction(action);
		setState(state);
	}

	@Override
	public IAction getAction() {
		return action;
	}
	
	private void setAction(IAction value) {
		action = value;
	}
	
	@Override
	public IState getState() {
		return state;
	}
	
	private void setState(IState value) {
		state = value;
	}

}
