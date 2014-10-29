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

import java.util.concurrent.ConcurrentHashMap;

import org.apache.uima.ducc.container.common.ContainerLogger;
import org.apache.uima.ducc.container.common.IEntityId;
import org.apache.uima.ducc.container.common.IContainerLogger;
import org.apache.uima.ducc.container.common.MessageBuffer;
import org.apache.uima.ducc.container.common.Standardize;
import org.apache.uima.ducc.container.common.fsm.iface.IAction;
import org.apache.uima.ducc.container.common.fsm.iface.IEvent;
import org.apache.uima.ducc.container.common.fsm.iface.IFsmBuilder;
import org.apache.uima.ducc.container.common.fsm.iface.IState;
import org.apache.uima.ducc.container.common.fsm.iface.IStateEventKey;
import org.apache.uima.ducc.container.common.fsm.iface.IStateEventValue;

public class Fsm implements IFsmBuilder {
	
	private static IContainerLogger logger = ContainerLogger.getLogger(Fsm.class, IContainerLogger.Component.JD.name());
	
	private ConcurrentHashMap<IStateEventKey, IStateEventValue> map = new ConcurrentHashMap<IStateEventKey, IStateEventValue>();
	
	private IState stateCurrent = null;
	private IState statePrevious = null;
	
	@Override
	public void add(IState current, IEvent event, IAction action, IState next) throws FsmException {
		String location = "add";
		IStateEventKey key = new StateEventKey(current, event);
		IStateEventValue value = new StateEventValue(action, next);
		IStateEventValue result = putIfAbsent(key, value);
		if(result != null) {
			MessageBuffer mb = new MessageBuffer();
			mb.append("duplicate");
			mb.append(Standardize.Label.state.get()+current.getName());
			mb.append(Standardize.Label.event.get()+event.getName());
			throw new FsmException(mb.toString());
		}
		MessageBuffer mb = new MessageBuffer();
		mb.append(Standardize.Label.state.get()+current.getName());
		mb.append(Standardize.Label.event.get()+event.getName());
		logger.debug(location, IEntityId.null_id, mb.toString());
	}

	@Override
	public void addInitial(IState current, IEvent event, IAction action, IState next) throws FsmException {
		add(current, event, action, next);
		setStateCurrent(current);
	}
	
	private void setStateCurrent(IState value) {
		stateCurrent = value;
	}

	@Override
	public IState getStateCurrent() {
		return stateCurrent;
	}
	
	private void setStatePrevious(IState value) {
		statePrevious = value;
	}
	
	@Override
	public IState getStatePrevious() {
		return statePrevious;
	}
	
	private IStateEventValue putIfAbsent(IStateEventKey key, IStateEventValue value) {
		String location = "putIfAbsent";
		MessageBuffer mb = new MessageBuffer();
		mb.append(Standardize.Label.state.get()+key.getState().getName());
		mb.append(Standardize.Label.event.get()+key.getEvent().getName());
		mb.append(Standardize.Label.hash.get()+key.hashCode());
		logger.debug(location, IEntityId.null_id, mb.toString());
		return map.putIfAbsent(key, value);
	}
	
	private IStateEventValue get(IStateEventKey key) {
		String location = "get";
		MessageBuffer mb = new MessageBuffer();
		mb.append(Standardize.Label.state.get()+key.getState().getName());
		mb.append(Standardize.Label.event.get()+key.getEvent().getName());
		mb.append(Standardize.Label.hash.get()+key.hashCode());
		logger.debug(location, IEntityId.null_id, mb.toString());
		IStateEventValue value = map.get(key);
		return value;
	}
	
	@Override
	public void transition(IEvent event, Object actionData) throws FsmException {
		String location = "transition";
		try {
			synchronized(map) {
				IState _stateCurrent = getStateCurrent();
				IStateEventKey key = new StateEventKey(_stateCurrent, event);
				IStateEventValue value = get(key);
				if(value == null) {
					MessageBuffer mb = new MessageBuffer();
					mb.append("undefined");
					mb.append(Standardize.Label.state.get()+_stateCurrent.getName());
					mb.append(Standardize.Label.event.get()+event.getName());
					throw new FsmException(mb.toString());
				}
				IAction action = value.getAction();
				action.engage(actionData);
				IState _statePrevious = _stateCurrent;
				_stateCurrent = value.getState();
				setStateCurrent(_stateCurrent);
				setStatePrevious(_statePrevious);
				MessageBuffer mb = new MessageBuffer();
				mb.append(Standardize.Label.curr.get()+_stateCurrent.getName());
				mb.append(Standardize.Label.prev.get()+_statePrevious.getName());
				logger.info(location, IEntityId.null_id, mb.toString());
			}
		}
		catch(Exception e) {
			throw new FsmException(e);
		}
	}

}
