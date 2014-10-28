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
