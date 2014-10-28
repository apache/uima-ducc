package org.apache.uima.ducc.container.common.fsm.iface;

import org.apache.uima.ducc.container.common.fsm.FsmException;

public interface IFsm {
	
	public void transition(IEvent event, Object actionData) throws FsmException;
	public IState getStateCurrent();
	public IState getStatePrevious();
}
