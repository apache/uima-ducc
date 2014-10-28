package org.apache.uima.ducc.container.common.fsm.iface;

import org.apache.uima.ducc.container.common.fsm.FsmException;

public interface IFsmBuilder extends IFsm {
	
	public void add(IState current, IEvent event, IAction action, IState next) throws FsmException;
	public void addInitial(IState current, IEvent event, IAction action, IState next) throws FsmException;
}