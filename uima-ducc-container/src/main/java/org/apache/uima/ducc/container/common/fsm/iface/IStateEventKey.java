package org.apache.uima.ducc.container.common.fsm.iface;

public interface IStateEventKey extends Comparable<Object> {
	
	public IState getState();
	public IEvent getEvent();
}
