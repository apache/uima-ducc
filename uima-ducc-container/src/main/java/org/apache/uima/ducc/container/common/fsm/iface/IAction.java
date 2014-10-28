package org.apache.uima.ducc.container.common.fsm.iface;

public interface IAction {
	
	public String getName();
	public void engage(Object actionData);
}
