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
