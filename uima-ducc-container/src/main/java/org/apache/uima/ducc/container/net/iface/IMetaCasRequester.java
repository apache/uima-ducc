package org.apache.uima.ducc.container.net.iface;

import java.io.Serializable;

public interface IMetaCasRequester extends Serializable {
	
	public String getRequesterName();
	public void setRequesterName(String value);
	
	public int getRequesterProcessId();
	public void setRequesterProcessId(int value);
	
	public int getRequesterThreadId();
	public void setRequesterThreadId(int value);
}
