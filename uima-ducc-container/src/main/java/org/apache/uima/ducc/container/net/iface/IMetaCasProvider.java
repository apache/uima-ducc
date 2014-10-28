package org.apache.uima.ducc.container.net.iface;

import java.io.Serializable;

public interface IMetaCasProvider extends Serializable {
	
	public String getProviderKey();
	public void setProviderKey(String value);
	
	public String getProviderName();
	public void setProviderName(String value);
	
	public int getProviderPort();
	public void setProviderPort(int value);
}
