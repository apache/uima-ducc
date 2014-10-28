package org.apache.uima.ducc.container.net.iface;

import java.io.Serializable;

public interface IMetaCas extends IMetaCasKeys, Serializable {
	
	// Performance metrics
	public IPerformanceMetrics getPerformanceMetrics();
	public void setPerformanceMetrics(IPerformanceMetrics value);
	
	// CAS (accessible in user space only)
	public Object getUserSpaceCas();
	public void setUserSpaceCas(Object value);
	
	// Exception (accessible in user space only)
	public Object getUserSpaceException();
	public void setUserSpaceException(Object value);
}
