package org.apache.uima.ducc.container.net.iface;

import java.io.Serializable;

public interface IMetaCasKeys extends Serializable {

	// System assigned key, e.g. a CAS sequence number
	public String getSystemKey();			
	public void setSystemKey(String value);

	// User provided key, e.g. getDocumentText()
	public String getUserKey();
	public void setUserKey(String value);
}
