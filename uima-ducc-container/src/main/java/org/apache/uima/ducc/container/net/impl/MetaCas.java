package org.apache.uima.ducc.container.net.impl;

import org.apache.uima.ducc.container.net.iface.IMetaCas;
import org.apache.uima.ducc.container.net.iface.IPerformanceMetrics;

public class MetaCas implements IMetaCas {

	private static final long serialVersionUID = 1L;
	
	private String systemKey = (new Integer(-1)).toString();
	private String userKey = null;
	private IPerformanceMetrics performanceMetrics = null;
	private Object userSpaceCas = null;
	private Object userSpaceException = null;

	/////
	
	public MetaCas(int seqNo, String documentText, Object userSpaceCas) {
		setSeqNo(seqNo);
		setDocumentText(documentText);
		setUserSpaceCas(userSpaceCas);
	}
	
	public int getSeqNo() {
		return Integer.parseInt(getSystemKey());
	}
	
	public void setSeqNo(int value) {
		setSystemKey(Integer.toString(value));
	}
	
	public String getDocumentText() {
		return getUserKey();
	}
	
	public void setDocumentText(String value) {
		setUserKey(value);
	}
	
	public String getSerializedCas() {
		return (String)getUserSpaceCas();
	}
	
	public void setSerializedCas(String value) {
		setUserSpaceCas(value);
	}
	
	/////
	
	@Override
	public String getSystemKey() {
		return systemKey;
	}

	@Override
	public void setSystemKey(String value) {
		systemKey = value;
	}

	@Override
	public String getUserKey() {
		return userKey;
	}

	@Override
	public void setUserKey(String value) {
		userKey = value;
	}

	@Override
	public IPerformanceMetrics getPerformanceMetrics() {
		return performanceMetrics;
	}

	@Override
	public void setPerformanceMetrics(IPerformanceMetrics value) {
		performanceMetrics = value;
	}

	@Override
	public Object getUserSpaceCas() {
		return userSpaceCas;
	}

	@Override
	public void setUserSpaceCas(Object value) {
		userSpaceCas = value;
	}

	@Override
	public Object getUserSpaceException() {
		return userSpaceException;
	}

	@Override
	public void setUserSpaceException(Object value) {
		userSpaceException = value;
	}

	
}
