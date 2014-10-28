package org.apache.uima.ducc.container.jd;

public class JobDriverException extends Exception {

	private static final long serialVersionUID = 1L;
	
	public JobDriverException(String text) {
		super(text);
	}
	
	public JobDriverException(Exception e) {
		super(e);
	}
}
