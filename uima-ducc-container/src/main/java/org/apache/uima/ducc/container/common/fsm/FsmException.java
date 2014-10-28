package org.apache.uima.ducc.container.common.fsm;

public class FsmException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public FsmException(String text) {
		super(text);
	}
	
	public FsmException(Exception e) {
		super(e);
	}
}
