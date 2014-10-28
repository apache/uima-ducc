package org.apache.uima.ducc.container.common;

public class MessageBuffer {

	private StringBuffer sb = new StringBuffer();
	
	public void append(String text) {
		if(sb.length() > 0) {
			sb.append(" ");
		}
		sb.append(text);
	}
	
	@Override
	public String toString() {
		return sb.toString();
	}
}
