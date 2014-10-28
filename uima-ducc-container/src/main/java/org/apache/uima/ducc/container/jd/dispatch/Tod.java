package org.apache.uima.ducc.container.jd.dispatch;

public class Tod {

	private long tod = 0;
	
	public void set() {
		tod = System.currentTimeMillis();
	}
	
	public long get() {
		return tod;
	}
}
