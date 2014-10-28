package org.apache.uima.ducc.container.net.impl;

public class TransactionId {

	private int major = 0;
	private int minor = 0;
	
	public TransactionId(int major, int minor) {
		setMajor(major);
		setMinor(minor);
	}
	
	private int getMajor() {
		return major;
	}
	
	private void setMajor(int value) {
		major = value;
	}
	
	private int getMinor() {
		return minor;
	}
	
	private void setMinor(int value) {
		minor = value;
	}
	
	public void next() {
		minor++;
	}
	
	@Override
	public String toString() {
		return getMajor()+"."+getMinor();
	}
}
