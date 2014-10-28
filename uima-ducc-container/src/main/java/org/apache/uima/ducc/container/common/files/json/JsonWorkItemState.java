package org.apache.uima.ducc.container.common.files.json;

public class JsonWorkItemState implements IJsonWorkItemState {

	private static final long serialVersionUID = 1L;
	
	private String systemKey = null;
	private String userKey = null;
	
	private String node = null;
	private int pid = 0;
	private int tid = 0;
	
	private String status = null;
	
	private long transferTime = 0;
	private long processingTime = 0;
	
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
	public String getNode() {
		return node;
	}

	@Override
	public void setNode(String value) {
		node = value;
	}

	@Override
	public int getPid() {
		return pid;
	}

	@Override
	public void setPid(int value) {
		pid = value;
	}

	@Override
	public int getTid() {
		return tid;
	}

	@Override
	public void setTid(int value) {
		tid = value;
	}

	@Override
	public String getStatus() {
		return status;
	}

	@Override
	public void setStatus(String value) {
		status = value;
	}

	@Override
	public long getTransferTime() {
		return transferTime;
	}

	@Override
	public void setTransferTime(long value) {
		transferTime = value;
	}

	@Override
	public long getProcessingTime() {
		return processingTime;
	}

	@Override
	public void setProcessingTime(long value) {
		processingTime = value;
	}

}
