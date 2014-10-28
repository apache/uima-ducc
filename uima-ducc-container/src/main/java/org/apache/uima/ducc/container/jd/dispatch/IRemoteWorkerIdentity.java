package org.apache.uima.ducc.container.jd.dispatch;

public interface IRemoteWorkerIdentity {

	public String getNode();
	public void setNode(String value);
	
	public int getPid();
	public void setPid(int value);
	
	public int getTid();
	public void setTid(int value);
}
