package org.apache.uima.ducc.container.common.files.json;

import org.apache.uima.ducc.container.jd.dispatch.IRemoteWorkerIdentity;
import org.apache.uima.ducc.container.net.iface.IMetaCasKeys;

public interface IJsonWorkItemState extends IMetaCasKeys, IRemoteWorkerIdentity {

	public String getStatus();
	public void setStatus(String value);
	
	public long getTransferTime();
	public void setTransferTime(long value);
	
	public long getProcessingTime();
	public void setProcessingTime(long value);
}
