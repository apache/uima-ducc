package org.apache.uima.ducc.container.jd.dispatch;

import org.apache.uima.ducc.container.common.fsm.iface.IFsm;
import org.apache.uima.ducc.container.net.iface.IMetaCas;

public interface IWorkItem {
	
	public void setMetaCas(IMetaCas value);
	public IMetaCas getMetaCas();
	
	public void setFsm(IFsm value);
	public IFsm getFsm();
	
	public void setTodGet();
	public long getTodGet();
	
	public void setTodAck();
	public long getTodAck();
	
	public void setTodEnd();
	public long getTodEnd();
}
