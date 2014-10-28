package org.apache.uima.ducc.container.jd.fsm.wi;

import org.apache.uima.ducc.container.jd.dispatch.IRemoteWorkerIdentity;
import org.apache.uima.ducc.container.jd.dispatch.IWorkItem;
import org.apache.uima.ducc.container.net.iface.IMetaCasTransaction;

public class ActionData implements IActionData {
	
	private IWorkItem workItem = null;
	private IRemoteWorkerIdentity remoteWorkerIdentity = null;
	private IMetaCasTransaction metaCasTransaction = null;
	
	public ActionData(IWorkItem workItem, IRemoteWorkerIdentity remoteWorkerIdentity, IMetaCasTransaction metaCasTransaction) {
		setWorkItem(workItem);
		setRemoteWorkerIdentity(remoteWorkerIdentity);
		setMetaCasTransaction(metaCasTransaction);
	}
	
	@Override
	public IWorkItem getWorkItem() {
		return workItem;
	}
	
	private void setWorkItem(IWorkItem value) {
		workItem = value;
	}
	
	@Override
	public IRemoteWorkerIdentity getRemoteWorkerIdentity() {
		return remoteWorkerIdentity;
	}
	
	private void setRemoteWorkerIdentity(IRemoteWorkerIdentity value) {
		remoteWorkerIdentity = value;
	}
	
	@Override
	public IMetaCasTransaction getMetaCasTransaction() {
		return metaCasTransaction;
	}
	
	private void setMetaCasTransaction(IMetaCasTransaction value) {
		metaCasTransaction = value;
	}
}
