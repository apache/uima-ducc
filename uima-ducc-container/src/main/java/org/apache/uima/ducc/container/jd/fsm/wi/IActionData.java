package org.apache.uima.ducc.container.jd.fsm.wi;

import org.apache.uima.ducc.container.jd.dispatch.IRemoteWorkerIdentity;
import org.apache.uima.ducc.container.jd.dispatch.IWorkItem;
import org.apache.uima.ducc.container.net.iface.IMetaCasTransaction;

public interface IActionData {

	public IWorkItem getWorkItem();
	public IRemoteWorkerIdentity getRemoteWorkerIdentity();
	public IMetaCasTransaction getMetaCasTransaction();
}
