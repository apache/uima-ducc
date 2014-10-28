package org.apache.uima.ducc.container.jd.dispatch;

import org.apache.uima.ducc.container.common.fsm.iface.IFsm;
import org.apache.uima.ducc.container.net.iface.IMetaCas;

public class WorkItem implements IWorkItem {

	private IMetaCas metaCas = null;
	private IFsm fsm = null;
	
	private Tod todGet = new Tod();
	private Tod todAck = new Tod();
	private Tod todEnd = new Tod();
	
	public WorkItem(IMetaCas metaCas, IFsm fsm) {
		setMetaCas(metaCas);
		setFsm(fsm);
	}
	
	@Override
	public void setMetaCas(IMetaCas value) {
		metaCas = value;
	}
	
	@Override
	public IMetaCas getMetaCas() {
		return metaCas;
	}
	
	@Override
	public void setFsm(IFsm value) {
		fsm = value;
	}	
	
	@Override
	public IFsm getFsm() {
		return fsm;
	}

	@Override
	public void setTodGet() {
		todGet.set();
	}

	@Override
	public long getTodGet() {
		return todGet.get();
	}

	@Override
	public void setTodAck() {
		todAck.set();
	}

	@Override
	public long getTodAck() {
		return todAck.get();
	}

	@Override
	public void setTodEnd() {
		todEnd.set();
	}

	@Override
	public long getTodEnd() {
		return todEnd.get();
	}
}
