package org.apache.uima.ducc.container.jd.fsm.wi;

import org.apache.uima.ducc.container.common.DuccLogger;
import org.apache.uima.ducc.container.common.IDuccId;
import org.apache.uima.ducc.container.common.IDuccLogger;
import org.apache.uima.ducc.container.common.MessageBuffer;
import org.apache.uima.ducc.container.common.Standardize;
import org.apache.uima.ducc.container.common.fsm.iface.IAction;
import org.apache.uima.ducc.container.jd.dispatch.IRemoteWorkerIdentity;
import org.apache.uima.ducc.container.jd.dispatch.IWorkItem;
import org.apache.uima.ducc.container.jd.dispatch.RemoteWorkerIdentity;
import org.apache.uima.ducc.container.net.iface.IMetaCas;
import org.apache.uima.ducc.container.net.iface.IMetaCasTransaction;

public class ActionEndCAS implements IAction {
	
	private IDuccLogger logger = DuccLogger.getLogger(ActionEndCAS.class, IDuccLogger.Component.JD.name());
	
	@Override
	public String getName() {
		return ActionEndCAS.class.getName();
	}

	@Override
	public void engage(Object objectData) {
		String location = "engage";
		logger.debug(location, IDuccId.null_id, "");
		IActionData actionData = (IActionData) objectData;
		try {
			IWorkItem wi = actionData.getWorkItem();
			IMetaCasTransaction trans = actionData.getMetaCasTransaction();
			IRemoteWorkerIdentity rwi = new RemoteWorkerIdentity(trans);
			IMetaCas metaCas = wi.getMetaCas();
			//
			if(metaCas != null) {
				wi.setTodEnd();
				MessageBuffer mb = new MessageBuffer();
				mb.append(Standardize.Label.transNo.get()+trans.getTransactionId().toString());
				mb.append(Standardize.Label.seqNo.get()+metaCas.getSystemKey());
				mb.append(Standardize.Label.remote.get()+rwi.toString());
				logger.info(location, IDuccId.null_id, mb.toString());
			}
			else {MessageBuffer mb = new MessageBuffer();
				mb.append("No CAS found for processing");
				logger.info(location, IDuccId.null_id, mb.toString());
			}
		}
		catch(Exception e) {
			logger.error(location, IDuccId.null_id, e);
		}
	}

}
