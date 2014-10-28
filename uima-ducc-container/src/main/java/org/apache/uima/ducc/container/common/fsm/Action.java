package org.apache.uima.ducc.container.common.fsm;

import org.apache.uima.ducc.container.common.DuccLogger;
import org.apache.uima.ducc.container.common.IDuccId;
import org.apache.uima.ducc.container.common.IDuccLogger;
import org.apache.uima.ducc.container.common.MessageBuffer;
import org.apache.uima.ducc.container.common.Standardize;
import org.apache.uima.ducc.container.common.fsm.iface.IAction;

public class Action implements IAction {

	private IDuccLogger logger = DuccLogger.getLogger(Action.class, IDuccLogger.Component.JD.name());
	
	@Override
	public String getName() {
		return "default";
	}

	@Override
	public void engage(Object actionData) {
		String location = "engage";
		MessageBuffer mb = new MessageBuffer();
		mb.append(Standardize.Label.action.get()+getName());
		logger.debug(location, IDuccId.null_id, mb.toString());
	}

}
