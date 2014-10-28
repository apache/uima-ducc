package org.apache.uima.ducc.container.jd.fsm.wi;

import org.apache.uima.ducc.container.common.DuccLogger;
import org.apache.uima.ducc.container.common.IDuccId;
import org.apache.uima.ducc.container.common.IDuccLogger;
import org.apache.uima.ducc.container.common.fsm.iface.IAction;

public class ActionSendCAS implements IAction {
	
	private IDuccLogger logger = DuccLogger.getLogger(ActionSendCAS.class, IDuccLogger.Component.JD.name());
	
	@Override
	public String getName() {
		return ActionSendCAS.class.getName();
	}

	@Override
	public void engage(Object objectData) {
		String location = "engage";
		logger.debug(location, IDuccId.null_id, "");
		IActionData actionData = (IActionData) objectData;
	}

}
