package org.apache.uima.ducc.ws;

import org.apache.uima.ducc.transport.event.OrchestratorStateDuccEvent;

public interface IListenerOrchestrator {
	public void update(OrchestratorStateDuccEvent duccEvent);
}
