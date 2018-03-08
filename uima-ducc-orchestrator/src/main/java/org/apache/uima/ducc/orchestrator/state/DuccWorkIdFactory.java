package org.apache.uima.ducc.orchestrator.state;

import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.common.utils.id.IDuccIdFactory;

public class DuccWorkIdFactory implements IDuccIdFactory {

	private static IOrchestratorState orchestratorState = OrchestratorState.getInstance();
	
	@Override
	public DuccId next() {
		long value = orchestratorState.getNextDuccWorkSequenceNumber();
		return new DuccId(value);
	}

	@Override
	public long setIfMax(long seqNo) {
		orchestratorState.setDuccWorkSequenceNumberIfGreater(seqNo);
		return orchestratorState.getDuccWorkSequenceNumber();
	}

}
