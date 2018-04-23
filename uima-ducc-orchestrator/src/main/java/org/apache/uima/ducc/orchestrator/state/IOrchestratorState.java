package org.apache.uima.ducc.orchestrator.state;

import org.apache.uima.ducc.common.NodeIdentity;

public interface IOrchestratorState {
	public long getNextPublicationSequenceNumber();
	public void setNextPublicationSequenceNumber(long seqNo);
	public void setNextPublicationSequenceNumberIfGreater(long seqNo, NodeIdentity nodeIdentity);
	//
	public long getDuccWorkSequenceNumber();
	public void setDuccWorkSequenceNumber(long seqNo);
	public void setDuccWorkSequenceNumberIfGreater(long seqNo);
	//
	public long getNextDuccWorkSequenceNumber();
}
