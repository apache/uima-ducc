package org.apache.uima.ducc.common.agent.metrics.swap;

import org.apache.uima.ducc.common.node.metrics.ByteBufferParser;

public class DuccProcessMemoryPageLoadUsage extends ByteBufferParser implements
		ProcessMemoryPageLoadUsage {
	private static final long serialVersionUID = 1L;
	public static final int MAJORFAULTSFLD=11;
	
	public DuccProcessMemoryPageLoadUsage(byte[] memInfoBuffer,
			int[] memInfoFieldOffsets, int[] memInfoFiledLengths) {
		super(memInfoBuffer, memInfoFieldOffsets, memInfoFiledLengths);
	}	
	public long getMajorFaults() {
		return super.getFieldAsLong(MAJORFAULTSFLD);
	}
}
