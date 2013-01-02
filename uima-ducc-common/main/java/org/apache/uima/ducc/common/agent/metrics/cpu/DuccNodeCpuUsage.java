/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
*/
package org.apache.uima.ducc.common.agent.metrics.cpu;

import org.apache.uima.ducc.common.node.metrics.ByteBufferParser;

/**
 * All values in terms of Jiffies, typically hundreds of a sec 
 *
 */
public class DuccNodeCpuUsage extends ByteBufferParser 
implements NodeCpuUsage {

	private static final long serialVersionUID = 1L;
	public static final int USERJIFFIES=1;
	public static final int NICEJIFFIES=2;
	public static final int SYSTEMJIFFIES=3;
	public static final int IDLEJIFFIES=4;
	public static final int IOWAITJIFFIES=5;
	public static final int IRQJIFFIES=6;
	public static final int SOFTIRQJIFFIES=7;
	
	public DuccNodeCpuUsage(byte[] memInfoBuffer,
			int[] memInfoFieldOffsets, int[] memInfoFiledLengths) {
		super(memInfoBuffer, memInfoFieldOffsets, memInfoFiledLengths);
	}	
	public long getUserJiffies() {
		return super.getFieldAsLong(USERJIFFIES);
	}

	public long getNiceJiffies() {
		return super.getFieldAsLong(NICEJIFFIES);
	}

	public long getSystemJiffies() {
		return super.getFieldAsLong(SYSTEMJIFFIES);
	}

	public long getIdleJiffies() {
		return super.getFieldAsLong(IDLEJIFFIES);
	}

	public long getIowaitJiffies() {
		return super.getFieldAsLong(IOWAITJIFFIES);
	}

	public long getIrqJiffies() {
		return super.getFieldAsLong(IRQJIFFIES);
	}

	public long getSoftirqs() {
		return super.getFieldAsLong(USERJIFFIES);
	}
	
	public long getTotal() {
		return getUserJiffies() +
				getNiceJiffies() +
				getSystemJiffies() +
				getIdleJiffies() +
				getIowaitJiffies() +
				getIrqJiffies() +
				getSoftirqs();
	}

}
