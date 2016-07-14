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
package org.apache.uima.ducc.common.agent.metrics.memory;

import org.apache.uima.ducc.common.node.metrics.ByteBufferParser;

public class DuccProcessResidentMemory extends ByteBufferParser implements
		ProcessResidentMemory {

	private static final long serialVersionUID = 8563460863767404377L;
	private static final int TOTAL = 0;
	private static final int RESIDENT = 1;

	public DuccProcessResidentMemory(byte[] memInfoBuffer,
			int[] memInfoFieldOffsets, int[] memInfoFiledLengths) {
		super(memInfoBuffer, memInfoFieldOffsets, memInfoFiledLengths);
	}

	public long get() {
		return super.getFieldAsLong(RESIDENT);
	}
	
	public long getTotal() {
		return super.getFieldAsLong(TOTAL);
	}
}
