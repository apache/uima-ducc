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

public class DuccProcessCpuUsage extends ByteBufferParser 
implements ProcessCpuUsage {
	private static final long serialVersionUID = 1L;
	public static final int USERJIFFIES=13;
	public static final int SYSTEMJIFFIES=14;
    
	public DuccProcessCpuUsage(byte[] memInfoBuffer,
			int[] memInfoFieldOffsets, int[] memInfoFiledLengths) {
		super(memInfoBuffer, memInfoFieldOffsets, memInfoFiledLengths);
	}	
	public long getUserJiffies() {
		return super.getFieldAsLong(USERJIFFIES);
	}
	public long getKernelJiffies() {
		return super.getFieldAsLong(SYSTEMJIFFIES);
	}
	public long getTotalJiffies() {
		return (getUserJiffies()+getKernelJiffies());
	}
}
