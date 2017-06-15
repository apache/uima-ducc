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
package org.apache.uima.ducc.user.dgen;

public class DuccUimaReferenceByName implements IDuccGeneratorUimaReferenceByName {

	/**
	 * please increment this sUID when removing or modifying a field 
	 */
	private static final long serialVersionUID = 1L;

	private String referenceByName;
	
	private int threadCount = 1;
	
	public DuccUimaReferenceByName(int threadCount,String ddName) {
		setThreadCount(threadCount);
		setReferenceByName(ddName);
	}
	
	public String getReferenceByName() {
		return referenceByName;
	}
	
	public void setReferenceByName(String referenceByName) {
		this.referenceByName = referenceByName;
	}
	
	public int getThreadCount() {
		return threadCount;
	}

	
	public void setThreadCount(int threadCount) {
		this.threadCount = threadCount;
	}

}
