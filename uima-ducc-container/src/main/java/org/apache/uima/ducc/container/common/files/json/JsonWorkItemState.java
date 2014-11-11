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
package org.apache.uima.ducc.container.common.files.json;

public class JsonWorkItemState implements IJsonWorkItemState {

	private static final long serialVersionUID = 1L;
	
	private String systemKey = null;
	private String userKey = null;
	
	private String nodeName = null;
	private String nodeAddress = null;
	private int pid = 0;
	private int tid = 0;
	
	private String status = null;
	
	private long transferTime = 0;
	private long processingTime = 0;
	
	@Override
	public String getSystemKey() {
		return systemKey;
	}

	@Override
	public void setSystemKey(String value) {
		systemKey = value;
	}

	@Override
	public String getUserKey() {
		return userKey;
	}

	@Override
	public void setUserKey(String value) {
		userKey = value;
	}
	
	@Override
	public String getNodeName() {
		return nodeName;
	}

	@Override
	public void setNodeName(String value) {
		nodeName = value;
	}
	
	@Override
	public String getNodeAddress() {
		return nodeAddress;
	}

	@Override
	public void setNodeAddress(String value) {
		nodeAddress = value;
	}
	
	@Override
	public int getPid() {
		return pid;
	}

	@Override
	public void setPid(int value) {
		pid = value;
	}

	@Override
	public int getTid() {
		return tid;
	}

	@Override
	public void setTid(int value) {
		tid = value;
	}

	@Override
	public String getStatus() {
		return status;
	}

	@Override
	public void setStatus(String value) {
		status = value;
	}

	@Override
	public long getTransferTime() {
		return transferTime;
	}

	@Override
	public void setTransferTime(long value) {
		transferTime = value;
	}

	@Override
	public long getProcessingTime() {
		return processingTime;
	}

	@Override
	public void setProcessingTime(long value) {
		processingTime = value;
	}

}
