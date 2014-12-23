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
package org.apache.uima.ducc.container.jd.mh.impl;

import org.apache.uima.ducc.container.jd.mh.iface.IWorkItemInfo;

public class WorkItemInfo implements IWorkItemInfo {

	private static final long serialVersionUID = 1L;
	
	String nodeName = null;
	String nodeAddress = null;
	String pidName = null;
	int pid = 0;
	int tid = 0;
	long operatingMillis = 0;
	int seqNo = 0;
	
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
	public String getPidName() {
		return pidName;
	}

	@Override
	public void setPidName(String value) {
		pidName = value;
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
	public long getOperatingMillis() {
		return operatingMillis;
	}

	@Override
	public void setOperatingMillis(long value) {
		operatingMillis = value;
	}

	@Override
	public int getSeqNo() {
		return seqNo;
	}

	@Override
	public void setSeqNo(int value) {
		seqNo = value;
	}

}
