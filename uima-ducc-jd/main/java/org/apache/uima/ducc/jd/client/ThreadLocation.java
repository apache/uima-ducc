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
package org.apache.uima.ducc.jd.client;

import java.util.concurrent.Future;

public class ThreadLocation {

	private String seqNo;
	private String nodeId;
	private String processId;
	private Future<?> pendingWork;
	
	public ThreadLocation() {
	}
	
	public ThreadLocation(String seqNo) {
		this.seqNo = seqNo;
	}
	
	public ThreadLocation(String seqNo, String nodeId, String processId) {
		this.seqNo = seqNo;
		this.nodeId = nodeId;
		this.processId = processId;
	}
	
	public String getLocationId() {
		return getNodeId()+":"+getProcessId();
	}
	
	public String getInfo() {
		return "seqNo:"+getSeqNo()+" "+"node:"+getNodeId()+" "+"PID:"+getProcessId();
	}
	
	public String getSeqNo() {
		return seqNo;
	}
	public void setSeqNo(String seqNo) {
		this.seqNo = seqNo;
	}
	
	public String getNodeId() {
		return nodeId;
	}
	public void setNodeId(String nodeId) {
		this.nodeId = nodeId;
	}
	
	public String getProcessId() {
		return processId;
	}
	public void setProcessId(String processId ) {
		this.processId = processId;
	}
	
	public void setPendingWork(Future<?> pendingWork) {
		this.pendingWork = pendingWork;
	}
	public Future<?> getPendingWork() {
		return pendingWork;
	}
	
}
