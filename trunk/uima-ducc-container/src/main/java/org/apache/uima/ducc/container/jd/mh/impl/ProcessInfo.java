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

import org.apache.uima.ducc.container.jd.mh.iface.IProcessInfo;
import org.apache.uima.ducc.container.jd.wi.IProcessStatistics;

public class ProcessInfo implements IProcessInfo {

	private static final long serialVersionUID = 1L;
	
	private String nodeName = null;
	private String nodeAddress = null;
	private String pidName = null;
	private int pid = 0;
	
	private long dispatch = 0;
	private long done = 0;
	private long error = 0;
	private long preempt = 0;
	private long retry = 0;
	
	private long avg = 0;
	private long max = 0;
	private long min = 0;
	
	private String reasonStopped = null;
	private String reasonDeallocated = null;
	
	public ProcessInfo(String nodeName, String nodeAddress, String pidName, int pid) {
		setNodeName(nodeName);
		setPidName(pidName);
		setPid(pid);
	}
	
	public ProcessInfo(String nodeName, String nodeAddress, String pidName, int pid, String reasonStopped, String reasonDeallocated) {
		setNodeName(nodeName);
		setPidName(pidName);
		setPid(pid);
		setReasonStopped(reasonStopped);
		setReasonDeallocated(reasonDeallocated);
	}
	
	public ProcessInfo(String nodeName, String nodeAddress, String pidName, int pid, IProcessStatistics pStats) {
		setNodeName(nodeName);
		setNodeAddress(nodeAddress);
		setPidName(pidName);
		setPid(pid);
		setDispatch(pStats.getCountDispatch());
		setDone(pStats.getCountDone());
		setError(pStats.getCountError());
		setPreempt(pStats.getCountPreempt());
		setRetry(pStats.getCountRetry());
		setAvg(pStats.getMillisAvg());
		setMax(pStats.getMillisMax());
		setMin(pStats.getMillisMin());
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
	public void setDispatch(long value) {
		dispatch = value;
	}

	@Override
	public long getDispatch() {
		return dispatch;
	}

	@Override
	public void setDone(long value) {
		done = value;
	}

	@Override
	public long getDone() {
		return done;
	}

	@Override
	public void setError(long value) {
		error = value;
	}

	@Override
	public long getError() {
		return error;
	}

	@Override
	public void setPreempt(long value) {
		preempt = value;
	}

	@Override
	public long getPreempt() {
		return preempt;
	}

	@Override
	public void setRetry(long value) {
		retry = value;
	}

	@Override
	public long getRetry() {
		return retry;
	}

	@Override
	public void setAvg(long value) {
		avg = value;
	}

	@Override
	public long getAvg() {
		return avg;
	}

	@Override
	public void setMax(long value) {
		max = value;
	}

	@Override
	public long getMax() {
		return max;
	}

	@Override
	public void setMin(long value) {
		min = value;
	}

	@Override
	public long getMin() {
		return min;
	}

	@Override
	public void setReasonStopped(String value) {
		reasonStopped = value;
	}

	@Override
	public String getReasonStopped() {
		return reasonStopped;
	}

	@Override
	public void setReasonDeallocated(String value) {
		reasonDeallocated = value;
	}

	@Override
	public String getReasonDeallocated() {
		return reasonDeallocated;
	}

}
