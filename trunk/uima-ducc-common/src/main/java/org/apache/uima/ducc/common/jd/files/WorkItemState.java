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
package org.apache.uima.ducc.common.jd.files;

public class WorkItemState implements IWorkItemState {

	private static final long serialVersionUID = 1L;
	
	private String seqNo = null;
	private String wiId = null;
	private String node = null;
	private String pid = null;
	private String tid = null;
	@Deprecated
	private Name name = null;
	private State state = State.unknown;
	
	private long millisAtStart = -1;
	private long millisAtQueued = -1;
	private long millisAtOperating = -1;
	private long millisAtFinish = -1;
	private long millisAtInvestment = -1;
	
	@Deprecated
	public enum Name {
		start,
		queued,
		operating,
		ended,
		error,
		retry,
	}
	
	public WorkItemState(int seqNo) {
		this.seqNo = ""+seqNo;
	}
	
	
	public String getSeqNo() {
		return seqNo;
	}

	
	public String getWiId() {
		return wiId;
	}

	
	public void setWiId(String wiId) {
		this.wiId = wiId;
	}

	
	public String getNode() {
		return node;
	}
	
	
	public void setNode(String node) {
		this.node = node;
	}

	
	public String getPid() {
		return pid;
	}
	
	
	public void setPid(String pid) {
		this.pid = pid;
	}
	
	
	public String getTid() {
		return tid;
	}
	
	
	public void setTid(String tid) {
		this.tid = tid;
	}
	
	
	public State getState() {
		State retVal = state;
		// legacy
		if(name != null) {
			switch (name) {
				case start:
					state = State.start;
					break;
				case queued:
					state = State.queued;
					break;
				case operating:
					state = State.operating;
					break;
				case ended:
					state = State.ended;
					break;
				case retry:
					state = State.retry;
					break;
				default:
					state = State.unknown;
			}
		}
		return retVal;
	}

	
	public void stateStart() {
		state = State.start;
		millisAtStart = System.currentTimeMillis();
	}

	
	public void stateQueued() {
		state = State.queued;
		millisAtQueued = System.currentTimeMillis();
	}

	
	public void stateOperating() {
		state = State.operating;
		//jpIp = ip;
		//jpId = id;
		millisAtOperating = System.currentTimeMillis();
		millisAtInvestment = millisAtOperating;
	}
	
	public void investmentReset() {
		millisAtInvestment = System.currentTimeMillis();
	}
	
	public void stateEnded() {
		state = State.ended;
		millisAtFinish = System.currentTimeMillis();
	}

	
	public void stateError() {
		state = State.error;
		millisAtFinish = System.currentTimeMillis();
	}

	
	public void stateLost() {
		state = State.lost;
		millisAtFinish = System.currentTimeMillis();
	}
	
	
	public void stateRetry() {
		state = State.retry;
	}
	
	
	public void statePreempt() {
		state = State.preempt;
	}

	public long getMillisOverhead() {
		return getMillisOverhead(System.currentTimeMillis());
	}
	
	public long getMillisOverhead(long now) {
		long retVal = 0;
		if(millisAtStart > 0) {
			if(millisAtQueued > 0) {
				retVal = millisAtQueued - millisAtStart;
			}
			if(millisAtOperating > 0) {
				retVal = millisAtOperating - millisAtStart;
			}
			else {
				retVal = now - millisAtStart;
			}
		}
		return retVal;
	}

	public long getMillisProcessing() {
		return getMillisProcessing(System.currentTimeMillis());
	}
	
	public long getMillisProcessing(long now) {
		long retVal = 0;
		if(millisAtOperating > 0) {
			if(millisAtFinish > 0) {
				retVal = millisAtFinish - millisAtOperating;
			}
			else {
				retVal =  now - millisAtOperating;
			}
		}
		return retVal;
	}
	
	public long getMillisInvestment() {
		return getMillisInvestment(System.currentTimeMillis());
	}
	
	public long getMillisInvestment(long now) {
		long retVal = 0;
		if(millisAtFinish < 0) {
			if(millisAtInvestment > 0) {
				retVal = now - millisAtInvestment;
			}
			else {
				retVal = getMillisProcessing();
			}
		}
		return retVal;
	}
	
	private static int stateOrder(State state) {
		int retVal = 0;
		if(state != null) {
			switch(state) {
			case start:
				retVal = -5; 
				break;
			case queued:
				retVal = -4; 
				break;
			case operating:
				retVal = -3; 
				break;
			case ended:
				retVal = -6; 
				break;
			case error:
				retVal = -1; 
				break;
			case retry:
				retVal = -2; 
				break;
			default:
				break;
			}
		}
		return retVal;
	}
	
	private static int compareState(State s1, State s2) {
		int so1 = stateOrder(s1);
		int so2 = stateOrder(s2);
		if(so2 > so1) {
			return 1;
		}
		else if(so2 < so1) {
			return -1;
		}
		return 0;
	}
	
	private static int compareSeqNo(String s1, String s2) {
		long so1 = Long.parseLong(s1);
		long so2 = Long.parseLong(s2);
		if(so2 > so1) {
			return 1;
		}
		else if(so2 < so1) {
			return -1;
		}
		return 0;
	}
	
	public int compareTo(IWorkItemState wis) {
		int retVal = 0;
		IWorkItemState w1 = this;
		IWorkItemState w2 = wis;
		retVal = compareState(w1.getState(), w2.getState());
		if (retVal == 0) {
			retVal = compareSeqNo(w1.getSeqNo(), w2.getSeqNo());
		}
		return retVal;
	}
}
