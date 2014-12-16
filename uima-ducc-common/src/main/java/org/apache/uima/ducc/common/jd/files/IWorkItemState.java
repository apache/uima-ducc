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

import java.io.Serializable;

public interface IWorkItemState extends Serializable, Comparable<IWorkItemState> {

	public String getSeqNo();
	
	public String getWiId();
	public void setWiId(String wiId);
	public String getNode();
	public void setNode(String node);
	public String getPid();
	public void setPid(String pid);
	public String getTid();
	public void setTid(String tid);
	
	public enum State {
		start,
		queued,
		operating,
		ended,
		error,
		retry,
		preempt,
		lost,
		unknown
	}
	
	public State getState();
	public void stateStart();
	public void stateQueued();
	public void stateOperating();
	public void stateEnded();
	public void stateError();
	public void stateRetry();
	public void statePreempt();
	public void stateLost();
	
	public long getMillisOverhead();
	public long getMillisProcessing();
}
