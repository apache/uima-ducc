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
package org.apache.uima.ducc.transport.agent;

import org.apache.uima.analysis_engine.AnalysisEngineManagement.State;

public class UimaPipelineAEComponent implements IUimaPipelineAEComponent {
	
	private static final long serialVersionUID = 1L;
	
	String name;
	State state;
	long threadId;
	long initializationTime;
	public transient long startInitialization;
	
	public UimaPipelineAEComponent(String name, long threadId, State state) {
		this.name = name;
		this.threadId = threadId;
		this.state = state;
	}
	public long getInitializationTime() {
		return initializationTime;
	}
	public void setInitializationTime(long initializationTime) {
		this.initializationTime = initializationTime;
	}

	
	@Override
	public String getAeName() {
		// TODO Auto-generated method stub
		return name;
	}

	@Override
	public State getAeState() {
		// TODO Auto-generated method stub
		return state;
	}

	public void setAeState(State state ){
		this.state = state;
	}
	public long getAeThreadId() {
		// TODO Auto-generated method stub
		return threadId;
	}

}
