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

import java.io.Serializable;
import java.util.List;

import org.apache.uima.ducc.transport.event.common.IProcessState.ProcessState;


public class ProcessStateUpdate implements Serializable {
	  public static final String ProcessStateUpdatePort = "ducc.agent.process.state.update.port";

	/**
   * 
   */
  private static final long serialVersionUID = 1L;
  ProcessState state;
	String pid;
	String socketEndpoint=null;
	String duccProcessId;
	String processJmxUrl;
	String message;
	List<IUimaPipelineAEComponent> uimaPipeline;
	
	public ProcessStateUpdate(ProcessState state, String pid, String duccProcessId) {
		this(state,pid,duccProcessId,null,null);
	}
	public ProcessStateUpdate(ProcessState state, String pid, String duccProcessId,String processJmxUrl) {
		this(state,pid,duccProcessId,processJmxUrl,null);
	}
	public ProcessStateUpdate(ProcessState state, String pid,
			String duccProcessId, String processJmxUrl, List<IUimaPipelineAEComponent> uimaPipeline) {
		super();
		this.state = state;
		this.pid = pid;
		this.duccProcessId = duccProcessId;
		this.processJmxUrl = processJmxUrl;
		this.message = processJmxUrl;
		this.uimaPipeline = uimaPipeline;
	}
	/**
	 * @return the uimaPipeline
	 */
	public List<IUimaPipelineAEComponent> getUimaPipeline() {
		return uimaPipeline;
	}
	/**
	 * @param uimaPipeline the uimaPipeline to set
	 */
	public void setUimaPipeline(List<IUimaPipelineAEComponent> uimaPipeline) {
		this.uimaPipeline = uimaPipeline;
	}
	/**
	 * @return the state
	 */
	public ProcessState getState() {
		return state;
	}
	/**
	 * @return the pid
	 */
	public String getPid() {
		return pid;
	}
	/**
	 * @return the duccProcessId
	 */
	public String getDuccProcessId() {
		return duccProcessId;
	}
	public String getProcessJmxUrl() {
		return processJmxUrl;
	}
	public String getMessage() {
		return message;
	}
  public String getSocketEndpoint() {
    return socketEndpoint;
  }
  public void setSocketEndpoint(String socketEndpoint ) {
    this.socketEndpoint = socketEndpoint;
  }
	
}
