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
package org.apache.uima.ducc.transport.event;

import java.util.List;

import org.apache.uima.ducc.transport.agent.IUimaPipelineAEComponent;
import org.apache.uima.ducc.transport.agent.ProcessStateUpdate;
import org.apache.uima.ducc.transport.event.common.IProcessState.ProcessState;


public class ProcessStateUpdateDuccEvent extends AbstractDuccEvent {
	private static final long serialVersionUID = -9138045039215135857L;
	private ProcessStateUpdate processUpdate;
	private String message;
	
	public ProcessStateUpdateDuccEvent(ProcessStateUpdate processUpdate) {
		super(EventType.PROCESS_STATE);
		this.processUpdate = processUpdate;
	}
	public String getMessage() {
		return message;
	}
	public void setMessage( String message) {
		this.message = message;
	}
	/**
	 * @return the state
	 */
	public ProcessState getState() {
		return processUpdate.getState();
	}
	/**
	 * @return the pid
	 */
	public String getPid() {
		return processUpdate.getPid();
	}
	/**
	 * @param pid the pid to set
	 */
	/**
	 * @return the duccProcessId
	 */
	public String getDuccProcessId() {
		return processUpdate.getDuccProcessId();
	}
	/**
	 * @param duccProcessId the duccProcessId to set
	 */
	public List<IUimaPipelineAEComponent> getUimaPipeline() {
		return processUpdate.getUimaPipeline();
	}
	public String getProcessJmxUrl() {
		return processUpdate.getProcessJmxUrl();
	}
	public String getServiceEdnpoint() {
	  return processUpdate.getSocketEndpoint();
	}
}
