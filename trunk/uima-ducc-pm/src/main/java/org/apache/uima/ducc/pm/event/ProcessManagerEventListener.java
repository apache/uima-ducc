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
package org.apache.uima.ducc.pm.event;

import org.apache.camel.Body;
import org.apache.uima.ducc.pm.ProcessManager;
import org.apache.uima.ducc.pm.ProcessManagerComponent;
import org.apache.uima.ducc.transport.dispatcher.DuccEventDispatcher;
import org.apache.uima.ducc.transport.event.OrchestratorStateDuccEvent;
import org.apache.uima.ducc.transport.event.delegate.DuccEventDelegateListener;


/**
 * 
 *
 */
public class ProcessManagerEventListener 
implements DuccEventDelegateListener { 
	
//	private DuccEventDispatcher eventDispatcher;
//	private String targetEndpoint;
	private ProcessManager processManager;
	
	public ProcessManagerEventListener(ProcessManager processManager) {
		this.processManager = processManager;
	}
	public void setDuccEventDispatcher( DuccEventDispatcher eventDispatcher ) {
//		this.eventDispatcher = eventDispatcher;
	}
	public void setEndpoint( String endpoint ) {
//		this.targetEndpoint = endpoint;
	}
	/**
	 * Handles Job Manager state changes. 
	 *  
	 * @param jobMap - state Map sent by the Job Manager
	 */
	public void onJobManagerStateUpdate(@Body OrchestratorStateDuccEvent duccEvent) {
		// process OR state only if the JD has been assigned
		if ( !duccEvent.getWorkMap().isJobDriverNodeAssigned() ) {
			((ProcessManagerComponent)processManager).getLogger().info("onJobManagerStateUpdate", null, "Orchestrator JD node not assigned. Ignoring Orchestrator state update");
			return;
		}
		processManager.dispatchStateUpdateToAgents(duccEvent.getWorkMap().getMap(), duccEvent.getSequence());
	}
}
