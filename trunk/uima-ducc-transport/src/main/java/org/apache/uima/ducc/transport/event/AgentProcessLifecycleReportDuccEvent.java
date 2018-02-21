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

import org.apache.uima.ducc.common.NodeIdentity;
import org.apache.uima.ducc.transport.event.common.IDuccProcess;

public class AgentProcessLifecycleReportDuccEvent extends AbstractDuccEvent {

	private static final long serialVersionUID = 1L;

	public enum LifecycleEvent {
		Launch, Terminate, Undefined
	}

	private IDuccProcess process = null;
	private NodeIdentity nodeIdentity = null;
	private LifecycleEvent lifecycleEvent = LifecycleEvent.Undefined;

	public AgentProcessLifecycleReportDuccEvent(IDuccProcess process, NodeIdentity nodeIdentity, LifecycleEvent lifecycleEvent) {
		super(EventType.AGENT_PROCESS_LIFECYCLE_REPORT);
		setProcess(process);
		setNodeIdentity(nodeIdentity);
		setLifecycleEvent(lifecycleEvent);
	}
	
	private void setProcess(IDuccProcess value) {
		this.process = value;
	}

	public IDuccProcess getProcess() {
		return process;
	}

	private void setNodeIdentity(NodeIdentity value) {
		this.nodeIdentity = value;
	}

	public NodeIdentity getNodeIdentity() {
		return nodeIdentity;
	}
	
	private void setLifecycleEvent(LifecycleEvent value) {
		this.lifecycleEvent = value;
	}

	public LifecycleEvent getLifecycleEvent() {
		return lifecycleEvent;
	}
	
}
