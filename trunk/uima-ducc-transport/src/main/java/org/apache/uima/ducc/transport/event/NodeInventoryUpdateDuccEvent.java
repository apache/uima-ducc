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

import java.util.HashMap;
import java.util.Map;

import org.apache.uima.ducc.common.NodeIdentity;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.common.IDuccProcess;


public class NodeInventoryUpdateDuccEvent extends AbstractDuccEvent {

	private static final long serialVersionUID = -240986007026771587L;

	private Map<DuccId, IDuccProcess> processes = null;
	private long lastORSequence;
	private NodeIdentity nodeIdentity;
	
	public NodeInventoryUpdateDuccEvent(Map<DuccId, IDuccProcess> processes, long lastORSequence, NodeIdentity node) {
		super(EventType.START_PROCESS);
		this.processes = processes;
		this.lastORSequence = lastORSequence;
		this.nodeIdentity = node;
	}
	
	public NodeIdentity getNodeIdentity() {
		return nodeIdentity;
	}
	public long getSequence() {
		return lastORSequence;
	}
	public HashMap<DuccId, IDuccProcess> getProcesses() {
		HashMap<DuccId, IDuccProcess> processMap =
				new HashMap<>();
		processMap.putAll(processes);
		return processMap;
	}
}
