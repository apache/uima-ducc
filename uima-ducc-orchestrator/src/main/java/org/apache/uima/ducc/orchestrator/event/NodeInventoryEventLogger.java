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
package org.apache.uima.ducc.orchestrator.event;

import java.util.HashMap;
import java.util.Iterator;

import org.apache.uima.ducc.common.internationalization.Messages;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.orchestrator.OrchestratorCommonArea;
import org.apache.uima.ducc.transport.event.NodeInventoryUpdateDuccEvent;
import org.apache.uima.ducc.transport.event.common.IDuccProcess;


public class NodeInventoryEventLogger {
	
	private static final DuccLogger logger = DuccLogger.getLogger(NodeInventoryEventLogger.class);
	
	private static final OrchestratorCommonArea orchestratorCommonArea = OrchestratorCommonArea.getInstance();
	private static final Messages messages = orchestratorCommonArea.getSystemMessages();
	
	public static void receiver(NodeInventoryUpdateDuccEvent nodeInventoryUpdateDuccEvent) {
		String methodName = "receiver";
		logger.trace(methodName, null, messages.fetch("enter"));
		HashMap<DuccId, IDuccProcess> processMap = nodeInventoryUpdateDuccEvent.getProcesses();
		if(processMap != null) {
			logger.debug(methodName, null, processMap.size());
			Iterator<DuccId> iterator = processMap.keySet().iterator();
			while(iterator.hasNext()) {
				DuccId processId = iterator.next();
				DuccId jobId = orchestratorCommonArea.getProcessAccounting().getJobId(processId);
				IDuccProcess process = processMap.get(processId);
				String processState = ""+process.getProcessState();
				String nodeIdentity = ""+process.getNodeIdentity();
				String PID = ""+process.getPID();
				String resourceState = ""+process.getResourceState();
				logger.debug(methodName, jobId, processId, processState+" "+resourceState+" "+nodeIdentity+" "+PID);
			}
		}
		logger.trace(methodName, null, messages.fetch("exit"));
		return;
	}
}
