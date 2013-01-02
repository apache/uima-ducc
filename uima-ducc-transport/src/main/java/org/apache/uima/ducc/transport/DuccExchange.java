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
package org.apache.uima.ducc.transport;

public class DuccExchange {
	public static final String Event = "event";

	public static final String InvalidNodeEvent = "invalidNodeEvent";
	public static final String InvalidCommandLineEvent = "invalidCommandLineEvent";

	public static final String HeartbeatEvent = "heartbeat";
	public static final String NodeStatsEvent = "nodestats";
	public static final String ClientPingEvent = "clientPing";
	public static final String StartProcessEvent = "startProcess";
	public static final String StopProcessEvent = "stopProcess";
	public static final String StopAgentEvent = "stopAgent";
	public static final String StartGroupEvent = "startProcessGroup";
	public static final String StopGroupEvent = "stopProcessGroup";
	public static final String PurgeProcessEvent = "purgeProcess";
	public static final String ProcessUpdateEvent = "processUpdate";
	public static final String ProcessType = "processType";
	public static final String Command = "command";
	public static final String Selector = "selector";
	public static final String ProcessPID = "processPID";
	public static final String ProcessGroup = "processGroup";
	public static final String ProcessGroupName = "groupName";
	public static final String ProcessGroupUniqueId = "processGroupUniqueId";
	public static final String ProcessGroupOwner = "processGroupOwner";
	public static final String ProcessGroupParent = "processGroupParent";
	public static final String GetAgentListEvent = "getAgentList";
	public static final String AgentListEvent = "agentList";

	public static final String ClientEndpoint = "clientEndpoint";
	public static final String InstancesPerNode = "numberOfInstancesPerNode";
	public static final String ProcessCorrelationId = "processCorrelationId";
	public static final String ProcessState = "processState";

	public static final String StopPriority = "stopPriority";
	public static final String EndDate = "endDate";
	public static final String StartDate = "startDate";

	public static final String ControllerInventoryUpdate = "controllerInventoryUpdate";
	public static final String ProcessGroupUpdate = "processGroupUpdate";
	public static final String NodeUpdate = "nodeUpdate";
	
	public static final String DUCCNODENAME="nodename";
	public static final String DUCCNODEIP="nodeip";
	
	public static final String TARGET_NODES_HEADER_NAME="target-nodes";
	
	public static final String JP_PORT="jp-port";
}
