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
package org.apache.uima.ducc.agent;

import java.util.HashMap;
import java.util.Set;
import java.util.TreeMap;

import org.apache.uima.ducc.agent.metrics.collectors.NodeUsersCollector;
import org.apache.uima.ducc.common.NodeIdentity;
import org.apache.uima.ducc.common.node.metrics.NodeUsersInfo;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.common.IDuccProcess;


public interface Agent extends ProcessLifecycleController {
	public static final String DUCC_NODE_METRICS_ENDPOINT="ducc.agent.node.metrics.endpoint";
	public static final String COMPONENT_NAME="Agent";
	
	public NodeIdentity getIdentity();
	
	public HashMap<DuccId, IDuccProcess> getInventoryCopy();
	public HashMap<DuccId,IDuccProcess> getInventoryRef();
	public boolean isRogueProcess(String uid, Set<NodeUsersCollector.ProcessInfo> processList, NodeUsersCollector.ProcessInfo cpi ) throws Exception;	
	public void copyAllUserReservations(TreeMap<String,NodeUsersInfo> map);
	public RogueProcessReaper getRogueProcessReaper();
  public boolean isManagedProcess(Set<NodeUsersCollector.ProcessInfo> processList, NodeUsersCollector.ProcessInfo cpi);
}
