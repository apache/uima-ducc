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
import org.apache.uima.ducc.common.utils.IDuccLoggerComponents.Daemon;

public class DaemonDuccEvent extends AbstractDuccEvent {

	private static final long serialVersionUID = 1L;

	private Daemon daemon = null;
	private NodeIdentity nodeIdentity = null;
	private long tod = System.currentTimeMillis();
	
	public static DaemonDuccEvent createBoot(Daemon daemon, NodeIdentity nodeIdentity) {
		DaemonDuccEvent dde = new DaemonDuccEvent(daemon, EventType.BOOT, nodeIdentity);
		return dde;
	}

	public static DaemonDuccEvent createShutdown(Daemon daemon, NodeIdentity nodeIdentity) {
		DaemonDuccEvent dde = new DaemonDuccEvent(daemon, EventType.SHUTDOWN, nodeIdentity);
		return dde;
	}
	
	public static DaemonDuccEvent createSwitchToMaster(Daemon daemon, NodeIdentity nodeIdentity) {
		DaemonDuccEvent dde = new DaemonDuccEvent(daemon, EventType.SWITCH_TO_MASTER, nodeIdentity);
		return dde;
	}
	
	public static DaemonDuccEvent createSwitchToBackup(Daemon daemon, NodeIdentity nodeIdentity) {
		DaemonDuccEvent dde = new DaemonDuccEvent(daemon, EventType.SWITCH_TO_BACKUP, nodeIdentity);
		return dde;
	}
	
	public static DaemonDuccEvent create(Daemon daemon, EventType eventType, NodeIdentity nodeIdentity) {
		DaemonDuccEvent dde = new DaemonDuccEvent(daemon, eventType, nodeIdentity);
		return dde;
	}
	
	public DaemonDuccEvent(Daemon daemon, EventType eventType, NodeIdentity nodeIdentity) {
		super(eventType);
		setDaemon(daemon);
		setNodeIdentity(nodeIdentity);
	}
	
	//
	
	private void setDaemon(Daemon value) {
		daemon = value;
	}
	
	public Daemon getDaemon() {
		return daemon;
	}
	
	//
	
	public EventType getEventType() {
		return super.getEventType();
	}
	
	//
	
	private void setNodeIdentity(NodeIdentity value) {
		nodeIdentity = value;
	}
	
	public NodeIdentity getNodeIdentity() {
		return nodeIdentity;
	}
	
	//
	
	public long getTod() {
		return tod;
	}
}
