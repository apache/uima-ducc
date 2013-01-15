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
package org.apache.uima.ducc.ws.utils;

import java.io.Serializable;

import org.apache.uima.ducc.transport.event.NodeMetricsUpdateDuccEvent;


@SuppressWarnings("serial")
public class DatedNodeMetricsUpdateDuccEvent implements Serializable {
	
	private NodeMetricsUpdateDuccEvent nodeMetricsUpdateDuccEvent = null;
	private long millis = -1;
	
	public DatedNodeMetricsUpdateDuccEvent(NodeMetricsUpdateDuccEvent nodeMetricsUpdateDuccEvent) {
		this.nodeMetricsUpdateDuccEvent = nodeMetricsUpdateDuccEvent;
		this.millis = System.currentTimeMillis();
	}
	
	public NodeMetricsUpdateDuccEvent getNodeMetricsUpdateDuccEvent() {
		return nodeMetricsUpdateDuccEvent;
	}
	
	public Long getEventSize() {
		Long retVal = new Long(0);
		if(nodeMetricsUpdateDuccEvent != null) {
			retVal = nodeMetricsUpdateDuccEvent.getEventSize();
		}
		return retVal;
	}
	
	public long getMillis() {
		return millis;
	}
	
	public long getElapsed() {
		return System.currentTimeMillis() - getMillis();
	}
	
	public boolean isExpired(long millis) {
		return getElapsed() > millis;
	}
}
