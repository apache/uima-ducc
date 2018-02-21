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
package org.apache.uima.ducc.container.jd.mh.impl;

import org.apache.uima.ducc.container.jd.mh.iface.INodeInfo;

public class NodeInfo implements INodeInfo {

	private static final long serialVersionUID = 1L;
	
	private String nodeName = null;
	private String nodeAddress = null;
	
	public NodeInfo(String nodeName, String nodeAddress) {
		setNodeName(nodeName);
		setNodeAddress(nodeAddress);
	}
	
	@Override
	public String getNodeName() {
		return nodeName;
	}
	
	@Override
	public void setNodeName(String value) {
		nodeName = value;
	}
	
	@Override
	public String getNodeAddress() {
		return nodeAddress;
	}
	
	@Override
	public void setNodeAddress(String value) {
		nodeAddress = value;
	}

}
