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
package org.apache.uima.ducc.ws.types;

/**
 * A class to manage the machine name (ie node identity) of a
 * resource in the DUCC cluster. 
 */
public class NodeId implements Comparable<NodeId> {
	
	private String machine;		// The name of the machine
	
	/**
	 * @param machine - the name of the machine, nominally including domain
	 */
	public NodeId(String machine) {
		this.machine = machine;
	}
	
	/**
	 * @return the name of the machine w/o the domain
	 */
	public String getShortName() {
		String retVal = this.machine;
		if(retVal != null) {
			if(retVal.contains(".")) {
				String expr = "\\.";
				retVal = retVal.split(expr)[0];
			}
		}
		return retVal;
	}
	
	/**
	 * @return the name of the machine including the domain, if present
	 */
	public String getLongName() {
		return this.machine;
	}
	
	// @return the name of the machine including the domain, if present
	
	@Override
	public String toString() {
		return getLongName();
	}
	
	// @return 0 if the long names match
	
	@Override
	public int compareTo(NodeId nodeId) {
		int retVal = 0;
		if(nodeId != null) {
			NodeId that = nodeId;
			String thatNodeId = that.toString();
			String thisNodeId = this.toString();
			retVal = thisNodeId.compareTo(thatNodeId);
		}
		return retVal;
	}
	
	// @return true if the long names match
	
	@Override
	public boolean equals(Object object) {
		boolean retVal = false;
		if(object != null) {
			if(object instanceof NodeId) {
				NodeId that = (NodeId) object;
				String thatNodeId = that.toString();
				String thisNodeId = this.toString();
				retVal = thisNodeId.equals(thatNodeId);
			}
		}
		return retVal;
	}
}
