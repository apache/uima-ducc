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
package org.apache.uima.ducc.common.admin.event;

public class DuccAdminEventStopMetrics extends DuccAdminEvent  {
	private static final long serialVersionUID = 6499822168988392919L;
	// comma separated list of nodes that are target for this message
	private String targetNodes = new String();

	
	public DuccAdminEventStopMetrics(String nodes, String user, byte[] auth) 
	{
		super(user, auth);
		this.targetNodes = nodes;
	}
	/**
	 * Returns comma separated list of target nodes for this message
	 * @return
	 */
	public String getTargetNodes() {
		return this.targetNodes;
	}

}
