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

public abstract class TargetableDuccAdminEvent extends DuccAdminEvent {
	private static final long serialVersionUID = 3941231616804023047L;
	// comma separated list of nodes that are target for this message
	private String targetList;


	public TargetableDuccAdminEvent(String targetList, String user, byte[] auth)  {
		super(user, auth);
		this.targetList = targetList;

	}
	/**
	 * Returns comma separated list of target nodes for this message
	 * @return
	 */
	public String getTargets() {
		return this.targetList;
	}
	
	public String[] getTargetList() {
		String[] retVal = null;
		if(targetList != null) {
			retVal = targetList.split(",");
		}
		return retVal;
	}

}
