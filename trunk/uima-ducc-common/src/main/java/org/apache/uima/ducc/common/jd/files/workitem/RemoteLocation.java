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
package org.apache.uima.ducc.common.jd.files.workitem;

public class RemoteLocation implements IRemoteLocation {
	
	private static final long serialVersionUID = 1L;
	
	private String nodeIP = null;
	private String pid = null;
	
	public RemoteLocation(String nodeIP, String pid) {
		this.nodeIP = nodeIP;
		this.pid = pid;
	}
	
	public String getNodeIP() {
		return nodeIP;
	}
	
	public String getPid() {
		return pid;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((nodeIP == null) ? 0 : nodeIP.hashCode()) + ((pid == null) ? 0 : pid.hashCode());
		return result;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		RemoteLocation that = (RemoteLocation)obj;
		if(this.nodeIP == null) {
			return false;
		}
		if(that.nodeIP == null) {
			return false;
		}
		if(this.pid == null) {
			return false;
		}
		if(that.pid == null) {
			return false;
		}
		if(!this.nodeIP.equals(that.nodeIP)) {
			return false;
		}
		if(!this.pid.equals(that.pid)) {
			return false;
		}
		return true;
	}

}
