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
package org.apache.uima.ducc.container.jd.mh;

import org.apache.uima.ducc.container.common.logger.IComponent;
import org.apache.uima.ducc.container.common.logger.ILogger;
import org.apache.uima.ducc.container.common.logger.Logger;
import org.apache.uima.ducc.container.jd.mh.iface.remote.IRemoteWorkerProcess;
import org.apache.uima.ducc.container.jd.mh.iface.remote.IRemoteWorkerThread;
import org.apache.uima.ducc.container.net.iface.IMetaCasRequester;

public class RemoteWorkerProcess implements IRemoteWorkerProcess {

	private static final long serialVersionUID = 1L;

	private static Logger logger = Logger.getLogger(RemoteWorkerProcess.class, IComponent.Id.JD.name());
	
	private String nodeName = null;
	private String nodeAddress = null;
	private String pidName = null;
	private int pid = 0;
	
	public RemoteWorkerProcess(IMetaCasRequester metaCasRequester) {
		setNodeName(metaCasRequester.getRequesterNodeName());
		setNodeAddress(metaCasRequester.getRequesterAddress());
		setPidName(metaCasRequester.getRequesterProcessName());
		setPid(metaCasRequester.getRequesterProcessId());
	}
	
	public RemoteWorkerProcess(String nodeName, String nodeAddress, String pidName, int pid) {
		setNodeName(nodeName);
		setNodeAddress(nodeAddress);
		setPidName(pidName);
		setPid(pid);
	}
	
	public RemoteWorkerProcess(IRemoteWorkerThread rwt) {
		setNodeName(rwt.getNodeName());
		setNodeAddress(rwt.getNodeAddress());
		setPidName(rwt.getPidName());
		setPid(rwt.getPid());
	}
	
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		if(nodeName != null) {
			sb.append(nodeName);
			sb.append(".");
		}
		if(nodeAddress != null) {
			sb.append("[");
			sb.append(nodeAddress);
			sb.append("]");
			sb.append(".");
		}
		sb.append(pid);
		if(pidName != null) {
			sb.append(".");
			sb.append("[");
			sb.append(pidName);
			sb.append("]");
		}
		return sb.toString();
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

	@Override
	public String getPidName() {
		return pidName;
	}

	@Override
	public void setPidName(String value) {
		pidName = value;
	}
	
	@Override
	public int getPid() {
		return pid;
	}
	
	@Override
	public void setPid(int value) {
		pid = value;
	}
	
	private int compareNodeName(RemoteWorkerProcess that) {
		int retVal = 0;
		String thisNodeName = this.getNodeName();
		String thatNodeName = that.getNodeName();
		if(thisNodeName != null) {
			if(thatNodeName != null) {
				retVal = thisNodeName.compareTo(thatNodeName);
			}
		}
		return retVal;
	}
	
	private int comparePid(RemoteWorkerProcess that) {
		int retVal = 0;
		Integer thisPid = new Integer(this.pid);
		Integer thatPid = new Integer(that.pid);
		retVal = thisPid.compareTo(thatPid);
		return retVal;
	}
	
	@Override
	public int compareTo(Object o) {
		String location = "compareTo";
		int retVal = 0;
		try {
			if(o != null) {
				RemoteWorkerProcess that = (RemoteWorkerProcess) o;
				if(retVal == 0) {
					retVal = compareNodeName(that);
				}
				if(retVal == 0) {
					retVal = comparePid(that);
				}
			}
		}
		catch(Exception e) {
			logger.error(location, ILogger.null_id, e);
		}
		return retVal;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		String thisNodeName = this.getNodeName();
		Integer thisPid = new Integer(this.pid);
		result = prime * result + ((thisNodeName == null) ? 0 : thisNodeName.hashCode());
		result = prime * result + ((thisPid == null) ? 0 : thisPid.hashCode());
		return result;
	}
	
	@Override
	public boolean equals(Object obj) {
		String location = "equals";
		boolean retVal = false;
		try {
			if(obj != null) {
				if(this == obj) {
					retVal = true;
				}
				else {
					RemoteWorkerProcess that = (RemoteWorkerProcess) obj;
					if(this.compareTo(that) == 0) {
						retVal = true;
					}
				}
			}
		}
		catch(Exception e) {
			logger.error(location, ILogger.null_id, e);
		}
		return retVal;
	}

}
