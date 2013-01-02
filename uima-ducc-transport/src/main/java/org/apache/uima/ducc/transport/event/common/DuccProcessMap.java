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
package org.apache.uima.ducc.transport.event.common;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang.SerializationUtils;
import org.apache.uima.ducc.common.NodeIdentity;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.common.IDuccProcess.ReasonForStoppingProcess;
import org.apache.uima.ducc.transport.event.common.IProcessState.ProcessState;


public class DuccProcessMap extends TreeMap<DuccId,IDuccProcess> implements IDuccProcessMap {
	
	/**
	 * please increment this sUID when removing or modifying a field 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public void addProcess(IDuccProcess process) {
		synchronized(this) {
			put(process.getDuccId(),process);
		}
	}
	
	@Override
	public IDuccProcess getProcess(DuccId duccId) {
		synchronized(this) {
			return get(duccId);
		}
	}
	
	@Override
	public void removeProcess(DuccId duccId) {
		synchronized(this) {
			remove(duccId);
		}
	}
	
	@Override
	public IDuccProcess findProcess(String nodeIP, String processId) {
		IDuccProcess retVal = null;
		synchronized(this) {
			Iterator<IDuccProcess> iterator = values().iterator();
			while(iterator.hasNext()) {
				IDuccProcess duccProcess = iterator.next();
				NodeIdentity nodeIdentity = duccProcess.getNodeIdentity();
				if(nodeIdentity != null) {		
					String IP = nodeIdentity.getIp();
					if(IP != null) {
						String PID = duccProcess.getPID();
						if(PID != null ) {
							if(IP.equals(nodeIP)) {
								if(PID.equals(processId)) {
									retVal = duccProcess;
									break;
								}
				
							}
						}
					}
				}
			}
		}
		return retVal;
	}
	
	public IDuccProcess findProcess(DuccLogger duccLogger, String nodeIP, String processId) {
		String methodName = "findProcess";
		duccLogger.debug(methodName, null, "enter");
		IDuccProcess retVal = null;
		duccLogger.debug(methodName, null, "input nodeIP:"+nodeIP+" "+"processId:"+processId);
		synchronized(this) {
			Iterator<IDuccProcess> iterator = values().iterator();
			duccLogger.debug(methodName, null, "iterator:"+iterator);
			while(iterator.hasNext()) {
				IDuccProcess duccProcess = iterator.next();
				duccLogger.debug(methodName, null, "duccProcess:"+duccProcess);
				NodeIdentity nodeIdentity = duccProcess.getNodeIdentity();
				duccLogger.debug(methodName, null, "nodeIdentity:"+nodeIdentity);
				String IP = nodeIdentity.getIp();
				duccLogger.debug(methodName, null, "IP:"+IP);
				String PID = duccProcess.getPID();
				duccLogger.debug(methodName, null, "PID:"+PID);
				if(Util.compareNotNull(IP, nodeIP)) {
					if(Util.compareNotNull(PID,processId)) {
						retVal = duccProcess;
						duccLogger.debug(methodName, null, "retVal:"+retVal);
						break;
					}
					else {
						duccLogger.debug(methodName, null, ">"+PID+"<");
						duccLogger.debug(methodName, null, ">"+processId+"<");
					}
				}
				else {
					duccLogger.debug(methodName, null, ">"+IP+"<");
					duccLogger.debug(methodName, null, ">"+nodeIP+"<");
				}
			}
		}
		duccLogger.debug(methodName, null, "exit");
		return retVal;
	}
	
	/*
	@Override
	public int getFailedProcessCount() {
		int retVal = 0;
		synchronized(this) {
			Iterator<IDuccProcess> iterator = this.values().iterator();
			while(iterator.hasNext()) {
				IDuccProcess process = iterator.next();
				if(process.isFailed()) {
					retVal++;
				}
			}
		}
		return retVal;
	}
	*/
	
	@Override
	public int getFailedUnexpectedProcessCount() {
		int retVal = 0;
		synchronized(this) {
			Iterator<IDuccProcess> iterator = this.values().iterator();
			while(iterator.hasNext()) {
				IDuccProcess process = iterator.next();
				if(process.isFailed()) {
					switch(process.getProcessDeallocationType()) {
					case Forced:
						continue;
					}
					retVal++;
				}
			}
		}
		return retVal;
	}
	
	@Override
	public int getReadyProcessCount() {
		int retVal = 0;
		synchronized(this) {
			Iterator<IDuccProcess> iterator = this.values().iterator();
			while(iterator.hasNext()) {
				IDuccProcess process = iterator.next();
				if(process.isReady()) {
					retVal++;
				}
			}
			
		}
		return retVal;
	}
	
	@Override
	public int getUsableProcessCount() {
		int retVal = 0;
		synchronized(this) {
			Iterator<IDuccProcess> iterator = this.values().iterator();
			while(iterator.hasNext()) {
				IDuccProcess process = iterator.next();
				if(process.isDeallocated()) {
					continue;
				}
				if(process.isReady()) {
					retVal++;
				}
			}
		}
		return retVal;
	}

	@Override
	public int getAliveProcessCount() {
		int retVal = 0;
		synchronized(this) {
			Iterator<IDuccProcess> iterator = this.values().iterator();
			while(iterator.hasNext()) {
				IDuccProcess process = iterator.next();
				if(!process.isDefunct()) {
					retVal++;
				}
			}
			
		}
		return retVal;
	}

	@Override
	public Map<DuccId, IDuccProcess> getMap() {
		return this;
	}
	
	@Override
	public IDuccProcessMap deepCopy() {
		synchronized (this) {
			return (IDuccProcessMap) SerializationUtils.clone(this);
		}
	}


	@Override
	public ArrayList<DuccId> getFailedInitialization() {
		ArrayList<DuccId> list = new ArrayList<DuccId>();
		String failedInitialization = ReasonForStoppingProcess.FailedInitialization.toString().trim();
		String initializationTimeout = ReasonForStoppingProcess.InitializationTimeout.toString().trim();
		synchronized(this) {
			Iterator<IDuccProcess> iterator = this.values().iterator();
			while(iterator.hasNext()) {
				IDuccProcess process = iterator.next();
				ProcessState processState = process.getProcessState();
				String reason = process.getReasonForStoppingProcess();
				switch(processState) {
				case Failed:
				case Killed:
				case Stopped:
					if(reason != null) {
						reason = reason.trim();
						if(reason.equals(failedInitialization)) {
							list.add(process.getDuccId());
						}
						else if(reason.equals(initializationTimeout)) {
							list.add(process.getDuccId());
						}
					}
					break;
				}
			}
		}
		return list;
	}

	@Override
	public ArrayList<DuccId> getFailedNotInitialization() {
		ArrayList<DuccId> list = new ArrayList<DuccId>();
		String failedInitialization = ReasonForStoppingProcess.FailedInitialization.toString().trim();
		String initializationTimeout = ReasonForStoppingProcess.InitializationTimeout.toString().trim();
		synchronized(this) {
			Iterator<IDuccProcess> iterator = this.values().iterator();
			while(iterator.hasNext()) {
				IDuccProcess process = iterator.next();
				ProcessState processState = process.getProcessState();
				String reason = process.getReasonForStoppingProcess();
				switch(processState) {
				case Failed:
				case Stopped:
				case Killed:
					if(reason != null) {
						reason = reason.trim();
						if(reason.equals("")) {
							continue;
						}
						if(reason.equals(failedInitialization)) {
							continue;
						}
						else if(reason.equals(initializationTimeout)) {
							continue;
						}
						list.add(process.getDuccId());
					}
					break;
				}
			}
		}
		return list;
	}

	@Override
	public int getFailedInitializationCount() {
		ArrayList<DuccId> list = getFailedInitialization();
		return list.size();
	}

	@Override
	public int getFailedNotInitializationCount() {
		ArrayList<DuccId> list = getFailedNotInitialization();
		return list.size();
	}

}
