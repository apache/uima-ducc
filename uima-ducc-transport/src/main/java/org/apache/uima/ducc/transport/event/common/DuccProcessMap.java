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
import org.apache.uima.ducc.transport.Constants;
import org.apache.uima.ducc.transport.event.common.IDuccProcess.ReasonForStoppingProcess;
import org.apache.uima.ducc.transport.event.common.IProcessState.ProcessState;

public class DuccProcessMap extends TreeMap<DuccId,IDuccProcess> implements IDuccProcessMap {
	
	/**
	 * please increment this sUID when removing or modifying a field 
	 */
	private static final long serialVersionUID = 1L;

	
	public void addProcess(IDuccProcess process) {
		synchronized(this) {
			put(process.getDuccId(),process);
		}
	}
	
	
	public IDuccProcess getProcess(DuccId duccId) {
		synchronized(this) {
			return get(duccId);
		}
	}
	
	
	public void removeProcess(DuccId duccId) {
		synchronized(this) {
			remove(duccId);
		}
	}
	
	
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

	public int getNoPidProcessCount() {
		int retVal = 0;
		synchronized(this) {
			Iterator<IDuccProcess> iterator = this.values().iterator();
			while(iterator.hasNext()) {
				IDuccProcess process = iterator.next();
				if(process.getPID() == null) {
					retVal++;
				}
			}
		}
		return retVal;
	}
	
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

	
	public Map<DuccId, IDuccProcess> getMap() {
		return this;
	}
	
	
	public IDuccProcessMap deepCopy() {
		synchronized (this) {
			return (IDuccProcessMap) SerializationUtils.clone(this);
		}
	}

	public static boolean isUserFailureReasonForStoppingProcess(String reason) {
		boolean retVal = false;
		if(reason != null) {
			if(reason.equals(ReasonForStoppingProcess.Croaked.name())) {
				retVal = true;
			}
			else if(reason.equals(ReasonForStoppingProcess.ExceededShareSize.name())) {
				retVal = true;
			}
			else if(reason.equals(ReasonForStoppingProcess.ExceededSwapThreshold.name())) {
				retVal = true;
			}
		}
		return retVal;
	}
	
	private boolean isFailedProcess(IDuccProcess process) {
		boolean retVal = false;
		ProcessState processState = process.getProcessState();
		String reason = process.getReasonForStoppingProcess();
		switch(processState) {
		case Failed:
		case Stopped:
		case Killed:
			retVal = isUserFailureReasonForStoppingProcess(reason);
		}
		return retVal;
	}
	
	// <UIMA-3489>
	private boolean isFailedInitialization(IDuccProcess process) {
		boolean retVal = false;
		try {
			String reason = process.getReasonForStoppingProcess();
			if(ProcessState.FailedInitialization.name().equals(reason)) {
				retVal = true;
			}
		}
		catch(Exception e) {
		}
		return retVal;
	}
	// </UIMA-3489>
	
	public ArrayList<DuccId> getFailedInitialization() {
		ArrayList<DuccId> list = new ArrayList<DuccId>();
		synchronized(this) {
			Iterator<IDuccProcess> iterator = this.values().iterator();
			while(iterator.hasNext()) {
				IDuccProcess process = iterator.next();
				if(process.isInitialized()) {
					// Nevermind
				}
				else {
					if(isFailedInitialization(process)) {
						list.add(process.getDuccId());
					}
					else if(isFailedProcess(process)) {
						list.add(process.getDuccId());
					}
				}
			}
		}
		return list;
	}

	public ArrayList<DuccId> getFailedNotInitialization() {
		ArrayList<DuccId> list = new ArrayList<DuccId>();
		synchronized(this) {
			Iterator<IDuccProcess> iterator = this.values().iterator();
			while(iterator.hasNext()) {
				IDuccProcess process = iterator.next();
				if(process.isInitialized()) {
					if(isFailedProcess(process)) {
						list.add(process.getDuccId());
					}
				}
				else {
					// Nevermind
				}
			}
		}
		return list;
	}

	public int getFailedInitializationCount() {
		ArrayList<DuccId> list = getFailedInitialization();
		return list.size();
	}

	
	public int getFailedNotInitializationCount() {
		ArrayList<DuccId> list = getFailedNotInitialization();
		return list.size();
	}
	
	public ArrayList<DuccId> getAbnormalDeallocations() {
		ArrayList<DuccId> list = new ArrayList<DuccId>();
		synchronized(this) {
			Iterator<IDuccProcess> iterator = this.values().iterator();
			while(iterator.hasNext()) {
				IDuccProcess process = iterator.next();
				if(!process.isDeallocationNormal()) {
					list.add(process.getDuccId());
				}
				else {
					// Nevermind
				}
			}
		}
		return list;
	}
	
	public int getAbnormalDeallocationCount() {
		ArrayList<DuccId> list = getAbnormalDeallocations();
		return list.size();
	}
	
	public long getPgInCount() {
		long retVal = 0;
		synchronized(this) {
			Iterator<IDuccProcess> iterator = this.values().iterator();
			while(iterator.hasNext()) {
				IDuccProcess process = iterator.next();
				retVal += process.getMajorFaults();
			}
		}
		return retVal;
	}
	
	public double getSwapUsageGb() {
		double retVal = 0;
		synchronized(this) {
			Iterator<IDuccProcess> iterator = this.values().iterator();
			while(iterator.hasNext()) {
				IDuccProcess process = iterator.next();
				double swap = process.getSwapUsage();
				retVal += swap/Constants.GB;
			}
		}
		return retVal;
	}
	
	public double getSwapUsageGbMax() {
		double retVal = 0;
		synchronized(this) {
			Iterator<IDuccProcess> iterator = this.values().iterator();
			while(iterator.hasNext()) {
				IDuccProcess process = iterator.next();
				double swap = process.getSwapUsageMax();
				retVal += swap/Constants.GB;
			}
		}
		return retVal;
	}
	
}
