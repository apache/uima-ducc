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
package org.apache.uima.ducc.ws.sort;

import org.apache.uima.ducc.transport.event.common.IDuccProcess;
import org.apache.uima.ducc.transport.event.common.IDuccProcess.ReasonForStoppingProcess;
import org.apache.uima.ducc.transport.event.common.IProcessState.ProcessState;

public class JobDetailsProcesses implements Comparable<JobDetailsProcesses> {
	
	private IDuccProcess _process = null;

	public JobDetailsProcesses(IDuccProcess process) {
		_process = process;
	}

	public IDuccProcess getProcess() {
		return _process;
	}
	
	private boolean isRunning() {
		boolean retVal = false;
		try {
			ProcessState processState = getProcess().getProcessState();
			switch(processState) {
			case Running:
				retVal = true;
				break;
			default:
				//retVal = false;
				break;
			}
		}
		catch(Exception e) {
		}
		return retVal;
	}
	
	private boolean isInitializing() {
		boolean retVal = false;
		try {
			ProcessState processState = getProcess().getProcessState();
			switch(processState) {
			case Initializing:
				retVal = true;
				break;
			default:
				//retVal = false;
				break;
			}
		}
		catch(Exception e) {
		}
		return retVal;
	}
	
	private boolean isStarting() {
		boolean retVal = false;
		try {
			ProcessState processState = getProcess().getProcessState();
			switch(processState) {
			case Starting:
				retVal = true;
				break;
			default:
				//retVal = false;
				break;
			}
		}
		catch(Exception e) {
		}
		return retVal;
	}
	
	private boolean isStopping() {
		boolean retVal = false;
		try {
			ProcessState processState = getProcess().getProcessState();
			switch(processState) {
			case Stopping:
				retVal = true;
				break;
			default:
				//retVal = false;
				break;
			}
		}
		catch(Exception e) {
		}
		return retVal;
	}
	
	private boolean isFailed() {
		boolean retVal = true;
		try {
			IDuccProcess process = getProcess();
			String agentReason = process.getReasonForStoppingProcess();
			if(agentReason.equalsIgnoreCase(ReasonForStoppingProcess.KilledByDucc.toString())) {
				retVal = false;
			}
			else if(agentReason.equalsIgnoreCase(ReasonForStoppingProcess.Other.toString())) {
				retVal = false;
			}
		}
		catch(Exception e) {
		}
		return retVal;
	}
	
	private long workItemErrors() {
		long retVal = 0;
		try {
			retVal = getProcess().getProcessWorkItems().getCountError();
		}
		catch(Exception e) {
		}
		return retVal;
	}
	
	private long workItemDone() {
		long retVal = 0;
		try {
			retVal = getProcess().getProcessWorkItems().getCountDone();
		}
		catch(Exception e) {
		}
		return retVal;
	}
	
	@Override
	public int compareTo(JobDetailsProcesses jdp) {
		int retVal = 0;
		try {
			JobDetailsProcesses j1 = this;
			JobDetailsProcesses j2 = jdp;
			IDuccProcess p1 = j1.getProcess();
			IDuccProcess p2 = j2.getProcess();
			long f1 = p1.getDuccId().getFriendly();
			long f2 = p2.getDuccId().getFriendly();
			if(f1 != f2) {
				if(!j1.isRunning() && j2.isRunning()) {
					retVal = 1;
				}
				else if(j1.isRunning() && !j2.isRunning()) {
					retVal = -1;
				}
				else if(!j1.isInitializing() && j2.isInitializing()) {
					retVal = 1;
				}
				else if(j1.isInitializing() && !j2.isInitializing()) {
					retVal = -1;
				}
				else if(!j1.isStarting() && j2.isStarting()) {
					retVal = 1;
				}
				else if(j1.isStarting() && !j2.isStarting()) {
					retVal = -1;
				}
				else if(!j1.isStopping() && j2.isStopping()) {
					retVal = 1;
				}
				else if(j1.isStopping() && !j2.isStopping()) {
					retVal = -1;
				}
				else if(!j1.isFailed() && j2.isFailed()) {
					retVal = 1;
				}
				else if(j1.isFailed() && !j2.isFailed()) {
					retVal = -1;
				}
				else if(j1.workItemErrors() < j2.workItemErrors()) {
					retVal = 1;
				}
				else if(j1.workItemErrors() > j2.workItemErrors()) {
					retVal = -1;
				}
				else if(j1.workItemDone() < j2.workItemDone()) {
					retVal = 1;
				}
				else if(j1.workItemDone() > j2.workItemDone()) {
					retVal = -1;
				}
				else if(f1 > f2) {
					retVal = -1;
				}
				else if(f1 < f2) {
					retVal = 1;
				}
			}
		}
		catch(Exception e) {
		}
		return retVal;
	}
	
	@Override
	public boolean equals(Object object) {
		boolean retVal = false;
		try {
			JobDetailsProcesses j1 = this;
			JobDetailsProcesses j2 = (JobDetailsProcesses)object;
			IDuccProcess p1 = j1.getProcess();
			IDuccProcess p2 = j2.getProcess();
			String s1 = p1.getDuccId().toString();
			String s2 = p2.getDuccId().toString();
			retVal = s1.equals(s2);
		}
		catch(Throwable t) {	
		}
		return retVal;
	}

	@Override
	public int hashCode() {
		JobDetailsProcesses j1 = this;
		IDuccProcess p1 = j1.getProcess();
		String s1 = p1.getDuccId().toString();
		return s1.hashCode();
	}
}
