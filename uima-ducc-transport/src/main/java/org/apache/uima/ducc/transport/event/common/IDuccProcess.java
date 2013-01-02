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

import java.io.Serializable;
import java.util.List;

import org.apache.uima.ducc.common.NodeIdentity;
import org.apache.uima.ducc.common.node.metrics.ProcessGarbageCollectionStats;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.agent.IUimaPipelineAEComponent;
import org.apache.uima.ducc.transport.event.common.IDuccProcessType.ProcessType;
import org.apache.uima.ducc.transport.event.common.IProcessState.ProcessState;
import org.apache.uima.ducc.transport.event.common.IResourceState.ProcessDeallocationType;
import org.apache.uima.ducc.transport.event.common.IResourceState.ResourceState;


public interface IDuccProcess extends Serializable {
	
	public DuccId getDuccId();
	public void setDuccId(DuccId duccId);
	
	public NodeIdentity getNodeIdentity();
	public void setNodeIdentity(NodeIdentity nodeIdentity);
	
	public String getPID();
	public void setPID(String pid);
	
	public ProcessState getProcessState();
	public void setProcessState(ProcessState processState);
	public void advanceProcessState(ProcessState processState);
	
	public ResourceState getResourceState();
	public void setResourceState(ResourceState resourceState);
	
	public ProcessDeallocationType getProcessDeallocationType();
	public void setProcessDeallocationType(ProcessDeallocationType deallocationType);
	
	public ITimeWindow getTimeWindowInit();
	public void setTimeWindowInit(ITimeWindow timeWindow);
	
	public ITimeWindow getTimeWindowRun();
	public void setTimeWindowRun(ITimeWindow timeWindow);
	
	public boolean isInitialized();
	public void setInitialized();
	public void resetInitialized();
	
	public boolean stateChange(ProcessState state);
	public boolean isActive();
	public boolean isReady();
	public boolean isFailed();
	public boolean isPreempted();
	public boolean isComplete();

	public boolean isDeallocated();
	public boolean isDefunct();
	
	public void setResidentMemory(long residentMemory);
	public long getResidentMemory();
	public long getResidentMemoryMax();
	
	public void setGarbageCollectionStats(ProcessGarbageCollectionStats gcStats);
	public ProcessGarbageCollectionStats getGarbageCollectionStats();
	
	public ProcessType getProcessType();
	public void setProcessType(ProcessType processType);
    
	public IDuccProcessWorkItems getProcessWorkItems();
	public void  setProcessWorkItems(IDuccProcessWorkItems processWorkItems);
	
	public void setProcessJmxUrl(String processJmxUrl);
	public String getProcessJmxUrl();
	
	public List<IUimaPipelineAEComponent> getUimaPipelineComponents(); 
	
	public void setCpuTime(long cpuTime);
	public long getCpuTime();
	
	public enum ReasonForStoppingProcess {
		AgentTimedOutWaitingForORState,
		Croaked,
		Deallocated,
		ExceededShareSize,
		FailedInitialization,
		InitializationTimeout,
		JPHasNoActiveJob, 
		LowSwapSpace,
	}
	
	public String getReasonForStoppingProcess();
	public void setReasonForStoppingProcess(String reason);
}
