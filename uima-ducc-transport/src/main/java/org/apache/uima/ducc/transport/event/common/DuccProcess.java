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
import java.util.List;

import org.apache.uima.ducc.common.Node;
import org.apache.uima.ducc.common.NodeIdentity;
import org.apache.uima.ducc.common.node.metrics.ProcessGarbageCollectionStats;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.agent.IUimaPipelineAEComponent;
import org.apache.uima.ducc.transport.event.common.IDuccProcessType.ProcessType;
import org.apache.uima.ducc.transport.event.common.IProcessState.ProcessState;
import org.apache.uima.ducc.transport.event.common.IResourceState.ProcessDeallocationType;
import org.apache.uima.ducc.transport.event.common.IResourceState.ResourceState;


public class DuccProcess implements IDuccProcess {

	/**
	 * please increment this sUID when removing or modifying a field
	 */
	private static final long serialVersionUID = 1L;
	private DuccId duccId = null;
	private Node  node = null;
	private NodeIdentity  nodeIdentity = null;
	private String pid = null;
	private ProcessState processState = ProcessState.Undefined;
	private ResourceState resourceState = ResourceState.Undefined;
	private ProcessDeallocationType deallocationType = ProcessDeallocationType.Undefined;
	private ITimeWindow timeWindowInit = null;
	private ITimeWindow timeWindowRun = null;
	private long residentMemory=0;
	private long residentMemoryMax=0;
	private ProcessType processType;
	private List<IUimaPipelineAEComponent> uimaPipelineComponentList =
		new ArrayList<IUimaPipelineAEComponent>(); // empty list
	private ProcessGarbageCollectionStats gcStats=null;
	private String processJmxUrl;
	private IDuccProcessWorkItems processWorkItems= null;
	private long cpuUsage;
	private String reason;
	private boolean initialized = false;
	private int exitCode;
	private CGroup cgroup;
	private long majorFaults;
	private long swapUsage;
	private long swapUsageMax;
	
	public DuccProcess(DuccId duccId, NodeIdentity nodeIdentity) {
		setDuccId(duccId);
		setNodeIdentity(nodeIdentity);
	}
	
	public DuccProcess(DuccId duccId, Node node) {
		setDuccId(duccId);
		setNode(node);
		NodeIdentity nodeIdentity = node.getNodeIdentity();
		setNodeIdentity(nodeIdentity);
	}
	
	public DuccProcess(DuccId duccId, NodeIdentity nodeIdentity, ProcessType processType) {
		setDuccId(duccId);
		setNodeIdentity(nodeIdentity);
		setProcessType(processType);
	}
	
	public DuccProcess(DuccId duccId, Node node, ProcessType processType) {
		setDuccId(duccId);
		setNode(node);
		NodeIdentity nodeIdentity = node.getNodeIdentity();
		setNodeIdentity(nodeIdentity);
		setProcessType(processType);
	}
	
	public List<IUimaPipelineAEComponent> getUimaPipelineComponents() {
		return uimaPipelineComponentList;
	}

	public void setUimaPipelineComponents(List<IUimaPipelineAEComponent> uimaPipelineComponentList) {
		this.uimaPipelineComponentList = uimaPipelineComponentList;
	}

	@Override
	public DuccId getDuccId() {
		return duccId;
	}

	@Override
	public void setDuccId(DuccId duccId) {
		this.duccId = duccId;
	}

	@Override
	public NodeIdentity getNodeIdentity() {
		return nodeIdentity;
	}

	@Override
	public void setNodeIdentity(NodeIdentity nodeIdentity) {
		this.nodeIdentity = nodeIdentity;
	}

	@Override
	public String getPID() {
		return pid;
	}

	@Override
	public void setPID(String pid) {
		this.pid = pid;
	}

	@Override
	public ProcessState getProcessState() {
		return processState;
	}

	@Override
	public void setProcessState(ProcessState processState) {
		this.processState = processState;
	}
	
/*
	Starting,               // Process Manager sent request to start the Process
	Initializing,			// Process Agent is initializing process
	Running,				// Process Agent is available for processing work items
	Stopped,				// Process Agent reports process stopped
	Failed,					// Process Agent reports process failed
	FailedInitialization,	// Process Agent reports process failed initialization
	InitializationTimeout,  // Process Agent reports process initialization timeout
	Killed,         		// Agent forcefully killed the process
	Undefined	
*/	
	@Override
	public void advanceProcessState(ProcessState nextProcessState) {
		switch(getProcessState()) {
			case Starting:
				switch(nextProcessState) {
				case Undefined:
				case Starting:
					break;
				default:
					setProcessState(nextProcessState);
					break;
				}
				break;
			case Initializing:
				switch(nextProcessState) {
				case Undefined:
				case Starting:
				case Initializing:
					break;
				default:
					setProcessState(nextProcessState);
					break;
				}
				break;
			case Running:
				switch(nextProcessState) {
				case Undefined:
				case Starting:
				case Initializing:
				case Running:
					break;
				default:
					setProcessState(nextProcessState);
					break;
				}
				break;
			case Stopped:
			case Failed:
			case FailedInitialization:
			case InitializationTimeout:
			case Killed:
				break;
			case Undefined:
			default:
				switch(nextProcessState) {
				case Undefined:
					break;
				default:
					setProcessState(nextProcessState);
					break;
				}
				break;
		}
	}
	
	@Override
	public ResourceState getResourceState() {
		return resourceState;
	}

	@Override
	public void setResourceState(ResourceState resourceState) {
		this.resourceState = resourceState;
	}

	@Override
	public ProcessDeallocationType getProcessDeallocationType() {
		return deallocationType;
	}

	@Override
	public void setProcessDeallocationType(ProcessDeallocationType deallocationType) {
		this.deallocationType = deallocationType;
	}
	
	@Override
	public ITimeWindow getTimeWindowInit() {
		return timeWindowInit;
	}

	@Override
	public void setTimeWindowInit(ITimeWindow timeWindow) {
		this.timeWindowInit = timeWindow;
	}

	@Override
	public ITimeWindow getTimeWindowRun() {
		return timeWindowRun;
	}

	@Override
	public void setTimeWindowRun(ITimeWindow timeWindow) {
		this.timeWindowRun = timeWindow;
	}

	@Override
	public boolean stateChange(ProcessState state) {
		boolean retVal = false;
		if(retVal==false) {
			throw new RuntimeException("not implemented");
		}
		return retVal;
	}

	@Override
	public boolean isActive() {
		boolean retVal = false;
		switch(processState) {
		case Initializing:
		case Running:
			retVal = true;	
			break;
		}
		return retVal;
	}

	@Override
	public boolean isReady() {
		boolean retVal = false;
		switch(processState) {
		case Running:
			retVal = true;	
			break;
		}
		return retVal;
	}
	
	@Override
	public boolean isFailed() {
		boolean retVal = false;
		switch(processState) {
		case Failed:
		case Killed:
			retVal = true;	
			break;
		case Stopped:
			String reason = getReasonForStoppingProcess();
			if(reason != null) {
				if(reason.equals("FailedInitialization")) {
					retVal = true;
				}
				else if(reason.equals("InitializationTimeout")) {
					retVal = true;
				}
			}
			break;
		//TODO: Deprecated
		case FailedInitialization:
		//TODO: Deprecated
		case InitializationTimeout:
			retVal = true;	
			break;
		}
		return retVal;
	}
	
	@Override
	public boolean isPreempted() {
		boolean retVal = false;
		switch(deallocationType) {
		case Forced:
			retVal = true;	
			break;
		}
		return retVal;
	}
	
	@Override
	public boolean isComplete() {
		boolean retVal = false;
		switch(processState) {
		case Stopped:
		case Failed:
		case FailedInitialization:
		case InitializationTimeout:
		case Killed:
		case Abandoned:
			retVal = true;	
			break;
		}
		return retVal;
	}
	
	@Override
	public boolean isDeallocated() {
		boolean retVal = false;
		switch(resourceState) {
		case Deallocated:
			retVal = true;
			break;
		}
		return retVal;
	}
	
	@Override
	public boolean isDefunct() {
		boolean retVal = false;
		switch(resourceState) {
		case Deallocated:
			retVal = isComplete();
			break;
		}
		return retVal;
	}
	
	// **********
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((getDuccId() == null) ? 0 : getDuccId().hashCode());
		result = prime * result + ((getNodeIdentity() == null) ? 0 : getNodeIdentity().hashCode());
		result = prime * result + ((getPID() == null) ? 0 : getPID().hashCode());
		result = prime * result + ((getProcessState() == null) ? 0 : getProcessState().hashCode());
		result = prime * result + ((getResourceState() == null) ? 0 : getResourceState().hashCode());
		result = prime * result + ((getTimeWindowInit() == null) ? 0 : getTimeWindowInit().hashCode());
		result = prime * result + ((getTimeWindowRun() == null) ? 0 : getTimeWindowRun().hashCode());
		return result;
	}
	
	public boolean equals(Object obj) {
		boolean retVal = false;
		if(this == obj) {
			retVal = true;
		}
		else if(getClass() == obj.getClass()) {
			DuccProcess that = (DuccProcess) obj;
			if( 	Util.compare(this.getDuccId(),that.getDuccId()) 
				&&	Util.compare(this.getNodeIdentity(),that.getNodeIdentity()) 
				&&	Util.compare(this.getPID(),that.getPID()) 
				&&	Util.compare(this.getProcessState(),that.getProcessState()) 
				&&	Util.compare(this.getResourceState(),that.getResourceState()) 
				//	These changes ignored:
				&&	Util.compare(this.getTimeWindowInit(),that.getTimeWindowInit()) 
				&&	Util.compare(this.getTimeWindowRun(),that.getTimeWindowRun())
//				&& super.equals(obj)
				) 
			{
				retVal = true;
			}
		}
		return retVal;
	}

	public void setResidentMemory(long residentMemory) {
		if(residentMemory > this.residentMemoryMax) {
			this.residentMemoryMax = residentMemory;
		}
		this.residentMemory = residentMemory;
	}

	public long getResidentMemory() {
		return residentMemory;
	}

	public long getResidentMemoryMax() {
		long retVal = 0;
		try {
			retVal = residentMemoryMax;
		}
		catch(Exception e) {
		}
		return retVal;
	}
	
	/**
	 * @return the processType
	 */
	public ProcessType getProcessType() {
		return processType;
	}

	/**
	 * @param processType the processType to set
	 */
	public void setProcessType(ProcessType processType) {
		this.processType = processType;
	}

	/**
	 * @param ProcessGarbageCollectionStats - this process GC stats
	 */
	public void setGarbageCollectionStats(ProcessGarbageCollectionStats gcStats) {
		this.gcStats = gcStats;
	}
	/**
	 * @param ProcessGarbageCollectionStats - this process GC stats
	 */
	public ProcessGarbageCollectionStats getGarbageCollectionStats() {
		return this.gcStats;
	}
	/**
	 * @param processJmxUrl - this process jmx connect URL. 
	 */
	public void setProcessJmxUrl(String processJmxUrl) {
		this.processJmxUrl = processJmxUrl;
	}

	/**
	 * @return the processJmxUrl, connect URL for connecting jconsole
	 * to this process.
	 */
	public String getProcessJmxUrl() {
		return processJmxUrl;
	}

	@Override
	public IDuccProcessWorkItems getProcessWorkItems() {
		return processWorkItems;
	}

	@Override
	public void setProcessWorkItems(IDuccProcessWorkItems processWorkItems) {
		this.processWorkItems = processWorkItems;
	}

	public void setCpuTime(long cpuTime) {
		this.cpuUsage = cpuTime;
	}

	public long getCpuTime() {
		return cpuUsage;
	}

  public String getReasonForStoppingProcess() {
    return reason;
  }

  public void setReasonForStoppingProcess(String reason) {
    this.reason = reason;
  }
  	
	@Override
	public boolean isInitialized() {
		return initialized;
	}

	@Override
	public void setInitialized() {
		initialized = true;
	}

	@Override
	public void resetInitialized() {
		initialized = false;
	}

	public void setProcessExitCode(int pExitCode) {
		this.exitCode = pExitCode;
	}
	
	public int getProcessExitCode() {
		return this.exitCode;
	}
	public void setCGroup( CGroup cgroup) {
		this.cgroup = cgroup;
	}
	public CGroup getCGroup() {
		return cgroup;
	}
	@Override
	public Node getNode() {
		return node;
	}

	@Override
	public void setNode(Node node) {
		this.node = node;
	}

	@Override
	public void setMajorFaults(long faultCount) {
		this.majorFaults = faultCount;	
	}

	@Override
	public long getMajorFaults() {
		return majorFaults;
	}

	@Override
	public void setSwapUsage(long susage) {
		this.swapUsage = susage;
	}

	@Override
	public long getSwapUsage() {
		return swapUsage;
	}

	@Override
	public void setSwapUsageMax(long susage) {
		this.swapUsageMax = susage;
	}

	@Override
	public long getSwapUsageMax() {
		return swapUsageMax;
	}
}
