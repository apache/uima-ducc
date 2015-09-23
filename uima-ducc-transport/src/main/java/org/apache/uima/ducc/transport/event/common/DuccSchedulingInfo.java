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

import java.util.concurrent.ConcurrentHashMap;

import org.apache.uima.ducc.common.jd.files.perf.PerformanceMetricsSummaryMap;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.common.IDuccUnits.MemoryUnits;

/**
 * Data utilized by the work scheduler.
 */
public class DuccSchedulingInfo implements IDuccSchedulingInfo {
	
	/**
	 * please increment this sUID when removing or modifying a field 
	 */
	private static final long serialVersionUID = 1L;
	private String schedulingClass = defaultSchedulingClass;
	private String schedulingPriority = defaultSchedulingPriority;
	@Deprecated
	private String memorySize = defaultMemorySize;
	private String memorySizeRequested = defaultMemorySize;
	private MemoryUnits memoryUnits = defaultMemoryUnits;
	private long memorySizeAllocatedInBytes = 0;
	private String instancesCount = defaultInstancesCount;
	
	@Deprecated
	private String machinesCount = defaultMachinesCount;
	
	private String processesMax = defaultProcessesMax;
	private String processesMin = defaultProcessesMin;
	private String threadsPerProcess = defaultThreadsPerProcess;
	
	private String workItemsTotal = defaultWorkItemsTotal;
	private String workItemsCompleted = defaultWorkItemsCompleted;
	private String workItemsDispatched = defaultWorkItemsDispatched;
	private String workItemsError = defaultWorkItemsError;
	private String workItemsRetry = defaultWorkItemsRetry;
	private String workItemsLost = defaultWorkItemsLost;
	private String workItemsPreempt= defaultWorkItemsPreempt;
	
	private ConcurrentHashMap<Integer,DuccId> limboMap = new  ConcurrentHashMap<Integer,DuccId>();
	private ConcurrentHashMap<String,DuccId> casQueuedMap = new  ConcurrentHashMap<String,DuccId>();
	
	private IDuccPerWorkItemStatistics perWorkItemStatistics = null;
	private PerformanceMetricsSummaryMap performanceMetricsSummaryMap = null;
	
	private long mostRecentWorkItemStart = 0;
	
	@Deprecated
	private String workItemsPending = defaultWorkItemsPending;
	
	
	public String getSchedulingClass() {
		return schedulingClass;
	}

	
	public void setSchedulingClass(String schedulingClass) {
		if(schedulingClass != null) {
			this.schedulingClass = schedulingClass;
		}
	}

	
	public String getSchedulingPriority() {
		return schedulingPriority;
	}

	
	public void setSchedulingPriority(String schedulingPriority) {
		if(schedulingPriority != null) {
			this.schedulingPriority = schedulingPriority;
		}
	}


	
	public String getInstancesCount() {
		return instancesCount;
	}

	
	public void setInstancesCount(String instancesCount) {
		if(instancesCount != null) {
			this.instancesCount = instancesCount;
		}
	}
	
	
	@Deprecated
	public String getMachinesCount() {
		return machinesCount;
	}

	
	@Deprecated
	public void setMachinesCount(String machinesCount) {
		if(machinesCount != null) {
			this.machinesCount = machinesCount;
		}
	}

/*	
	@Deprecated
	public long getMemorySizeInBytes() {
		long retVal = 0;
		try {
			long value = Long.parseLong(getMemorySize());
			switch(memoryUnits) {
			case GB:
				retVal = value * GB;
				break;
			case MB:
				retVal = value * MB;
				break;	
			case KB:
				retVal = value * KB;
				break;
			}
		}
		catch(Exception e) {
		}
		return retVal;
	}
*/
	
	@Deprecated
	public String getMemorySize() {
		String retVal = defaultMemorySize;
		if(memorySize != null) {
			retVal = memorySize;
		}
		return retVal;
	}

/*
	@Deprecated
	public void setMemorySize(String size) {
		if(size != null) {
			this.memorySize = size;
		}
	}
*/
	
	
	public String getMemorySizeRequested() {
		String retVal = memorySizeRequested;
		if(retVal == null) {
			retVal = getMemorySize();
		}
		return retVal;
	}

	
	public void setMemorySizeRequested(String size) {
		if(size != null) {
			this.memorySizeRequested = size;
		}
	}
	
	
	public long getMemorySizeAllocatedInBytes() {
		return memorySizeAllocatedInBytes;
	}

	
	public void setMemorySizeAllocatedInBytes(long value) {
		this.memorySizeAllocatedInBytes = value;
	}
	
	
	public MemoryUnits getMemoryUnits() {
		MemoryUnits retVal = MemoryUnits.GB;
		if(memoryUnits != null) {
			retVal = memoryUnits;
		}
		return retVal;
	}

	
	public void setMemoryUnits(MemoryUnits units) {
		if(units != null) {
			this.memoryUnits = units;
		}
	}
	

	public String getThreadsPerProcess() {
		return threadsPerProcess;
	}
	
	
	public long getLongProcessesMax() {
		long retVal = 0;
		try {
			retVal = Long.parseLong(processesMax);
		}
		catch(Exception e) {
			
		}
		return retVal;
	}
	
	
	public void setLongProcessesMax(long number) {
		this.processesMax = ""+number;
	}
	
	
	public String getProcessesMax() {
		return processesMax;
	}

	
	public void setProcessesMax(String number) {
		if(number != null) {
			this.processesMax = number.trim();
		}
	}

	
	public String getProcessesMin() {
		return this.processesMin;
	}

	
	public void setProcessesMin(String number) {
		if(number != null) {
			this.processesMin = number;
		}
	}
	
	public int getIntThreadsPerProcess() {
		return Integer.parseInt(threadsPerProcess);
	}

	
	public void setThreadsPerProcess(String number) {
		if(number != null) {
			this.threadsPerProcess = number;
		}
	}
	
	
	public String getWorkItemsTotal() {
		return workItemsTotal;
	}

	
	public void setWorkItemsTotal(String number) {
		if(number != null) {
			this.workItemsTotal = number;
		}
	}
	
	
	public int getIntWorkItemsTotal() {
		return Integer.parseInt(workItemsTotal);
	}
	
	
	public String getWorkItemsCompleted() {
		return workItemsCompleted;
	}

	
	public void setWorkItemsCompleted(String number) {
		if(number != null) {
			this.workItemsCompleted = number;
		}
	}
	
	
	public int getIntWorkItemsCompleted() {
		return Integer.parseInt(workItemsCompleted);
	}
	
	
	public String getWorkItemsDispatched() {
		return workItemsDispatched;
	}

	
	public void setWorkItemsDispatched(String number) {
		if(number != null) {
			this.workItemsDispatched = number;
		}
	}

	
	public ConcurrentHashMap<Integer,DuccId> getLimboMap() {
		if(limboMap == null) {
			return new ConcurrentHashMap<Integer,DuccId>();
		}
		else {
			return limboMap;
		}
		
	}

	
	public void setLimboMap(ConcurrentHashMap<Integer,DuccId> map) {
		if(map != null) {
			this.limboMap = map;
		}
	}

	
	public ConcurrentHashMap<String,DuccId> getCasQueuedMap() {
		if(casQueuedMap == null) {
			return new ConcurrentHashMap<String,DuccId>();
		}
		else {
			return casQueuedMap;
		}
		
	}

	
	public void setCasQueuedMap(ConcurrentHashMap<String,DuccId> map) {
		if(map != null) {
			this.casQueuedMap = map;
		}
	}
	
	
	public String getWorkItemsError() {
		return workItemsError;
	}

	
	public void setWorkItemsError(String number) {
		if(number != null) {
			this.workItemsError = number;
		}
	}
	
	
	public int getIntWorkItemsError() {
		return Integer.parseInt(workItemsError);
	}
	
	
	public String getWorkItemsRetry() {
		return workItemsRetry;
	}

	
	public void setWorkItemsRetry(String number) {
		if(number != null) {
			this.workItemsRetry = number;
		}
	}
	
	
	public String getWorkItemsLost() {
		return workItemsLost;
	}

	
	public void setWorkItemsLost(String number) {
		if(number != null) {
			this.workItemsLost = number;
		}
	}
	
	
	public int getIntWorkItemsLost() {
		return Integer.parseInt(workItemsLost);
	}
	
	
	public String getWorkItemsPreempt() {
		if(workItemsPreempt == null) {
			workItemsPreempt = "0";
		}
		return workItemsPreempt;
	}

	
	public void setWorkItemsPreempt(String number) {
		if(number != null) {
			this.workItemsPreempt = number;
		}
	}
	
	
	public IDuccPerWorkItemStatistics getPerWorkItemStatistics() {
		return perWorkItemStatistics;
	}
	
	
	public void setPerWorkItemStatistics(IDuccPerWorkItemStatistics value) {
		perWorkItemStatistics = value;
	}
	
	
	public PerformanceMetricsSummaryMap getPerformanceMetricsSummaryMap() {
		return performanceMetricsSummaryMap;
	}
	
	
	public void setMostRecentWorkItemStart(long time) {
		mostRecentWorkItemStart = time;
	}
	
	
	public long getMostRecentWorkItemStart() {
		return mostRecentWorkItemStart;
	}
	
	
	@Deprecated
	
	public String getWorkItemsPending() {
		return workItemsPending;
	}
	
	@Deprecated
	
	public void setWorkItemsPending(String number) {
		if(number != null) {
			this.workItemsPending = number;
		}
	}

	
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((instancesCount == null) ? 0 : instancesCount.hashCode());
		result = prime * result
				+ ((machinesCount == null) ? 0 : machinesCount.hashCode());
		result = prime * result
				+ ((schedulingClass == null) ? 0 : schedulingClass.hashCode());
		result = prime
				* result
				+ ((schedulingPriority == null) ? 0 : schedulingPriority
						.hashCode());
		result = prime * result
				+ ((memorySize == null) ? 0 : memorySize.hashCode());
		result = prime
				* result
				+ ((memoryUnits == null) ? 0 : memoryUnits.hashCode());
		result = prime * result
				+ ((threadsPerProcess == null) ? 0 : threadsPerProcess.hashCode());
		result = prime
				* result
				+ ((workItemsCompleted == null) ? 0 : workItemsCompleted
						.hashCode());
		result = prime
				* result
				+ ((workItemsDispatched == null) ? 0 : workItemsDispatched
						.hashCode());
		result = prime * result
				+ ((workItemsError == null) ? 0 : workItemsError.hashCode());
		result = prime
				* result
				+ ((workItemsPending == null) ? 0 : workItemsPending.hashCode());
		result = prime * result
				+ ((workItemsRetry == null) ? 0 : workItemsRetry.hashCode());
		result = prime * result
				+ ((workItemsTotal == null) ? 0 : workItemsTotal.hashCode());
		return result;
	}

	
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DuccSchedulingInfo other = (DuccSchedulingInfo) obj;
		if (instancesCount == null) {
			if (other.instancesCount != null)
				return false;
		} else if (!instancesCount.equals(other.instancesCount))
			return false;
		if (machinesCount == null) {
			if (other.machinesCount != null)
				return false;
		} else if (!machinesCount.equals(other.machinesCount))
			return false;
		if (schedulingClass == null) {
			if (other.schedulingClass != null)
				return false;
		} else if (!schedulingClass.equals(other.schedulingClass))
			return false;
		if (schedulingPriority == null) {
			if (other.schedulingPriority != null)
				return false;
		} else if (!schedulingPriority.equals(other.schedulingPriority))
			return false;
		if (memorySize == null) {
			if (other.memorySize != null)
				return false;
		} else if (!memorySize.equals(other.memorySize))
			return false;
		if (memoryUnits != other.memoryUnits)
			return false;
		if (threadsPerProcess == null) {
			if (other.threadsPerProcess != null)
				return false;
		} else if (!threadsPerProcess.equals(other.threadsPerProcess))
			return false;
		if (workItemsCompleted == null) {
			if (other.workItemsCompleted != null)
				return false;
		} else if (!workItemsCompleted.equals(other.workItemsCompleted))
			return false;
		if (workItemsDispatched == null) {
			if (other.workItemsDispatched != null)
				return false;
		} else if (!workItemsDispatched.equals(other.workItemsDispatched))
			return false;
		if (workItemsError == null) {
			if (other.workItemsError != null)
				return false;
		} else if (!workItemsError.equals(other.workItemsError))
			return false;
		if (workItemsPending == null) {
			if (other.workItemsPending != null)
				return false;
		} else if (!workItemsPending.equals(other.workItemsPending))
			return false;
		if (workItemsRetry == null) {
			if (other.workItemsRetry != null)
				return false;
		} else if (!workItemsRetry.equals(other.workItemsRetry))
			return false;
		if (workItemsTotal == null) {
			if (other.workItemsTotal != null)
				return false;
		} else if (!workItemsTotal.equals(other.workItemsTotal))
			return false;
		if (mostRecentWorkItemStart != other.mostRecentWorkItemStart)
			return false;
		return true;
	}

}
