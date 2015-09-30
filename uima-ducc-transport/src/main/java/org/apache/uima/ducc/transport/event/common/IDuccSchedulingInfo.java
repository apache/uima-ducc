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
import java.util.concurrent.ConcurrentHashMap;

import org.apache.uima.ducc.common.jd.files.perf.PerformanceMetricsSummaryMap;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.common.IDuccUnits.MemoryUnits;


public interface IDuccSchedulingInfo extends Serializable {
	
	public static final String defaultSchedulingClass = "normal";
	public static final String defaultSchedulingPriority = "0";
	public static final String defaultMemorySize = "1";
	public static final MemoryUnits defaultMemoryUnits = MemoryUnits.GB;
	public static final String defaultInstancesCount = "1";
	
	@Deprecated
	public static final String defaultMachinesCount = "0";
	
	public static final String defaultProcessesMax = "0";
	public static final String defaultProcessesMin = "0";
	public static final String defaultThreadsPerProcess = "1";
	
	public static final String minThreadsPerProcess = "1";
	
	public static final String defaultWorkItemsTotal = "unknown";
	public static final String defaultWorkItemsCompleted = "0";
	public static final String defaultWorkItemsDispatched = "0";
	public static final String defaultWorkItemsError = "0";
	public static final String defaultWorkItemsRetry = "0";
	public static final String defaultWorkItemsLost = "0";
	public static final String defaultWorkItemsPreempt = "0";
	@Deprecated
	public static final String defaultWorkItemsPending = "unknown";
	
	// common
	
	public String getSchedulingClass();
	public void setSchedulingClass(String schedulingClass);
	
	public String getSchedulingPriority();
	public void setSchedulingPriority(String schedulingPriority);
	
	public String getMemorySizeRequested();
	public void setMemorySizeRequested(String size);
	
	public void setMemorySizeAllocatedInBytes(long value);
	public long getMemorySizeAllocatedInBytes();
	
	public MemoryUnits getMemoryUnits();
	public void setMemoryUnits(MemoryUnits units);
	
	// reservations
	
	public String getInstancesCount();
	public void setInstancesCount(String instancesCount);
	
	@Deprecated
	public String getMachinesCount();
	@Deprecated
	public void setMachinesCount(String machinesCount);
	
	// processes
	
	public long getLongProcessesMax();
	public void setLongProcessesMax(long number);
	
	public String getProcessesMax();
	public void setProcessesMax(String number);
	
	public String getProcessesMin();
	public void setProcessesMin(String number); 
	
	public String getThreadsPerProcess();
	public void setThreadsPerProcess(String number);
	
	public int getIntThreadsPerProcess();
	
	public String getWorkItemsTotal();
	public void setWorkItemsTotal(String number);
	
	public int getIntWorkItemsTotal();
	
	public String getWorkItemsCompleted();
	public void setWorkItemsCompleted(String number);
	
	public int getIntWorkItemsCompleted();
	
	public String getWorkItemsDispatched();
	public void setWorkItemsDispatched(String number);
	
	public String getWorkItemsError();
	public void setWorkItemsError(String number);
	
	public int getIntWorkItemsError();
	
	public String getWorkItemsRetry();
	public void setWorkItemsRetry(String number);
	
	@Deprecated
	public String getWorkItemsLost();
	@Deprecated
	public void setWorkItemsLost(String number);
	@Deprecated
	public int getIntWorkItemsLost();
	
	public String getWorkItemsPreempt();
	public void setWorkItemsPreempt(String number);
	
	@Deprecated
	public ConcurrentHashMap<Integer,DuccId> getLimboMap();
	@Deprecated
	public void setLimboMap(ConcurrentHashMap<Integer,DuccId> map);
	
	public ConcurrentHashMap<String,DuccId> getCasQueuedMap();
	public void setCasQueuedMap(ConcurrentHashMap<String,DuccId> map);
	
	public IDuccPerWorkItemStatistics getPerWorkItemStatistics();
	public void setPerWorkItemStatistics(IDuccPerWorkItemStatistics value);
	
	@Deprecated
	public PerformanceMetricsSummaryMap getPerformanceMetricsSummaryMap();
	
	public void setMostRecentWorkItemStart(long time);
	public long getMostRecentWorkItemStart();
	
	@Deprecated
	public String getWorkItemsPending();
	@Deprecated
	public void setWorkItemsPending(String number);
}
