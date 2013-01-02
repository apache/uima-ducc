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

import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.common.IDuccUnits.MemoryUnits;
import org.apache.uima.ducc.transport.event.jd.PerformanceMetricsSummaryMap;


public interface IDuccSchedulingInfo extends Serializable {
	
	public static final String defaultSchedulingClass = "normal";
	public static final String defaultSchedulingPriority = "0";
	public static final String defaultShareMemorySize = "13";
	public static final MemoryUnits defaultShareMemoryUnits = MemoryUnits.GB;
	public static final String defaultInstancesCount = "1";
	
	@Deprecated
	public static final String defaultMachinesCount = "0";
	
	public static final String defaultSharesMax = "0";
	public static final String defaultSharesMin = "0";
	public static final String defaultThreadsPerShare = "1";
	
	public static final String minThreadsPerShare = "1";
	public static final String maxThreadsPerShare = "100";
	
	public static final String defaultWorkItemsTotal = "unknown";
	public static final String defaultWorkItemsCompleted = "0";
	public static final String defaultWorkItemsDispatched = "0";
	public static final String defaultWorkItemsError = "0";
	public static final String defaultWorkItemsRetry = "0";
	public static final String defaultWorkItemsPreempt = "0";
	@Deprecated
	public static final String defaultWorkItemsPending = "unknown";
	
	// common
	
	public String getSchedulingClass();
	public void setSchedulingClass(String schedulingClass);
	
	public String getSchedulingPriority();
	public void setSchedulingPriority(String schedulingPriority);
	
	public String getShareMemorySize();
	public void setShareMemorySize(String size);
	
	public MemoryUnits getShareMemoryUnits();
	public void setShareMemoryUnits(MemoryUnits units);
	
	// reservations
	
	public String getInstancesCount();
	public void setInstancesCount(String instancesCount);
	
	@Deprecated
	public String getMachinesCount();
	@Deprecated
	public void setMachinesCount(String machinesCount);
	
	// processes
	
	public long getLongSharesMax();
	public void setLongSharesMax(long shares);
	
	public String getSharesMax();
	public void setSharesMax(String shares);
	
	public String getSharesMin();
	public void setSharesMin(String shares);
	
	public String getThreadsPerShare();
	public void setThreadsPerShare(String number);
	
	public int getIntThreadsPerShare();
	
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
	
	public String getWorkItemsPreempt();
	public void setWorkItemsPreempt(String number);
	
	public ConcurrentHashMap<Integer,DuccId> getLimboMap();
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
