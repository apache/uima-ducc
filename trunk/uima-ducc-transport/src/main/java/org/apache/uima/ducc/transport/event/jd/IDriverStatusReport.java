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
package org.apache.uima.ducc.transport.event.jd;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.uima.ducc.common.jd.files.workitem.IRemoteLocation;
import org.apache.uima.ducc.common.jd.files.workitem.RemoteLocation;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.common.IDuccCompletionType.JobCompletionType;
import org.apache.uima.ducc.transport.event.common.IDuccPerWorkItemStatistics;
import org.apache.uima.ducc.transport.event.common.IRationale;
import org.apache.uima.ducc.transport.event.common.IResourceState.ProcessDeallocationType;
import org.apache.uima.ducc.transport.event.jd.IDriverState.DriverState;

public interface IDriverStatusReport {

	public long getVersion();
	
	public DuccId getDuccId();
	
	public String getNode();
	public void setNode(String value);
	
	public int getPort();
	public void setPort(int value);
	
	public void setJmxUrl(String value);
	
	public DriverState getDriverState();
	
	public String getJdState();
	public void setJdState(String value);
	
	public String getLogReport();
	
	public long getWorkItemsTotal();
	
	public int getWorkItemsProcessingCompleted();
	public int getWorkItemsProcessingError();
	public int getWorkItemsRetry();
	public int getWorkItemsDispatched();
	public int getWorkItemsPreempted();
	
	// min of finished & running
	public long getWiMillisMin();
	
	// max of finished & running
	public long getWiMillisMax();
	
	// avg of finished
	public long getWiMillisAvg();
	
	// min of running
	public long getWiMillisOperatingLeast();
	
	// max of finished
	public long getWiMillisCompletedMost();
	
	// tod most recent started work item
	public long getMostRecentStart();
	
	// true if CR not yet exhausted
	public boolean isPending();
	
	public boolean isKillJob();
	
	public boolean isOperating(String nodeIP, String PID);
	
	public String getJdJmxUrl();
	
	public String getUimaDeploymentDescriptor();
	public String getUimaAnalysisEngine();
	
	public Map<IRemoteLocation, ProcessDeallocationType> getProcessKillMap();
	
	public JobCompletionType getJobCompletionType();
	public IRationale getJobCompletionRationale();
	
	public IDuccPerWorkItemStatistics getPerWorkItemStatistics();
	
	public double getAvgTimeForWorkItemsSkewedByActive();
	
	public IDuccProcessWorkItemsReport getDuccProcessWorkItemsMap();
	
	public ConcurrentHashMap<RemoteLocation, Long> getOperatingMillisMap();
	public ConcurrentHashMap<RemoteLocation, Long> getInvestmentMillisMap();
}
