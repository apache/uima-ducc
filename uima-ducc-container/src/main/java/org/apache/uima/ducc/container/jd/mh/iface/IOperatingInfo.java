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
package org.apache.uima.ducc.container.jd.mh.iface;

import java.io.Serializable;
import java.util.ArrayList;

public interface IOperatingInfo extends Serializable {

	public void setJobId(String value);
	public String getJobId();

	public void setJpAe(String value);
	public String getJpAe();
	
	public void setWorkItemCrTotal(int value);
	public int getWorkItemCrTotal();
	
	public void setWorkItemCrFetches(int value);
	public int getWorkItemCrFetches();
	
	public boolean isWorkItemCrPending();

	public void setWorkItemJpGets(int value);
	public int getWorkItemJpGets();
	
	public void setWorkItemJpAcks(int value);
	public int getWorkItemJpAcks();
	
	public void setWorkItemEndSuccesses(int value);
	public int getWorkItemEndSuccesses();
	
	public void setWorkItemEndFailures(int value);
	public int getWorkItemEndFailures();
	
	public void setWorkItemEndRetrys(int value);
	public int getWorkItemEndRetrys();

	public void setWorkItemDispatcheds(int value);
	public int getWorkItemDispatcheds();
	
	public void setWorkItemRetrys(int value);
	public int getWorkItemRetrys();
	
	public void setWorkItemPreemptions(int value);
	public int getWorkItemPreemptions();
	
	public void setWorkItemUserProcessingTimeouts(int value);
	public int getWorkItemUserProcessingTimeouts();
	
	public void setWorkItemUserProcessingErrorRetries(int value);
	public int getWorkItemUserProcessingErrorRetries();
	
	//
	
	public void setWorkItemFinishedMillisMin(long value);
	public long getWorkItemFinishedMillisMin();
	
	public void setWorkItemFinishedMillisMax(long value);
	public long getWorkItemFinishedMillisMax();
	
	public void setWorkItemFinishedMillisAvg(long value);
	public long getWorkItemFinishedMillisAvg();
	
	public void setWorkItemFinishedMillisStdDev(long value);
	public long getWorkItemFinishedMillisStdDev();
	
	//
	
	public void setWorkItemRunningMillisMin(long value);
	public long getWorkItemRunningMillisMin();
	
	public void setWorkItemRunningMillisMax(long value);
	public long getWorkItemRunningMillisMax();
	
	//
	
	public void setWorkItemTodMostRecentStart(long value);
	public long getWorkItemTodMostRecentStart();
	
	//
	
	public void setJdState(String value);
	public String getJdState();
	
	public void setKillJob();
	public boolean isKillJob();
	
	//
	
	public void setActiveWorkItemInfo(ArrayList<IWorkItemInfo> value);
	public ArrayList<IWorkItemInfo> getActiveWorkItemInfo();
	
	public void setProcessInfo(ArrayList<IProcessInfo> value);
	public ArrayList<IProcessInfo> getProcessItemInfo();
}
