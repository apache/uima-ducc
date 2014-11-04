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

public interface IOperatingInfo {

	public void setWorkItemCrTotal(int value);
	public int getWorkItemCrTotal();
	
	public void setWorkItemCrFetches(int value);
	public int getWorkItemCrFetches();
	
	public void setWorkItemJpSends(int value);
	public int getWorkItemJpSends();
	
	public void setWorkItemJpAcks(int value);
	public int getWorkItemJpAcks();
	
	public void setWorkItemEndSuccesses(int value);
	public int getWorkItemEndSuccesses();
	
	public void setWorkItemEndFailures(int value);
	public int getWorkItemEndFailures();
	
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
	
	//
	
	public void setWorkItemRunningMillisMin(long value);
	public long getWorkItemRunningMillisMin();
	
	public void setWorkItemRunningMillisMax(long value);
	public long getWorkItemRunningMillisMax();
}
