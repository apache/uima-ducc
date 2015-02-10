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

import org.apache.uima.ducc.transport.event.common.IDuccCompletionType.JobCompletionType;
import org.apache.uima.ducc.transport.event.common.IDuccState.JobState;


public interface IDuccWorkJob extends Identifiable, IDuccWorkService, IDuccWorkExecutable, Serializable {
	
	public boolean isJdURLSpecified();
	public void setJdURLSpecified();
	
	public boolean isDdSpecified();
	public void setDdSpecified();
	
	public String getUserLogsDir();
	
	public JobState getJobState();
	public void setJobState(JobState jobState);
	
	public long getCompletingTOD();
	public void setCompletingTOD();
	
	public void setCompletion(JobCompletionType completionType, IRationale completionRationale);
	
	public JobCompletionType getCompletionType();
	public void setCompletionType(JobCompletionType completionType);
	
	public IRationale getCompletionRationale();
	public void setCompletionRationale(IRationale completionRationale);
	
	public DuccWorkPopDriver getDriver();
	public void setDriver(DuccWorkPopDriver driver);
	
	public String getjobBroker();
	public void setJobBroker(String broker);
	
	public String getjobQueue();
	public void setJobQueue(String queue);
	
	public boolean isActive();
	public boolean isInitialized();
	public boolean isRunnable();
	public boolean isCompleting();
	public boolean isFinished();
	
	public boolean isProcessReady();
	public int getFailedUnexpectedProcessCount();
	
	public String getLogDirectory();
	
	public long getWorkItemCapacity();
	
	public long getProcessInitFailureCap();
	public void setProcessInitFailureCap(long cap);
	
	public long getProcessInitFailureCount();
	
	public long getProcessInitFailureLimit();
	public void setProcessInitFailureLimit(long limit);
	
	public long getProcessFailureCount();
	
	public long getProcessFailureLimit();
	public void setProcessFailureLimit(long limit);
	
	public long getDebugPortDriver();
	public void setDebugPortDriver(long port);
	
	public long getDebugPortProcess();
	public void setDebugPortProcess(long port);
	
	public long getAliveProcessCount();
	public boolean hasAliveProcess();
	
	public long getPgInCount();
	public double getSwapUsageGb();
	public double getSwapUsageGbMax();
	
	public long getWiVersion();
	
	public long getWiMillisMin();
	public void setWiMillisMin(long value);
	
	public long getWiMillisMax();
	public void setWiMillisMax(long value);
	
	public long getWiMillisAvg();
	public void setWiMillisAvg(long value);
	
	public long getWiMillisOperatingLeast();
	public void setWiMillisOperatingLeast(long value);
	
	public long getWiMillisCompletedMost();
	public void setWiMillisCompletedMost(long value);
}
