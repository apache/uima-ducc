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

import java.io.File;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.common.IDuccCompletionType.JobCompletionType;
import org.apache.uima.ducc.transport.event.common.IDuccState.JobState;
import org.apache.uima.ducc.transport.event.common.IDuccTypes.DuccType;


public class DuccWorkJob extends ADuccWorkExecutable implements IDuccWorkJob {
	/**
	 * please increment this sUID when removing or modifying a field 
	 */
	private static final long serialVersionUID = 1L;

	private DuccWorkPopDriver driver = null;
	private String jobBroker = null;
	private String jobQueue = null;
	
	private long defaultInitFailureLimit = 1;
	
	private AtomicLong processInitFailureCap = new AtomicLong(0);
	private AtomicLong processInitFailureLimit = new AtomicLong(defaultInitFailureLimit);
	
	private long defaultFailureLimit = 2;
	
	private AtomicLong processFailureLimit = new AtomicLong(defaultFailureLimit);
	
	private IRationale completionRationale = null;

	public DuccWorkJob() {
		init(null);
	}
	
	public DuccWorkJob(DuccId duccId) {
		init(duccId);
	}
	
	private void init(DuccId duccId) {
		setDuccType(DuccType.Job);
		setDuccId(duccId);
		setStateObject(IDuccState.JobState.Undefined);
		setCompletionTypeObject(IDuccCompletionType.JobCompletionType.Undefined);
	}
	
	@Override
	public DuccWorkPopDriver getDriver() {
		return driver;
	}
	
	@Override
	public void setDriver(DuccWorkPopDriver driver) {
		this.driver = driver;
	}


	@Override
	public String getjobBroker() {
		return jobBroker;
	}

	@Override
	public void setJobBroker(String broker) {
		this.jobBroker = broker;
	}

	@Override
	public String getjobQueue() {
		return this.jobQueue;
	}

	@Override
	public void setJobQueue(String queue) {
		this.jobQueue = queue;
	}

	@Override
	public JobState getJobState() {
		return (JobState)getStateObject();
	}

	@Override
	public void setJobState(JobState jobState) {
		setStateObject(jobState);
	}

	@Override
	public void setCompletion(JobCompletionType completionType, IRationale completionRationale) {
		setCompletionType(completionType);
		setCompletionRationale(completionRationale);
	}
	
	@Override
	public JobCompletionType getCompletionType() {
		return (JobCompletionType)getCompletionTypeObject();
	}

	@Override
	public void setCompletionType(JobCompletionType completionType) {
		setCompletionTypeObject(completionType);
	}
	
	@Override
	public IRationale getCompletionRationale() {
		IRationale retVal = null;
		try {
			if(this.completionRationale != null) {
				retVal = this.completionRationale;
			}
			else {
				retVal = new Rationale();
			}
		}
		catch(Exception e) {
			retVal = new Rationale();
		}
		return retVal;
	}
	
	@Override
	public void setCompletionRationale(IRationale completionRationale) {
		this.completionRationale = completionRationale;
	}
	
	public boolean isActive() {
		boolean retVal = false;
		switch(getJobState()) {
		case WaitingForDriver:
		case WaitingForServices:
		case WaitingForResources:
		case Initializing:
		case Running:
			retVal = true;	
			break;
		}
		return retVal;
	}
	
	public boolean isSchedulable() {
		boolean retVal = false;
		switch(getJobState()) {
		case WaitingForResources:
		case Initializing:
		case Running:
			retVal = true;	
			break;
		}
		return retVal;
	}
	
	
	public boolean isInitialized() {
		boolean retVal = false;
		switch(getJobState()) {
		case Running:
		case Completing:
		case Completed:
			retVal = true;	
			break;
		}
		return retVal;
	}
	
	public boolean isRunnable() {
		boolean retVal = false;
		switch(getJobState()) {
		case Running:
			retVal = true;	
			break;
		}
		return retVal;
	}
	
	public boolean isCompleting() {
		boolean retVal = false;
		switch(getJobState()) {
		case Completing:
			retVal = true;	
			break;
		}
		return retVal;
	}
	
	public boolean isCompleted() {
		boolean retVal = false;
		switch(getJobState()) {
		case Completed:
			retVal = true;	
			break;
		}
		return retVal;
	}
	
	public boolean isFinished() {
		boolean retVal = false;
		switch(getJobState()) {
		case Completing:
		case Completed:
			retVal = true;	
			break;
		}
		return retVal;
	}
	
	@Override
	public boolean isOperational() {
		boolean retVal = true;
		switch(getJobState()) {
		case Completed:
			retVal = false;	
			break;
		}
		return retVal;
	}
	
	public boolean isProcessReady() {
		IDuccProcessMap processMap = (IDuccProcessMap) getProcessMap().deepCopy();
		return processMap.getReadyProcessCount() > 0;
	}
	
	/*
	public int getFailedProcessCount() {
		IDuccProcessMap processMap = (IDuccProcessMap) getProcessMap().deepCopy();
		return processMap.getFailedProcessCount();
	}
	*/
	
	public int getFailedUnexpectedProcessCount() {
		IDuccProcessMap processMap = (IDuccProcessMap) getProcessMap().deepCopy();
		return processMap.getFailedUnexpectedProcessCount();
	}
	
	public String getLogDirectory() {
		String retVal = System.getProperty("user.home");
		IDuccStandardInfo standardInfo = getStandardInfo();
		if(standardInfo != null) {
			String logDirectory = standardInfo.getLogDirectory();
			if(logDirectory != null) {
				if(logDirectory != "") {
					retVal = logDirectory;
				}
			}
		}
		if(!retVal.endsWith(File.separator)) {
			retVal += File.separator;
		}
		return retVal;
	}
	
	// **********
	
	public long getProcessInitFailureCap() {
		long retVal = 0;
		try {
			retVal = processInitFailureCap.get();
		}
		catch(Exception e) {
		}
		return retVal;
	}
	
	
	public void setProcessInitFailureCap(long value) {
		processInitFailureCap.set(value);
	}
	
	public long getProcessInitFailureCount() {
		long retVal = getProcessMap().getFailedInitializationCount();
		return retVal;
	}
	
	public long getProcessInitFailureLimit() {
		long retVal = defaultInitFailureLimit;
		try {
			retVal = processInitFailureLimit.get();
		}
		catch(Exception e) {
		}
		return retVal;
	}
	
	public void setProcessInitFailureLimit(long limit) {
		processInitFailureLimit.set(limit);
		return;
	}
	
	// **********
	
	public long getProcessFailureCount() {
		long retVal = getProcessMap().getFailedNotInitializationCount();
		return retVal;
	}
	
	public long getProcessFailureLimit() {
		long retVal = defaultFailureLimit;
		try {
			retVal = processFailureLimit.get();
		}
		catch(Exception e) {
		}
		return retVal;
	}
	
	public void setProcessFailureLimit(long limit) {
		processFailureLimit.set(limit);
		return;
	}
	
	// **********
	
	@Override
	public int hashCode() {
		//return super.hashCode();
		final int prime = 31;
		int result = 1;
		result = prime * result + ((driver == null || driver.getProcessMap() == null) ? 0 : driver.getProcessMap().hashCode());
		result = prime * result + super.hashCode();
		return result;

	}
	
	public boolean equals(Object obj) {
		if(getClass() == obj.getClass()) {
			DuccWorkJob that = (DuccWorkJob)obj;

            // if one of these is a service then there is no driver
            if ( (this.driver == null) && (that.driver != null) ) return false;               // svc and a job
            if ( (this.driver != null) && (that.driver == null) ) return false;               // job and a svc
            if ( (this.driver == null) && (that.driver == null) ) return super.equals(obj);   // svc and a svc
            return (Util.compare(this.driver.getProcessMap(),that.driver.getProcessMap()) &&  // job and a job
                    super.equals(obj));

		} else {
			System.out.println(">>>>>>>>>>>>>>>> What Class is it? "+obj.getClass().getName());
			return super.equals(obj);
		}
	}

}
