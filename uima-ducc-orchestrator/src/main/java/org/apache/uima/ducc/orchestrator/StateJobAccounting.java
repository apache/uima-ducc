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
package org.apache.uima.ducc.orchestrator;

import java.io.File;

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.TimeStamp;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.orchestrator.user.UserLogging;
import org.apache.uima.ducc.transport.event.common.IDuccTypes.DuccType;
import org.apache.uima.ducc.transport.event.common.IDuccWorkService.ServiceDeploymentType;
import org.apache.uima.ducc.transport.event.common.IDuccWorkJob;
import org.apache.uima.ducc.transport.event.common.IRationale;
import org.apache.uima.ducc.transport.event.common.IDuccCompletionType.JobCompletionType;
import org.apache.uima.ducc.transport.event.common.IDuccState.JobState;


public class StateJobAccounting {

	private static final DuccLogger logger = DuccLoggerComponents.getOrLogger(StateJobAccounting.class.getName());
	
	private static StateJobAccounting instance = new StateJobAccounting();
	
	public static StateJobAccounting getInstance() {
		return instance;
	}
	
	private boolean advance(IDuccWorkJob job) {
		String methodName = "advance";
		boolean retVal = false;
		try {
			DuccType duccType = job.getDuccType();
			switch(duccType) {
			case Service:
				ServiceDeploymentType sdt = job.getServiceDeploymentType();
				switch(sdt) {
				case other:
					JobState state = job.getJobState();
					switch(state) {
					case Initializing:
						JobState next = JobState.Running;
						JobState prev = state;
						logger.info(methodName, job.getDuccId(),"current["+next+"] previous["+prev+"]"+" "+"-> skipped");
						retVal = stateChange(job, next);
						break;
					default:
						logger.debug(methodName, job.getDuccId(), "State is not "+JobState.Initializing+" state");
						break;
					}
					break;
				default:
					logger.debug(methodName, job.getDuccId(), "Service is not ManagedReservation (other); ServiceDeploymentType="+sdt);
					break;
				}
				break;
			default:
				logger.debug(methodName, job.getDuccId(), "DuccType="+duccType);
				break;
			}
		}
		catch(Exception e) {
			logger.error(methodName, job.getDuccId(), e);
		}
		return retVal;
	}
	
	public boolean stateChange(IDuccWorkJob job, JobState state) {
		String methodName = "stateChange";
		boolean retVal = false;
		JobState prev = job.getJobState();
		JobState next = state;
		switch(prev) {
		case Completing:
			retVal = stateChangeFromCompleting(prev, next);
			break;
		case Completed:
			switch(next) {
			case Completing:
				next = prev;
				break;
			default:
				break;
			}
			retVal = stateChangeFromCompleted(prev, next);
			break;
		case Assigned:
			retVal = stateChangeFromAssigned(prev, next);
			break;	
		case Initializing:
			retVal = stateChangeFromInitializing(prev, next);
			break;
		case Received:
			retVal = stateChangeFromReceived(prev, next);
			break;
		case Running:
			retVal = stateChangeFromRunning(prev, next);
			break;
		case Undefined:
			retVal = stateChangeFromUndefined(prev, next);
			break;
		case WaitingForDriver:
			retVal = stateChangeFromWaitingForDriver(prev, next);
			break;
		case WaitingForResources:
			retVal = stateChangeFromWaitingForResources(prev, next);
			break;	
		case WaitingForServices:
			retVal = stateChangeFromWaitingForServices(prev, next);
			break;
		}
		if(retVal) {
			job.setJobState(state);
			switch(state) {
			case Completing:
				job.getStandardInfo().setDateOfCompletion(TimeStamp.getCurrentMillis());
			}
			switch(state) {
			case Completed:
				recordUserState(job);
				recordUserCompletion(job);
				break;
			default:
				recordUserState(job);
				break;
			}
			boolean advanceVal = advance(job);
			if(!advanceVal) {
				logger.info(methodName, job.getDuccId(),"current["+next+"] previous["+prev+"]");
			}
		}
		else {
			try {
				throw new RuntimeException();
			} 
			catch(Exception e) {
				logger.error(methodName, job.getDuccId(),"current["+prev+"] requested["+next+"]"+" ignored", e);
			}
		}
		return retVal;
	}
	
	private boolean stateChangeFromCompleting(JobState prev, JobState next) {
		boolean retVal = false;
		switch(next) {
		case Completing:							break;
		case Completed:				retVal = true;	break;
		case Assigned:								break;
		case Initializing:							break;
		case Received:								break;
		case Running:								break;
		case Undefined:								break;
		case WaitingForDriver:						break;
		case WaitingForResources:					break;
		case WaitingForServices:					break;
		}
		return retVal;
	}
	
	private boolean stateChangeFromCompleted(JobState prev, JobState next) {
		boolean retVal = false;
		switch(next) {
		case Completing:							break;
		case Completed:								break;
		case Assigned:								break;
		case Initializing:							break;
		case Received:								break;
		case Running:								break;
		case Undefined:								break;
		case WaitingForDriver:						break;
		case WaitingForResources:					break;
		case WaitingForServices:					break;
		}
		return retVal;
	}
	
	private boolean stateChangeFromAssigned(JobState prev, JobState next) {
		boolean retVal = false;
		switch(next) {
		case Completing:			retVal = true;	break;
		case Completed:				retVal = true;	break;
		case Assigned:								break;
		case Initializing:			retVal = true;	break;
		case Received:								break;
		case Running:				retVal = true;	break;
		case Undefined:								break;
		case WaitingForDriver:						break;
		case WaitingForResources:					break;
		case WaitingForServices:					break;
		}
		return retVal;
	}
	
	private boolean stateChangeFromInitializing(JobState prev, JobState next) {
		boolean retVal = false;
		switch(next) {
		case Completing:			retVal = true;	break;
		case Completed:				retVal = true;	break;
		case Assigned:								break;
		case Initializing:							break;
		case Received:								break;
		case Running:				retVal = true;	break;
		case Undefined:								break;
		case WaitingForDriver:						break;
		case WaitingForResources:					break;
		case WaitingForServices:					break;
		}
		return retVal;
	}
	
	private boolean stateChangeFromReceived(JobState prev, JobState next) {
		boolean retVal = false;
		switch(next) {
		case Completing:			retVal = true;	break;
		case Completed:				retVal = true;	break;
		case Assigned:								break;
		case Initializing:							break;
		case Received:								break;
		case Running:								break;
		case Undefined:								break;
		case WaitingForDriver:		retVal = true;	break;
		case WaitingForResources:	retVal = true;	break;
		case WaitingForServices:	retVal = true;	break;
		}
		return retVal;
	}
	
	private boolean stateChangeFromRunning(JobState prev, JobState next) {
		boolean retVal = false;
		switch(next) {
		case Completing:			retVal = true;	break;
		case Completed:				retVal = true;	break;
		case Assigned:								break;
		case Initializing:							break;
		case Received:								break;
		case Running:								break;
		case Undefined:								break;
		case WaitingForDriver:						break;
		case WaitingForResources:					break;
		case WaitingForServices:					break;
		}
		return retVal;
	}
	
	private boolean stateChangeFromUndefined(JobState prev, JobState next) {
		boolean retVal = false;
		switch(next) {
		case Completing:							break;
		case Completed:								break;
		case Assigned:								break;
		case Initializing:							break;
		case Received:				retVal = true;	break;
		case Running:								break;
		case Undefined:								break;
		case WaitingForDriver:						break;
		case WaitingForResources:					break;
		case WaitingForServices:					break;
		}
		return retVal;
	}
	
	private boolean stateChangeFromWaitingForDriver(JobState prev, JobState next) {
		boolean retVal = false;
		switch(next) {
		case Completing:			retVal = true;	break;
		case Completed:				retVal = true;	break;
		case Assigned:								break;
		case Initializing:							break;
		case Received:								break;
		case Running:								break;
		case Undefined:								break;
		case WaitingForDriver:						break;
		case WaitingForResources:	retVal = true;	break;
		case WaitingForServices:	retVal = true;	break;
		}
		return retVal;
	}
	
	private boolean stateChangeFromWaitingForResources(JobState prev, JobState next) {
		boolean retVal = false;
		switch(next) {
		case Completing:			retVal = true;	break;
		case Completed:				retVal = true;	break;
		case Assigned:				retVal = true;	break;
		case Initializing:			retVal = true;	break;
		case Received:								break;
		case Running:								break;
		case Undefined:								break;
		case WaitingForDriver:						break;
		case WaitingForResources:					break;
		case WaitingForServices:					break;
		}
		return retVal;
	}
	
	private boolean stateChangeFromWaitingForServices(JobState prev, JobState next) {
		boolean retVal = false;
		switch(next) {
		case Completing:			retVal = true;	break;
		case Completed:				retVal = true;	break;
		case Assigned:								break;
		case Initializing:							break;
		case Received:								break;
		case Running:								break;
		case Undefined:								break;
		case WaitingForDriver:						break;
		case WaitingForResources:	retVal = true;	break;
		case WaitingForServices:					break;
		}
		return retVal;
	}

	public boolean complete(IDuccWorkJob job, JobCompletionType completionType, IRationale completionRationale) {
		String methodName = "complete";
		boolean retVal = false;
		logger.debug(methodName, job.getDuccId(), job.getCompletionType()+" "+job.getCompletionRationale());
		switch(job.getCompletionType()) {
		case Undefined:
			retVal = true;
			break;
		}
		if(retVal) {
			job.setCompletion(completionType,completionRationale);
			logger.info(methodName, job.getDuccId(), completionType+" "+completionRationale);
		}
		else {
			logger.info(methodName, job.getDuccId(), completionType+" "+"ignored");
		}
		return retVal;
	}
	
	private void recordUserState(IDuccWorkJob job) {
		String methodName = "recordUserState";
		DuccId jobid = null;
		String text = "";
		try {
			jobid = job.getDuccId();
			String userName = job.getStandardInfo().getUser();
			String userLogDir = job.getUserLogsDir()+job.getDuccId().getFriendly()+File.separator;
			JobState jobState = job.getJobState();
			if(jobState != null) {
				text = jobState.toString();
				UserLogging.record(userName, userLogDir, text);
				logger.debug(methodName, job.getDuccId(), text);
			}
		}
		catch(Exception e) {
			logger.error(methodName, jobid, e);
		}
	}
	
	private void recordUserCompletion(IDuccWorkJob job) {
		String methodName = "recordUserCompletion";
		DuccId jobid = null;
		String text = "";
		try {
			jobid = job.getDuccId();
			String userName = job.getStandardInfo().getUser();
			String userLogDir = job.getUserLogsDir()+job.getDuccId().getFriendly()+File.separator;
			JobCompletionType jobCompletionType = job.getCompletionType();
			if(jobCompletionType != null) {
				text = "completion type: "+jobCompletionType.toString();
				UserLogging.record(userName, userLogDir, text);
				logger.debug(methodName, job.getDuccId(), text);
			}
			IRationale rationale = job.getCompletionRationale();
			if(rationale != null) {
				text = "rationale: "+rationale.toString();
				UserLogging.record(userName, userLogDir, text);
				logger.debug(methodName, job.getDuccId(), text);
			}
		}
		catch(Exception e) {
			logger.error(methodName, jobid, e);
		}
	}

}
