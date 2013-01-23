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

public interface IResourceState extends Serializable {
	
	public enum ResourceState {
		Allocated,				// RM has allocated resource
		Deallocated,			// RM has deallocated resource
		Undefined				// None of the above
	};
	
	public ResourceState getResourceState();
	
	public enum ProcessDeallocationType {
		Voluntary,				// No more work for process
		Forced,					// Competition caused take away (preemption by RM)
		JobCanceled,			// Job canceled by user (cancel command)
		JobFailure,				// Job canceled by JD
		JobCompleted,			// Process active prior to DUCC restart
		Exception,				// Process killed by JD exception handler
		AutonomousStop,			// Process unexpectedly vanished (not terminated by Agent)
		Stopped,				// Process terminated normally
		Failed,					// Process failure detected by Agent
		FailedInitialization,	// Process terminated due to initialization failure
		InitializationTimeout,	// Process terminated due to initialization timeout
		Killed,					// Process terminated by Agent
		Purged,					// Process purged (by RM)
		Canceled,				// Process canceled (by user)
		Undefined				// None of the above
		;
		
		public static String getToolTip(ProcessDeallocationType type) {
			String retVal = null;
			switch(type) {
			case Voluntary:				retVal = "No more work for process"; 								break;
			case Forced:				retVal = "Competition caused take away (preemption by RM)";			break;
			case JobCanceled:			retVal = "Job canceled by user (cancel command)"; 					break;
			case JobFailure:			retVal = "Job canceled by JD"; 										break;
			case JobCompleted:			retVal = "Process active prior to DUCC restart"; 					break;
			case Exception:				retVal = "Process killed by JD exception handler"; 					break;
			case AutonomousStop:		retVal = "Process unexpectedly vanished (not terminated by Agent)"; break;
			case Stopped:				retVal = "Process terminated normally"; 							break;
			case Failed:				retVal = "Process failure detected by Agent"; 						break;
			case FailedInitialization:	retVal = "Process terminated due to initialization failure"; 		break;
			case InitializationTimeout:	retVal = "Process terminated due to initialization timeout"; 		break;
			case Killed:				retVal = "Process terminated by Agent"; 							break;
			case Purged:				retVal = "Process purged (by RM)"; 									break;
			}
			return retVal;
		}
	}
}
