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

public interface IDuccCompletionType extends Serializable {
	
	public enum JobCompletionType {
		Premature,						// Job was terminated, but not all work items processed
		EndOfJob,						// Job finished without error
		Error,							// Job finished with error
		CanceledByUser,					// Job was canceled by user
		CanceledByDriver,				// Job was canceled by driver
		CanceledBySystem,				// Job was canceled by system
		ServicesUnavailable,			// Job was terminated by system - one or more services unavailable
		ResourcesUnavailable,			// Job was terminated by system - insufficient resources
		DriverInitializationFailure,	// Job was terminated by system - driver initialization failure
		CanceledByHealthMonitor,		// Job was terminated by health monitor
		DriverProcessFailed,			// Job was terminated by health monitor - driver process failed
		@Deprecated
		ExcessiveInitializationFailures,// Job was terminated by health monitor - excessive initialization failures
		@Deprecated
		ExcessiveProcessFailures,		// Job was terminated by health monitor - excessive process failures
		ProcessInitializationFailure,	// Job was terminated by health monitor - (excessive) process initialization failure
		ProcessFailure,					// Job was terminated by health monitor - (excessive) process failure
		Undefined						// None of the above
		;
		
		public static String getToolTip(JobCompletionType type) {
			String retVal = null;
			switch(type) {
			case Premature:						retVal = "Job was terminated, but not all work items processed"; break;
			case EndOfJob:						retVal = "Job finished without error"; break;
			case Error:							retVal = "Job finished with error"; break;
			case CanceledByUser:				retVal = "Job was canceled by user"; break;
			case CanceledByDriver:				retVal = "Job was canceled by driver"; break;
			case CanceledBySystem:				retVal = "Job was canceled by system"; break;
			case ServicesUnavailable:			retVal = "Job was terminated by system - one or more services unavailable"; break;
			case ResourcesUnavailable:			retVal = "Job was terminated by system - insufficient resources"; break;
			case DriverInitializationFailure:	retVal = "Job was terminated by system - driver initialization failure"; break;
			case CanceledByHealthMonitor:		retVal = "Job was terminated by health monitor"; break;
			case DriverProcessFailed:			retVal = "Job was terminated by health monitor - driver process failed"; break;
			case ProcessInitializationFailure:	retVal = "Job was terminated by health monitor - (excessive) process initialization failure"; break;
			case ProcessFailure:				retVal = "Job was terminated by health monitor - (excessive) process failure"; break;
			}
			return retVal;
		}
	}
	
	public enum ReservationCompletionType {
		Error,							// Reservation finished with error
		CanceledByAdmin,				// Reservation was canceled by administrator
		CanceledByUser,					// Reservation was canceled by user
		CanceledBySystem,				// Reservation was canceled by system
		ResourcesUnavailable,			// Reservation was terminated by system - insufficient resources
		Undefined						// None of the above
		;
		
		public static String getToolTip(ReservationCompletionType type) {
			String retVal = null;
			switch(type) {
			case Error:							retVal = "Reservation finished with error"; break;
			case CanceledByAdmin:				retVal = "Reservation was canceled by administrator"; break;
			case CanceledByUser:				retVal = "Reservation was canceled by user"; break;
			case CanceledBySystem:				retVal = "Reservation was canceled by system"; break;
			case ResourcesUnavailable:			retVal = "Job was terminated by system - insufficient resources"; break;
			}
			return retVal;
		}
	}
	
}
