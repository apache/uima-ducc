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

public interface IDuccState extends Serializable {
	
	public enum JobState {
		Received,				// Job has been vetted, persisted, and assigned unique Id
		WaitingForDriver,		// Process Manager is launching Job Driver
		WaitingForServices,		// Service Manager is checking/starting services for Job
		WaitingForResources,	// Scheduler is assigning resources to Job
		Initializing,			// Process Agents are initializing pipelines
		Running,				// At least one Process Agent has reported process initialization complete
		Completing,				// Job processing is completing
		Completed,				// Job processing is completed
		Undefined				// None of the above
	};
	
	public enum ReservationState {
		Received,				// Reservation has been vetted, persisted, and assigned unique Id
		WaitingForResources,	// Scheduler is assigning resources to Reservation
		Assigned,				// Scheduler has assigned resources
		Completed,				// Resources have been unassigned
		Undefined				// None of the above
	}
}
