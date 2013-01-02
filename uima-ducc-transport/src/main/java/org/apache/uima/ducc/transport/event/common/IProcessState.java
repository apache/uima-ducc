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

public interface IProcessState extends Serializable {

	public enum ProcessState {
		Starting,               // Process Manager sent request to start the Process
		Initializing,			// Process Agent is initializing process
		Running,				// Process Agent is available for processing work items
		Stopped,				// Process Agent reports process stopped
		Failed,					// Process Agent reports process failed
		FailedInitialization,	// Process Agent reports process failed initialization
		InitializationTimeout,  // Process Agent reports process initialization timeout
		Killed,         		// Agent forcefully killed the process
		Undefined				// None of the above
	};
	
	public ProcessState getProcessState();
}
