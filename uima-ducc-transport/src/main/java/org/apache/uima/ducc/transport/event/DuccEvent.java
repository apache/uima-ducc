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
package org.apache.uima.ducc.transport.event;

import java.io.Serializable;

public interface DuccEvent extends Serializable { // extends Map<Object, Object> {
	public enum EventType{ 
			BOOT, 
			SHUTDOWN, 
			//
        	SUBMIT_JOB, 
            CANCEL_JOB, 
            MODIFY_JOB, 
            JOB_REPLY, 
            END_OF_JOB,
            //
            SUBMIT_RESERVATION, 
            CANCEL_RESERVATION, 
            MODIFY_RESERVATION, 
            RESERVATION_REPLY, 
            END_OF_RESERVATION,
            //
            SUBMIT_SERVICE, 
            CANCEL_SERVICE, 
            MODIFY_SERVICE, 
            SERVICE_REPLY, 
            END_OF_SERVICE,
            //
            SUBMIT_MANAGED_RESERVATION, 
            CANCEL_MANAGED_RESERVATION, 
            END_OF_MANAGED_RESERVATION, 
            //
            START_PROCESS, 
            STOP_PROCESS, 
            WEBSERVER_STATE,
            JD_STATE, 
            ORCHESTRATOR_STATE, 
            RM_STATE, 
            PM_STATE,
            SM_HEARTBEAT, 
            SM_STATE, 
            PROCESS_STATE, 
            NODE_METRICS, 
            PURGE_PROCESS,
            SERVICE_REGISTER,
            SERVICE_UNREGISTER,
            SERVICE_START,
            SERVICE_STOP,
            SERVICE_QUERY,
            DUCCWORK,
            AGENT_PING
            };
	
	public EventType getEventType();
}
