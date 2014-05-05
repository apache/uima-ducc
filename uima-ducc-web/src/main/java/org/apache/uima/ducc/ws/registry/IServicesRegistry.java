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
package org.apache.uima.ducc.ws.registry;

import org.apache.uima.ducc.common.persistence.services.IStateServices;

public class IServicesRegistry {
	
	public static final String meta = IStateServices.meta;
	public static final String svc = IStateServices.svc;
	
	// meta
	public static final String autostart = IStateServices.autostart;
	public static final String endpoint = IStateServices.endpoint;
	public static final String implementors = IStateServices.implementors;
	public static final String instances = IStateServices.instances;
	public static final String numeric_id = IStateServices.numeric_id;
	public static final String ping_active = IStateServices.ping_active;
	public static final String ping_only = IStateServices.ping_only;
	public static final String service_class = IStateServices.service_class;
	public static final String service_dependency = IStateServices.service_dependency;
	public static final String service_healthy = IStateServices.service_healthy;
	public static final String service_state = IStateServices.service_state;
	public static final String last_use = IStateServices.last_use;
	public static final String service_statistics = IStateServices.service_statistics;
	public static final String service_type = IStateServices.service_type;
	public static final String submit_error = IStateServices.submit_error;
	public static final String user = IStateServices.user;
	
	public static final String service_type_CUSTOM = "CUSTOM";
	
	public static final String constant_Available = "Available";
	public static final String constant_true = "true";
	public static final String constant_false = "false";
	
	public static final String constant_NotKnown = "NotKnown";
	public static final String constant_NotAvailable = "NotAvailable";
	public static final String constant_NotPinging = "NotPinging";
	public static final String constant_NotHealthy = "NotHealthy";
	public static final String constant_OK = "OK";
	
	// svc
	public static final String description = IStateServices.description;
	public static final String process_memory_size = IStateServices.process_memory_size;
	public static final String scheduling_class = IStateServices.scheduling_class;
	public static final String log_directory = IStateServices.log_directory;
	
}
