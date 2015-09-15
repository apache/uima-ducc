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
	public static final String autostart = IStateServices.SvcProps.autostart.pname();
	public static final String reference = IStateServices.SvcProps.reference.pname();
	public static final String enabled = IStateServices.SvcProps.enabled.pname();
	public static final String disable_reason = IStateServices.SvcProps.disable_reason.pname();
	public static final String endpoint = IStateServices.SvcProps.endpoint.pname();
	public static final String implementors = IStateServices.SvcProps.implementors.pname();
	public static final String instances = IStateServices.SvcProps.instances.pname();
	public static final String numeric_id = IStateServices.SvcProps.numeric_id.pname();
	public static final String ping_active = IStateServices.SvcProps.ping_active.pname();
	public static final String ping_only = IStateServices.SvcProps.ping_only.pname();
	public static final String service_alive = IStateServices.SvcProps.service_alive.pname();
	public static final String service_class = IStateServices.SvcProps.service_class.pname();
	public static final String service_dependency = IStateServices.SvcProps.service_dependency.pname();
	public static final String service_healthy = IStateServices.SvcProps.service_healthy.pname();
	public static final String service_state = IStateServices.SvcProps.service_state.pname();
	public static final String last_use = IStateServices.SvcProps.last_use.pname();
	public static final String service_statistics = IStateServices.SvcProps.service_statistics.pname();
	public static final String service_type = IStateServices.SvcProps.service_type.pname();
	public static final String submit_error = IStateServices.SvcProps.submit_error.pname();
	public static final String user = IStateServices.SvcProps.user.pname();
	
	public static final String service_type_CUSTOM = IStateServices.SvcProps.CUSTOM.pname();
	
	public static final String constant_Available = "Available";
	public static final String constant_true = "true";
	public static final String constant_false = "false";
	
	public static final String constant_NotKnown = "NotKnown";
	public static final String constant_NotAvailable = "NotAvailable";
	public static final String constant_NotPinging = "NotPinging";
	public static final String constant_NotHealthy = "NotHealthy";
	public static final String constant_OK = "OK";
	
	// svc
	public static final String description = IStateServices.SvcProps.description.pname();
	public static final String process_memory_size = IStateServices.SvcProps.process_memory_size.pname();
	public static final String scheduling_class = IStateServices.SvcProps.scheduling_class.pname();
	public static final String log_directory = IStateServices.SvcProps.log_directory.pname();
	
}
