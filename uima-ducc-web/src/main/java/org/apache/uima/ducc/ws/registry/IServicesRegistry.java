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
	public static final String autostart = IStateServices.SvcMetaProps.autostart.pname();
	public static final String reference = IStateServices.SvcMetaProps.reference.pname();
	public static final String enabled = IStateServices.SvcMetaProps.enabled.pname();
	public static final String disable_reason = IStateServices.SvcMetaProps.disable_reason.pname();
	public static final String endpoint = IStateServices.SvcMetaProps.endpoint.pname();
	public static final String implementors = IStateServices.SvcMetaProps.implementors.pname();
	public static final String instances = IStateServices.SvcMetaProps.instances.pname();
	public static final String numeric_id = IStateServices.SvcMetaProps.numeric_id.pname();
	public static final String ping_active = IStateServices.SvcMetaProps.ping_active.pname();
	public static final String ping_only = IStateServices.SvcMetaProps.ping_only.pname();
	public static final String service_alive = IStateServices.SvcMetaProps.service_alive.pname();
	public static final String service_class = IStateServices.SvcMetaProps.service_class.pname();
	public static final String service_dependency = IStateServices.SvcMetaProps.service_dependency.pname();
	public static final String service_healthy = IStateServices.SvcMetaProps.service_healthy.pname();
	public static final String service_state = IStateServices.SvcMetaProps.service_state.pname();
	public static final String last_use = IStateServices.SvcMetaProps.last_use.pname();
	public static final String service_statistics = IStateServices.SvcMetaProps.service_statistics.pname();
	public static final String service_type = IStateServices.SvcMetaProps.service_type.pname();
	public static final String submit_error = IStateServices.SvcMetaProps.submit_error.pname();
	public static final String user = IStateServices.SvcMetaProps.user.pname();
	
	public static final String service_type_CUSTOM = IStateServices.CUSTOM;
	
	public static final String constant_Available = "Available";
	public static final String constant_true = "true";
	public static final String constant_false = "false";
	
	public static final String constant_NotKnown = "NotKnown";
	public static final String constant_NotAvailable = "NotAvailable";
	public static final String constant_NotPinging = "NotPinging";
	public static final String constant_NotHealthy = "NotHealthy";
	public static final String constant_OK = "OK";
	
	// svc
	public static final String description = IStateServices.SvcRegProps.description.pname();
	public static final String process_memory_size = IStateServices.SvcRegProps.process_memory_size.pname();
	public static final String scheduling_class = IStateServices.SvcRegProps.scheduling_class.pname();
	public static final String log_directory = IStateServices.SvcRegProps.log_directory.pname();
	
}
