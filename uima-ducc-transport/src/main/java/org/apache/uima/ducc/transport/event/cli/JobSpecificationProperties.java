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
package org.apache.uima.ducc.transport.event.cli;

import java.io.Serializable;

/**
 * Job characteristics provided by user.
 */
@SuppressWarnings("serial")
public class JobSpecificationProperties extends SpecificationProperties implements Serializable {
	
	public static String key_user = "user";
	public static String key_specification = "specification";
	
	public static String key_notifications = "notifications";
	public static String key_log_directory = "log_directory";
	public static String key_working_directory = "working_directory";
	
	public static String key_scheduling_priority = "scheduling_priority";
	
	public static String key_jvm = "jvm";
	public static String key_classpath = "classpath";
	public static String key_environment = "environment";
	
	public static String key_process_debug          = "process_debug";
	public static String key_driver_debug           = "driver_debug";
	public static String key_process_attach_console = "process_attach_console";
	public static String key_driver_attach_console  = "driver_attach_console";
	
	public static String key_driver_jvm_args = "driver_jvm_args";
	public static String key_driver_descriptor_CR = "driver_descriptor_CR";
	public static String key_driver_descriptor_CR_overrides = "driver_descriptor_CR_overrides";
	public static String key_driver_exception_handler = "driver_exception_handler";
	public static String key_driver_exception_handler_arguments = "driver_exception_handler_arguments";
	
	public static String key_process_jvm_args = "process_jvm_args";
	public static String key_process_memory_size = "process_memory_size";
	public static String key_process_DD = "process_descriptor_DD";
	public static String key_process_descriptor_CM = "process_descriptor_CM";
	public static String key_process_descriptor_CM_overrides = "process_descriptor_CM_overrides";
	public static String key_process_descriptor_AE = "process_descriptor_AE";
	public static String key_process_descriptor_AE_overrides = "process_descriptor_AE_overrides";
	public static String key_process_descriptor_CC = "process_descriptor_CC";
	public static String key_process_descriptor_CC_overrides = "process_descriptor_CC_overrides";
	public static String key_process_descriptor_FC = "process_descriptor_FC";
	public static String key_process_descriptor_FC_overrides = "process_descriptor_FC_overrides";
	
	public static String key_process_deployments_max = "process_deployments_max";
	public static String key_process_deployments_min = "process_deployments_min";
	public static String key_process_initialization_failures_cap = "process_initialization_failures_cap";
	public static String key_process_failures_limit = "process_failures_limit";
	public static String key_process_thread_count = "process_thread_count";
	
	/*
	public static String key_process_get_meta_time_max = "process_get_meta_time_max";
	*/
	public static String key_process_initialization_time_max = "process_initialization_time_max";
	public static String key_process_per_item_time_max = "process_per_item_time_max";
	
	public static String key_process_executable = "process_executable";
	public static String key_process_executable_args = "process_executable_args";
	
    public static String key_service_dependency = "service_dependency";
    
    public static String key_wait_for_completion = "wait_for_completion";
    
	public static String key_classpath_order = "classpath_order";
	
	public static String[] keys = {
		JobRequestProperties.key_signature,
		JobRequestProperties.key_description,
		JobRequestProperties.key_scheduling_class,
		JobRequestProperties.key_submitter_pid_at_host,
		key_user,
		key_specification,
		key_notifications,
		key_log_directory,
		key_working_directory,
		key_scheduling_priority,
		key_jvm,
		key_driver_jvm_args,
		key_driver_descriptor_CR,
		key_driver_descriptor_CR_overrides,
		key_driver_exception_handler,
		key_driver_exception_handler_arguments,
		key_process_jvm_args,
		key_process_memory_size,
		key_process_DD,
		key_process_descriptor_CM,
		key_process_descriptor_CM_overrides,
		key_process_descriptor_AE,
		key_process_descriptor_AE_overrides,
		key_process_descriptor_CC,
		key_process_descriptor_CC_overrides,
		key_process_descriptor_FC,
		key_process_descriptor_FC_overrides,
		key_process_deployments_max,
		key_process_deployments_min,
		key_process_initialization_failures_cap,
		key_process_failures_limit,
		key_process_thread_count,
		key_process_initialization_time_max,
		key_process_per_item_time_max,
        key_service_dependency,
        key_wait_for_completion,
        key_classpath_order,
	};
	
	public boolean isRecognized(String key) {
		boolean retVal = false;
		for (String known : keys) {
			if(known != null) {
				if (known.equals(key)) {
					retVal = true;
				}
			}
		}
		return retVal;
	}
}
