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
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;

import org.apache.uima.ducc.common.utils.DuccLogger;


/**
 * Request characteristics provided by user.
 */
@SuppressWarnings("serial")
public class JobRequestProperties extends JobSpecificationProperties implements Serializable {
	
	/*
	 * request type: one per request, else error
	 */
	public static String key_request_type_submit = "request_type_submit";
	public static String key_request_type_cancel = "request_type_cancel";
	public static String key_request_type_modify = "request_type_modify";
	
	/*
	 * id: required for cancel/modify requests; error for submit request
	 */
	public static String key_id = "id";	
	public static String key_dpid = "dpid";	
	
	// Not used?
	public static List<String> keys_requiring_values = Arrays.asList(
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
			key_process_jvm_args,
			key_process_memory_size,
			key_process_DD,
			key_process_descriptor_CM,
			key_process_descriptor_CM_overrides,
			key_process_descriptor_AE,
			key_process_descriptor_AE_overrides,
			key_process_descriptor_CC,
			key_process_descriptor_CC_overrides,
			key_process_deployments_max,
			key_process_deployments_min,
			key_process_initialization_failures_cap,
			key_process_failures_limit,
			key_process_thread_count,
			/*
			key_process_get_meta_time_max,
			*/
			key_process_initialization_time_max,
			key_process_per_item_time_max,
	        key_service_dependency
	        );
	
	/*
	 * Disable place-holder expansion in DuccProperties.getProperty as CLI does it when processing the options,
	 * and any unresolved ones must be left asis, e.g. DUCC_SERVICE_INSTANCE
	 */
	public JobRequestProperties() {
		resolvePlaceholders = false;
	}

	public void specification(DuccLogger logger) {
		String methodName = "specification";
		Enumeration<Object> keys = keys();
		while(keys.hasMoreElements()) {
			String key = (String) keys.nextElement();
			String value = getProperty(key);
			logger.info(methodName, null, key+":"+value);
		}
	}
	
	public void normalize() {
		Enumeration<Object> keys = keys();
		while(keys.hasMoreElements()) {
			String key = (String) keys.nextElement();
			if(key != null) {
				String tkey = key.trim();
				if(!tkey.equals(key)) {
					String value = getProperty(key);
					remove(key);
					put(tkey,value);
				}
			}
		}
	}
	
	public void dump() {
		Enumeration<Object> keys = keys();
		while(keys.hasMoreElements()) {
			String key = (String) keys.nextElement();
			String value = getProperty(key);
			System.out.println(key+":"+value);
		}
	}
}
