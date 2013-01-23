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
import java.util.Enumeration;

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
	 * The broker and endpoint servicing submit, modify and cancel
	 */
	public static String key_service_broker = "service_broker";
	public static String key_service_endpoint = "service_endpoint";
	
	/*
	 * The broker and endpoint servicing the user specified (at submission)
	 * job CR and corresponding UIMA-AS services
	 */
	public static String key_job_broker = "job_broker";
	public static String key_job_endpoint = "job_endpoint";
	
	/*
	 * id: required for cancel/modify requests; error for submit request
	 */
	public static String key_id = "id";	
	public static String key_dpid = "dpid";	
	
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
