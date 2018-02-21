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
import java.util.ArrayList;
import java.util.List;

import org.apache.uima.ducc.common.utils.DuccProperties;

/**
 * Job characteristics provided by user.
 */
@SuppressWarnings("serial")
public class SpecificationProperties extends DuccProperties implements Serializable {
	
	public static String key_cancel_on_interrupt = "cancel_on_interrupt";
	
	public static String key_submit_errors = "submit_errors";
	public static String key_submit_warnings = "submit_warnings";
	
	public static String key_user = "user";
	public static String key_date = "date";
	
	public static String key_role_administrator = "role_administrator";
	public static String key_role_user = "role_user";
	
	public static String key_description = "description";
	
	public static String key_scheduling_class = "scheduling_class";
	
	public static String key_specification = "specification";

	public static String key_signature = "signature";
	
	public static String key_submitter_pid_at_host = "submitter_pid_at_host";
	
	public static String key_reason = "reason";
	
	/*
	 * A list keys for which the "user" provided the value.
	 * The values for keys not listed here were therefore
	 * provided by "system" (aka DUCC).
	 */
	private List<String> userProvided = new ArrayList<String>();
	
	/*
	 * Disable place-holder expansion in DuccProperties.getProperty as CLI does it when processing the options,
	 * and any unresolved ones must be left asis, e.g. DUCC_SERVICE_INSTANCE
	 * All CLI apis should use this or a subclass of it.
	 */
	public SpecificationProperties() {
		resolvePlaceholders = false;
	}
	
	/*
	 * <convenience methods to manage "userProvided" list>
	 */
	public boolean isUserProvided(String name) {
		boolean retVal = false;
		try {
			if(name != null) {
				retVal = userProvided.contains(name);
			}
		}
		catch(Exception e) {
			// legacy object
		}
		return retVal;
	}
	public void addUserProvided(String name) {
		try {
			if(name != null) {
				if(!isUserProvided(name)) {
					userProvided.add(name);
				}
			}
		}
		catch(Exception e) {
			// legacy object
		}
	}
	public void delUserProvided(String name) {
		try {
			if(name != null) {
				if(isUserProvided(name)) {
					userProvided.remove(name);
				}
			}
		}
		catch(Exception e) {
			// legacy object
		}
	}
	/*
	 * </convenience methods to manage "userProvided" list>
	 */
}
