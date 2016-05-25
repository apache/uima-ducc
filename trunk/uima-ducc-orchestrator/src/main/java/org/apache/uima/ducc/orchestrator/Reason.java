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
package org.apache.uima.ducc.orchestrator;

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.cli.SpecificationProperties;

public class Reason {
	
	private static final DuccLogger logger = DuccLoggerComponents.getOrLogger(Reason.class.getName());
	
	private String user = null;
	private String role = null;
	private String message = null;
	
	public Reason(DuccId duccId, String user, String role, String message) {
		String methodName = "Reason";
		setUser(user);
		setRole(role);
		setMessage(message);
		logger.info(methodName, duccId, "user:"+user+" "+"role:"+role+" "+"message:"+message);
	}
	
	private String normalize(String value) {
		String retVal = value;
		if(value != null) {
			retVal = retVal.trim();
			if(retVal.startsWith("\"")) {
				int start = 1;
				retVal = retVal.substring(start);
				if(retVal.endsWith("\"")) { 
					start = 0;
					int end = retVal.length()-1;
					retVal = retVal.substring(start, end);
				}
			}
		}
		return retVal;
	}
	
	private void setUser(String value) {
		user = normalize(value);
	}
	
	private void setRole (String value) {
		role = normalize(value);
	}
	
	private void setMessage(String value) {
		message = normalize(value);
	}
	
	private boolean isRoleAdministrator() {
		boolean retVal = false;
		if(role != null) {
			if(role.equals(SpecificationProperties.key_role_administrator)) {
				retVal = true;
			}
		}
		return retVal;
	}
	
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		if(isRoleAdministrator()) {
			if(user != null) {
				sb.append("Canceled by "+user);
				if(message != null) {
					sb.append(" : ");
					sb.append(message);
				}
			}
			else {
				if(message != null) {
					sb.append(message);
				}
			}
		}
		else {
			if(message != null) {
				sb.append(message);
			}
		}
		String retVal = sb.toString();
		if(retVal.length() > 0) {
			retVal = "\""+retVal+"\"";
		}
		return retVal;
	}
}
