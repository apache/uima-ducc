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
package org.apache.uima.ducc.ws.utils.alien;

import javax.servlet.http.HttpServletRequest;

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.ws.server.DuccWebSessionManager;

public class EffectiveUser {
	
	private static DuccLogger logger = DuccLogger.getLogger(EffectiveUser.class);
	private static DuccId jobid = null;
	
	protected static DuccWebSessionManager duccWebSessionManager = DuccWebSessionManager.getInstance();
	
	public static EffectiveUser create(HttpServletRequest request) {
		String name = null;
		if(duccWebSessionManager.isAuthentic(request)) {
			name = duccWebSessionManager.getUserId(request);
		}
		return new EffectiveUser(name);
	}
	
	protected static EffectiveUser create(String name) {
		return new EffectiveUser(name);
	}
	
	private String user = null;
	private boolean loggedin = false;
	
	private EffectiveUser(String user) {
		set(user);
	}
	
	private void set(String value) {
		String location = "set";
		if(value != null) {
			user = value;
			loggedin = true;
			logger.debug(location, jobid, "value: "+user);
		}
		else {
			user = System.getProperty("user.name");
			loggedin = false;
			logger.debug(location, jobid, "property: "+user);
		}
	}
	
	public String get() {
		return user;
	}
	
	public boolean isLoggedin() {
	    return loggedin;
	}
}
