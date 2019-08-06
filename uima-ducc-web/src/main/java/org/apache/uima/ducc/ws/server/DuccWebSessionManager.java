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
package org.apache.uima.ducc.ws.server;

import java.security.SecureRandom;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.database.login.DbUserLogin;

/*
 * Class to manage user login/logout, in coordination with browser and cookies
 */

public class DuccWebSessionManager {
	
	private static DuccLogger duccLogger = DuccLogger.getLogger(DuccWebSessionManager.class);
	private static DuccId jobid = null;
	
	public static DuccWebSessionManager instance = new DuccWebSessionManager();
	
	public static DuccWebSessionManager getInstance() {
		return instance;
	}
	
	private static DbUserLogin dbUserLogin = new DbUserLogin(duccLogger);
	
	private static SecureRandom sr = new SecureRandom();
	
	private static final int SEGMENTS = 8;

	// Token given to browser.  When present upon subsequent calls
	// user is considered logged-in.
	private String generateValidationId() {
		StringBuffer sb = new StringBuffer();
		sb.append(sr.nextLong());
		int segments = SEGMENTS;
		for(int i=0; i<segments; i++) {
			sb.append(sr.nextLong());
		}
		return sb.toString();
	}
	
	// login user
	public void login(HttpServletRequest request, HttpServletResponse response, String userId) {
		String location = "login";
		if(request == null) {
			duccLogger.debug(location, jobid, "request is null");
			return;
		}
		if(response == null) {
			duccLogger.debug(location, jobid, "response is null");
			return;
		}
		if(userId == null) {
			duccLogger.debug(location, jobid, "userId is null");
			return;
		}
		// generate validation id
		String validationId = generateValidationId();
		// tell browser
		DuccCookies.setLoginUid(response, userId);
		DuccCookies.setLoginToken(response, validationId);
		duccLogger.debug(location, jobid, userId, validationId);
		// tell database
		dbUserLogin.addOrReplace(userId, validationId);
		return;
	}
	
	// louout user
	public boolean logout(HttpServletRequest request, HttpServletResponse response, String userId) {
		String location = "logout";
		boolean retVal = false;
		if(request == null) {
			duccLogger.debug(location, jobid, "request is null");
			return retVal;
		}
		if(response == null) {
			duccLogger.debug(location, jobid, "response is null");
			return retVal;
		}
		if(userId == null) {
			duccLogger.debug(location, jobid, "userId is null");
			return retVal;
		}
		retVal = isAuthentic(request);
		// tell browser
		DuccCookies.expireLoginToken(response);
		// tell database
		dbUserLogin.delete(userId);
		return retVal;
	}
	
	// check token from db with token present by browser cookie
	private boolean stringCompare(String s1, String s2) {
		boolean retVal = false;
		if(s1 != null) {
			if(s2 != null) {
				retVal = s1.equals(s2);
			}
		}
		return retVal;
	}
	
	// check if browser userid+token match same in db
	public boolean isAuthentic(HttpServletRequest request) {
		String location = "isAuthentic";
		boolean retVal = false;
		if(request == null) {
			duccLogger.debug(location, jobid, "request is null");
			return false;
		}
		String userId = getUserId(request);
		// fetch browser
		String s1 = DuccCookies.getLoginToken(request);
		duccLogger.debug(location, jobid, "cookie  ", retVal, userId, s1);
		// fetch database
		String s2 = dbUserLogin.fetch(userId);
		duccLogger.debug(location, jobid, "database", retVal, userId, s2);
		// compare
		retVal = stringCompare(s1,s2);
		return retVal;
	}
	
	// fetch userid from browser
	public String getUserId(HttpServletRequest request) {
		String location = "getUserId";
		String retVal = null;
		if(request == null) {
			duccLogger.debug(location, jobid, "request is null");
			return retVal;
		}
		// fetch browser
		retVal = DuccCookies.getLoginUid(request);
		duccLogger.debug(location, jobid, retVal);
		return retVal;
	}
}
