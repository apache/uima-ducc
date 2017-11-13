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
import java.util.concurrent.ConcurrentHashMap;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.id.DuccId;

public class DuccWebSessionManager {
	
	private static DuccLogger duccLogger = DuccLogger.getLogger(DuccWebSessionManager.class);
	private static DuccId jobid = null;
	
	public static DuccWebSessionManager instance = new DuccWebSessionManager();
	
	public static DuccWebSessionManager getInstance() {
		return instance;
	}
	
	private static SecureRandom sr = new SecureRandom();
	
	public static final String ducc_user_id = "ducc.user.id";
	public static final String ducc_validation_id = "ducc.validation.id";
	
	private static final int SEGMENTS = 8;
	
	private class IdSet {
		public String sessionId;
		public String validationId;
	}
	
	private ConcurrentHashMap<String,IdSet> map = new ConcurrentHashMap<String,IdSet>();

	private String generateValidationId() {
		StringBuffer sb = new StringBuffer();
		sb.append(sr.nextLong());
		int segments = SEGMENTS;
		for(int i=0; i<segments; i++) {
			sb.append(sr.nextLong());
		}
		return sb.toString();
	}
	
	private void iRemove(String method, String userId, IdSet idSet) {
		String location = "iRemove"+"."+method;
		if(idSet != null) {
			duccLogger.info(location, jobid, "uid:"+userId);
			duccLogger.info(location, jobid, "sid:"+idSet.sessionId);
			duccLogger.info(location, jobid, "vid:"+idSet.validationId);
		}
	}
	
	private void iPut(String method, String userId, IdSet idSet) {
		String location = "iPut"+"."+method;
		if(idSet != null) {
			duccLogger.info(location, jobid, "uid:"+userId);
			duccLogger.info(location, jobid, "sid:"+idSet.sessionId);
			duccLogger.info(location, jobid, "vid:"+idSet.validationId);
		}
	}
	
	public void login(HttpServletRequest request, String userId) {
		String location = "login";
		if(request == null) {
			duccLogger.debug(location, jobid, "request is null");
			return;
		}
		if(userId == null) {
			duccLogger.debug(location, jobid, "userId is null");
			return;
		}
		HttpSession session = request.getSession();
		if(session == null) {
			duccLogger.debug(location, jobid, "session is null");
			return;
		}
		String sessionId =  session.getId();
		if(sessionId == null) {
			duccLogger.debug(location, jobid, "sessionId is null");
			return;
		}
		iRemove(location, userId, map.get(userId));
		String validationId = generateValidationId();
		session.setAttribute(ducc_validation_id, validationId);
		session.setAttribute(ducc_user_id, userId);
		IdSet idSet = new IdSet();
		idSet.validationId = validationId;
		idSet.sessionId = sessionId;
		map.put(userId, idSet);
		iPut(location, userId, map.get(userId));
		return;
	}
	
	public boolean logout(HttpServletRequest request) {
		String location = "logout";
		boolean retVal = false;
		if(request == null) {
			duccLogger.debug(location, jobid, "request is null");
			return retVal;
		}
		HttpSession session = request.getSession();
		if(session == null) {
			duccLogger.debug(location, jobid, "session is null");
			return retVal;
		}
		String userId = (String) session.getAttribute(ducc_user_id);
		if(userId == null) {
			duccLogger.debug(location, jobid, "userId is null");
			return retVal;
		}
		String validationId = (String) session.getAttribute(ducc_validation_id);
		if(validationId == null) {
			duccLogger.debug(location, jobid, "validationId is null");
			return retVal;
		}
		String sessionId =  session.getId();
		if(sessionId == null) {
			duccLogger.debug(location, jobid, "sessionId is null");
			return retVal;
		}
		IdSet idSet = map.get(userId);
		if(idSet == null) {
			duccLogger.debug(location, jobid, "idSet is null");
			return retVal;
		}
		if(!validationId.equals(idSet.validationId)) {
			duccLogger.debug(location, jobid, "given:"+validationId);
			duccLogger.debug(location, jobid, "known:"+idSet.validationId);
			duccLogger.debug(location, jobid, "validation mismatch!");
			return retVal;
		}
		if(!sessionId.equals(idSet.sessionId)) {
			duccLogger.debug(location, jobid, "given:"+sessionId);
			duccLogger.debug(location, jobid, "known:"+idSet.sessionId);
			duccLogger.debug(location, jobid, "session mismatch!");
			return retVal;
		}
		session.removeAttribute(ducc_validation_id);
		session.removeAttribute(ducc_user_id);
		map.remove(userId);
		iRemove(location, userId, idSet);
		retVal = true;
		return retVal;
	}
	
	public boolean isAuthentic(HttpServletRequest request) {
		String location = "isAuthentic";
		if(request == null) {
			duccLogger.debug(location, jobid, "request is null");
			return false;
		}
		HttpSession session = request.getSession();
		if(session == null) {
			duccLogger.debug(location, jobid, "session is null");
			return false;
		}
		String userId = (String) session.getAttribute(ducc_user_id);
		if(userId == null) {
			duccLogger.debug(location, jobid, "userId is null");
			return false;
		}
		String validationId = (String) session.getAttribute(ducc_validation_id);
		if(validationId == null) {
			duccLogger.debug(location, jobid, "validationId is null");
			return false;
		}
		String sessionId =  session.getId();
		if(sessionId == null) {
			duccLogger.debug(location, jobid, "sessionId is null");
			return false;
		}
		IdSet idSet = map.get(userId);
		if(idSet == null) {
			duccLogger.debug(location, jobid, "idSet is null");
			return false;
		}
		if(!validationId.equals(idSet.validationId)) {
			duccLogger.debug(location, jobid, "given:"+validationId);
			duccLogger.debug(location, jobid, "known:"+idSet.validationId);
			duccLogger.debug(location, jobid, "validation mismatch!");
			return false;
		}
		if(!sessionId.equals(idSet.sessionId)) {
			duccLogger.debug(location, jobid, "given:"+sessionId);
			duccLogger.debug(location, jobid, "known:"+idSet.sessionId);
			duccLogger.debug(location, jobid, "session mismatch!");
			return false;
		}
		return true;
	}
	
	public String getUserId(HttpServletRequest request) {
		String retVal = null;
		String location = "getUserId";
		if(request == null) {
			duccLogger.debug(location, jobid, "request is null");
			return retVal;
		}
		HttpSession session = request.getSession();
		if(session == null) {
			duccLogger.debug(location, jobid, "session is null");
			return retVal;
		}
		retVal = (String) session.getAttribute(ducc_user_id);
		return retVal;
	}
}
