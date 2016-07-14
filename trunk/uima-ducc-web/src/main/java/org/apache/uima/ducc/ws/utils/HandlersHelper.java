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
package org.apache.uima.ducc.ws.utils;

import javax.servlet.http.HttpServletRequest;

import org.apache.uima.ducc.common.internationalization.Messages;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.orchestrator.authentication.DuccWebAdministrators;
import org.apache.uima.ducc.ws.server.DuccCookies;
import org.apache.uima.ducc.ws.server.DuccWebSessionManager;

public class HandlersHelper {
	
	private static DuccLogger duccLogger = DuccLoggerComponents.getWsLogger(HandlersHelper.class.getName());
	private static Messages messages = Messages.getInstance();
	
	private static DuccId jobid = null;
	
	public static DuccWebAdministrators duccWebAdministrators = DuccWebAdministrators.getInstance();
	public static DuccWebSessionManager duccWebSessionManager = DuccWebSessionManager.getInstance();
	
	public static enum AuthorizationStatus { LoggedInOwner, LoggedInAdministrator, LoggedInNotOwner, LoggedInNotAdministrator, NotLoggedIn };
	
	private static boolean match(String s1, String s2) {
		String methodName = "match";
		duccLogger.trace(methodName, jobid, messages.fetch("enter"));
		boolean retVal = false;
		if(s1 != null) {
			if(s2 != null) {
				if(s1.trim().equals(s2.trim())) {
					retVal = true;
				}
			}
		}
		duccLogger.trace(methodName, jobid, messages.fetch("exit"));
		return retVal;
	}
	
	public static AuthorizationStatus getAuthorizationStatus(HttpServletRequest request, String resourceOwnerUserid) {
		String methodName = "getAuthorizationStatus";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		AuthorizationStatus retVal = AuthorizationStatus.NotLoggedIn;
		try {
			String text = "";
			boolean authenticated = duccWebSessionManager.isAuthentic(request);
			String userId = duccWebSessionManager.getUserId(request);
			if(authenticated) {
				if(match(resourceOwnerUserid,userId)) {
					text = "user "+userId+" is resource owner";
					retVal = AuthorizationStatus.LoggedInOwner;
				}
				else {
					DuccCookies.RequestRole requestRole = DuccCookies.getRole(request);
					switch(requestRole) {
					case User:
						text = "user "+userId+" is not resource owner "+resourceOwnerUserid;
						retVal = AuthorizationStatus.LoggedInNotOwner;
						break;
					case Administrator:
						if(duccWebAdministrators.isAdministrator(userId)) {
							text = "user "+userId+" is administrator";
							retVal = AuthorizationStatus.LoggedInAdministrator;
						}
						else {
							text = "user "+userId+" is not administrator ";
							retVal = AuthorizationStatus.LoggedInNotAdministrator;
						}
						break;
					}
				}
			}
			else {
				text = "user "+userId+" is not authenticated";
				retVal = AuthorizationStatus.NotLoggedIn;
			}
			duccLogger.debug(methodName, null, messages.fetch(text));
		}
		catch(Exception e) {
			duccLogger.error(methodName, null, e);
		}
		duccLogger.trace(methodName, null, messages.fetch("exit"));
		return retVal;
	}
	
	public static boolean isUserAuthorized(HttpServletRequest request, String resourceOwnerUserid) {
		String methodName = "isUserAuthorized";
		duccLogger.trace(methodName, jobid, messages.fetch("enter"));
		boolean retVal = false;
		try {
			AuthorizationStatus authorizationStatus = getAuthorizationStatus(request, resourceOwnerUserid);
			switch(authorizationStatus) {
			case LoggedInOwner:
			case LoggedInAdministrator:
				retVal = true;
				break;
			case LoggedInNotOwner:
			case LoggedInNotAdministrator:
			case NotLoggedIn:
				break;
			default:
				break;
			}
		}
		catch(Exception e) {
			duccLogger.error(methodName, jobid, e);
		}
		duccLogger.trace(methodName, jobid, messages.fetch("exit"));
		return retVal;
	}
}
