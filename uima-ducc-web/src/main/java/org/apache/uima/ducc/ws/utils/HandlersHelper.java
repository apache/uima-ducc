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

import java.io.File;
import java.util.Properties;

import javax.servlet.http.HttpServletRequest;

import org.apache.uima.ducc.common.internationalization.Messages;
import org.apache.uima.ducc.common.utils.AlienFile;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.DuccPropertiesResolver;
import org.apache.uima.ducc.common.utils.LinuxUtils;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.orchestrator.authentication.DuccWebAdministrators;
import org.apache.uima.ducc.ws.registry.IServicesRegistry;
import org.apache.uima.ducc.ws.registry.ServicesRegistry;
import org.apache.uima.ducc.ws.registry.ServicesRegistryMapPayload;
import org.apache.uima.ducc.ws.server.DuccCookies;
import org.apache.uima.ducc.ws.server.DuccWebSessionManager;
import org.eclipse.jetty.server.Request;

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
	
	/**
	 * Enumeration of possible Service Authorizations
	 */
	public enum ServiceAuthorization { None, Read, Write };
	
	/**
	 * Transform service number into DuccId
	 */
	private static DuccId getDuccId(Properties meta) {
		String location = "getDuccId";
		DuccId retVal = null;
		String numeric_id = null;
		try {
			numeric_id = meta.getProperty(IServicesRegistry.numeric_id);
			Long svcNo = new Long(numeric_id);
			retVal = new DuccId(svcNo);
		}
		catch(Exception e) {
			duccLogger.error(location, retVal, numeric_id);
		}
		return retVal;
	}
	
	/**
	 * Check if user is service owner
	 */
	private static boolean isServiceOwner(String reqUser, Properties meta) {
		String location = "isServiceOwner";
		boolean retVal = false;
		if(reqUser != null) {
			if(meta != null) {
				String svcOwner = meta.getProperty(IServicesRegistry.user);
				if(svcOwner != null) {
					if(reqUser.trim().equals(svcOwner.trim())) {
						retVal = true;
						duccLogger.debug(location, getDuccId(meta), "user="+reqUser+" "+retVal);
					}
				}
			}
		}
		return retVal;
	}
	
	/**
	 * Check if user is a service administrator
	 */
	private static boolean isServiceAdministrator(String reqUser, Properties meta) {
		String location = "isServiceAdministrator";
		boolean retVal = false;
		if(reqUser != null) {
			if(meta != null) {
				String svcAdministrators = meta.getProperty(IServicesRegistry.administrators);
				if(svcAdministrators != null) {
					String[] tokens = svcAdministrators.split("\\s+");
					for(String token : tokens) {
						if(reqUser.trim().equals(token.trim())) {
							retVal = true;
							duccLogger.debug(location, getDuccId(meta), "user="+reqUser+" "+retVal);
							break;
						}
					}
				}
			}
		}
		return retVal;
	}

	/**
	 * Check if user is a ducc administrator
	 */
	private static boolean isDuccAdministrator(String reqUser, Properties meta) {
		String location = "isDuccAdministrator";
		boolean retVal = false;
		if(reqUser != null) {
			retVal = DuccWebAdministrators.getInstance().isAdministrator(reqUser);
			if(retVal) {
				duccLogger.debug(location, getDuccId(meta), "user="+reqUser+" "+retVal+" "+"(forced)");
			}
		}
		return retVal;
	}
	
	/**
	 * Check if user has permission to read specified path-to-file
	 */
	public static boolean isFileReadable(String user, String path) {
		String location = "isFileReadable";
		boolean retVal = false;
		try {
			AlienFile alienFile = new AlienFile(user, path);
			String contents = alienFile.getString();
			if(contents != null) {
				retVal = true;
			}
		}
		catch(Exception e) {
			duccLogger.debug(location, jobid, e);
		}
		return retVal;
	}
	
	/**
	 * Look in service owner's security home for db.access file
	 * and use it's permissions to determine of logged-in user
	 * (or ducc user, as the case may be) is authorized by virtue
	 * of being able to read it
	 */
	private static boolean isServiceFileAccessForRead(String reqUser, Properties meta) {
		String location = "isServiceFileAccessForRead";
		boolean retVal = false;
		if(reqUser == null) {
			reqUser = System.getProperty("user.name");
		}
		if(reqUser != null) {
			if(meta != null) {
				String svcOwner = meta.getProperty(IServicesRegistry.user);
				if(svcOwner != null) {
					String home = getSecurityHome(svcOwner.trim());
					if(home != null) {
						if(!home.endsWith(File.separator)) {
							home = home+File.separator;
						}
						String path = home+".ducc"+File.separator+"db.access";
						retVal = isFileReadable(reqUser, path);
						if(retVal) {
							duccLogger.debug(location, getDuccId(meta), "user="+reqUser+" "+retVal);
						}
					}
				}
			}
		}
		return retVal;
	}
	
	/**
	 * Composite security home as specified in ducc.properties, 
	 * else user's home directory
	 */
	public static String getSecurityHome(String user) {
		// Check if in test mode with simulated users
		String runmode = DuccPropertiesResolver.get(DuccPropertiesResolver.ducc_runmode);
		boolean testMode = runmode != null && runmode.equals("Test");
		// Get special security home directory if specified
		// In test-mode (single-user) must use the current userid as the
		// simulated user doesn't have a home
		String dirHome = null;
		String ducc_security_home = DuccPropertiesResolver.get(DuccPropertiesResolver.ducc_security_home);
		if(ducc_security_home != null && !ducc_security_home.isEmpty()) {
			String realUser = testMode ? System.getProperty("user.name") : user;
			dirHome = ducc_security_home + File.separator + realUser;
		}
		if(dirHome == null) {
			if(testMode) {
				dirHome = System.getProperty("user.home");
			} 
			else {
				dirHome = LinuxUtils.getUserHome(user);
			}
		}
		return dirHome;
	}
	
	/**
	 * Determine service authorization level for request
	 */
	public static ServiceAuthorization getServiceAuthorization(Request request) {
		String methodName = "getServiceAuthorization";
		duccLogger.trace(methodName, jobid, messages.fetch("enter"));
		ServiceAuthorization retVal = ServiceAuthorization.None;
		try {
			if(request == null) {
				// request not specified ==> ServiceAuthorization.None
			}
			else {
				String reqUser = duccWebSessionManager.getUserId(request);
				String name = request.getParameter("name");
				ServicesRegistry servicesRegistry = ServicesRegistry.getInstance();
				ServicesRegistryMapPayload payload = servicesRegistry.findService(name);
				if(payload == null) {
					// service not found ==> ServiceAuthorization.None
				}
				else {
					Properties svc = payload.svc;
					Properties meta = payload.meta;
					// write access for service owner
					if(isServiceOwner(reqUser, meta)) {
						retVal = ServiceAuthorization.Write;
					}
					// write access for service administrator
					else if(isServiceAdministrator(reqUser, svc)) {
						retVal = ServiceAuthorization.Write;
					}
					// Don't permit write access for ducc administrator
					//else if(isDuccAdministrator(reqUser, meta)) {
					//	retVal = ServiceAuthorization.Write;
					//}
					// read access only if user can read db.access file
					else if(isServiceFileAccessForRead(reqUser, meta)) {
						retVal = ServiceAuthorization.Read;
					}
					else {
						// none of the above ==> ServiceAuthorization.None
					}
				}
			}
		}
		catch(Exception e) {
			duccLogger.error(methodName, jobid, e);
		}
		duccLogger.trace(methodName, jobid, messages.fetch("exit"));
		return retVal;
	}
}
