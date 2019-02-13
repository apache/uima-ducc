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

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import javax.servlet.http.HttpServletRequest;

import org.apache.uima.ducc.common.internationalization.Messages;
import org.apache.uima.ducc.common.utils.AlienFile;
import org.apache.uima.ducc.common.utils.DuccLogger;
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
	
	private static DuccLogger duccLogger = DuccLogger.getLogger(HandlersHelper.class);
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
	
	public static boolean isServiceController(String user, String serviceId) {
		String methodName = "isServiceController";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		boolean retVal = false;
		duccLogger.debug(methodName, null, user, serviceId);
		try {
			ServicesRegistry servicesRegistry = ServicesRegistry.getInstance();
			ServicesRegistryMapPayload payload = servicesRegistry.findServiceById(serviceId);
			Properties meta = payload.meta;
			Properties svc = payload.svc;
			if(isServiceOwner(user,meta)) {
				retVal = true;
				duccLogger.debug(methodName, null, "owner");
			}
			else if(isServiceAdministrator(user,svc)) {
				retVal = true;
				duccLogger.debug(methodName, null, "admin");
			}
			else if(isDuccAdministrator(user,meta)) {
				retVal = true;
				duccLogger.debug(methodName, null, "ducc admin");
			}
		}
		catch(Exception e) {
			duccLogger.error(methodName, null, e);
		}
		duccLogger.trace(methodName, null, messages.fetch("exit"));
		return retVal;
	}
	
	public static boolean isLoggedIn(HttpServletRequest request) {
		boolean retVal = duccWebSessionManager.isAuthentic(request);
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
	 * Enumeration of possible data access permissions
	 */
	public enum DataAccessPermission { None, Read, Write };
	
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
				else {
					duccLogger.debug(location, getDuccId(meta), "svcOwner="+svcOwner+" "+retVal);
				}
			}
			else {
				duccLogger.debug(location, getDuccId(meta), "meta="+meta+" "+retVal);
			}
		}
		else {
			duccLogger.debug(location, getDuccId(meta), "user="+reqUser+" "+retVal);
		}
		return retVal;
	}
	
	/**
	 * Check if user is a service administrator
	 */
	private static boolean isServiceAdministrator(String reqUser, Properties svc) {
		String location = "isServiceAdministrator";
		boolean retVal = false;
		if(reqUser != null) {
			if(svc != null) {
				String svcAdministrators = svc.getProperty(IServicesRegistry.administrators);
				if(svcAdministrators != null) {
					String[] tokens = svcAdministrators.split("\\s+");
					for(String token : tokens) {
						if(reqUser.trim().equals(token.trim())) {
							retVal = true;
							duccLogger.debug(location, getDuccId(svc), "user="+reqUser+" "+retVal);
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
	protected static boolean isDuccAdministrator(String reqUser, Properties meta) {
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
	
	private static File devNull = new File("/dev/null");
	
	/**
	 * Look in service owner's security home for db.access file
	 * and use it's permissions to determine of logged-in user
	 * (or ducc user, as the case may be) is authorized by virtue
	 * of being able to read it
	 */
	private static boolean isServiceFileAccessForRead(String reqUser, Properties meta) {
		String location = "isServiceFileAccessForRead";
		boolean retVal= false;
		String DUCC_HOME = System.getProperty("DUCC_HOME");
		String duccmon_pwgen = DUCC_HOME+"/admin/db_access_check.py";
		String owner = meta.getProperty(IServicesRegistry.user);
		String looker = reqUser;
		if(looker == null) {
			duccLogger.debug(location, null, "looker not specified");
		}
		else {
			List<String> cmd = new ArrayList<String>();
			cmd.add(duccmon_pwgen);
			cmd.add("--owner");
			cmd.add(owner);
			cmd.add("--looker");
			cmd.add(looker);
			String cmdline = String.join(" ", cmd);
			duccLogger.debug(location, null, "cmdline: "+cmdline);
			ProcessBuilder pb = new ProcessBuilder(cmd);
			String authorizedCode = "1";
			try {
				pb = pb.redirectError(devNull);
				Process process = pb.start();
				String line;
				BufferedReader bri = new BufferedReader(new InputStreamReader(process.getInputStream()));
				duccLogger.trace(location, null, "read stdout: start");
				while ((line = bri.readLine()) != null) {
					duccLogger.debug(location, null, "stdout: "+line);
					if(line.startsWith(authorizedCode)) {
						duccLogger.trace(location, null, "authorized!");
						retVal = true;
					}
				}
				bri.close();
				duccLogger.trace(location, null, "read stdout: end");
				duccLogger.trace(location, null, "process waitfor: start");
				process.waitFor();
				duccLogger.trace(location, null, "process waitfor: end");
			}
			catch(Exception e) {
				duccLogger.error(location, null, e);
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
	public static DataAccessPermission getServiceAuthorization(Request request) {
		String methodName = "getServiceAuthorization";
		duccLogger.trace(methodName, jobid, messages.fetch("enter"));
		DataAccessPermission retVal = DataAccessPermission.None;
		try {
			if(request == null) {
				// request not specified ==> DataAccessPermissions.None
			}
			else {
				String reqUser = duccWebSessionManager.getUserId(request);
				String name = request.getParameter("name");
				ServicesRegistry servicesRegistry = ServicesRegistry.getInstance();
				ServicesRegistryMapPayload payload = servicesRegistry.findService(name);
				if(payload == null) {
					// service not found ==> DataAccessPermissions.None
				}
				else {
					Properties svc = payload.svc;
					Properties meta = payload.meta;
					// write access for service owner
					if(isServiceOwner(reqUser, meta)) {
						retVal = DataAccessPermission.Write;
					}
					// write access for service administrator
					else if(isServiceAdministrator(reqUser, svc)) {
						retVal = DataAccessPermission.Write;
					}
					// Don't permit write access for ducc administrator
					//else if(isDuccAdministrator(reqUser, meta)) {
					//	retVal = DataAccessPermissions.Write;
					//}
					// read access only if user can read db.access file
					else if(isServiceFileAccessForRead(reqUser, meta)) {
						retVal = DataAccessPermission.Read;
					}
					else {
						// none of the above ==> DataAccessPermissions.None
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
	
	/**
	 * Determine resource authorization level, given resource owner and resource access requester
	 */
	public static DataAccessPermission getResourceAuthorization(String resOwner, String reqUser) {
		String methodName = "getServiceAuthorization";
		duccLogger.trace(methodName, jobid, messages.fetch("enter"));
		DataAccessPermission retVal = DataAccessPermission.None;
		try {
			if(resOwner == reqUser) {
				retVal = DataAccessPermission.Read;
			}
			else {
				String home = getSecurityHome(resOwner.trim());
				if(home != null) {
					if(!home.endsWith(File.separator)) {
						home = home+File.separator;
					}
					String path = home+".ducc"+File.separator+"db.access";
					boolean readable = isFileReadable(reqUser, path);
					if(readable) {
						retVal = DataAccessPermission.Read;
						duccLogger.debug(methodName, jobid, "owner="+resOwner+" "+"user="+reqUser+" "+retVal);
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
	
	/**
	 * Determine resource accessibility, given resource owner and resource access requester
	 */
	public static boolean isResourceAuthorized(String resOwner, String reqUser) {
		boolean retVal = false;
		DataAccessPermission dap = getResourceAuthorization(resOwner, reqUser);
		switch(dap) {
			case Read:
			case Write:
				retVal = true;
				break;
			default:
				break;
		}
		return retVal;
	}
}
