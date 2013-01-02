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

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.uima.ducc.common.internationalization.Messages;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;


public class DuccWebUtil {
	
	private static DuccLogger duccLogger = DuccLoggerComponents.getWsLogger(DuccWebUtil.class.getName());
	private static Messages messages = Messages.getInstance();
	
	public static final String cookieUri = "/";
	
	private static final String join = ":";
	
	private static final String application = "ducc";
	
	public static final String cookieUser = application+join+"user";
	public static final String cookieSession = application+join+"session";
	
	private static final String jobs = "jobs";
	private static final String reservations = "reservations";
	private static final String services = "services";
	
	private static final String max = "max";
	private static final String users = "users";
	private static final String qualifier = "qualifier";
	
	public static final String cookieJobsMax = application+join+jobs+max;
	public static final String cookieJobsUsers = application+join+jobs+users;
	public static final String cookieJobsUsersQualifier = application+join+jobs+users+qualifier;
	public static final String cookieReservationsMax = application+join+reservations+max;
	public static final String cookieReservationsUsers = application+join+reservations+users;
	public static final String cookieReservationsUsersQualifier = application+join+reservations+users+qualifier;
	public static final String cookieServicesMax = application+join+services+max;
	public static final String cookieServicesUsers = application+join+services+users;
	public static final String cookieServicesUsersQualifier = application+join+services+users+qualifier;
	
	protected static final String getCookieKey(String name) {
		return application+join+"name";
	}
	
	protected static String getCookie(String defaultValue, HttpServletRequest request, String name) {
		String methodName = "getCookie";
		String retVal = defaultValue;
		Cookie[] cookies = request.getCookies();
		if(cookies != null) {
			for(int i=0; i < cookies.length; i++) {
				Cookie cookie = cookies[i];
				if(cookie != null) {
					String cookieName = cookie.getName();
					if(cookieName != null) {
						if(cookieName.equals(name)) {
							retVal = cookie.getValue();
							break;
						}
					}
				}
			}
		}
		duccLogger.debug(methodName, null, messages.fetchLabel("name")+name+" "+messages.fetchLabel("value")+retVal);
		return retVal;
	}
	
	protected static String getCookie(HttpServletRequest request, String name) {
		return getCookie("",request,name);
	}
	
	protected static String getCookieOrNull(HttpServletRequest request, String name) {
		return getCookie(null,request,name);
	}
	
	protected static void putCookie(HttpServletResponse response, String name, String value) {
		String methodName = "putCookie";
		Cookie cookie = new Cookie(name, value);
		cookie.setPath(cookieUri);
		response.addCookie(cookie);
		duccLogger.debug(methodName, null, messages.fetchLabel("name")+name+" "+messages.fetchLabel("value")+value);
	}
	
	protected static void expireCookie(HttpServletResponse response, String name, String value) {
		String methodName = "expireCookie";
		Cookie cookie = new Cookie(name, value);
		cookie.setMaxAge(0);
		response.addCookie(cookie);
		duccLogger.debug(methodName, null, messages.fetchLabel("name")+name+" "+messages.fetchLabel("value")+value);
	}
	
	/*
	@Deprecated
	protected String getUserHome(String userName) throws IOException{
	    return new BufferedReader(new InputStreamReader(Runtime.getRuntime().exec(new String[]{"sh", "-c", "echo ~" + userName}).getInputStream())).readLine();
	}
	*/

}
