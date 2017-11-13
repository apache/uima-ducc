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
import org.apache.uima.ducc.common.utils.id.DuccId;

public class DuccCookies {
	
	private static DuccLogger duccLogger = DuccLogger.getLogger(DuccCookies.class);
	private static DuccId jobid = null;
	private static Messages messages = Messages.getInstance();

	public static final String cookieUri = "/";
	
	public static final String duccCookiePrefix = "DUCC";
	
	private static final String refreshmode = "refreshmode";
	private static final String valueRefreshmodeAutomatic = "automatic";
	private static final String valueRefreshmodeManual = "manual";
	
	private static final String jobs = "jobs";
	private static final String reservations = "reservations";
	private static final String services = "services";
	
	public static final String max = "max";
	public static final String users = "users";
	//private static final String qualifier = "qualifier";
	
	public static final String cookieRefreshMode = duccCookiePrefix+refreshmode;
	public static final String cookieJobsMax = duccCookiePrefix+jobs+max;
	public static final String cookieJobsUsers = duccCookiePrefix+jobs+users;
	//public static final String cookieJobsUsersQualifier = duccCookiePrefix+jobs+users+qualifier;
	public static final String cookieReservationsMax = duccCookiePrefix+reservations+max;
	public static final String cookieReservationsUsers = duccCookiePrefix+reservations+users;
	//public static final String cookieReservationsUsersQualifier = duccCookiePrefix+reservations+users+qualifier;
	public static final String cookieServicesMax = duccCookiePrefix+services+max;
	public static final String cookieServicesUsers = duccCookiePrefix+services+users;
	//public static final String cookieServicesUsersQualifier = duccCookiePrefix+services+users+qualifier;
	
	private static final String agents = "agents";
	
	public static final String cookieAgents = duccCookiePrefix+agents;
	public static final String valueAgentsShow = "show";
	
	private static final String table_style = "table_style";
	private static final String date_style = "date_style";
	private static final String description_style = "description_style";
	private static final String display_style = "display_style";
	private static final String filter_users_style = "filter_users_style";
	private static final String role = "role";
	
	private static final String uid = "uid";
	
	public static final String cookieStyleTable = duccCookiePrefix+table_style;
	public static final String cookieStyleDate = duccCookiePrefix+date_style;
	public static final String cookieStyleDescription = duccCookiePrefix+description_style;
	public static final String cookieStyleDisplay = duccCookiePrefix+display_style;
	public static final String cookieStyleFilterUsers = duccCookiePrefix+filter_users_style;
	public static final String cookieRole = duccCookiePrefix+role;
	
	public static final String cookieUid = duccCookiePrefix+uid;
	
	public static final String valueStyleDateLong = "long";
	public static final String valueStyleDateMedium = "medium";
	public static final String valueStyleDateShort = "short";
	public static final String valueStyleDateDefault = valueStyleDateLong;
	
	public static final String valueStyleDescriptionLong = "long";
	public static final String valueStyleDescriptionShort = "short";
	public static final String valueStyleDescriptionDefault = valueStyleDescriptionLong;
	
	public static final String valueStyleDisplayTextual = "textual";
	public static final String valueStyleDisplayVisual = "visual";
	public static final String valueStyleDisplayDefault = valueStyleDisplayTextual;
	
	public static final String valueStyleFilterUsersInclude = "include";
	public static final String valueStyleFilterUsersIncludePlusActive = "include+active";
	public static final String valueStyleFilterUsersExclude = "exclude";
	public static final String valueStyleFilterUsersExcludePlusActive = "exclude+active";
	
	public static final String valueRoleAdministrator = "administrator";
	public static final String valueRoleUser = "user";

	protected static final String getCookieKey(String name) {
		return duccCookiePrefix+"name";
	}
	
	public static String getCookie(String defaultValue, HttpServletRequest request, String name) {
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
		duccLogger.trace(methodName, null, messages.fetchLabel("name")+name+" "+messages.fetchLabel("value")+retVal);
		return retVal;
	}
	
	public static String getCookie(HttpServletRequest request, String name) {
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
		duccLogger.trace(methodName, null, messages.fetchLabel("name")+name+" "+messages.fetchLabel("value")+value);
	}
	
	protected static void expireCookie(HttpServletResponse response, String name, String value) {
		String methodName = "expireCookie";
		Cookie cookie = new Cookie(name, value);
		cookie.setMaxAge(0);
		response.addCookie(cookie);
		duccLogger.trace(methodName, null, messages.fetchLabel("name")+name+" "+messages.fetchLabel("value")+value);
	}
	
	public static enum DateStyle { Long, Medium, Short };
	
	public static DateStyle getDateStyle(HttpServletRequest request) {
		DateStyle dateStyle = DateStyle.Long;
		try {
			String cookie = getCookie(request,cookieStyleDate);
			if(cookie.equals(valueStyleDateLong)) {
				dateStyle = DateStyle.Long;
			}
			else if(cookie.equals(valueStyleDateMedium)) {
				dateStyle = DateStyle.Medium;
			}
			else if(cookie.equals(valueStyleDateShort)) {
				dateStyle = DateStyle.Short;
			}
		}
		catch(Exception e) {
		}
		return dateStyle;
	}
	
	public static enum RefreshMode { Automatic, Manual };
	
	public static RefreshMode getRefreshMode(HttpServletRequest request) {
		RefreshMode refreshMode = RefreshMode.Automatic;
		try {
			String cookie = getCookie(request,cookieRefreshMode);
			if(cookie.equals(valueRefreshmodeAutomatic)) {
				refreshMode = RefreshMode.Automatic;
			}
			else if(cookie.equals(valueRefreshmodeManual)) {
				refreshMode = RefreshMode.Manual;
			}
		}
		catch(Exception e) {
		}
		return refreshMode;
	}
	
	public static enum DescriptionStyle { Long, Short };
	
	public static DescriptionStyle getDescriptionStyle(HttpServletRequest request) {
		DescriptionStyle descriptionStyle = DescriptionStyle.Long;
		try {
			String cookie = getCookie(request,cookieStyleDescription);
			if(cookie.equals(valueStyleDescriptionLong)) {
				descriptionStyle = DescriptionStyle.Long;
			}
			else if(cookie.equals(valueStyleDescriptionShort)) {
				descriptionStyle = DescriptionStyle.Short;
			}
		}
		catch(Exception e) {
		}
		return descriptionStyle;
	}
	
	public static enum DisplayStyle { Textual, Visual };
	
	public static DisplayStyle getDisplayStyle(HttpServletRequest request) {
		DisplayStyle displayStyle = DisplayStyle.Textual;
		try {
			String cookie = getCookie(request,cookieStyleDisplay);
			if(cookie.equals(valueStyleDisplayTextual)) {
				displayStyle = DisplayStyle.Textual;
			}
			else if(cookie.equals(valueStyleDisplayVisual)) {
				displayStyle = DisplayStyle.Visual;
			}
		}
		catch(Exception e) {
		}
		return displayStyle;
	}
	
	public static enum FilterUsersStyle { Include, IncludePlusActive, Exclude, ExcludePlusActive };
	
	public static FilterUsersStyle getFilterUsersStyle(HttpServletRequest request) {
		FilterUsersStyle filterUsersStyle = FilterUsersStyle.Include;
		try {
			String cookie = getCookie(request,cookieStyleFilterUsers);
			if(cookie.equals(valueStyleFilterUsersInclude)) {
				filterUsersStyle = FilterUsersStyle.Include;;
			}
			else if(cookie.equals(valueStyleFilterUsersIncludePlusActive)) {
				filterUsersStyle = FilterUsersStyle.IncludePlusActive;
			}
			else if(cookie.equals(valueStyleFilterUsersExclude)) {
				filterUsersStyle = FilterUsersStyle.Exclude;
			}
			else if(cookie.equals(valueStyleFilterUsersExcludePlusActive)) {
				filterUsersStyle = FilterUsersStyle.ExcludePlusActive;
			}
		}
		catch(Exception e) {
		}
		return filterUsersStyle;
	}

	public static enum RequestRole { Administrator, User};
	
	public static RequestRole getRole(HttpServletRequest request) {
		RequestRole role = RequestRole.User;
		try {
			String cookie = getCookie(request,cookieRole);
			if(cookie.equals(valueRoleAdministrator)) {
				role = RequestRole.Administrator;;
			}
			/*
			else if(cookie.equals(valueRoleUser)) {
				role = RequestRole.User;
			}
			*/
		}
		catch(Exception e) {
		}
		return role;
	}
	
	public static String getUid(HttpServletRequest request) {
		String location = "getUid";
		String uid = null;
		try {
			String cookie = getCookie(null,request,cookieUid);
			uid = cookie;
			duccLogger.debug(location, jobid, cookieUid+":"+uid);
		}
		catch(Exception e) {
		}
		return uid;
	}
}
