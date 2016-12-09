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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.uima.ducc.common.IDuccEnv;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.DuccPropertiesResolver;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.ws.server.DuccWebServer.ConfigValue;
import org.eclipse.jetty.server.Request;

public class DuccHandlerHttpRequestFilter extends DuccAbstractHandler {
	
	private static DuccLogger duccLogger = DuccLoggerComponents.getWsLogger(DuccHandlerHttpRequestFilter.class.getName());
	private static volatile DuccId jobid = null;
	
	// refresh interval for both ducc.properties and URI encryption exception list
	private long expiryMillis = 60*1000;
	
	// TOD of last refresh
	private AtomicLong saveMillisUpdate = new AtomicLong(0);
	
	// valid values for ducc.ws.user.data.access
	private enum UserDataAccessMode { unrestricted, encrypted, blocked };
	
	// present value of ducc.ws.user.data.access
	private volatile UserDataAccessMode userDataAccessMode = UserDataAccessMode.unrestricted;
	
	// present list of encryption exempt URI prefixes
	private volatile List<String> listEncryptionException = new ArrayList<String>();
	
	// file containing list of encryption exempt URI prefixes
	// - one per line
	// - comments start with #
	private String filePath = IDuccEnv.DUCC_HOME_DIR
					+File.separator
					+"webserver"
					+File.separator
					+"etc"
					+File.separator
					+"http-uri-encryption-exemption.list"
					;
	
	public DuccHandlerHttpRequestFilter(DuccWebServer duccWebServer) {
		super.init(duccWebServer);
	}
	
	// re-read file containing list of encryption exempt URI prefixes
	private void refreshListEncryptionException() {
		String location = "refreshListEncryptionException";
		try {
			List<String> listRefresh = new ArrayList<String>();
			BufferedReader br = null;
	        try {
	            br = new BufferedReader(new FileReader(filePath));
	            String line;
	            while ((line = br.readLine()) != null) {
	                line = line.trim();
	                if(line.startsWith("#")) {
	                	continue;
	                }
	                if(line.length() > 0) {
	                	listRefresh.add(line);
	                }
	            }
	            listEncryptionException = listRefresh;
	        } 
	        catch (IOException e) {
	        	duccLogger.trace(location, jobid, e);
	        } 
	        finally {
	            try {
	                if (br != null) {
	                    br.close();
	                }
	            } 
	            catch (IOException e) {
	            	duccLogger.error(location, jobid, e);
	            }
	        }
	        duccLogger.debug(location, jobid, "size:"+listEncryptionException.size());
		}
		catch (Exception e) {
        	duccLogger.error(location, jobid, e);
        }
	}
	
	private List<String> getListEncryptionException() {
		return listEncryptionException;
	}
	
	// re-read value of ducc.ws.user.data.access
	private void refreshUserDataAccessMode() {
		String location = "refreshUserDataAccessMode";
		try {
			DuccPropertiesResolver dpr = DuccPropertiesResolver.getInstance();
			String property = dpr.getFileProperty(DuccPropertiesResolver.ducc_ws_user_data_access);
			if(property != null) {
				property = property.trim();
				if(property.equals(UserDataAccessMode.unrestricted.name())) {
					userDataAccessMode = UserDataAccessMode.unrestricted;
				}
				else if(property.equals(UserDataAccessMode.encrypted.name())) {
					userDataAccessMode = UserDataAccessMode.encrypted;
				}
				else if(property.equals(UserDataAccessMode.blocked.name())) {
					userDataAccessMode = UserDataAccessMode.blocked;
				}
				else {
					String message = "no change, unrecognized value: "+property;
					duccLogger.warn(location, jobid, message);
				}
			}
			else {
				userDataAccessMode = UserDataAccessMode.unrestricted;
			}
		}
		catch(Exception e) {
			duccLogger.error(location, jobid, e);
		}
	}

	private UserDataAccessMode getUserDataAccessMode() {
		return userDataAccessMode;
	}
	
	private boolean isRestrictedHttp() {
		boolean retVal = false;
		UserDataAccessMode uda = getUserDataAccessMode();
		switch(uda) {
		default:
		case unrestricted:
			break;
		case encrypted:
		case blocked:
			retVal = true;
		}
		return retVal;
	}
	
	private boolean isRestrictedHttps() {
		boolean retVal = false;
		UserDataAccessMode uda = getUserDataAccessMode();
		switch(uda) {
		default:
		case unrestricted:
		case encrypted:
			break;
		case blocked:
			retVal = true;
		}
		return retVal;
	}
	
	// check if request scheme is restricted
	private boolean isRestrictedScheme(String scheme) {
		boolean retVal = false;
		if(scheme.equals("http")) {
			retVal = isRestrictedHttp();
		}
		else if(scheme.equals("https")) {
			retVal = isRestrictedHttps();
		}
		return retVal;
	}
	
	// if refresh time has elapsed, refresh ducc property and encryption exception list
	private void refresh() {
		long timeMillisNow = System.currentTimeMillis();
		long timeMillisLastUpdate = saveMillisUpdate.get();
		if(timeMillisNow-timeMillisLastUpdate > expiryMillis) {
			refreshUserDataAccessMode();
			refreshListEncryptionException();
			saveMillisUpdate.set(timeMillisNow);
		}
	}
	
	private String getContentForbiddenPage() {
		StringBuffer sb = new StringBuffer();
		sb.append("<html>");
		sb.append("<body>");
		sb.append("<p>");
		sb.append("Forbidden.");
		sb.append("</p>");
		sb.append("</body>");
		sb.append("</html>");
		return sb.toString();
	}
	
	private boolean isRestrictedUri(String reqUri) {
		boolean retVal = true;
		List<String> list = getListEncryptionException();
		for(String includeUri : list) {
			if(reqUri.startsWith(includeUri)) {
				retVal = false;
				break;
			}
		}
		return retVal;
	}
	
	// forbid http for mode "encrypted" & "blocked"
	// forbid https for mode "blocked"
	public void handleForbidden(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
		String location = "handleForbidden";
		if(!baseRequest.isHandled()) {
			String scheme = request.getScheme();
			if(isRestrictedScheme(scheme)) {
				String reqUri = request.getRequestURI();
				if(isRestrictedUri(reqUri)) {
					String content = getContentForbiddenPage();
					response.getWriter().println(content);
					StringBuffer sb = new StringBuffer();
					sb.append("forbidden:"+" "+reqUri);
					duccLogger.info(location, jobid, sb);
					response.setContentType("text/html;charset=utf-8");
					response.setStatus(HttpServletResponse.SC_FORBIDDEN);
					baseRequest.setHandled(true);
					DuccWebUtil.noCache(response);
				}
			}
		}
	}
	
	private String getContentRedirect(String url) {
		StringBuffer sb = new StringBuffer();
		sb.append("<html>");
		sb.append("<head>");
		sb.append("<script type=\"text/javascript\">");
		sb.append(" window.location.href = \""+url+"\"");
		sb.append("</script>");
		sb.append("</head>");
		sb.append("<body>");
		sb.append("Redirecting to ");
		sb.append("<a href='"+url+"'>"+url+"</a>");
		sb.append("</body>");
		sb.append("</html>");
		return sb.toString();
	}
	
	// redirect http to https when possible for mode "encrypted"
	public void handleRedirect(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
		String location = "handleRedirect";
		if(!baseRequest.isHandled()) {
			UserDataAccessMode uda = getUserDataAccessMode();
			switch(uda) {
			case encrypted:
				String scheme = request.getScheme();
				if(scheme.equals("http")) {
					String reqUri = request.getRequestURI();
					if(isRestrictedUri(reqUri)) {
						String url = request.getRequestURL().toString();
				        String portHttps = ""+ConfigValue.PortHttps.getInt(DuccPropertiesResolver.get(DuccPropertiesResolver.ducc_ws_port_ssl));
				        String portHttp = ""+ConfigValue.PortHttp.getInt(DuccPropertiesResolver.get(DuccPropertiesResolver.ducc_ws_port));
				        String s1Before = "http"+":";
				        String s1After = "https"+":";
				        String s2Before = ":"+portHttp;
				        String s2After = ":"+portHttps;
				        if(url.contains(s1Before)) {
				        	if(url.contains(s2Before)) {
				        		String redirect = new String(url);
				        		redirect = redirect.replace(s1Before, s1After);
				        		redirect = redirect.replace(s2Before, s2After);
				        		if(!url.equals(redirect)) {
				        			String content = getContentRedirect(redirect);
									response.getWriter().println(content);
									duccLogger.info(location, jobid, redirect);
									response.setContentType("text/html;charset=utf-8");
									response.setStatus(HttpServletResponse.SC_OK);
									baseRequest.setHandled(true);
									DuccWebUtil.noCache(response);
				        		}
				        	}
				        }
					}
				}
				break;
			case unrestricted:
			case blocked:
			default:
				break;
			}
		}
	}
	
	@Override
	public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
		String location = "handle";
		try{ 
			duccLogger.debug(location, jobid,request.toString());
			refresh();
			handleRedirect(target, baseRequest, request, response);
			handleForbidden(target, baseRequest, request, response);
		}
		catch(Throwable t) {
			if(isIgnorable(t)) {
				duccLogger.debug(location, jobid, t);
			}
			else {
				duccLogger.info(location, jobid, "", t.getMessage(), t);
				duccLogger.error(location, jobid, t);
			}
		}
	}
	
}
