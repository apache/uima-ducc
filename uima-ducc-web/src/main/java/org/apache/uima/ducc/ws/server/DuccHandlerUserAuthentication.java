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

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.uima.ducc.common.authentication.IAuthenticationManager.Role;
import org.apache.uima.ducc.common.authentication.IAuthenticationResult;
import org.apache.uima.ducc.common.internationalization.Messages;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.ws.authentication.DuccAuthenticator;
import org.eclipse.jetty.server.Request;

public class DuccHandlerUserAuthentication extends DuccAbstractHandler {
	
	private static DuccLogger duccLogger = DuccLoggerComponents.getWsLogger(DuccHandlerUserAuthentication.class.getName());
	private static Messages messages = Messages.getInstance();
	private static DuccId jobid = null;

	public final String userLogout 					= duccContextUser+"-logout";
	public final String userLogin 					= duccContextUser+"-login";
	public final String userAuthenticationStatus 	= duccContextUser+"-authentication-status";
	
	private DuccAuthenticator duccAuthenticator = DuccAuthenticator.getInstance();
	
	private DuccWebSessionManager duccWebSessionManager = DuccWebSessionManager.getInstance();
	
	public DuccHandlerUserAuthentication() {
	}
	
	protected boolean isAuthenticated(HttpServletRequest request,HttpServletResponse response) {
		String methodName = "isAuthenticated";
		boolean retVal = false;
		try {
			retVal = duccWebSessionManager.isAuthentic(request);
		}
		catch(Exception e) {
			duccLogger.error(methodName, jobid, e);
		}
		return retVal;
	}
	
	private void handleDuccServletAuthenticationStatus(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletStatus";
		duccLogger.trace(methodName, jobid, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		boolean userAuth = isAuthenticated(request,response);
        if (userAuth) {
        	sb.append("<span class=\"status_on\">");
        	sb.append("logged in");
        	sb.append("<span>");
        }
        else {
        	sb.append("<span class=\"status_off\">");
        	sb.append("logged out");
        	sb.append("<span>");
        }
		response.getWriter().println(sb);
		duccLogger.trace(methodName, jobid, messages.fetch("exit"));
	}
	
	private void handleDuccServletLogout(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletLogout";
		duccLogger.trace(methodName, jobid, messages.fetch("enter"));
		String userId = null;
		StringBuffer sb = new StringBuffer();
		try {
			userId = duccWebSessionManager.getUserId(request);
			boolean result = duccWebSessionManager.logout(request);
			if(result) {
				duccLogger.info(methodName, jobid, messages.fetch("logout ")+userId+" "+messages.fetch("success"));
				sb.append("success");
			}
			else {
				duccLogger.info(methodName, jobid, messages.fetch("logout ")+userId+" "+messages.fetch("failed"));
				sb.append("failure");
			}
			
		}
		catch(Throwable t) {
			sb.append("failure"+" "+t.getMessage());
			duccLogger.error(methodName, jobid, "userid="+userId);
		}
		response.getWriter().println(sb);
		duccLogger.trace(methodName, jobid, messages.fetch("exit"));
	}	
	
	private void handleDuccServletLogin(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletLogin";
		duccLogger.trace(methodName, jobid, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		String userId = request.getParameter("userid");
		String password = request.getParameter("password");
		try {
			if((userId == null) || (userId.trim().length() == 0)) {
				duccLogger.info(methodName, jobid, messages.fetch("login ")+userId+" "+messages.fetch("failed"));
				sb.append("failure");
			}
			else if(duccAuthenticator.isPasswordChecked() && (((password == null) || (password.trim().length() == 0)))) {
				duccLogger.info(methodName, jobid, messages.fetch("login ")+userId+" "+messages.fetch("failed"));
				sb.append("failure");
			}
			else {
				Role role = Role.User;
				String domain = null;
				if(userId != null) {
					if(userId.contains("@")) {
						String[] parts = userId.split("@",2);
						userId = parts[0];
						domain = parts[1];
					}
				}
				duccLogger.debug(methodName, jobid, messages.fetchLabel("version")+duccAuthenticator.getVersion());
				IAuthenticationResult result1 = duccAuthenticator.isAuthenticate(userId, domain, password);
				IAuthenticationResult result2 = duccAuthenticator.isGroupMember(userId, domain, role);
				duccLogger.debug(methodName, jobid, messages.fetch("login ")+userId+" "+"group reason: "+result2.getReason());
				if(result1.isSuccess() && result2.isSuccess()) {
					duccWebSessionManager.login(request, userId);
					duccLogger.info(methodName, jobid, messages.fetch("login ")+userId+" "+messages.fetch("success"));
					sb.append("success");
				}
				else {
					IAuthenticationResult result;
					if(!result1.isSuccess()) {
						result = result1;
					}
					else {
						result = result2;
					}
					int code = result.getCode();
					String reason = result.getReason();
					Exception exception = result.getException();
					StringBuffer text = new StringBuffer();
					text.append("code:"+code);
					if(reason != null) {
						text.append(", "+"reason:"+reason);
					}
					sb.append("failure"+" "+text);
					if(exception != null) {
						text.append(", "+"exception:"+exception);
					}
					duccLogger.info(methodName, jobid, messages.fetch("login ")+userId+" "+messages.fetch("failed")+" "+text);
				}
			}
		}
		catch(Throwable t) {
			sb.append("failure"+" "+t.getMessage());
			duccLogger.error(methodName, jobid, "userid="+userId);
		}
		response.getWriter().println(sb);
		duccLogger.trace(methodName, jobid, messages.fetch("exit"));
	}	
	
	private void handleServletUnknown(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleServletUnknown";
		duccLogger.trace(methodName, jobid, messages.fetch("enter"));
		duccLogger.info(methodName, jobid, request.toString());
		duccLogger.trace(methodName, jobid, messages.fetch("exit"));
	}
	
	private void handleDuccRequest(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleDuccRequest";
		duccLogger.trace(methodName, jobid, messages.fetch("enter"));
		duccLogger.debug(methodName, jobid,request.toString());
		duccLogger.debug(methodName, jobid,"getRequestURI():"+request.getRequestURI());
		String reqURI = request.getRequestURI()+"";
		if(reqURI.startsWith(userAuthenticationStatus)) {
			handleDuccServletAuthenticationStatus(target, baseRequest, request, response);
		}
		else if(reqURI.startsWith(userLogout)) {
			duccLogger.info(methodName, jobid,"getRequestURI():"+request.getRequestURI());
			handleDuccServletLogout(target, baseRequest, request, response);
		}
		else if(reqURI.startsWith(userLogin)) {
			duccLogger.info(methodName, jobid,"getRequestURI():"+request.getRequestURI());
			handleDuccServletLogin(target, baseRequest, request, response);
		}
		
		else {
			handleServletUnknown(target, baseRequest, request, response);
		}
		duccLogger.trace(methodName, jobid, messages.fetch("exit"));
	}
	
	
	public void handle(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException {
		String methodName = "handle";
		try{ 
			duccLogger.debug(methodName, jobid,request.toString());
			duccLogger.debug(methodName, jobid,"getRequestURI():"+request.getRequestURI());
			String reqURI = request.getRequestURI()+"";
			if(reqURI.startsWith(duccContextUser)) {
				response.setContentType("text/html;charset=utf-8");
				response.setStatus(HttpServletResponse.SC_OK);
				baseRequest.setHandled(true);
				handleDuccRequest(target, baseRequest, request, response);
				DuccWebUtil.noCache(response);
			}
		}
		catch(Throwable t) {
			if(isIgnorable(t)) {
				duccLogger.debug(methodName, jobid, t);
			}
			else {
				duccLogger.info(methodName, jobid, "", t.getMessage(), t);
				duccLogger.error(methodName, jobid, t);
			}
		}
		
	}

}
