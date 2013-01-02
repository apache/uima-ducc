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
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;
import org.apache.uima.ducc.common.NodeIdentity;
import org.apache.uima.ducc.common.authentication.AuthenticationManager;
import org.apache.uima.ducc.common.authentication.IAuthenticationManager;
import org.apache.uima.ducc.common.authentication.IAuthenticationResult;
import org.apache.uima.ducc.common.authentication.SessionManager;
import org.apache.uima.ducc.common.authentication.IAuthenticationManager.Role;
import org.apache.uima.ducc.common.boot.DuccDaemonRuntimeProperties;
import org.apache.uima.ducc.common.boot.DuccDaemonRuntimeProperties.DaemonName;
import org.apache.uima.ducc.common.internationalization.Messages;
import org.apache.uima.ducc.common.jd.JdConstants;
import org.apache.uima.ducc.common.jd.files.IWorkItemState;
import org.apache.uima.ducc.common.jd.files.WorkItemStateManager;
import org.apache.uima.ducc.common.jd.files.IWorkItemState.State;
import org.apache.uima.ducc.common.persistence.services.IStateServices;
import org.apache.uima.ducc.common.persistence.services.StateServices;
import org.apache.uima.ducc.common.persistence.services.StateServicesDirectory;
import org.apache.uima.ducc.common.persistence.services.StateServicesSet;
import org.apache.uima.ducc.common.system.SystemState;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.DuccProperties;
import org.apache.uima.ducc.common.utils.TimeStamp;
import org.apache.uima.ducc.common.utils.Version;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.orchestrator.authentication.DuccWebAdministrators;
import org.apache.uima.ducc.transport.event.ProcessInfo;
import org.apache.uima.ducc.transport.event.common.DuccWorkJob;
import org.apache.uima.ducc.transport.event.common.DuccWorkMap;
import org.apache.uima.ducc.transport.event.common.DuccWorkReservation;
import org.apache.uima.ducc.transport.event.common.IDuccPerWorkItemStatistics;
import org.apache.uima.ducc.transport.event.common.IDuccProcess;
import org.apache.uima.ducc.transport.event.common.IDuccProcessMap;
import org.apache.uima.ducc.transport.event.common.IDuccProcessWorkItems;
import org.apache.uima.ducc.transport.event.common.IDuccReservation;
import org.apache.uima.ducc.transport.event.common.IDuccReservationMap;
import org.apache.uima.ducc.transport.event.common.IDuccSchedulingInfo;
import org.apache.uima.ducc.transport.event.common.IDuccStandardInfo;
import org.apache.uima.ducc.transport.event.common.IDuccWork;
import org.apache.uima.ducc.transport.event.common.IDuccWorkJob;
import org.apache.uima.ducc.transport.event.common.IDuccWorkReservation;
import org.apache.uima.ducc.transport.event.common.IRationale;
import org.apache.uima.ducc.transport.event.common.TimeWindow;
import org.apache.uima.ducc.transport.event.common.IDuccCompletionType.JobCompletionType;
import org.apache.uima.ducc.transport.event.common.IDuccTypes.DuccType;
import org.apache.uima.ducc.transport.event.common.IDuccUnits.MemoryUnits;
import org.apache.uima.ducc.transport.event.common.IResourceState.ProcessDeallocationType;
import org.apache.uima.ducc.transport.event.jd.PerformanceMetricsSummaryItem;
import org.apache.uima.ducc.transport.event.jd.PerformanceMetricsSummaryMap;
import org.apache.uima.ducc.transport.event.jd.PerformanceSummary;
import org.apache.uima.ducc.transport.event.jd.UimaStatistic;
import org.apache.uima.ducc.ws.DuccDaemonsData;
import org.apache.uima.ducc.ws.DuccData;
import org.apache.uima.ducc.ws.DuccMachinesData;
import org.apache.uima.ducc.ws.JobInfo;
import org.apache.uima.ducc.ws.MachineInfo;
import org.apache.uima.ducc.ws.MachineSummaryInfo;
import org.apache.uima.ducc.ws.ReservationInfo;
import org.apache.uima.ducc.ws.types.NodeId;
import org.apache.uima.ducc.ws.types.UserId;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;


public class DuccHandler extends AbstractHandler {
	
	private static DuccLogger duccLogger = DuccLoggerComponents.getWsLogger(DuccHandler.class.getName());
	private static Messages messages = Messages.getInstance();
	
	private static DuccId jobid = null;
	
	private static DuccWebAdministrators duccWebAdministrators = DuccWebAdministrators.getInstance();
	
	private static IAuthenticationManager iAuthenticationManager = AuthenticationManager.getInstance();
	
	private int maximumRecordsJobs = 4096;
	private int defaultRecordsJobs = 16;
	private int maximumRecordsReservations = 4096;
	private int defaultRecordsReservations = 8;
	private int maximumRecordsServices = 4096;
	private int defaultRecordsServices = 12;
	
	private String dir_home = System.getenv("DUCC_HOME");
	private String dir_resources = "resources";
	
	private String duccContext = "/ducc-servlet";
	
	private String duccVersion						= duccContext+"/version";
	private String duccLogData						= duccContext+"/log-data";
	private String duccLoginLink					= duccContext+"/login-link";
	private String duccAuthenticationStatus 		= duccContext+"/authentication-status";
	private String duccAuthenticatorVersion 		= duccContext+"/authenticator-version";
	private String duccUserLogout					= duccContext+"/user-logout";
	private String duccUserLogin					= duccContext+"/user-login";
	private String duccJobsData    					= duccContext+"/jobs-data";
	private String duccJobIdData					= duccContext+"/job-id-data";
	private String duccJobWorkitemsCountData		= duccContext+"/job-workitems-count-data";
	private String duccJobProcessesData    			= duccContext+"/job-processes-data";
	private String duccJobWorkitemsData				= duccContext+"/job-workitems-data";
	private String duccJobPerformanceData			= duccContext+"/job-performance-data";
	private String duccJobSpecificationData 		= duccContext+"/job-specification-data";
	private String duccJobInitializationFailData	= duccContext+"/job-initialization-fail-data";
	private String duccJobRuntimeFailData			= duccContext+"/job-runtime-fail-data";
	private String duccReservationsData 			= duccContext+"/reservations-data";
	private String duccServicesDeploymentsData  	= duccContext+"/services-deployments-data";
	private String duccServiceProcessesData    		= duccContext+"/service-processes-data";
	private String duccServiceSpecificationData 	= duccContext+"/service-specification-data";
	@Deprecated
	private String duccMachinesData    				= duccContext+"/machines-data";
	private String duccSystemAdminAdminData 		= duccContext+"/system-admin-admin-data";
	private String duccSystemAdminControlData 		= duccContext+"/system-admin-control-data";
	private String duccSystemJobsControl			= duccContext+"/jobs-control-request";
	@Deprecated
	private String duccSystemClassesData 			= duccContext+"/system-classes-data";
	@Deprecated
	private String duccSystemDaemonsData 			= duccContext+"/system-daemons-data";
	private String duccClusterName 					= duccContext+"/cluster-name";
	private String duccTimeStamp   					= duccContext+"/timestamp";
	private String duccJobSubmit   					= duccContext+"/job-submit-request";
	private String duccJobCancel   					= duccContext+"/job-cancel-request";
	private String duccReservationSubmit    		= duccContext+"/reservation-submit-request";
	private String duccReservationCancel    		= duccContext+"/reservation-cancel-request";
	
	private String jsonServicesDefinitionsData		= duccContext+"/json-services-definitions-data";
	private String jsonMachinesData 				= duccContext+"/json-machines-data";
	private String jsonSystemClassesData 			= duccContext+"/json-system-classes-data";
	private String jsonSystemDaemonsData 			= duccContext+"/json-system-daemons-data";
	
	private String duccjConsoleLink					= duccContext+"/jconsole-link.jnlp";
	
	private String duccJobSubmitForm	    		= duccContext+"/job-submit-form";
	
	private String duccJobSubmitButton    			= duccContext+"/job-get-submit-button";
	private String duccReservationSubmitButton  	= duccContext+"/reservation-get-submit-button";
	
	private String duccReservationSchedulingClasses     = duccContext+"/reservation-scheduling-classes";
	private String duccReservationInstanceMemorySizes   = duccContext+"/reservation-instance-memory-sizes";
	private String duccReservationInstanceMemoryUnits   = duccContext+"/reservation-instance-memory-units";
	private String duccReservationNumberOfInstances	    = duccContext+"/reservation-number-of-instances";

	private DuccWebServer duccWebServer = null;
	
	private boolean terminateEnabled = true;
	
	public DuccHandler(DuccWebServer duccWebServer) {
		this.duccWebServer = duccWebServer;
		initializeAuthenticator();
	}

	private void initializeAuthenticator() {
		String methodName = "initializeAuthenticator";
		try {
			Properties properties = DuccWebProperties.get();
			String key = "ducc.authentication.implementer";
			if(properties.containsKey(key)) {
				String value = properties.getProperty(key);
				Class<?> authenticationImplementer = Class.forName(value);
				iAuthenticationManager = (IAuthenticationManager)authenticationImplementer.newInstance();
			}
		}
		catch(Exception e) {
			duccLogger.error(methodName, jobid, e);
		}
	}
	
	private DuccWebServer getDuccWebServer() {
		return duccWebServer;
	}
	
	private String getFileName() {
		return dir_home+File.separator+dir_resources+File.separator+getDuccWebServer().getClassDefinitionFile();
	}
	
	private String stringNormalize(String value,String defaultValue) {
		String methodName = "stringNormalize";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		String retVal;
		if(value== null) {
			retVal = defaultValue;
		}
		else {
			retVal = value;
		}
		duccLogger.trace(methodName, null, messages.fetch("exit"));
		return retVal;
	}
	
	/*
	 * non-authenticated
	 */
	
	private void handleDuccServletLoginLink(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletLoginLink";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		String link = "https://"+request.getServerName()+":"+getDuccWebServer().getPortSsl()+"/";
		String href = "<a href=\""+link+"login.html\" onclick=\"var newWin = window.open(this.href,'child','height=600,width=475,scrollbars');  newWin.focus(); return false;\">Login</a>";
		sb.append(href);
		response.getWriter().println(sb);
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}	
	
	private void updateSession() {
		String methodName = "updateSession";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		try {
			Properties properties = DuccWebProperties.get();
			String key = "ducc.ws.session.minutes";
			if(properties.containsKey(key)) {
				String sessionMinutes = properties.getProperty(key).trim();
				int currSessionLifetimeMillis = Integer.parseInt(sessionMinutes) * 60 * 1000;
				int prevSessionLifetimeMillis = SessionManager.getInstance().getSessionLifetimeMillis();
				if(currSessionLifetimeMillis != prevSessionLifetimeMillis) {
					SessionManager.getInstance().setSessionLifetimeMillis(currSessionLifetimeMillis);
					duccLogger.info(methodName, null, "login session expiry (minutes):"+sessionMinutes);
				}
			}
		}
		catch(Throwable t) {
			duccLogger.error(methodName, null, t);
		}
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}
	
	private boolean isAuthenticated(HttpServletRequest request, HttpServletResponse response) {
		String methodName = "isAuthenticated";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		boolean authenticated = false;
		String userId = DuccWebUtil.getCookie(request,DuccWebUtil.cookieUser);
		String sessionId = DuccWebUtil.getCookie(request,DuccWebUtil.cookieSession);
		String text;
		try {
			updateSession();
			authenticated = SessionManager.getInstance().validateRegistration(userId, sessionId);
			if(authenticated) {
				text = "user "+userId+" is authenticated";
			}
			else {
				text = "user "+userId+" is not authenticated";
			}
			duccLogger.debug(methodName, null, messages.fetch(text));
		}
		catch(Exception e) {
			duccLogger.error(methodName, null, e);
			DuccWebUtil.expireCookie(response, DuccWebUtil.cookieSession, sessionId);
			DuccWebUtil.expireCookie(response, DuccWebUtil.cookieUser, userId);
		}
		duccLogger.trace(methodName, null, messages.fetch("exit"));
		return authenticated;
	}
	
	private boolean match(String s1, String s2) {
		String methodName = "match";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		boolean retVal = false;
		if(s1 != null) {
			if(s2 != null) {
				if(s1.trim().equals(s2.trim())) {
					retVal = true;
				}
			}
		}
		duccLogger.trace(methodName, null, messages.fetch("exit"));
		return retVal;
	}
	
	private boolean isAuthorized(HttpServletRequest request, String resourceOwnerUserid) {
		String methodName = "isAuthorized";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		boolean retVal = false;
		String userId = DuccWebUtil.getCookie(request,DuccWebUtil.cookieUser);
		String sessionId = DuccWebUtil.getCookie(request,DuccWebUtil.cookieSession);
		String text;
		try {
			updateSession();
			boolean authenticated = SessionManager.getInstance().validateRegistration(userId, sessionId);
			if(authenticated) {
				if(duccWebAdministrators.isAdministrator(userId)) {
					text = "user "+userId+" is administrator";
					retVal = true;
				}
				else {
					if(match(resourceOwnerUserid,userId)) {
						text = "user "+userId+" is resource owner";
						retVal = true;
					}
					else {
						text = "user "+userId+" is not resource owner "+resourceOwnerUserid;
					}
				}
			}
			else {
				text = "user "+userId+" is not authenticated";
			}
			duccLogger.debug(methodName, null, messages.fetch(text));
		}
		catch(Exception e) {
			duccLogger.error(methodName, null, e);
		}
		duccLogger.trace(methodName, null, messages.fetch("exit"));
		return retVal;
	}
	
	private void handleDuccServletVersion(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletVersion";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		String version = Version.version();
		sb.append(version);
		response.getWriter().println(sb);
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}	
	
	private void handleDuccServletAuthenticationStatus(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletAuthenticationStatus";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
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
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}	
	
	private void handleDuccServletAuthenticatorVersion(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletAuthenticatorVersion";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		sb.append(iAuthenticationManager.getVersion());
		response.getWriter().println(sb);
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}	
	
	private void handleDuccServletLogout(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletLogout";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		String sessionId = DuccWebUtil.getCookie(request,DuccWebUtil.cookieSession);
		String userId = DuccWebUtil.getCookie(request,DuccWebUtil.cookieUser);
		SessionManager.getInstance().unRegister(userId,sessionId);
		DuccWebUtil.expireCookie(response, DuccWebUtil.cookieSession, sessionId);
		DuccWebUtil.expireCookie(response, DuccWebUtil.cookieUser, userId);
		StringBuffer sb = new StringBuffer();
		response.getWriter().println(sb);
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}	
	
	private void handleDuccServletLogin(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletLogin";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		String userId = request.getParameter("userid");
		String password = request.getParameter("password");
		if((userId == null) || (userId.trim().length() == 0)) {
			duccLogger.info(methodName, null, messages.fetch("login ")+userId+" "+messages.fetch("failed"));
			sb.append("failure");
		}
		else if((password == null) || (password.trim().length() == 0)) {
			duccLogger.info(methodName, null, messages.fetch("login ")+userId+" "+messages.fetch("failed"));
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
			duccLogger.info(methodName, null, messages.fetchLabel("version")+iAuthenticationManager.getVersion());
			IAuthenticationResult result1 = iAuthenticationManager.isAuthenticate(userId, domain, password);
			IAuthenticationResult result2 = iAuthenticationManager.isGroupMember(userId, domain, role);
			if(result1.isSuccess() && result2.isSuccess()) {
				String sessionId = SessionManager.getInstance().register(userId);
				DuccWebUtil.putCookie(response, DuccWebUtil.cookieSession, sessionId);
				DuccWebUtil.putCookie(response, DuccWebUtil.cookieUser, userId);
				duccLogger.info(methodName, null, messages.fetch("login ")+userId+" "+messages.fetch("success"));
				sb.append("success"+","+DuccWebUtil.cookieSession+","+sessionId+","+DuccWebUtil.cookieUser+","+userId);
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
				duccLogger.info(methodName, null, messages.fetch("login ")+userId+" "+messages.fetch("failed")+" "+text);
			}
		}
		response.getWriter().println(sb);
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}	
	
	private String getDisabled(HttpServletRequest request, IDuccWork duccWork) {
		String resourceOwnerUserId = duccWork.getStandardInfo().getUser();
		String disabled = "disabled=\"disabled\"";
		if(isAuthorized(request, resourceOwnerUserId)) {
			disabled = "";
		}
		return disabled;
	}
	
	private void handleDuccServletJobSubmitForm(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletJobSubmitForm";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		DuccWebSchedulerClasses schedulerClasses = new DuccWebSchedulerClasses(getFileName());
		sb.append(DuccWebJobSpecificationProperties.getHtmlForm(request,schedulerClasses));
		response.getWriter().println(sb);
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}
	
	private String getProcessMemorySize(DuccId id, String type, String size, MemoryUnits units) {
		String methodName = "getProcessMemorySize";
		String retVal = "?";
		double multiplier = 1;
		switch(units) {
		case KB:
			multiplier = Math.pow(10, -6);
			break;
		case MB:
			multiplier = Math.pow(10, -3);
			break;
		case GB:
			multiplier = Math.pow(10, 0);
			break;
		case TB:
			multiplier = Math.pow(10, 3);
			break;
		}
		try {
			double dSize = Double.parseDouble(size) * multiplier;
			retVal = dSize+"";
		}
		catch(Exception e) {
			duccLogger.trace(methodName, id, messages.fetchLabel("type")+type+" "+messages.fetchLabel("size")+size, e);
		}
		return retVal;	
	}
	
	private String getCompletionOrProjection(IDuccWorkJob job) {
		String methodName = "getCompletionOrProjection";
		String retVal = "";
		try {
			String tVal = job.getStandardInfo().getDateOfCompletion();
			duccLogger.trace(methodName, null, tVal);
			retVal = getTimeStamp(job.getDuccId(),tVal);
		}
		catch(Exception e) {
			duccLogger.trace(methodName, null, "no worries", e);
		}
		catch(Throwable t) {
			duccLogger.trace(methodName, null, "no worries", t);
		}
		try {
			if(retVal == "") {
				IDuccSchedulingInfo schedulingInfo = job.getSchedulingInfo();
				IDuccPerWorkItemStatistics perWorkItemStatistics = schedulingInfo.getPerWorkItemStatistics();
				if (perWorkItemStatistics == null) {
					return "";
				}
				//
				int total = schedulingInfo.getIntWorkItemsTotal();
				int completed = schedulingInfo.getIntWorkItemsCompleted();
				int error = schedulingInfo.getIntWorkItemsError();
				int remainingWorkItems = total - (completed + error);
				if(remainingWorkItems > 0) {
					int usableProcessCount = job.getProcessMap().getUsableProcessCount();
					if(usableProcessCount > 0) {
						if(completed > 0) {
							int threadsPerProcess = schedulingInfo.getIntThreadsPerShare();
							int totalThreads = usableProcessCount * threadsPerProcess;
							double remainingIterations = remainingWorkItems / totalThreads;
							double avgMillis = perWorkItemStatistics.getMean();
							double projectedTime = 0;
							if(remainingIterations > 0) {
								projectedTime = avgMillis * remainingIterations;
							}
							else {
								projectedTime = avgMillis - (Calendar.getInstance().getTimeInMillis() - job.getSchedulingInfo().getMostRecentWorkItemStart());
							}
							if(projectedTime < 0) {
								projectedTime = 0;
							}
							long millis = Math.round(projectedTime);
							long days = TimeUnit.MILLISECONDS.toDays(millis);
							millis -= TimeUnit.DAYS.toMillis(days);
							long hours = TimeUnit.MILLISECONDS.toHours(millis);
							millis -= TimeUnit.HOURS.toMillis(hours);
							long minutes = TimeUnit.MILLISECONDS.toMinutes(millis);
							millis -= TimeUnit.MINUTES.toMillis(minutes);
							long seconds = TimeUnit.MILLISECONDS.toSeconds(millis);
							String remainingTime = String.format("%02d", hours)+":"+String.format("%02d", minutes)+":"+String.format("%02d", seconds);
							if(days > 0) {
								remainingTime = days+":"+remainingTime;
							}
							retVal = "projected +"+remainingTime;
							double max = Math.round(perWorkItemStatistics.getMax()/100.0)/10.0;
							double min = Math.round(perWorkItemStatistics.getMin()/100.0)/10.0;
							double avg = Math.round(perWorkItemStatistics.getMean()/100.0)/10.0;
							double dev = Math.round(perWorkItemStatistics.getStandardDeviation()/100.0)/10.0;
							retVal = "<span title=\""+"seconds-per-work-item "+"Max:"+max+" "+"Min:"+min+" "+"Avg:"+avg+" "+"Dev:"+dev+"\""+">"+retVal+"</span>";
						}
					}
					else {
						if(job.isRunnable()) {
							retVal = "suspended";
						}
					}
				}
				else {
					retVal = "finished";
				}
			}
		}
		catch(Throwable t) {
			duccLogger.trace(methodName, null, t);
		}
		return retVal;
	}
	
	private String getUserLogsDir(IDuccWorkJob job) {
		String retVal = job.getLogDirectory();
		if(!retVal.endsWith(File.separator)) {
			retVal += File.separator;
		}
		return retVal;
	}
	
	private String buildInitializeFailuresLink(IDuccWorkJob job) {
		StringBuffer sb = new StringBuffer();
		IDuccProcessMap processMap = job.getProcessMap();
		ArrayList<DuccId> list = processMap.getFailedInitialization();
		int count = list.size();
		if(count > 0) {
			String href = "/ducc-servlet/job-initialization-fail-data?id="+job.getDuccId();
			String anchor = "<a class=\"logfileLink\" title=\""+job.getDuccId()+" init fails"+"\" href=\""+href+"\" rel=\""+href+"\">"+count+"</a>";
			sb.append(anchor);
		}
		else {
			sb.append(count);
		}
		String retVal = sb.toString();
		return retVal;
	}

	private String buildRuntimeFailuresLink(IDuccWorkJob job) {
		StringBuffer sb = new StringBuffer();
		IDuccProcessMap processMap = job.getProcessMap();
		ArrayList<DuccId> list = processMap.getFailedNotInitialization();
		int count = list.size();
		if(count > 0) {
			String href = "/ducc-servlet/job-runtime-fail-data?id="+job.getDuccId();
			String anchor = "<a class=\"logfileLink\" title=\""+job.getDuccId()+" run fails"+"\" href=\""+href+"\" rel=\""+href+"\">"+count+"</a>";
			sb.append(anchor);
		}
		else {
			sb.append(count);
		}
		String retVal = sb.toString();
		return retVal;
	}
	
	private String buildErrorLink(IDuccWorkJob job, String name) {
		String retVal = job.getSchedulingInfo().getWorkItemsError();
		if(!retVal.equals("0")) {
			String errorCount = retVal;
			if(name == null) {
				name = errorCount;
			}
			String logsjobdir = getUserLogsDir(job)+job.getDuccId().getFriendly()+File.separator;
			String logfile = "jd.err.log";
			String href = "<a href=\""+duccLogData+"?"+"fname="+logsjobdir+logfile+"\" onclick=\"var newWin = window.open(this.href,'child','height=800,width=1200,scrollbars');  newWin.focus(); return false;\">"+name+"</a>";
			retVal = href;
		}
		return retVal;
	}
	
	private String buildErrorLink(IDuccWorkJob job) {
		return(buildErrorLink(job,null));
	}
	
	private String getTimeStamp(DuccId jobId, String millis) {
		String methodName = "";
		String retVal = "";
		try {
			retVal = TimeStamp.simpleFormat(millis);
		}
		catch(Throwable t) {
			duccLogger.warn(methodName, jobId, "millis:"+millis);
		}
		return retVal;
	}
	
	private ArrayList<String> getSwappingMachines(IDuccWorkJob job) {
		ArrayList<String> retVal = new ArrayList<String>();
		DuccMachinesData.getInstance();
		IDuccProcessMap map = job.getProcessMap();
		for(DuccId duccId : map.keySet()) {
			IDuccProcess jp = map.get(duccId);
			switch(jp.getProcessState()) {
			case Starting:
			case Initializing:
			case Running:
				NodeIdentity nodeId = jp.getNodeIdentity();
				if(nodeId != null) {
					String ip = nodeId.getIp();
					if(DuccMachinesData.getInstance().isMachineSwapping(ip)) {
						if(!retVal.contains(nodeId.getName())) {
							retVal.add(nodeId.getName());
						}
					}
				}
				break;
			}
		}
		return retVal;
	}
	
	private void buildJobsListEntry(HttpServletRequest request, StringBuffer sb, DuccId duccId, IDuccWorkJob job, DuccData duccData) {
		String type="Job";
		String id = normalize(duccId);
		sb.append("<td valign=\"bottom\" class=\"ducc-col-terminate\">");
		if(terminateEnabled) {
			if(!job.isFinished()) {
				sb.append("<input type=\"button\" onclick=\"ducc_confirm_terminate_job("+id+")\" value=\"Terminate\" "+getDisabled(request,job)+"/>");
			}
		}
		sb.append("</td>");
		// Id
		sb.append("<td valign=\"bottom\">");
		sb.append("<a href=\"job.details.html?id="+id+"\">"+id+"</a>");
		sb.append("</td>");
		// Start
		sb.append("<td valign=\"bottom\">");
		sb.append(getTimeStamp(job.getDuccId(), job.getStandardInfo().getDateOfSubmission()));
		sb.append("</td>");
		// End
		sb.append("<td valign=\"bottom\">");
		sb.append(getCompletionOrProjection(job));
		sb.append("</td>");
		// User
		sb.append("<td valign=\"bottom\">");
		sb.append(job.getStandardInfo().getUser());
		sb.append("</td>");
		// Class
		sb.append("<td valign=\"bottom\">");
		sb.append(stringNormalize(job.getSchedulingInfo().getSchedulingClass(),messages.fetch("default")));
		sb.append("</td>");
		/*
		sb.append("<td align=\"right\">");
		sb.append(stringNormalize(duccWorkJob.getSchedulingInfo().getSchedulingPriority(),messages.fetch("default")));
		sb.append("</td>");
		*/
		// State
		sb.append("<td valign=\"bottom\">");
		if(duccData.isLive(duccId)) {
			if(job.isOperational()) {
				sb.append("<span class=\"active_state\">");
			}
			else {
				sb.append("<span class=\"completed_state\">");
			}
		}
		else {
			sb.append("<span class=\"historic_state\">");
		}
		sb.append(job.getStateObject().toString());
		if(duccData.isLive(duccId)) {
			sb.append("</span>");
		}
		sb.append("</td>");
		// Reason
		if(job.isOperational()) {
			sb.append("<td valign=\"bottom\">");
			ArrayList<String> swappingMachines = getSwappingMachines(job);
			if(!swappingMachines.isEmpty()) {
				StringBuffer mb = new StringBuffer();
				for(String machine : swappingMachines) {
					mb.append(machine);
					mb.append(" ");
				}
				String ml = mb.toString().trim();
				sb.append("<span class=\"health_red\" title=\""+ml+"\">");
				sb.append("Swapping");
				sb.append("</span>");
			}
			sb.append("</td>");
		}
		else if(job.isCompleted()) {
			JobCompletionType jobCompletionType = job.getCompletionType();
			switch(jobCompletionType) {
			case EndOfJob:
				try {
					int total = job.getSchedulingInfo().getIntWorkItemsTotal();
					int done = job.getSchedulingInfo().getIntWorkItemsCompleted();
					int error = job.getSchedulingInfo().getIntWorkItemsError();
					if(total != (done+error)) {
						jobCompletionType = JobCompletionType.Premature;
					}
				}
				catch(Exception e) {
				}
				sb.append("<td valign=\"bottom\">");
				break;
			case Undefined:
				sb.append("<td valign=\"bottom\">");
				break;
			default:
				IRationale rationale = job.getCompletionRationale();
				if(rationale != null) {
					sb.append("<td valign=\"bottom\" title=\""+rationale+"\">");
				}
				else {
					sb.append("<td valign=\"bottom\">");
				}
				break;
			}
			sb.append(jobCompletionType);
			sb.append("</td>");
		}
		// Processes
		sb.append("<td valign=\"bottom\" align=\"right\">");
		if(duccData.isLive(duccId)) {
			sb.append(job.getProcessMap().getAliveProcessCount());
		}
		else {
			sb.append("0");
		}
		sb.append("</td>");
		// Initialize Failures
		sb.append("<td valign=\"bottom\" align=\"right\">");
		sb.append(buildInitializeFailuresLink(job));
		if(job.getSchedulingInfo().getLongSharesMax() < 0) {
			sb.append("<sup>");
			sb.append("<span title=\"capped at current number of running processes due to excessive initialization failures\">");
			sb.append("^");
			sb.append("</span>");
			sb.append("</sup>");
		}
		sb.append("</td>");
		// Runtime Failures
		sb.append("<td valign=\"bottom\" align=\"right\">");
		sb.append(buildRuntimeFailuresLink(job));
		sb.append("</td>");
		// Size
		sb.append("<td valign=\"bottom\" align=\"right\">");
		String size = job.getSchedulingInfo().getShareMemorySize();
		MemoryUnits units = job.getSchedulingInfo().getShareMemoryUnits();
		sb.append(getProcessMemorySize(duccId,type,size,units));
		sb.append("</td>");
		// Total
		sb.append("<td valign=\"bottom\" align=\"right\">");
		sb.append(job.getSchedulingInfo().getWorkItemsTotal());
		sb.append("</td>");
		// Done
		sb.append("<td valign=\"bottom\" align=\"right\">");
		sb.append(job.getSchedulingInfo().getWorkItemsCompleted());
		sb.append("</td>");
		// Error
		sb.append("<td valign=\"bottom\" align=\"right\">");
		sb.append(buildErrorLink(job));
		sb.append("</td>");
		// Dispatch
		sb.append("<td valign=\"bottom\" align=\"right\">");
		if(duccData.isLive(duccId)) {
			sb.append(job.getSchedulingInfo().getWorkItemsDispatched());
		}
		else {
			sb.append("0");
		}
		sb.append("</td>");
		// Retry
		sb.append("<td valign=\"bottom\" align=\"right\">");
		sb.append(job.getSchedulingInfo().getWorkItemsRetry());
		sb.append("</td>");
		// Preempt
		sb.append("<td valign=\"bottom\" align=\"right\">");
		sb.append(job.getSchedulingInfo().getWorkItemsPreempt());
		sb.append("</td>");
		// Description
		sb.append("<td valign=\"bottom\">");
		sb.append(stringNormalize(job.getStandardInfo().getDescription(),messages.fetch("none")));
		sb.append("</td>");
		sb.append("</tr>");
	} 
	
	private int getJobsMax(HttpServletRequest request) {
		int maxRecords = defaultRecordsJobs;
		try {
			String cookie = DuccWebUtil.getCookie(request,DuccWebUtil.cookieJobsMax);
			int reqRecords = Integer.parseInt(cookie);
			if(reqRecords <= maximumRecordsJobs) {
				if(reqRecords > 0) {
					maxRecords = reqRecords;
				}
			}
		}
		catch(Exception e) {
		}
		return maxRecords;
	}
	
	private ArrayList<String> getJobsUsers(HttpServletRequest request) {
		ArrayList<String> userRecords = new ArrayList<String>();
		try {
			String cookie = DuccWebUtil.getCookie(request,DuccWebUtil.cookieJobsUsers);
			String[] users = cookie.split(" ");
			if(users != null) {
				for(String user : users) {
					user = user.trim();
					if(user.length() > 0) {
						if(!userRecords.contains(user)) {
							userRecords.add(user);
						}
					}
				}
			}
		}
		catch(Exception e) {
		}
		return userRecords;
	}
	
	private enum JobsUsersQualifier {
		ActiveInclude,
		ActiveExclude,
		Include,
		Exclude,
		Unknown
	}
	
	private JobsUsersQualifier getJobsUsersQualifier(HttpServletRequest request) {
		JobsUsersQualifier retVal = JobsUsersQualifier.Unknown;
		try {
			String cookie = DuccWebUtil.getCookie(request,DuccWebUtil.cookieJobsUsersQualifier);
			String qualifier = cookie.trim();
			if(qualifier.equals("include")) {
				retVal = JobsUsersQualifier.Include;
			}
			else if(qualifier.equals("exclude")) {
				retVal = JobsUsersQualifier.Exclude;
			}
			else if(qualifier.equals("active+include")) {
				retVal = JobsUsersQualifier.ActiveInclude;
			}
			else if(qualifier.equals("active+exclude")) {
				retVal = JobsUsersQualifier.ActiveExclude;
			}
		}
		catch(Exception e) {
		}
		return retVal;
	}
	
	private void handleDuccServletJobsData(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletJobsData";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		int maxRecords = getJobsMax(request);
		ArrayList<String> users = getJobsUsers(request);
		StringBuffer sb = new StringBuffer();
		DuccData duccData = DuccData.getInstance();
		ConcurrentSkipListMap<JobInfo,JobInfo> sortedJobs = duccData.getSortedJobs();
		JobsUsersQualifier userQualifier = getJobsUsersQualifier(request);
		if(sortedJobs.size()> 0) {
			Iterator<Entry<JobInfo, JobInfo>> iterator = sortedJobs.entrySet().iterator();
			int counter = 0;
			while(iterator.hasNext()) {
				JobInfo jobInfo = iterator.next().getValue();
				DuccWorkJob job = jobInfo.getJob();
				boolean list = false;
				if(!users.isEmpty()) {
					String jobUser = job.getStandardInfo().getUser().trim();
					switch(userQualifier) {
					case ActiveInclude:
					case Unknown:
						if(!job.isCompleted()) {
							list = true;
						}
						else if(users.contains(jobUser)) {
							if(maxRecords > 0) {
								if (counter++ < maxRecords) {
									list = true;
								}
							}
						}
						break;
					case ActiveExclude:
						if(!job.isCompleted()) {
							list = true;
						}
						else if(!users.contains(jobUser)) {
							if(maxRecords > 0) {
								if (counter++ < maxRecords) {
									list = true;
								}
							}
						}
						break;
					case Include:
						if(users.contains(jobUser)) {
							if(maxRecords > 0) {
								if (counter++ < maxRecords) {
									list = true;
								}
							}
						}
						break;
					case Exclude:
						if(!users.contains(jobUser)) {
							if(maxRecords > 0) {
								if (counter++ < maxRecords) {
									list = true;
								}
							}
						}
						break;
					}	
				}
				else {
					if(!job.isCompleted()) {
						list = true;
					}
					else if(maxRecords > 0) {
						if (counter++ < maxRecords) {
							list = true;
						}
					}
				}
				if(list) {
					sb.append(trGet(counter));
					buildJobsListEntry(request, sb, job.getDuccId(), job, duccData);
				}
			}
		}
		else {
			sb.append("<tr>");
			sb.append("<td>");
			if(DuccData.getInstance().isPublished()) {
				sb.append(messages.fetch("no jobs"));
			}
			else {
				sb.append(messages.fetch("no data"));
			}
			sb.append("</td>");
			sb.append("</tr>");
		}
		response.getWriter().println(sb);
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}

	private String buildLogFileName(IDuccWorkJob job, IDuccProcess process, String type) {
		String retVal = "";
		if(type == "UIMA") {
			retVal = job.getDuccId().getFriendly()+"-"+type+"-"+process.getNodeIdentity().getName()+"-"+process.getPID()+".log";
		}
		else if(type == "JD") {
			retVal = "jd.out.log";
		}
		
		return retVal;
	}
	
	private String trGet(int counter) {
		if((counter % 2) > 0) {
			return "<tr class=\"ducc-row-odd\">";
		}
		else {
			return "<tr class=\"ducc-row-even\">";
		}
	}
	
	private String chomp(String leading, String whole) {
		String retVal = whole;
		while((retVal.length() > leading.length()) && (retVal.startsWith(leading))) {
			retVal = retVal.replaceFirst(leading, "");
		}
		/*
		if(retVal.equals("00:00")) {
			retVal = "0";
		}
		*/
		return retVal;
	}
	
	private double KB = 1000;
	private double MB = 1000*KB;
	private double GB = 1000*MB;
	
	DecimalFormat sizeFormatter = new DecimalFormat("##0.00");
	
	private boolean fileExists(String fileName) {
		String location = "fileExists";
		boolean retVal = false;
		try {
			File file = new File(fileName);
			retVal = file.exists();
		}
		catch(Exception e) {
			duccLogger.warn(location,jobid,e);
		}
		return retVal;
	}
	private String getFileSize(String fileName) {
		String location = "getFileSize";
		String retVal = "?";
		try {
			File file = new File(fileName);
			double size = file.length();
			size = size / MB;
			retVal = sizeFormatter.format(size);
		}
		catch(Exception e) {
			duccLogger.warn(location,jobid,e);
		}
		return retVal;
	}
	
	private void buildJobProcessListEntry(StringBuffer sb, DuccWorkJob job, IDuccProcess process, String type, int counter) {
		String logsjobdir = getUserLogsDir(job)+job.getDuccId().getFriendly()+File.separator;
		String logfile = buildLogFileName(job, process, type);
		String errfile = "jd.err.log";
		String href = "<a href=\""+duccLogData+"?"+"fname="+logsjobdir+logfile+"\" onclick=\"var newWin = window.open(this.href,'child','height=800,width=1200,scrollbars');  newWin.focus(); return false;\">"+logfile+"</a>";
		String tr = trGet(counter);
		sb.append(tr);
		// Id
		sb.append("<td align=\"right\">");
		/*
		long id = process.getDuccId().getFriendly();
		System.out.println(id);
		 */
		sb.append(process.getDuccId().getFriendly());
		sb.append("</td>");
		// Log
		sb.append("<td>");
		sb.append(href);
		sb.append("</td>");
		// Log Size (in MB)
		sb.append("<td align=\"right\">");
		sb.append(getFileSize(logsjobdir+logfile));
		sb.append("</td>");
		// Hostname
		sb.append("<td>");
		sb.append(process.getNodeIdentity().getName());
		sb.append("</td>");
		/*
		// Hostip
		sb.append("<td>");
		sb.append(process.getNodeIdentity().getIp());
		sb.append("</td>");
		*/
		// PID
		sb.append("<td align=\"right\">");
		sb.append(process.getPID());
		sb.append("</td>");
		// State:scheduler
		sb.append("<td>");
		sb.append(process.getResourceState());
		sb.append("</td>");
		// Reason:scheduler
		IDuccProcess jp = process;
		switch(jp.getProcessState()) {
		case Starting:
		case Initializing:
		case Running:
			sb.append("<td>");
			NodeIdentity nodeId = jp.getNodeIdentity();
			if(nodeId != null) {
				String ip = nodeId.getIp();
				if(DuccMachinesData.getInstance().isMachineSwapping(ip)) {
					sb.append("<span class=\"health_red\">");
					sb.append("Swapping");
					sb.append("</span>");
				}
			}
			sb.append("</td>");
			break;
		default:
			ProcessDeallocationType deallocationType = process.getProcessDeallocationType();
			String toolTip = ProcessDeallocationType.getToolTip(deallocationType);
			if(toolTip == null) {
				sb.append("<td>");
			}
			else {
				sb.append("<td title=\""+toolTip+"\">");
			}
			switch(deallocationType) {
			case Undefined:
				break;
			default:
				sb.append(process.getProcessDeallocationType());
				break;
			}
			sb.append("</td>");
			break;
		}
		// State:agent
		sb.append("<td>");
		sb.append(process.getProcessState());
		sb.append("</td>");
		// Reason:agent
		sb.append("<td>");
		String agentReason = process.getReasonForStoppingProcess();
		if(agentReason != null) {
			sb.append(process.getReasonForStoppingProcess());
		}
		sb.append("</td>");
		// Time:initialization
		TimeWindow t;
		t = (TimeWindow) process.getTimeWindowInit();
		long timeInitMillis = 0;
		try {
			timeInitMillis = t.getElapsedMillis();
		}
		catch(Exception e) {
		}
		String initTime = "?";
		if(t != null) {
			initTime = t.getElapsed(job);
		}
		sb.append("<td align=\"right\">");
		if((t != null) && (t.isEstimated())) {
			sb.append("<span title=\"estimated\" class=\"health_green\">");
		}
		else {
			sb.append("<span class=\"health_black\">");
		}
		if(initTime == null) {
			initTime = "0";
		}
		initTime = chomp("00:", initTime);
		sb.append(initTime);
		sb.append("</span>");
		sb.append("</td>");
		/*
		try {
			long diff = 0;
			long t0 = 0;
			long t1 = 0;
			t0 = Long.parseLong(t.getStart());
			t1 = Long.parseLong(t.getEnd());
			diff = t1-t0;
			System.out.println("init: "+diff+" "+t1+" "+t0);
		}
		catch(Exception e) {
		}
		*/
		// Time:run
		t = (TimeWindow) process.getTimeWindowRun();
		long timeRunMillis = 0;
		try {
			timeRunMillis = t.getElapsedMillis();
		}
		catch(Exception e) {
		}
		String runTime = "?";
		if(t != null) {
			runTime = t.getElapsed(job);
		}
		sb.append("<td align=\"right\">");
		if((t != null) && (t.isEstimated())) {
			sb.append("<span title=\"estimated\" class=\"health_green\">");
		}
		else {
			sb.append("<span class=\"health_black\">");
		}
		if(runTime == null) {
			runTime = "0";
		}
		runTime = chomp("00:", runTime);
		sb.append(runTime);
		sb.append("</span>");
		sb.append("</td>");
		/*
		try {
			long diff = 0;
			long t0 = 0;
			long t1 = 0;
			t0 = Long.parseLong(t.getStart());
			t1 = Long.parseLong(t.getEnd());
			diff = t1-t0;
			System.out.println("run: "+diff+" "+t1+" "+t0);
		}
		catch(Exception e) {
		}
		*/
		SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");
		// Time:gc
		long timeGC = 0;
		try {
			timeGC = process.getGarbageCollectionStats().getCollectionTime();
		}
		catch(Exception e) {
		}
		dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
		String displayGC = dateFormat.format(new Date(timeGC));
		displayGC = chomp("00:", displayGC);
		sb.append("<td align=\"right\">");
		sb.append(displayGC);
		sb.append("</td>");
		// Count:gc
		long countGC = 0;
		try {
			countGC = process.getGarbageCollectionStats().getCollectionCount();
		}
		catch(Exception e) {
		}
		sb.append("<td align=\"right\">");
		sb.append(countGC);
		sb.append("</td>");
		// %gc
		double pctGC = 0;
		double timeTotal = timeInitMillis + timeRunMillis;
		if(timeTotal > 0) {
			double denom = timeTotal;
			double numer = timeGC;
			pctGC = (numer/denom)*100;
		}
		DecimalFormat formatter = new DecimalFormat("##0.0");
		sb.append("<td align=\"right\">");
		sb.append(formatter.format(pctGC));
		sb.append("</td>");
		// Time:cpu
		sb.append("<td align=\"right\">");
		long timeCPU = process.getCpuTime();
		String fmtCPU = dateFormat.format(new Date(timeCPU));
		fmtCPU = chomp("00:", fmtCPU);
		sb.append(fmtCPU);
		sb.append("</td>");
		// %rss
		DuccId duccId = job.getDuccId();
		String size = job.getSchedulingInfo().getShareMemorySize();
		MemoryUnits units = job.getSchedulingInfo().getShareMemoryUnits();
		String residentMemory = getProcessMemorySize(duccId,type,size,units);
		double memory = Double.parseDouble(residentMemory);
		double rss = process.getResidentMemory();
		String rssType = "";
		if(job.isCompleted()) {
			rss = process.getResidentMemoryMax();
			rssType = ", maximum achieved";
		}
		rss = rss/GB;
		if(memory == 0) {
			memory = 1;
		}
		double pctRss = 100*(rss/memory);
		String displayRss = formatter.format(rss);
		String displayMemory = formatter.format(memory);
		if(displayRss.equals("0")) {
			pctRss = 0;
		}
		String displayPctRss = formatter.format(pctRss);
		String title = "title=\"100*"+displayRss+"/"+displayMemory+rssType+"\"";
		sb.append("<td align=\"right\" "+title+">");
		sb.append(displayPctRss);
		sb.append("</td>");
		// Time:avg
		IDuccProcessWorkItems pwi = process.getProcessWorkItems();
		sb.append("<td align=\"right\">");
		if(pwi != null) {
			sb.append(pwi.getSecsAvg());
		}
		sb.append("</td>");
		// Time:max
		sb.append("<td align=\"right\">");
		if(pwi != null) {
			sb.append(pwi.getSecsMax());
		}
		sb.append("</td>");
		// Time:min
		sb.append("<td align=\"right\">");
		if(pwi != null) {
			sb.append(pwi.getSecsMin());
		}
		sb.append("</td>");
		// Done
		sb.append("<td align=\"right\">");
		if(pwi != null) {
			sb.append(pwi.getCountDone());
		}
		sb.append("</td>");
		// Error
		sb.append("<td align=\"right\">");
		if(pwi != null) {
			sb.append(pwi.getCountError());
		}
		sb.append("</td>");
		// Retry
		sb.append("<td align=\"right\">");
		if(pwi != null) {
			sb.append(pwi.getCountRetry());
		}
		sb.append("</td>");
		// Preempt
		sb.append("<td align=\"right\">");
		if(pwi != null) {
			sb.append(pwi.getCountPreempt());
		}
		sb.append("</td>");
		// Jconsole:Url
		sb.append("<td>");
		switch(process.getProcessState()) {
		case Running:
			String jmxUrl = process.getProcessJmxUrl();
			if(jmxUrl != null) {
				sb.append(buildjConsoleLink(jmxUrl));
			}
		}
		sb.append("</td>");
		sb.append("</tr>");
		if(fileExists(logsjobdir+errfile)) {
			String href2 = null;
			if(type == "JD") {
				href2 = buildErrorLink(job,errfile);
				if(href2.equals("0")) {
					href2 = null;
				}
			}
			if(href2 != null) {
				sb.append(tr);
				// Id
				sb.append("<td>");
				sb.append("</td>");
				// Err Log
				sb.append("<td>");
				sb.append(href2);
				sb.append("</td>");
				// Err Log Size (in MB)
				sb.append("<td align=\"right\">");
				sb.append(getFileSize(logsjobdir+errfile));
				sb.append("</td>");
				sb.append("</tr>");
			}
		}
	}
	
	private void handleDuccServletJobIdData(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletJobIdData";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		String jobId = request.getParameter("id");
		sb.append(jobId);
		response.getWriter().println(sb);
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}
	
	private void handleDuccServletJobWorkitemsCountData(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletJobWorkitemsCountData";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		
		sb.append("<table>");
		sb.append("<tr>");
		// jobid
		sb.append("<th title=\"The system assigned id for this job\">");
		sb.append("Id:");
		String jobId = request.getParameter("id");
		sb.append(jobId);
		sb.append("&nbsp");
		// workitems
		IDuccWorkJob job = findJob(jobId);
		String jobWorkitemsCount = "?";
		if(job != null) {
			jobWorkitemsCount = job.getSchedulingInfo().getWorkItemsTotal();
		}
		sb.append("<th title=\"The total number of work items for this job\">");
		sb.append("Workitems:");
		sb.append(jobWorkitemsCount);
		sb.append("&nbsp");
		// done
		sb.append("<th title=\"The number of work items that completed successfully\">");
		sb.append("Done:");
		sb.append(job.getSchedulingInfo().getWorkItemsCompleted());
		sb.append("&nbsp");
		// error
		sb.append("<th title=\"The number of work items that failed to complete successfully\">");
		sb.append("Error:");
		sb.append(job.getSchedulingInfo().getIntWorkItemsError());
		sb.append("&nbsp");
		switch(job.getJobState()) {
		case Completed:
			break;
		default:
			// dispatch
			sb.append("<th title=\"The number of work items currently dispatched\">");
			sb.append("Dispatch:");
			sb.append(job.getSchedulingInfo().getWorkItemsDispatched());
			sb.append("&nbsp");
			// unassigned
			sb.append("<th title=\"The number of work items currently dispatched for which acknowledgement is yet to be received\">");
			sb.append("Unassigned:");
			sb.append(job.getSchedulingInfo().getCasQueuedMap().size());
			sb.append("&nbsp");
			// limbo
			sb.append("<th title=\"The number of work items previously dispatched for which process termination is yet to be received (pending re-dispatch)\">");
			sb.append("Limbo:");
			sb.append(job.getSchedulingInfo().getLimboMap().size());
			sb.append("&nbsp");
			break;
		}
		sb.append("</table>");
		response.getWriter().println(sb);
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}
	
	private IDuccWorkJob findJob(String jobno) {
		IDuccWorkJob job = null;
		DuccWorkMap duccWorkMap = DuccData.getInstance().get();
		if(duccWorkMap.getJobKeySet().size()> 0) {
			Iterator<DuccId> iterator = null;
			iterator = duccWorkMap.getJobKeySet().iterator();
			while(iterator.hasNext()) {
				DuccId jobId = iterator.next();
				String fid = ""+jobId.getFriendly();
				if(jobno.equals(fid)) {
					job = (DuccWorkJob) duccWorkMap.findDuccWork(jobId);
					break;
				}
			}
		}
		return job;
	}
	
	private void handleDuccServletJobProcessesData(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletJobProcessesData";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		String jobno = request.getParameter("id");
		DuccWorkMap duccWorkMap = DuccData.getInstance().get();
		DuccWorkJob job = null;
		if(duccWorkMap.getJobKeySet().size()> 0) {
			Iterator<DuccId> iterator = null;
			iterator = duccWorkMap.getJobKeySet().iterator();
			while(iterator.hasNext()) {
				DuccId jobId = iterator.next();
				String fid = ""+jobId.getFriendly();
				if(jobno.equals(fid)) {
					job = (DuccWorkJob) duccWorkMap.findDuccWork(jobId);
					break;
				}
			}
		}
		if(job != null) {
			Iterator<DuccId> iterator = null;
			iterator = job.getDriver().getProcessMap().keySet().iterator();
			int counter = 0;
			while(iterator.hasNext()) {
				DuccId processId = iterator.next();
				IDuccProcess process = job.getDriver().getProcessMap().get(processId);
				buildJobProcessListEntry(sb, job, process, "JD", ++counter);
			}
			iterator = job.getProcessMap().keySet().iterator();
			while(iterator.hasNext()) {
				DuccId processId = iterator.next();
				IDuccProcess process = job.getProcessMap().get(processId);
				buildJobProcessListEntry(sb, job, process, "UIMA", ++counter);
			}
		}
		if(sb.length() == 0) {
			sb.append("<tr>");
			sb.append("<td>");
			sb.append("not found");
			sb.append("</td>");
			sb.append("</tr>");
		}
		response.getWriter().println(sb);
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}
	
	private DuccWorkJob getJob(String jobNo) {
		DuccWorkJob job = null;
		DuccWorkMap duccWorkMap = DuccData.getInstance().get();
		if(duccWorkMap.getJobKeySet().size()> 0) {
			Iterator<DuccId> iterator = null;
			iterator = duccWorkMap.getJobKeySet().iterator();
			while(iterator.hasNext()) {
				DuccId jobId = iterator.next();
				String fid = ""+jobId.getFriendly();
				if(jobNo.equals(fid)) {
					job = (DuccWorkJob) duccWorkMap.findDuccWork(jobId);
					break;
				}
			}
		}
		return job;
	}
	
	private DuccWorkJob getService(String serviceNo) {
		DuccWorkJob job = null;
		DuccWorkMap duccWorkMap = DuccData.getInstance().get();
		if(duccWorkMap.getServiceKeySet().size()> 0) {
			Iterator<DuccId> iterator = null;
			iterator = duccWorkMap.getServiceKeySet().iterator();
			while(iterator.hasNext()) {
				DuccId serviceId = iterator.next();
				String fid = ""+serviceId.getFriendly();
				if(serviceNo.equals(fid)) {
					job = (DuccWorkJob) duccWorkMap.findDuccWork(serviceId);
					break;
				}
			}
		}
		return job;
	}
	
	private long getAdjustedTime(long time, IDuccWorkJob job) {
		long adjustedTime = time;
		if(job.isCompleted()) {
			IDuccStandardInfo stdInfo = job.getStandardInfo();
			long t1 = stdInfo.getDateOfCompletionMillis();
			long t0 = stdInfo.getDateOfSubmissionMillis();
			long tmax = t1-t0;
			if(time > tmax) {
				adjustedTime = tmax;
			}
		}
		return adjustedTime;
	}
	
	private void handleDuccServletJobWorkitemsData(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletJobWorkitemsData";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		String jobNo = request.getParameter("id");
		DuccWorkJob job = getJob(jobNo);
		if(job != null) {
			try {
				WorkItemStateManager workItemStateManager = new WorkItemStateManager(job.getLogDirectory()+jobNo);
				workItemStateManager.importData();
				ConcurrentSkipListMap<Long,IWorkItemState> map = workItemStateManager.getMap();
			    if( (map == null) || (map.size() == 0) ) {
			    	sb.append("no data (map empty?)");
			    }
			    else {
			    	ConcurrentSkipListMap<IWorkItemState,IWorkItemState> sortedMap = new ConcurrentSkipListMap<IWorkItemState,IWorkItemState>();
					for (Entry<Long, IWorkItemState> entry : map.entrySet()) {
						sortedMap.put(entry.getValue(), entry.getValue());
					}
					DuccMachinesData machinesData = DuccMachinesData.getInstance();
			    	DecimalFormat formatter = new DecimalFormat("##0.00");
					double time;
					int counter = 0;
			    	for (Entry<IWorkItemState, IWorkItemState> entry : sortedMap.entrySet()) {
			    		IWorkItemState wis = entry.getValue();
					    sb.append(trGet(counter++));
			    		// SeqNo
						sb.append("<td align=\"right\">");
						sb.append(wis.getSeqNo());
						// Id
						sb.append("<td align=\"right\">");
						sb.append(wis.getWiId());
						// Status
						sb.append("<td align=\"right\">");
						State state = wis.getState();
						sb.append(state);
						// Queuing Time (sec)
						time = getAdjustedTime(wis.getMillisOverhead(), job);
						time = time/1000;
						sb.append("<td align=\"right\">");
						sb.append(formatter.format(time));
						// Processing Time (sec)
						time = getAdjustedTime(wis.getMillisProcessing(), job);
						time = time/1000;
						sb.append("<td align=\"right\">");
						switch(state) {
						case start:
						case queued:
						case operating:
							sb.append("<span title=\"estimated\" class=\"health_green\">");
							break;
						default:
							sb.append("<span class=\"health_black\">");
							break;
						}
						sb.append(formatter.format(time));
						sb.append("</span>");
						// Node (IP)
						sb.append("<td>");
						String node = wis.getNode();
						if(node != null) {
							sb.append(node);
						}
						// Node (Name)
						sb.append("<td>");
						if(node != null) {
							String hostName = machinesData.getNameForIp(node);
							if(hostName != null) {
								sb.append(hostName);
							}
						}
						// PID
						sb.append("<td>");
						String pid = wis.getPid();
						if(pid != null) {
							sb.append(pid);
						}
			    	}
			    }
			}
			catch(Exception e) {
				duccLogger.warn(methodName, null, e);
				sb = new StringBuffer();
				sb.append("no data");
			}
		}
		else {
			sb.append("no data (no job?)");
		}
	
		response.getWriter().println(sb);
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}
	
	private String formatDuration(long ltime) {
		String displayTime;
		String displayDays;
	    SimpleDateFormat sdfTime = new SimpleDateFormat("HH:mm:ss");
		sdfTime.setTimeZone(TimeZone.getTimeZone("UTC"));
		SimpleDateFormat sdfDays = new SimpleDateFormat("DDD");
		sdfDays.setTimeZone(TimeZone.getTimeZone("UTC"));
		Date date = new Date(ltime);
		displayTime = sdfTime.format(date);
		displayDays = sdfDays.format(date);
		Integer days = Integer.valueOf(displayDays) - 1;
		if(days.longValue() > 0) {
			displayDays = chomp("00", displayDays);
			displayDays = chomp("0", displayDays);
			displayTime = displayDays+":"+displayTime;
		}
		displayTime = chomp("00:", displayTime);
		return displayTime;
	}
	
	private void handleDuccServletJobPerformanceData(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletJobPerformanceData";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		String jobNo = request.getParameter("id");
		DuccWorkJob job = getJob(jobNo);
		if(job != null) {
			try {
				PerformanceSummary performanceSummary = new PerformanceSummary(job.getLogDirectory()+jobNo);
			    PerformanceMetricsSummaryMap performanceMetricsSummaryMap = performanceSummary.readSummary();
			    if( (performanceMetricsSummaryMap == null) || (performanceMetricsSummaryMap.size() == 0) ) {
			    	sb.append("no data (map empty?)");
			    }
			    else {
			    	int casCount  = performanceMetricsSummaryMap.casCount();
			    	/*
			    	sb.append("<table>");
			    	sb.append("<tr>");
			    	sb.append("<th align=\"right\">");
			    	sb.append("Job Id:");
			    	sb.append("<th align=\"left\">");
			    	sb.append(jobNo);
			    	sb.append("<th>");
			    	sb.append("&nbsp");
			    	sb.append("<th align=\"right\">");
			    	sb.append("Workitems:");
			    	sb.append("<th align=\"left\">");
			    	sb.append(casCount);
			    	sb.append("</table>");
			    	sb.append("<br>");
			    	*/
			    	sb.append("<table>");
					sb.append("<tr class=\"ducc-head\">");
					sb.append("<th>");
					sb.append("Name");
					sb.append("</th>");
					sb.append("<th>");
					sb.append("Total<br><small>ddd:hh:mm:ss</small>");
					sb.append("</th>");
					sb.append("<th>");
					sb.append("% of<br>Total");
					sb.append("</th>");
					sb.append("<th>");
					sb.append("Avg<br><small>hh:mm:ss/workitem</small>");
					sb.append("</th>");
					sb.append("<th>");
					sb.append("Min<br><small>hh:mm:ss/workitem</small>");
					sb.append("</th>");
					sb.append("<th>");
					sb.append("Max<br><small>hh:mm:ss/workitem</small>");
					sb.append("</th>");
					sb.append("</tr>");
			    	
					ArrayList <UimaStatistic> uimaStats = new ArrayList<UimaStatistic>();
				    uimaStats.clear();
				    long analysisTime = 0;
				    for (Entry<String, PerformanceMetricsSummaryItem> entry : performanceMetricsSummaryMap.entrySet()) {
				    	String key = entry.getKey();
				    	int posName = key.lastIndexOf('=');
				    	long anTime = entry.getValue().getAnalysisTime();
				    	long anMinTime = entry.getValue().getAnalysisTimeMin();
				    	long anMaxTime = entry.getValue().getAnalysisTimeMax();
				    	analysisTime += anTime;
				    	if (posName > 0) {
				    		String shortname = key.substring(posName+1);
				    		UimaStatistic stat = new UimaStatistic(shortname, entry.getKey(), anTime, anMinTime, anMaxTime);
				    		uimaStats.add(stat);
				    	}
				    }
				    Collections.sort(uimaStats);
				    int numstats = uimaStats.size();
				    DecimalFormat formatter = new DecimalFormat("##0.0");
				    // pass 1
				    double time_total = 0;
				    double time_avg = 0;
				    double time_min = 0;
				    double time_max = 0;
				    for (int i = 0; i < numstats; ++i) {
						time_total += (uimaStats.get(i).getAnalysisTime());
						time_min += uimaStats.get(i).getAnalysisMinTime();
						time_max += uimaStats.get(i).getAnalysisMaxTime();
					}
				    time_avg = time_total/casCount;
				    int counter = 0;
				    sb.append(trGet(counter++));
				    // Totals
					sb.append("<td>");
					sb.append("<i><b>Total</b></i>");
					long ltime = 0;
					// Total
					sb.append("<td align=\"right\">");
					ltime = (long)time_total;
					sb.append(formatDuration(ltime));
					// % of Total
					sb.append("<td align=\"right\">");
					sb.append(formatter.format(100));
					// Avg
					sb.append("<td align=\"right\">");
					ltime = (long)time_avg;
					sb.append(formatDuration(ltime));
					// Min
					sb.append("<td align=\"right\">");
					ltime = (long)time_min;
					sb.append(formatDuration(ltime));
					// Max
					sb.append("<td align=\"right\">");
					ltime = (long)time_max;
					sb.append(formatDuration(ltime));
				    // pass 2
				    for (int i = 0; i < numstats; ++i) {
				    	sb.append(trGet(counter++));
						sb.append("<td>");
						sb.append(uimaStats.get(i).getShortName());
						double time;
						// Total
						sb.append("<td align=\"right\">");
						time = uimaStats.get(i).getAnalysisTime();
						ltime = (long)time;
						sb.append(formatDuration(ltime));
						// % of Total
						sb.append("<td align=\"right\">");
						double dtime = (time/time_total)*100;
						sb.append(formatter.format(dtime));
						// Avg
						sb.append("<td align=\"right\">");
						time = time/casCount;
						ltime = (long)time;
						sb.append(formatDuration(ltime));
						// Min
						sb.append("<td align=\"right\">");
						time = uimaStats.get(i).getAnalysisMinTime();
						ltime = (long)time;
						sb.append(formatDuration(ltime));
						// Max
						sb.append("<td align=\"right\">");
						time = uimaStats.get(i).getAnalysisMaxTime();
						ltime = (long)time;
						sb.append(formatDuration(ltime));
					}
					sb.append("</table>");
			    }
			}
			catch(Exception e) {
				duccLogger.warn(methodName, null, e);
				sb = new StringBuffer();
				sb.append("no data");
			}
		}
		else {
			sb.append("no data (no job?)");
		}
		
		response.getWriter().println(sb);
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}
	
	private void putJobSpecEntry(Properties properties, String key, String value, StringBuffer sb, int counter) {
		if(value != null) {
			sb.append(trGet(counter));
			sb.append("<td>");
			sb.append(key);
			sb.append("</td>");
			sb.append("<td>");
			sb.append(value);
			sb.append("</td>");
			sb.append("</tr>");
		}
	}
	
	private void handleDuccServletJobSpecificationData(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletJobSpecificationData";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		String jobNo = request.getParameter("id");
		DuccWorkJob job = getJob(jobNo);
		if(job != null) {
			try {
				String logsjobdir = getUserLogsDir(job)+job.getDuccId().getFriendly()+File.separator;
				String specfile = "job-specification.properties";
				File file = new File(logsjobdir+specfile);
				FileInputStream fis = new FileInputStream(file);
				Properties properties = new Properties();
				properties.load(fis);
				fis.close();
				TreeMap<String,String> map = new TreeMap<String,String>();
				Enumeration<?> enumeration = properties.keys();
				while(enumeration.hasMoreElements()) {
					String key = (String)enumeration.nextElement();
					map.put(key, key);
				}
				Iterator<String> iterator = map.keySet().iterator();
				sb.append("<table>");
				sb.append("<tr class=\"ducc-head\">");
				sb.append("<th>");
				sb.append("Key");
				sb.append("</th>");
				sb.append("<th>");
				sb.append("Value");
				sb.append("</th>");
				sb.append("</tr>");
				int i = 0;
				int counter = 0;
				while(iterator.hasNext()) {
					String key = iterator.next();
					String value = properties.getProperty(key);
					if(key.endsWith("classpath")) {
						String show = "<div class=\"hidedata\"><input type=\"submit\" name=\"showcp\" value=\"Show\" id=\"showbutton"+i+"\"/></div>";
						String hide = "<div class=\"showdata\"><input type=\"submit\" name=\"hidecp\" value=\"Hide\" id=\"hidebutton"+i+"\"/>"+" "+value+"</div>";
						value = show+hide;
						i++;
					}
					putJobSpecEntry(properties, key, value, sb, counter++);
				}
				sb.append("</table>");
				sb.append("<br>");
				sb.append("<br>");
			}
			catch(Exception e) {
				duccLogger.warn(methodName, null, e);
				sb = new StringBuffer();
				sb.append("no data");
			}
		}
		response.getWriter().println(sb);
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}

	private void handleDuccServletJobInitializationFailData(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletJobSpecificationData";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		String jobNo = request.getParameter("id");
		DuccWorkJob job = getJob(jobNo);
		StringBuffer data = new StringBuffer();
		data.append("no data");
		if(job != null) {
			try {
				IDuccProcessMap processMap = job.getProcessMap();
				ArrayList<DuccId> list = processMap.getFailedInitialization();
				int count = list.size();
				if(count > 0) {
					data = new StringBuffer();
					data.append("<table>");
					Iterator<DuccId> processIterator = list.iterator();
					while(processIterator.hasNext()) {
						data.append("<tr>");
						data.append("<td>");
						DuccId processId = processIterator.next();
						IDuccProcess process = processMap.get(processId);
						String logsjobdir = getUserLogsDir(job)+job.getDuccId().getFriendly()+File.separator;
						String logfile = buildLogFileName(job, process, "UIMA");
						String link = logfile;
						String reason = process.getReasonForStoppingProcess();
						if(reason != null) {
							if(reason.trim() != "") {
								link = logfile+":"+reason;
							}
						}
						String href = "<a href=\""+duccLogData+"?"+"loc=bot&"+"fname="+logsjobdir+logfile+"\" onclick=\"var newWin = window.open(this.href,'child','height=800,width=1200,scrollbars');  newWin.focus(); return false;\">"+link+"</a>";
						data.append(href);
					}
					data.append("</table>");
				}
			}
			catch(Exception e) {
				duccLogger.warn(methodName, null, e);
			}
		}
		sb.append("<html><head></head><body><span>"+data+"</span></body></html>");
		response.getWriter().println(sb);
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}

	private void handleDuccServletJobRuntimeFailData(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletJobSpecificationData";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		String jobNo = request.getParameter("id");
		DuccWorkJob job = getJob(jobNo);
		StringBuffer data = new StringBuffer();
		data.append("no data");
		if(job != null) {
			try {
				IDuccProcessMap processMap = job.getProcessMap();
				ArrayList<DuccId> list = processMap.getFailedNotInitialization();
				int count = list.size();
				if(count > 0) {
					data = new StringBuffer();
					data.append("<table>");
					Iterator<DuccId> processIterator = list.iterator();
					while(processIterator.hasNext()) {
						data.append("<tr>");
						data.append("<td>");
						DuccId processId = processIterator.next();
						IDuccProcess process = processMap.get(processId);
						String logsjobdir = getUserLogsDir(job)+job.getDuccId().getFriendly()+File.separator;
						String logfile = buildLogFileName(job, process, "UIMA");
						String link = logfile;
						String reason = process.getReasonForStoppingProcess();
						if(reason != null) {
							if(reason.trim() != "") {
								link = logfile+":"+reason;
							}
						}
						String href = "<a href=\""+duccLogData+"?"+"loc=bot&"+"fname="+logsjobdir+logfile+"\" onclick=\"var newWin = window.open(this.href,'child','height=800,width=1200,scrollbars');  newWin.focus(); return false;\">"+link+"</a>";
						data.append(href);
					}
					data.append("</table>");
				}
			}
			catch(Exception e) {
				duccLogger.warn(methodName, null, e);
			}
		}
		sb.append("<html><head></head><body><span>"+data+"</span></body></html>");
		response.getWriter().println(sb);
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}
	
	private void buildReservationsListEntry(HttpServletRequest request, StringBuffer sb, DuccId duccId, IDuccWorkReservation reservation, DuccData duccData) {
		String type="Reservation";
		String id = normalize(duccId);
		sb.append("<td class=\"ducc-col-terminate\">");
		if(terminateEnabled) {
			if(!reservation.isCompleted()) {
				String disabled = getDisabled(request,reservation);
				String user = reservation.getStandardInfo().getUser();
				if(user != null) {
					if(user.equals(JdConstants.reserveUser)) {
						disabled = "disabled=\"disabled\"";
					}
				}
				sb.append("<input type=\"button\" onclick=\"ducc_confirm_terminate_reservation("+id+")\" value=\"Terminate\" "+disabled+"/>");
			}
		}
		sb.append("</td>");
		// Id
		sb.append("<td>");
		sb.append(id);
		sb.append("</td>");
		// Start
		sb.append("<td>");
		sb.append(getTimeStamp(reservation.getDuccId(),reservation.getStandardInfo().getDateOfSubmission()));
		sb.append("</td>");
		// End
		sb.append("<td>");
		switch(reservation.getReservationState()) {
		case Completed:
			sb.append(getTimeStamp(reservation.getDuccId(),reservation.getStandardInfo().getDateOfCompletion()));
			break;
		default:
			
			break;
		}
		sb.append("</td>");
		// User
		sb.append("<td>");
		UserId userId = new UserId(reservation.getStandardInfo().getUser());
		sb.append(userId.toString());
		sb.append("</td>");
		// Class
		sb.append("<td>");
		sb.append(stringNormalize(reservation.getSchedulingInfo().getSchedulingClass(),messages.fetch("default")));
		sb.append("</td>");
		// State
		sb.append("<td>");
		if(duccData.isLive(duccId)) {
			if(reservation.isOperational()) {
				sb.append("<span class=\"active_state\">");
			}
			else {
				sb.append("<span class=\"completed_state\">");
			}
		}
		else {
			sb.append("<span class=\"historic_state\">");
		}
		sb.append(reservation.getStateObject().toString());
		if(duccData.isLive(duccId)) {
			sb.append("</span>");
		}
		sb.append("</td>");
		// Reason
		sb.append("<td>");
		switch(reservation.getCompletionType()) {
		case Undefined:
			break;
		case CanceledByUser:
		case CanceledByAdmin:
			try {
				String cancelUser = reservation.getStandardInfo().getCancelUser();
				if(cancelUser != null) {
					sb.append("<span title=\"canceled by "+cancelUser+"\">");
					sb.append(reservation.getCompletionTypeObject().toString());
					sb.append("</span>");
				}
				else {
										
					IRationale rationale = reservation.getCompletionRationale();
					if(rationale != null) {
						sb.append("<span title=\""+rationale+"\">");
						sb.append(reservation.getCompletionTypeObject().toString());
						sb.append("</span>");
					}
					else {
						sb.append(reservation.getCompletionTypeObject().toString());
					}
					
				}
			} 
			catch(Exception e) {
				IRationale rationale = reservation.getCompletionRationale();
				if(rationale != null) {
					sb.append("<span title=\""+rationale+"\">");
					sb.append(reservation.getCompletionTypeObject().toString());
					sb.append("</span>");
				}
				else {
					sb.append(reservation.getCompletionTypeObject().toString());
				}
			}
			break;
		default:
			IRationale rationale = reservation.getCompletionRationale();
			if(rationale != null) {
				sb.append("<span title=\""+rationale+"\">");
				sb.append(reservation.getCompletionTypeObject().toString());
				sb.append("</span>");
			}
			else {
				sb.append(reservation.getCompletionTypeObject().toString());
			}
			break;
		}
		sb.append("</td>");
		// Allocation
		sb.append("<td align=\"right\">");
		sb.append(reservation.getSchedulingInfo().getInstancesCount());
		sb.append("</td>");
		// 
		TreeMap<String,Integer> nodeMap = new TreeMap<String,Integer>(); 
		if(!reservation.getReservationMap().isEmpty()) {
			IDuccReservationMap map = reservation.getReservationMap();
			for (DuccId key : map.keySet()) { 
				IDuccReservation value = reservation.getReservationMap().get(key);
				String node = value.getNodeIdentity().getName();
				if(!nodeMap.containsKey(node)) {
					nodeMap.put(node,new Integer(0));
				}
				Integer count = nodeMap.get(node);
				count++;
				nodeMap.put(node,count);
			}
		}
		// User Processes
		boolean qualify = false;
		if(!nodeMap.isEmpty()) {
			if(nodeMap.keySet().size() > 1) {
				qualify = true;
			}
		}
		sb.append("<td align=\"right\">");
		ArrayList<String> qualifiedPids = new ArrayList<String>();
		if(reservation.isOperational()) {
			DuccMachinesData machinesData = DuccMachinesData.getInstance();
			for (String node: nodeMap.keySet()) { 
				NodeId nodeId = new NodeId(node);
				List<String> nodePids = machinesData.getPids(nodeId, userId);
				for( String pid : nodePids ) {
					if(qualify) {
						qualifiedPids.add(node+":"+pid);
					}
					else {
						qualifiedPids.add(pid);
					}
				}
			}
		}
		if(qualifiedPids.size() > 0) {
			String list = "";
			for( String entry : qualifiedPids ) {
				list += entry+" ";
			}
			sb.append("<span title=\""+list.trim()+"\">");
			sb.append(""+qualifiedPids.size());
			sb.append("</span>");
		}
		else {
			sb.append(""+qualifiedPids.size());
		}
		sb.append("</td>");
		// Size
		sb.append("<td align=\"right\">");
		String size = reservation.getSchedulingInfo().getShareMemorySize();
		MemoryUnits units = reservation.getSchedulingInfo().getShareMemoryUnits();
		sb.append(getProcessMemorySize(duccId,type,size,units));
		sb.append("</td>");
		// List
		sb.append("<td>");
		if(!nodeMap.isEmpty()) {
			sb.append("<select>");
			for (String node: nodeMap.keySet()) {
				Integer count = nodeMap.get(node);
				String option = node+" "+"["+count+"]";
				sb.append("<option>"+option+"</option>");
			}
			sb.append("</select>");
		}
		sb.append("</td>");
		// Description
		sb.append("<td>");
		sb.append(stringNormalize(reservation.getStandardInfo().getDescription(),messages.fetch("none")));
		sb.append("</td>");
		sb.append("</tr>");
	}
	
	private int getReservationsMax(HttpServletRequest request) {
		int maxRecords = defaultRecordsReservations;
		try {
			String cookie = DuccWebUtil.getCookie(request,DuccWebUtil.cookieReservationsMax);
			int reqRecords = Integer.parseInt(cookie);
			if(reqRecords <= maximumRecordsReservations) {
				if(reqRecords > 0) {
					maxRecords = reqRecords;
				}
			}
		}
		catch(Exception e) {
		}
		return maxRecords;
	}
	
	private ArrayList<String> getReservationsUsers(HttpServletRequest request) {
		ArrayList<String> userRecords = new ArrayList<String>();
		try {
			String cookie = DuccWebUtil.getCookie(request,DuccWebUtil.cookieReservationsUsers);
			String[] users = cookie.split(" ");
			if(users != null) {
				for(String user : users) {
					user = user.trim();
					if(user.length() > 0) {
						if(!userRecords.contains(user)) {
							userRecords.add(user);
						}
					}
				}
			}
		}
		catch(Exception e) {
		}
		return userRecords;
	}
	
	private enum ReservationsUsersQualifier {
		ActiveInclude,
		ActiveExclude,
		Include,
		Exclude,
		Unknown
	}
	
	private ReservationsUsersQualifier getReservationsUsersQualifier(HttpServletRequest request) {
		ReservationsUsersQualifier retVal = ReservationsUsersQualifier.Unknown;
		try {
			String cookie = DuccWebUtil.getCookie(request,DuccWebUtil.cookieReservationsUsersQualifier);
			String qualifier = cookie.trim();
			if(qualifier.equals("include")) {
				retVal = ReservationsUsersQualifier.Include;
			}
			else if(qualifier.equals("exclude")) {
				retVal = ReservationsUsersQualifier.Exclude;
			}
			else if(qualifier.equals("active+include")) {
				retVal = ReservationsUsersQualifier.ActiveInclude;
			}
			else if(qualifier.equals("active+exclude")) {
				retVal = ReservationsUsersQualifier.ActiveExclude;
			}
		}
		catch(Exception e) {
		}
		return retVal;
	}
	
	private void handleDuccServletReservationsData(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletReservationsData";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		int maxRecords = getReservationsMax(request);
		ArrayList<String> users = getReservationsUsers(request);
		StringBuffer sb = new StringBuffer();
		DuccData duccData = DuccData.getInstance();
		ConcurrentSkipListMap<ReservationInfo,ReservationInfo> sortedReservations = duccData.getSortedReservations();
		ReservationsUsersQualifier userQualifier = getReservationsUsersQualifier(request);
		if(sortedReservations.size()> 0) {
			Iterator<Entry<ReservationInfo, ReservationInfo>> iterator = sortedReservations.entrySet().iterator();
			int counter = 0;
			while(iterator.hasNext()) {
				ReservationInfo reservationInfo = iterator.next().getValue();
				DuccWorkReservation reservation = reservationInfo.getReservation();
				boolean list = false;
				if(!users.isEmpty()) {
					String reservationUser = reservation.getStandardInfo().getUser().trim();
					switch(userQualifier) {
					case ActiveInclude:
					case Unknown:
						if(!reservation.isCompleted()) {
							list = true;
						}
						else if(users.contains(reservationUser)) {
							if(maxRecords > 0) {
								if (counter++ < maxRecords) {
									list = true;
								}
							}
						}
						break;
					case ActiveExclude:
						if(!reservation.isCompleted()) {
							list = true;
						}
						else if(!users.contains(reservationUser)) {
							if(maxRecords > 0) {
								if (counter++ < maxRecords) {
									list = true;
								}
							}
						}
						break;
					case Include:
						if(users.contains(reservationUser)) {
							if(maxRecords > 0) {
								if (counter++ < maxRecords) {
									list = true;
								}
							}
						}
						break;
					case Exclude:
						if(!users.contains(reservationUser)) {
							if(maxRecords > 0) {
								if (counter++ < maxRecords) {
									list = true;
								}
							}
						}
						break;
					}	
				}
				else {
					if(!reservation.isCompleted()) {
						list = true;
					}
					else if(maxRecords > 0) {
						if (counter++ < maxRecords) {
							list = true;
						}
					}
				}
				if(list) {
					sb.append(trGet(counter));
					buildReservationsListEntry(request, sb, reservation.getDuccId(), reservation, duccData);
				}
			}
		}
		else {
			sb.append("<tr>");
			sb.append("<td>");
			if(DuccData.getInstance().isPublished()) {
				sb.append(messages.fetch("no reservations"));
			}
			else {
				sb.append(messages.fetch("no data"));
			}
			sb.append("</td>");
			sb.append("</tr>");
		}
		response.getWriter().println(sb);
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}
	
	private int getServicesMax(HttpServletRequest request) {
		int maxRecords = defaultRecordsServices;
		try {
			String cookie = DuccWebUtil.getCookie(request,DuccWebUtil.cookieServicesMax);
			int reqRecords = Integer.parseInt(cookie);
			if(reqRecords <= maximumRecordsServices) {
				if(reqRecords > 0) {
					maxRecords = reqRecords;
				}
			}
		}
		catch(Exception e) {
		}
		return maxRecords;
	}
	
	private ArrayList<String> getServicesUsers(HttpServletRequest request) {
		ArrayList<String> userRecords = new ArrayList<String>();
		try {
			String cookie = DuccWebUtil.getCookie(request,DuccWebUtil.cookieServicesUsers);
			String[] users = cookie.split(" ");
			if(users != null) {
				for(String user : users) {
					user = user.trim();
					if(user.length() > 0) {
						if(!userRecords.contains(user)) {
							userRecords.add(user);
						}
					}
				}
			}
		}
		catch(Exception e) {
		}
		return userRecords;
	}
	
	private enum ServicesUsersQualifier {
		ActiveInclude,
		ActiveExclude,
		Include,
		Exclude,
		Unknown
	}
	
	private ServicesUsersQualifier getServicesUsersQualifier(HttpServletRequest request) {
		ServicesUsersQualifier retVal = ServicesUsersQualifier.Unknown;
		try {
			String cookie = DuccWebUtil.getCookie(request,DuccWebUtil.cookieServicesUsersQualifier);
			String qualifier = cookie.trim();
			if(qualifier.equals("include")) {
				retVal = ServicesUsersQualifier.Include;
			}
			else if(qualifier.equals("exclude")) {
				retVal = ServicesUsersQualifier.Exclude;
			}
			else if(qualifier.equals("active+include")) {
				retVal = ServicesUsersQualifier.ActiveInclude;
			}
			else if(qualifier.equals("active+exclude")) {
				retVal = ServicesUsersQualifier.ActiveExclude;
			}
		}
		catch(Exception e) {
		}
		return retVal;
	}
	
	private String getValue(Properties properties, String key, String defaultValue) {
		String retVal = defaultValue;
		if(properties != null) {
			if(key != null) {
				retVal = properties.getProperty(key, defaultValue);
			}
		}
		return retVal;
	}
	
	private void handleServletJsonServicesDefinitionsData(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleServletJsonServicesDefinitionsData";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		sb.append("{ ");
		sb.append("\"aaData\": [ ");
		
		IStateServices iss = StateServices.getInstance();
		StateServicesDirectory ssd = iss.getStateServicesDirectory();
		String sep = "";
		for(Integer key : ssd.getDescendingKeySet()) {
			StateServicesSet entry = ssd.get(key);
			Properties propertiesSvc = entry.get(IStateServices.svc);
			Properties propertiesMeta = entry.get(IStateServices.meta);
			sb.append(sep);
			sb.append("[");
			// Service Id
			sb.append(quote(""+key));
			sb.append(",");
			// Endpoint
			sb.append(quote(getValue(propertiesMeta,IStateServices.endpoint,"")));
			sb.append(",");
			// No. of Instances
			sb.append(quote(getValue(propertiesMeta,IStateServices.instances,"")));
			sb.append(",");
			// Owning User
			sb.append(quote(getValue(propertiesMeta,IStateServices.user,"")));
			sb.append(",");
			// Scheduling Class
			sb.append(quote(getValue(propertiesSvc,IStateServices.scheduling_class,"")));
			sb.append(",");
			// Process Memory Size
			sb.append(quote(getValue(propertiesSvc,IStateServices.process_memory_size,"")));
			sb.append(",");
			// Description
			sb.append(quote(getValue(propertiesSvc,IStateServices.description,"")));
			sb.append("]");
			sep = ", ";
		}
		
		sb.append(" ]");
		sb.append(" }");
		duccLogger.debug(methodName, null, sb);
		response.getWriter().println(sb);
		response.setContentType("application/json");
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}	
	
	private void handleDuccServletServicesDeploymentsData(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletServicesDeploymentsData";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		int maxRecords = getServicesMax(request);
		ArrayList<String> users = getServicesUsers(request);
		StringBuffer sb = new StringBuffer();
		DuccData duccData = DuccData.getInstance();
		ConcurrentSkipListMap<JobInfo,JobInfo> sortedServices = duccData.getSortedServices();
		ServicesUsersQualifier userQualifier = getServicesUsersQualifier(request);
		if(sortedServices.size()> 0) {
			Iterator<Entry<JobInfo, JobInfo>> iterator = sortedServices.entrySet().iterator();
			int counter = 0;
			while(iterator.hasNext()) {
				JobInfo jobInfo = iterator.next().getValue();
				DuccWorkJob service = jobInfo.getJob();
				boolean list = false;
				if(!users.isEmpty()) {
					String serviceUser = service.getStandardInfo().getUser().trim();
					switch(userQualifier) {
					case ActiveInclude:
					case Unknown:
						if(!service.isCompleted()) {
							list = true;
						}
						else if(users.contains(serviceUser)) {
							if(maxRecords > 0) {
								if (counter++ < maxRecords) {
									list = true;
								}
							}
						}
						break;
					case ActiveExclude:
						if(!service.isCompleted()) {
							list = true;
						}
						else if(!users.contains(serviceUser)) {
							if(maxRecords > 0) {
								if (counter++ < maxRecords) {
									list = true;
								}
							}
						}
						break;
					case Include:
						if(users.contains(serviceUser)) {
							if(maxRecords > 0) {
								if (counter++ < maxRecords) {
									list = true;
								}
							}
						}
						break;
					case Exclude:
						if(!users.contains(serviceUser)) {
							if(maxRecords > 0) {
								if (counter++ < maxRecords) {
									list = true;
								}
							}
						}
						break;
					}	
				}
				else {
					if(!service.isCompleted()) {
						list = true;
					}
					else if(maxRecords > 0) {
						if (counter++ < maxRecords) {
							list = true;
						}
					}
				}
				if(list) {
					sb.append(trGet(counter));
					buildServicesListEntry(request, sb, service.getDuccId(), service, duccData);
				}
			}
		}
		else {
			sb.append("<tr>");
			sb.append("<td>");
			if(DuccData.getInstance().isPublished()) {
				sb.append(messages.fetch("no services"));
			}
			else {
				sb.append(messages.fetch("no data"));
			}
			sb.append("</td>");
			sb.append("</tr>");
		}
		response.getWriter().println(sb);
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}

	private void buildServicesListEntry(HttpServletRequest request, StringBuffer sb, DuccId duccId, IDuccWorkJob job, DuccData duccData) {
		String type = "Service";
		String id = normalize(duccId);
		sb.append("<td class=\"ducc-col-terminate\">");
		if(terminateEnabled) {
			if(!job.isFinished()) {
				sb.append("<input type=\"button\" onclick=\"ducc_confirm_terminate_job("+id+")\" value=\"Terminate\" "+getDisabled(request,job)+"/>");
			}
		}
		sb.append("</td>");
		// Id
		sb.append("<td>");
		sb.append("<a href=\"service.details.html?id="+id+"\">"+id+"</a>");
		sb.append("</td>");
		// Start
		sb.append("<td>");
		sb.append(getTimeStamp(job.getDuccId(), job.getStandardInfo().getDateOfSubmission()));
		sb.append("</td>");
		// End
		sb.append("<td>");
		sb.append(getCompletionOrProjection(job));
		sb.append("</td>");
		// User
		sb.append("<td>");
		sb.append(job.getStandardInfo().getUser());
		sb.append("</td>");
		// Class
		sb.append("<td>");
		sb.append(stringNormalize(job.getSchedulingInfo().getSchedulingClass(),messages.fetch("default")));
		sb.append("</td>");
		/*
		sb.append("<td align=\"right\">");
		sb.append(stringNormalize(duccWorkJob.getSchedulingInfo().getSchedulingPriority(),messages.fetch("default")));
		sb.append("</td>");
		*/
		// State
		sb.append("<td>");
		if(duccData.isLive(duccId)) {
			if(job.isOperational()) {
				sb.append("<span class=\"active_state\">");
			}
			else {
				sb.append("<span class=\"completed_state\">");
			}
		}
		else {
			sb.append("<span class=\"historic_state\">");
		}
		sb.append(job.getStateObject().toString());
		if(duccData.isLive(duccId)) {
			sb.append("</span>");
		}
		sb.append("</td>");
		// Reason
		if(job.isOperational()) {
			sb.append("<td valign=\"bottom\">");
			ArrayList<String> swappingMachines = getSwappingMachines(job);
			if(!swappingMachines.isEmpty()) {
				StringBuffer mb = new StringBuffer();
				for(String machine : swappingMachines) {
					mb.append(machine);
					mb.append(" ");
				}
				String ml = mb.toString().trim();
				sb.append("<span class=\"health_red\" title=\""+ml+"\">");
				sb.append("Swapping");
				sb.append("</span>");
			}
			sb.append("</td>");
		}
		else if(job.isCompleted()) {
			JobCompletionType jobCompletionType = job.getCompletionType();
			switch(jobCompletionType) {
			case EndOfJob:
			case Undefined:
				sb.append("<td valign=\"bottom\">");
				break;
			default:
				IRationale rationale = job.getCompletionRationale();
				if(rationale != null) {
					sb.append("<td valign=\"bottom\" title=\""+rationale+"\">");
				}
				else {
					sb.append("<td valign=\"bottom\">");
				}
				break;
			}
			sb.append(jobCompletionType);
			sb.append("</td>");
		}
		// Processes
		sb.append("<td align=\"right\">");
		if(duccData.isLive(duccId)) {
			sb.append(job.getProcessMap().getAliveProcessCount());
		}
		else {
			sb.append("0");
		}
		sb.append("</td>");
		// Initialize Failures
		sb.append("<td align=\"right\">");
		sb.append(buildInitializeFailuresLink(job));
		sb.append("</td>");
		// Runtime Failures
		sb.append("<td align=\"right\">");
		sb.append(buildRuntimeFailuresLink(job));
		sb.append("</td>");
		// Size
		sb.append("<td align=\"right\">");
		String size = job.getSchedulingInfo().getShareMemorySize();
		MemoryUnits units = job.getSchedulingInfo().getShareMemoryUnits();
		sb.append(getProcessMemorySize(duccId,type,size,units));
		sb.append("</td>");
		// Description
		sb.append("<td>");
		sb.append(stringNormalize(job.getStandardInfo().getDescription(),messages.fetch("none")));
		sb.append("</td>");
		sb.append("</tr>");
	}

	private void buildServiceProcessListEntry(StringBuffer sb, DuccWorkJob job, IDuccProcess process, String type, int counter) {
		String logsjobdir = getUserLogsDir(job)+job.getDuccId().getFriendly()+File.separator;
		String logfile = buildLogFileName(job, process, type);
		String href = "<a href=\""+duccLogData+"?"+"fname="+logsjobdir+logfile+"\" onclick=\"var newWin = window.open(this.href,'child','height=800,width=1200,scrollbars');  newWin.focus(); return false;\">"+logfile+"</a>";
		sb.append(trGet(counter));
		sb.append("<td>");
		sb.append(process.getDuccId().getFriendly());
		sb.append("</td>");
		sb.append("<td>");
		sb.append(href);
		sb.append("</td>");
		sb.append("<td>");
		sb.append(process.getNodeIdentity().getName());
		sb.append("</td>");
		sb.append("<td>");
		sb.append(process.getNodeIdentity().getIp());
		sb.append("</td>");
		sb.append("<td>");
		sb.append(process.getPID());
		sb.append("</td>");
		sb.append("<td>");
		sb.append(process.getResourceState());
		sb.append("</td>");
		ProcessDeallocationType deallocationType = process.getProcessDeallocationType();
		String toolTip = ProcessDeallocationType.getToolTip(deallocationType);
		if(toolTip == null) {
			sb.append("<td>");
		}
		else {
			sb.append("<td title=\""+toolTip+"\">");
		}
		switch(deallocationType) {
		case Undefined:
			break;
		default:
			sb.append(process.getProcessDeallocationType());
			break;
		}
		sb.append("</td>");
		sb.append("<td>");
		sb.append(process.getProcessState());
		sb.append("</td>");
		sb.append("<td>");
		String agentReason = process.getReasonForStoppingProcess();
		if(agentReason != null) {
			sb.append(process.getReasonForStoppingProcess());
		}
		sb.append("</td>");
		TimeWindow t;
		t = (TimeWindow) process.getTimeWindowInit();
		String initTime = "?";
		if(t != null) {
			initTime = t.getElapsed();
		}
		sb.append("<td>");
		if((t != null) && (t.isEstimated())) {
			sb.append("<span title=\"estimated\" class=\"health_green\">");
		}
		else {
			sb.append("<span class=\"health_black\">");
		}
		sb.append(initTime);
		sb.append("</span>");
		sb.append("</td>");
		t = (TimeWindow) process.getTimeWindowRun();
		String runTime = "?";
		if(t != null) {
			runTime = t.getElapsed();
		}
		sb.append("<td>");
		if((t != null) && (t.isEstimated())) {
			sb.append("<span title=\"estimated\" class=\"health_green\">");
		}
		else {
			sb.append("<span class=\"health_black\">");
		}
		sb.append(runTime);
		sb.append("</span>");
		sb.append("</td>");
		
		sb.append("<td>");
		switch(process.getProcessState()) {
		case Running:
			String jmxUrl = process.getProcessJmxUrl();
			if(jmxUrl != null) {
				sb.append(buildjConsoleLink(jmxUrl));
			}
		}
		sb.append("</td>");
		sb.append("</tr>");
	}
	
	private void handleDuccServletServiceProcessesData(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletServiceProcessesData";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		String serviceno = request.getParameter("id");
		DuccWorkMap duccWorkMap = DuccData.getInstance().get();
		DuccWorkJob service = null;
		if(duccWorkMap.getServiceKeySet().size()> 0) {
			Iterator<DuccId> iterator = null;
			iterator = duccWorkMap.getServiceKeySet().iterator();
			while(iterator.hasNext()) {
				DuccId serviceId = iterator.next();
				String fid = ""+serviceId.getFriendly();
				if(serviceno.equals(fid)) {
					service = (DuccWorkJob) duccWorkMap.findDuccWork(serviceId);
					break;
				}
			}
		}
		if(service != null) {
			Iterator<DuccId> iterator = null;
			IDuccProcessMap map = service.getProcessMap();
			iterator = map.keySet().iterator();
			int counter = 0;
			while(iterator.hasNext()) {
				DuccId processId = iterator.next();
				IDuccProcess process = map.get(processId);
				buildServiceProcessListEntry(sb, service, process, "UIMA", ++counter);
			}
		}
		if(sb.length() == 0) {
			sb.append("<tr>");
			sb.append("<td>");
			sb.append("not found");
			sb.append("</td>");
			sb.append("</tr>");
		}
		response.getWriter().println(sb);
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}
	
	private void handleDuccServletServiceSpecificationData(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletServiceSpecificationData";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		String jobNo = request.getParameter("id");
		DuccWorkJob service = getService(jobNo);
		if(service != null) {
			try {
				String logsjobdir = getUserLogsDir(service)+service.getDuccId().getFriendly()+File.separator;
				String specfile = "service-specification.properties";
				File file = new File(logsjobdir+specfile);
				FileInputStream fis = new FileInputStream(file);
				Properties properties = new Properties();
				properties.load(fis);
				fis.close();
				TreeMap<String,String> map = new TreeMap<String,String>();
				Enumeration<?> enumeration = properties.keys();
				while(enumeration.hasMoreElements()) {
					String key = (String)enumeration.nextElement();
					map.put(key, key);
				}
				Iterator<String> iterator = map.keySet().iterator();
				sb.append("<table>");
				sb.append("<tr class=\"ducc-head\">");
				sb.append("<th>");
				sb.append("Key");
				sb.append("</th>");
				sb.append("<th>");
				sb.append("Value");
				sb.append("</th>");
				sb.append("</tr>");
				int i = 0;
				int counter = 0;
				while(iterator.hasNext()) {
					String key = iterator.next();
					String value = properties.getProperty(key);
					if(key.endsWith("classpath")) {
						String show = "<div class=\"hidedata\"><input type=\"submit\" name=\"showcp\" value=\"Show\" id=\"showbutton"+i+"\"/></div>";
						String hide = "<div class=\"showdata\"><input type=\"submit\" name=\"hidecp\" value=\"Hide\" id=\"hidebutton"+i+"\"/>"+" "+value+"</div>";
						value = show+hide;
						i++;
					}
					putJobSpecEntry(properties, key, value, sb, counter++);
				}
				sb.append("</table>");
				sb.append("<br>");
				sb.append("<br>");
			}
			catch(Exception e) {
				duccLogger.warn(methodName, null, e);
				sb = new StringBuffer();
				sb.append("no data");
			}
		}
		response.getWriter().println(sb);
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}
	
	private void addMachineInfo(StringBuffer sb, MachineInfo machineInfo, int counter) {
		addMachineInfo(sb, machineInfo.getStatus(), machineInfo.getFileDef(), machineInfo.getIp(), machineInfo.getName(), machineInfo.getMemTotal(), machineInfo.getMemSwap(), machineInfo.getSharesTotal(), machineInfo.getSharesInuse(), machineInfo.getElapsed(), counter);
	}
	
	private void addMachineInfo(StringBuffer sb, String status, String fileDef, String ip, String name, String memTotal, String memSwap, String sharesTotal, String sharesInuse, String heartbeat, int counter) {
		sb.append(trGet(counter));
		sb.append("<td>");
		if(status.equals("down")) {
			sb.append("<span class=\"health_red\">");
			sb.append(status);
			sb.append("</span>");
		}
		else if(status.equals("up")) {
			sb.append("<span class=\"health_green\">");
			sb.append(status);
			sb.append("</span>");
		}
		else {
			sb.append("<span title=\""+"File:"+fileDef+"\""+">");
			sb.append(status);
			sb.append("</span>");
		}
		sb.append("</td>");
		sb.append("<td>");
		sb.append(ip);
		sb.append("</td>");
		sb.append("<td>");
		sb.append(name);
		sb.append("</td>");
		sb.append("<td align=\"right\">");
		sb.append(memTotal);
		sb.append("</td>");
		sb.append("<td align=\"right\">");
		if(memSwap.equals("0")) {
			sb.append(memSwap);
		}
		else {
			sb.append("<span class=\"health_red\">");
			sb.append(memSwap);
			sb.append("</span>");
		}
		sb.append("</td>");
		sb.append("<td align=\"right\">");
		sb.append(sharesTotal);
		sb.append("</td>");
		sb.append("<td align=\"right\">");
		sb.append(sharesInuse);
		sb.append("</td>");
		sb.append("<td align=\"right\">");
		sb.append(heartbeat);
		sb.append("</td>");
		sb.append("</tr>");
	}
	
	private long getMillisMIA(DaemonName daemonName) {
		String methodName = "getMillisMIA";
		long secondsMIA = -1;
		Properties properties = DuccWebProperties.get();
		switch(daemonName) {
		case Orchestrator:
			String or_rate = properties.getProperty("ducc.orchestrator.state.publish.rate");
			String or_ratio = "1";
			try {
				long rate = Long.parseLong(or_rate.trim());
				long ratio = Long.parseLong(or_ratio .trim());
				secondsMIA = 3 * rate * ratio;
			}
			catch(Throwable t) {
				duccLogger.debug(methodName, null, t);
			}
			break;
		case ResourceManager:
			String rm_rate = properties.getProperty("ducc.orchestrator.state.publish.rate");
			String rm_ratio = properties.getProperty("ducc.rm.state.publish.ratio");
			try {
				long rate = Long.parseLong(rm_rate.trim());
				long ratio = Long.parseLong(rm_ratio .trim());
				secondsMIA = 3 * rate * ratio;
			}
			catch(Throwable t) {
				duccLogger.debug(methodName, null, t);
			}
			break;
		case ServiceManager:
			String sm_rate = properties.getProperty("ducc.orchestrator.state.publish.rate");
			String sm_ratio = "1";
			try {
				long rate = Long.parseLong(sm_rate.trim());
				long ratio = Long.parseLong(sm_ratio .trim());
				secondsMIA = 3 * rate * ratio;
			}
			catch(Throwable t) {
				duccLogger.debug(methodName, null, t);
			}
			break;
		case ProcessManager:
			String pm_rate = properties.getProperty("ducc.pm.state.publish.rate");
			String pm_ratio = "1";
			try {
				long rate = Long.parseLong(pm_rate.trim());
				long ratio = Long.parseLong(pm_ratio .trim());
				secondsMIA = 3 * rate * ratio;
			}
			catch(Throwable t) {
				duccLogger.debug(methodName, null, t);
			}
			break;
		}
		return secondsMIA;
	}
	
	private void handleServletJsonMachinesData(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleJsonServletMachinesData";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		sb.append("{ ");
		sb.append("\"aaData\": [ ");
		DuccMachinesData instance = DuccMachinesData.getInstance();
		ConcurrentSkipListMap<MachineInfo,String> sortedMachines = instance.getSortedMachines();
		Iterator<MachineInfo> iterator;
		// pass 1
		iterator = sortedMachines.keySet().iterator();
		long memTotal = 0;
		long memSwap = 0;
		long alienPids = 0;
		long sharesTotal = 0;
		long sharesInuse = 0;
		while(iterator.hasNext()) {
			MachineInfo machineInfo = iterator.next();
			try {
				memTotal += Long.parseLong(machineInfo.getMemTotal());
			}
			catch(Exception e) {};
			try {
				memSwap += Long.parseLong(machineInfo.getMemSwap());
			}
			catch(Exception e) {};
			try {
				List<ProcessInfo> alienPidsList = machineInfo.getAlienPids();
				if(alienPidsList != null) {
					alienPids += alienPidsList.size();
				}
			}
			catch(Exception e) {};
			try {
				sharesTotal += Long.parseLong(machineInfo.getSharesTotal());
			}
			catch(Exception e) {};
			try {
				sharesInuse += Long.parseLong(machineInfo.getSharesInuse());
			}
			catch(Exception e) {};
		}
		// pass 2
		iterator = sortedMachines.keySet().iterator();
		
		sb.append("[");
		
		sb.append(quote("Total"));
		sb.append(",");
		sb.append(quote(""));
		sb.append(",");
		sb.append(quote(""));
		sb.append(",");
		sb.append(quote(""+memTotal));
		sb.append(",");
		sb.append(quote(""+memSwap));
		sb.append(",");
		sb.append(quote(""+alienPids));
		sb.append(",");
		sb.append(quote(""+sharesTotal));
		sb.append(",");
		sb.append(quote(""+sharesInuse));
		sb.append(",");
		sb.append(quote(""));
		
		sb.append("]");
		
		while(iterator.hasNext()) {
			sb.append(",");
			sb.append("[");
			
			MachineInfo machineInfo = iterator.next();
			sb.append(quote(machineInfo.getStatus()));
			sb.append(",");
			sb.append(quote(machineInfo.getIp()));
			sb.append(",");
			sb.append(quote(machineInfo.getName()));
			sb.append(",");
			sb.append(quote(machineInfo.getMemTotal()));
			sb.append(",");
			sb.append(quote(machineInfo.getMemSwap()));
			sb.append(",");
			List<ProcessInfo> alienPidsList = machineInfo.getAlienPids();
			String alienPidsDisplay = "";
			if(alienPidsList != null) {
				int size = alienPidsList.size();
				if(size > 0) {
					StringBuffer aliens = new StringBuffer();
					for( ProcessInfo processInfo : alienPidsList ) {
						String pid = processInfo.getPid();
						String uid = processInfo.getUid();
						aliens.append(pid+":"+uid+" ");
					}
					String entry = "<span title=\\\""+aliens.toString().trim()+"\\\">"+size+"</span>";
					alienPidsDisplay = entry;
				}
				else {
					
					alienPidsDisplay = ""+size;
				}
			}
			sb.append(quote(alienPidsDisplay));
			sb.append(",");
			sb.append(quote(machineInfo.getSharesTotal()));
			sb.append(",");
			sb.append(quote(machineInfo.getSharesInuse()));
			sb.append(",");
			sb.append(quote(machineInfo.getElapsed()));
			
			sb.append("]");
		}
		
		sb.append(" ]");
		sb.append(" }");
		duccLogger.debug(methodName, null, sb);
		response.getWriter().println(sb);
		response.setContentType("application/json");
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}	
	
	@Deprecated
	private void handleDuccServletMachinesData(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletMachinesData";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		int counter = 0;
		StringBuffer sb = new StringBuffer();
		DuccMachinesData instance = DuccMachinesData.getInstance();
		MachineSummaryInfo msi = instance.getTotals();
		addMachineInfo(sb, "*Totals-->", "", "", "", ""+msi.memoryTotal/*+memUnits*/, ""+msi.memorySwapped/*+memUnits*/, msi.sharesTotal+"", msi.sharesInuse+"", "", 0);
		ConcurrentSkipListMap<MachineInfo,String> sortedMachines = DuccMachinesData.getInstance().getSortedMachines();
		Iterator<MachineInfo> iterator = sortedMachines.keySet().iterator();
		while(iterator.hasNext()) {
			MachineInfo m = iterator.next();
			addMachineInfo(sb,m,++counter);
		}
		response.getWriter().println(sb);
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}
	
	private void handleDuccServletSystemAdminAdminData(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletSystemAdminAdminData";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		Iterator<String> iterator = DuccWebAdministrators.getInstance().getSortedAuthorizedUserids();
		if(!iterator.hasNext()) {
			sb.append("none");
		}
		else {
			sb.append("<table>");
			while(iterator.hasNext()) {
				sb.append("<tr>");
				sb.append("<td>");
				sb.append(iterator.next());
			}
			sb.append("</table>");
		}
		response.getWriter().println(sb);
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}
	
	private void handleDuccServletSystemAdminControlData(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletSystemAdminControlData";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		boolean authorized = isAuthorized(request,null);
		boolean accept = SystemState.getInstance().isAcceptJobs();
		String acceptMode = "disabled=disabled";
		String blockMode = "disabled=disabled";
		if(!accept) {
			if(authorized) {
				acceptMode = "";
			}
		}
		else {
			if(authorized) {
				blockMode = "";
			}
		}
		sb.append("<table>");
		sb.append("<tr>");
		sb.append("<td>");
		sb.append("<input type=\"button\" onclick=\"ducc_confirm_accept_jobs()\" value=\"Accept\" "+acceptMode+"/>");
		sb.append("<td>");
		sb.append("<input type=\"button\" onclick=\"ducc_confirm_block_jobs()\" value=\"Block\" "+blockMode+"/>");
		sb.append("<td>");
		sb.append("Jobs: ");
		sb.append("<td>");
		if(accept) {
			sb.append("<span title=\"job submit is enabled\" class=\"status_on\">");
			sb.append("accept");
        	sb.append("<span>");
		}
		else {
			sb.append("<span title=\"job submit is disabled\" class=\"status_off\">");
			sb.append("block");
        	sb.append("<span>");
		}
		sb.append("</table>");
		response.getWriter().println(sb);
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}
	
	private void handleDuccServletSystemJobsControl(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletSystemJobsControl";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		boolean authorized = isAuthorized(request,null);
		if(authorized) {
			String userId = DuccWebUtil.getCookie(request,DuccWebUtil.cookieUser);
			String name = "type";
			String value = request.getParameter(name).trim();
			duccLogger.info(methodName, null, messages.fetchLabel("user")+userId+" "+messages.fetchLabel("type")+value);
			if(value != null) {
				if(value.equals("block")) {
					SystemState.getInstance().resetAcceptJobs(userId);
				}
				else if(value.equals("accept")) {
					SystemState.getInstance().setAcceptJobs();
				}
			}
		}
		response.getWriter().println(sb);
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}
	
	private void handleServletJsonSystemClassesData(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleJsonServletSystemClassesData";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		sb.append("{ ");
		sb.append("\"aaData\": [ ");
		
		DuccWebSchedulerClasses schedulerClasses = new DuccWebSchedulerClasses(getFileName());
		DuccProperties properties = schedulerClasses.getClasses();
		String class_set = properties.getProperty("scheduling.class_set");
		class_set.trim();
		boolean first = true;
		if(class_set != null) {
			String[] class_array = StringUtils.split(class_set);
			for(int i=0; i<class_array.length; i++) {
				String class_name = class_array[i].trim();
				if(first) {
					first = false;
				}
				else {
					sb.append(",");
				}
				sb.append("[");
				sb.append(quote(class_name));
				sb.append(",");

                String policy = properties.getStringProperty("scheduling.class."+class_name+".policy");
				sb.append(quote(policy));
				sb.append(",");
				sb.append(quote(properties.getStringProperty("scheduling.class."+class_name+".share_weight", "100")));
				sb.append(",");
				sb.append(quote(properties.getStringProperty("scheduling.class."+class_name+".priority")));
				// cap is either absolute or proportional.  if proprotional, it ends with '%'.  It's always
                // either-or so at least one of these columns will have N/A
				String val = properties.getStringProperty("scheduling.class."+class_name+".cap", "0");
				if( (val == null) || val.equals("0") ) {
                    sb.append(",");
    				sb.append(quote("-"));
    				sb.append(",");
    				sb.append(quote("-"));
				} else if ( val.endsWith("%") ) {
					sb.append(",");
    				sb.append(quote(val));
    				sb.append(",");
    				sb.append(quote("-"));
                } else {
                	sb.append(",");
    				sb.append(quote("-"));
    				sb.append(",");
    				sb.append(quote(val));
                }
				val = properties.getStringProperty("scheduling.class."+class_name+".initialization.cap", System.getProperty("ducc.rm.initialization.cap"));
				if ( val == null ) {
                    val = "2";
                }
				sb.append(",");
				sb.append(quote(val));
				boolean bval = properties.getBooleanProperty("scheduling.class."+class_name+".expand.by.doubling", true);
				Boolean b = new Boolean(bval);
				sb.append(",");
				sb.append(quote(b.toString()));
				val = properties.getStringProperty("scheduling.class."+class_name+".prediction", System.getProperty("ducc.rm.prediction"));
				if ( val == null ) {
					val = "true";
				}
				sb.append(",");
				sb.append(quote(val));
				val = properties.getStringProperty("scheduling.class."+class_name+".prediction.fudge", System.getProperty("ducc.rm.prediction.fudge"));
				if ( val == null ) {
					val = "10000";
				}
				sb.append(",");
				sb.append(quote(val));

                // max for reserve in in machines.  For fixed is in processes.  No max on fair-share. So slightly
                // ugly code here.
                if ( policy.equals("RESERVE") ) {
                    val = properties.getStringProperty("scheduling.class."+class_name+".max_machines", "0");
                    if( val == null || val.equals("0")) {
                        val = "-";
                    }
                } else if ( policy.equals("FIXED_SHARE") ) {
                    val = properties.getStringProperty("scheduling.class."+class_name+".max_processes", "0");
                    if( val == null || val.equals("0")) {
                        val = "-";
                    }
                } else {
					val = "-";
                }

				sb.append(",");
				sb.append(quote(val));
				val = properties.getStringProperty("scheduling.class."+class_name+".nodepool", "--global--");
				sb.append(",");
				sb.append(quote(val));
				sb.append("]");
			}
		}
		
		sb.append(" ]");
		sb.append(" }");
		duccLogger.debug(methodName, null, sb);
		response.getWriter().println(sb);
		response.setContentType("application/json");
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}
	
	@Deprecated
	private void handleDuccServletSystemClassesData(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletSystemClassesData";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		DuccWebSchedulerClasses schedulerClasses = new DuccWebSchedulerClasses(getFileName());
		StringBuffer sb = new StringBuffer();
		DuccProperties properties = schedulerClasses.getClasses();
		String class_set = properties.getProperty("scheduling.class_set");
		class_set.trim();
		if(class_set != null) {
			String[] class_array = StringUtils.split(class_set);
			for(int i=0; i<class_array.length; i++) {
				String class_name = class_array[i].trim();
				sb.append(trGet(i+1));
				sb.append("<td>");
				sb.append(class_name);
				sb.append("</td>");	
				sb.append("<td>");

                String policy = properties.getStringProperty("scheduling.class."+class_name+".policy");
				sb.append(policy);
				sb.append("</td>");	
				sb.append("<td align=\"right\">");
				sb.append(properties.getStringProperty("scheduling.class."+class_name+".share_weight", "100"));
				sb.append("</td>");	
				sb.append("<td align=\"right\">");
				sb.append(properties.getStringProperty("scheduling.class."+class_name+".priority"));
				sb.append("</td>");	

                // cap is either absolute or proportional.  if proprotional, it ends with '%'.  It's always
                // either-or so at least one of these columns will have N/A
				String val = properties.getStringProperty("scheduling.class."+class_name+".cap", "0");
				if( (val == null) || val.equals("0") ) {
                    sb.append("<td align=\"right\">");
                    sb.append("-");
                    sb.append("</td>");
                    sb.append("<td align=\"right\">");
                    sb.append("-");
                    sb.append("</td>");
				} else if ( val.endsWith("%") ) {
                    sb.append("<td align=\"right\">");
                    sb.append(val);
                    sb.append("</td>");

                    sb.append("<td align=\"right\">");
                    sb.append("-");
                    sb.append("</td>");
                } else {
                    sb.append("<td align=\"right\">");
                    sb.append("-");
                    sb.append("</td>");

                    sb.append("<td align=\"right\">");
                    sb.append(val);
                    sb.append("</td>");
                }

				sb.append("<td align=\"right\">");
				val = properties.getStringProperty("scheduling.class."+class_name+".initialization.cap", 
                                                   System.getProperty("ducc.rm.initialization.cap"));
                if ( val == null ) {
                    val = "2";
                }

				sb.append(val);
				sb.append("</td>");	

				sb.append("<td align=\"right\">");
				boolean bval = properties.getBooleanProperty("scheduling.class."+class_name+".expand.by.doubling", true);
                sb.append(bval);
				sb.append("</td>");	

				sb.append("<td align=\"right\">");
				val = properties.getStringProperty("scheduling.class."+class_name+".prediction", 
                                                   System.getProperty("ducc.rm.prediction"));
                if ( val == null ) {
                    val = "true";
                }
                sb.append(val);
				sb.append("</td>");	

				sb.append("<td align=\"right\">");
				val = properties.getStringProperty("scheduling.class."+class_name+".prediction.fudge",
                                                   System.getProperty("ducc.rm.prediction.fudge"));
                if ( val == null ) {
                    val = "10000";
                }
                sb.append(val);
				sb.append("</td>");	

                // max for reserve in in machines.  For fixed is in processes.  No max on fair-share. So slightly
                // ugly code here.
 				sb.append("<td align=\"right\">");
                if ( policy.equals("RESERVE") ) {
                    val = properties.getStringProperty("scheduling.class."+class_name+".max_machines", "0");
                    if( val == null || val.equals("0")) {
                        val = "-";
                    }
                } else if ( policy.equals("FIXED_SHARE") ) {
                    val = properties.getStringProperty("scheduling.class."+class_name+".max_processes", "0");
                    if( val == null || val.equals("0")) {
                        val = "-";
                    }
                } else {
					val = "-";
                }

				val = properties.getStringProperty("scheduling.class."+class_name+".max_shares", "0");
				if( val == null || val.equals("0")) {
					val = "-";
				}
				sb.append(val);
				sb.append("</td>");	

				sb.append("<td align=\"right\">");
				val = properties.getStringProperty("scheduling.class."+class_name+".nodepool", "--global--");
                sb.append(val);
				sb.append("</td>");	


				sb.append("</tr>");
			}
		}
		response.getWriter().println(sb);
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}
	
	private String getPropertiesValue(Properties properties, String key, String defaultValue) {
		String retVal = defaultValue;
		if(properties != null) {
			String value = properties.getProperty(key);
			if(value != null) {
				retVal = properties.getProperty(key);
			}
		}
		return retVal;
	}
	
	private String buildjConsoleLink(String service) {
		String location = "buildjConsoleLink";
		String href = "<a href=\""+duccjConsoleLink+"?"+"service="+service+"\" onclick=\"var newWin = window.open(this.href,'child','height=800,width=1200,scrollbars');  newWin.focus(); return false;\">"+service+"</a>";
		duccLogger.trace(location, null, href);
		return href;
	}
	
	@Deprecated
	private void handleDuccServletSystemDaemonsData(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletSystemDaemonsData";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		for(DaemonName daemonName : DuccDaemonRuntimeProperties.daemonNames) {
			String status = "unknown";
			String heartbeat = "*";
			String heartmax = "*";
			Properties properties = DuccDaemonRuntimeProperties.getInstance().get(daemonName);
			switch(daemonName) {
			case Webserver:
				status = "up";
				break;
			default:
				status = "unknown";
				heartbeat = DuccDaemonsData.getInstance().getHeartbeat(daemonName);
				long timeout = getMillisMIA(daemonName)/1000;
				if(timeout > 0) {
					try {
						long overtime = timeout - Long.parseLong(heartbeat);
						if(overtime < 0) {
							status = "down";
						}
						else {
							status = "up";
						}
					}
					catch(Throwable t) {
					}
				}
				heartmax = DuccDaemonsData.getInstance().getMaxHeartbeat(daemonName);
				break;
			}
			sb.append("<tr>");
			sb.append("<td>");
			sb.append(status);
			sb.append("</td>");	
			sb.append("<td>");
			sb.append(getPropertiesValue(properties,DuccDaemonRuntimeProperties.keyDaemonName,daemonName.toString()));
			sb.append("</td>");	
			sb.append("<td>");
			sb.append(getPropertiesValue(properties,DuccDaemonRuntimeProperties.keyBootTime,""));
			sb.append("</td>");	
			sb.append("<td>");
			sb.append(getPropertiesValue(properties,DuccDaemonRuntimeProperties.keyNodeIpAddress,""));
			sb.append("</td>");	
			sb.append("<td>");
			sb.append(getPropertiesValue(properties,DuccDaemonRuntimeProperties.keyNodeName,""));
			sb.append("</td>");	
			sb.append("<td>");
			sb.append(getPropertiesValue(properties,DuccDaemonRuntimeProperties.keyPid,""));
			sb.append("</td>");	
			sb.append("<td align=\"right\">");
			sb.append(heartbeat);
			sb.append("</td>");	
			sb.append("<td align=\"right\">");
			sb.append(heartmax);
			sb.append("</td>");	
			sb.append("<td>");
			String jmxUrl = getPropertiesValue(properties,DuccDaemonRuntimeProperties.keyJmxUrl,"");
			if(jmxUrl != null) {
				sb.append(buildjConsoleLink(jmxUrl));
			}
			sb.append("</td>");	
			sb.append("</tr>");
		}
		response.getWriter().println(sb);
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}
	
	private String quote(String string) {
		return "\""+string+"\"";
	}
	
	private String buildjSonjConsoleLink(String service) {
		String location = "buildjConsoleLink";
		String href = "<a href=\\\""+duccjConsoleLink+"?"+"service="+service+"\\\" onclick=\\\"var newWin = window.open(this.href,'child','height=800,width=1200,scrollbars');  newWin.focus(); return false;\\\">"+service+"</a>";
		duccLogger.trace(location, null, href);
		return href;
	}
	
	private void handleServletJsonSystemDaemonsData(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleJsonServletSystemDaemonsData";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		sb.append("{ ");
		sb.append("\"aaData\": [ ");
		boolean first = true;
		for(DaemonName daemonName : DuccDaemonRuntimeProperties.daemonNames) {
			String status = "unknown";
			String heartbeat = "-1";
			String heartmax = "-1";
			Properties properties = DuccDaemonRuntimeProperties.getInstance().get(daemonName);
			switch(daemonName) {
			case Webserver:
				status = "up";
				heartbeat = "0";
				heartmax = "0";
				break;
			default:
				status = "unknown";
				String hb = DuccDaemonsData.getInstance().getHeartbeat(daemonName);
				try {
					Long.parseLong(hb);
					heartbeat = hb;
					long timeout = getMillisMIA(daemonName)/1000;
					if(timeout > 0) {
						long overtime = timeout - Long.parseLong(hb);
						if(overtime < 0) {
							status = "down";
						}
						else {
							status = "up";
						}
					}
				}
				catch(Throwable t) {	
				}
				String hx = DuccDaemonsData.getInstance().getMaxHeartbeat(daemonName);
				try {
					Long.parseLong(hx);
					heartmax = hx;
				}
				catch(Throwable t) {	
				}
				break;
			}
			String heartmaxTOD = TimeStamp.simpleFormat(DuccDaemonsData.getInstance().getMaxHeartbeatTOD(daemonName));
			if(first) {
				first = false;
			}
			else {
				sb.append(",");
			}
			sb.append("[");
			sb.append(quote(status));
			sb.append(",");
			sb.append(quote(getPropertiesValue(properties,DuccDaemonRuntimeProperties.keyDaemonName,daemonName.toString())));
			sb.append(",");
			sb.append(quote(getPropertiesValue(properties,DuccDaemonRuntimeProperties.keyBootTime,"")));
			sb.append(",");
			sb.append(quote(getPropertiesValue(properties,DuccDaemonRuntimeProperties.keyNodeIpAddress,"")));
			sb.append(",");
			sb.append(quote(getPropertiesValue(properties,DuccDaemonRuntimeProperties.keyNodeName,"")));
			sb.append(",");
			sb.append(quote(getPropertiesValue(properties,DuccDaemonRuntimeProperties.keyPid,"")));
			sb.append(",");
			Long pubSize = DuccDaemonsData.getInstance().getEventSize(daemonName);
			sb.append(quote(""+pubSize));
			sb.append(",");
			Long pubSizeMax = DuccDaemonsData.getInstance().getEventSizeMax(daemonName);
			sb.append(quote(""+pubSizeMax));
			sb.append(",");
			sb.append(quote(heartbeat));
			sb.append(",");
			sb.append(quote(heartmax));
			sb.append(",");
			sb.append(quote(heartmaxTOD));
			sb.append(",");
			String jmxUrl = getPropertiesValue(properties,DuccDaemonRuntimeProperties.keyJmxUrl,"");
			if(jmxUrl != null) {
				sb.append(quote(buildjSonjConsoleLink(jmxUrl)));
			}
			sb.append("]");
		}
		sb.append(" ]");
		sb.append(" }");
		duccLogger.debug(methodName, null, sb);
		response.getWriter().println(sb);
		response.setContentType("application/json");
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}
	
	private void handleDuccServletClusterName(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletClusterName";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		DuccWorkMap duccWorkMap = DuccData.getInstance().get();
		if(duccWorkMap.size()> 0) {
			sb.append(getDuccWebServer().getClusterName());
		}
		response.getWriter().println(sb);
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}	
	
	private void handleDuccServletTimeStamp(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletTimeStamp";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		DuccId jobid = null;
		StringBuffer sb = new StringBuffer(getTimeStamp(jobid,DuccData.getInstance().getPublished()));
		response.getWriter().println(sb);
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}	
	
	private void handleDuccServletReservationSchedulingClasses(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletReservationSchedulingCLasses";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		sb.append("<select id=\"scheduling_class\">");
		DuccWebSchedulerClasses schedulerClasses = new DuccWebSchedulerClasses(getFileName());
		String[] class_array = schedulerClasses.getReserveClasses();
		for(int i=0; i<class_array.length; i++) {
			sb.append("<option value=\""+class_array[i]+"\" selected=\"selected\">"+class_array[i]+"</option>");
		}
		sb.append("</select>");
		response.getWriter().println(sb);
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}	
	
	private void handleDuccServletReservationInstanceMemorySizes(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletReservationInstanceMemorySizes";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		sb.append("<select id=\"instance_memory_size\">");
		int shareSize = DuccConstants.defaultShareSize;
		Properties properties = DuccWebProperties.get();
		String key_share_size = "ducc.rm.default.memory";
		if(properties.containsKey(key_share_size)) {
			try {
				shareSize = Integer.parseInt(properties.getProperty(key_share_size).trim());
			}
			catch(Throwable t) {
				duccLogger.error(methodName, null, t);
			}
		}
		for(int i=0; i<DuccConstants.memorySizes.length; i++) {
			int memorySize = DuccConstants.memorySizes[i]*shareSize;
			sb.append("<option value=\""+memorySize+"\">"+memorySize+"</option>");
		}
		sb.append("</select>");
		response.getWriter().println(sb);
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}	
	
	private void handleDuccServletReservationInstanceMemoryUnits(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletReservationInstanceMemoryUnits";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		sb.append("<select id=\"instance_memory_units\">");
		sb.append("<option value=\"GB\" selected=\"selected\">GB</option>");
		sb.append("</select>");
		response.getWriter().println(sb);
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}	
	
	private void handleDuccServletReservationNumberOfInstances(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletReservationNumberOfInstances";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		sb.append("<select id=\"number_of_instances\">");
		sb.append("<option value=\"1\" selected=\"selected\">1</option>");
		int min = 1;
		int max = 9;
		for(int i=min+1;i<max+1;i++) {
			sb.append("<option value=\""+i+"\">"+i+"</option>");
		}
		sb.append("</select>");
		response.getWriter().println(sb);
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}	
	
	private void handleDuccServletReservationSubmitButton(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletReservationSubmitButton";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		String disabled = "disabled=\"disabled\"";
		if(isAuthenticated(request,response)) {
			disabled = "";
		}
		String button = "<input type=\"button\" onclick=\"ducc_submit_reservation()\" value=\"Submit\" "+disabled+"/>";
		sb.append(button);
		response.getWriter().println(sb);
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}	
	
	private void handleDuccServletJobSubmitButton(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletJobSubmitButton";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		String disabled = "disabled=\"disabled\"";
		if(isAuthenticated(request,response)) {
			disabled = "";
		}
		String button = "<input type=\"button\" onclick=\"ducc_confirm_submit_job()\" value=\"Submit\" "+disabled+"/>";
		sb.append(button);
		response.getWriter().println(sb);
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}
	
	private void handleDuccServletLogData(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletLogData";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		sb.append("<html>");
		sb.append("<head>");
		sb.append("<link rel=\"shortcut icon\" href=\"../ducc.ico\" />");
		sb.append("<title>ducc-mon</title>");
		sb.append("<meta http-equiv=\"CACHE-CONTROL\" content=\"NO-CACHE\">");
		String loc = request.getParameter("loc");
		if(loc != null) {
			if(loc.equals("bot")) {
				String js = "<script src=\"../js/scroll-to-bottom.js\"></script>";
				sb.append(js);
			}
		}
		sb.append("</head>");
		sb.append("<body>");
		sb.append("<h3>");
		String fname = request.getParameter("fname");
		sb.append(fname);
		sb.append("</h3>");
		try {
			FileInputStream fis = new FileInputStream(fname);
			DataInputStream dis = new DataInputStream(fis);
			BufferedReader br = new BufferedReader(new InputStreamReader(dis));
			String logLine;
			while ((logLine = br.readLine()) != null)   {
					sb.append(logLine+"<br>");
			}
			br.close();
		}
		catch(FileNotFoundException e) {
			sb.append("File not found");
		}
		sb.append("</body>");
		sb.append("</html>");
		response.getWriter().println(sb);
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}	
	
	private void handleDuccServletjConsoleLink(
			String target,
			Request baseRequest,
			HttpServletRequest request,
			HttpServletResponse response) 
	throws IOException, ServletException
	{
		String location = "handleDuccServletjConsoleLink";
		String host = ""+request.getLocalAddr();
		String port = ""+request.getLocalPort();
		String service = request.getParameter("service");
		StringBuffer sb = new StringBuffer();
		sb.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
		sb.append("<jnlp spec=\"1.0+\" codebase=\"http://"+host+":"+port+"/lib/webstart\">");
		sb.append("  <information>");
		sb.append("    <title>JConsole</title>");
		sb.append("    <vendor>DUCC</vendor>");
		sb.append("  </information>");
		sb.append("  <security>");   
		sb.append("    <all-permissions/>");
		sb.append("  </security>");
		sb.append("  <resources>");
		sb.append("    <j2se version=\"1.6+\" />");
		sb.append("    <jar href=\"jconsole.jar\" main=\"true\"/>");
		sb.append("  </resources>");
		sb.append("  <application-desc main-class=\"sun.tools.jconsole.JConsole\">");
		sb.append("    <argument>"+service+"</argument>");
		sb.append("  </application-desc>");
		sb.append("</jnlp>");
		duccLogger.trace(location, null, sb);
		response.getWriter().println(sb);
		response.setContentType("application/x-java-jnlp-file");
	}
	
	/*
	 * authenticated
	 */
	
	private void handleDuccServletJobSubmit(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletJobSubmit";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		if(isAuthenticated(request,response)) {
			duccLogger.info(methodName, null, messages.fetch("function not supported"));
		}
		else {
			duccLogger.warn(methodName, null, messages.fetch("user not authenticated"));
		}
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}	
	
	private void handleDuccServletJobCancel(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletJobCancel";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		try {
			String name = "id";
			String value = request.getParameter(name).trim();
			duccLogger.info(methodName, null, messages.fetchLabel("cancel")+value);
			DuccData duccData = DuccData.getInstance();
			DuccWorkMap duccWorkMap = duccData.get();
			String text;
			IDuccWorkJob duccWorkJob = (IDuccWorkJob) duccWorkMap.findDuccWork(DuccType.Job, value);
			if(duccWorkJob != null) {
				String resourceOwnerUserId = duccWorkJob.getStandardInfo().getUser().trim();
				if(isAuthorized(request,resourceOwnerUserId)) {
					String arg1 = "-"+name;
					String arg2 = value;
					String userId = DuccWebUtil.getCookie(request,DuccWebUtil.cookieUser);
					String cp = System.getProperty("java.class.path");
					String java = "/bin/java";
					String jclass = "org.apache.uima.ducc.cli.DuccJobCancel";
					String jhome = System.getProperty("java.home");
					String juser = System.getProperty("user.name");
					String jtmp = System.getProperty("java.io.tmpdir")+File.separator+"duckling"+"."+juser;
					duccLogger.info(methodName, null, "logfile:"+jtmp);
					new File(jtmp).mkdirs(); 
					String[] arglist = { "-u", userId, "-f", jtmp, "--", jhome+java, "-cp", cp, jclass, arg1, arg2 };
					DuccAsUser.duckling(arglist);
				}
			}
			else {
				text = "job "+value+" not found";
				duccLogger.debug(methodName, null, messages.fetch(text));
			}
		}
		catch(Exception e) {
			duccLogger.error(methodName, null, e);
		}
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}	
	
	private void handleDuccServletReservationSubmit(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletReservationSubmit";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		if(isAuthenticated(request,response)) {
			String scheduling_class = request.getParameter("scheduling_class");
			duccLogger.debug(methodName, null, "scheduling_class:"+scheduling_class);
			String instance_memory_size = request.getParameter("instance_memory_size");
			duccLogger.debug(methodName, null, "instance_memory_size:"+instance_memory_size);
			String instance_memory_units = request.getParameter("instance_memory_units");
			duccLogger.debug(methodName, null, "instance_memory_units:"+instance_memory_units);
			String number_of_instances = request.getParameter("number_of_instances");
			duccLogger.debug(methodName, null, "number_of_instances:"+number_of_instances);
			String description = request.getParameter("description");
			duccLogger.debug(methodName, null, "description:"+description);
			String arg1 = "";
			String arg2 = "";
			if(scheduling_class != null) {
				arg1 = "--scheduling_class";
				arg2 = scheduling_class;
			}
			String arg3 = "";
			String arg4 = "";
			if(instance_memory_size != null) {
				arg3 = "--instance_memory_size";
				if(instance_memory_units != null) {
					arg4 = instance_memory_size+instance_memory_units;
				}
				else {
					arg4 = instance_memory_size;
				}
			}
			String arg5 = "";
			String arg6 = "";
			if(number_of_instances != null) {
				arg5 = "--number_of_instances";
				arg6 = number_of_instances;
			}
			String arg7 = "";
			String arg8 = "";
			if(description != null) {
				arg7 = "--description";
				arg8 = description;
			}
			try {
				String userId = DuccWebUtil.getCookie(request,DuccWebUtil.cookieUser);
				String cp = System.getProperty("java.class.path");
				String java = "/bin/java";
				String jclass = "org.apache.uima.ducc.cli.DuccReservationSubmit";
				String jhome = System.getProperty("java.home");
				String juser = System.getProperty("user.name");
				String jtmp = System.getProperty("java.io.tmpdir")+File.separator+"duckling"+"."+juser;
				duccLogger.info(methodName, null, "logfile:"+jtmp);
				new File(jtmp).mkdirs(); 
				String[] arglist = { "-u", userId, "-f", jtmp, "--", jhome+java, "-cp", cp, jclass, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8 };
				DuccAsUser.duckling(arglist);
			} catch (Exception e) {
				duccLogger.error(methodName, null, e);
			}
		}
		else {
			duccLogger.warn(methodName, null, messages.fetch("user not authenticated"));
		}
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}	
	
	private void handleDuccServletReservationCancel(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletReservationCancel";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		try {
			String name = "id";
			String value = request.getParameter(name).trim();
			duccLogger.info(methodName, null, messages.fetchLabel("cancel")+value);
			DuccData duccData = DuccData.getInstance();
			DuccWorkMap duccWorkMap = duccData.get();
			String text;
			IDuccWorkReservation duccWorkReservation = (IDuccWorkReservation) duccWorkMap.findDuccWork(DuccType.Reservation, value);
			if(duccWorkReservation != null) {
				String resourceOwnerUserId = duccWorkReservation.getStandardInfo().getUser().trim();
				if(isAuthorized(request,resourceOwnerUserId)) {
					String arg1 = "-"+name;
					String arg2 = value;
					String userId = DuccWebUtil.getCookie(request,DuccWebUtil.cookieUser);
					String cp = System.getProperty("java.class.path");
					String java = "/bin/java";
					String jclass = "org.apache.uima.ducc.cli.DuccReservationCancel";
					String jhome = System.getProperty("java.home");
					String juser = System.getProperty("user.name");
					String jtmp = System.getProperty("java.io.tmpdir")+File.separator+"duckling"+"."+juser;
					duccLogger.info(methodName, null, "logfile:"+jtmp);
					new File(jtmp).mkdirs(); 
					String[] arglist = { "-u", userId, "-f", jtmp, "--", jhome+java, "-cp", cp, jclass, arg1, arg2 };
					DuccAsUser.duckling(arglist);
				}
			}
			else {
				text = "job "+value+" not found";
				duccLogger.debug(methodName, null, messages.fetch(text));
			}
		}
		catch(Exception e) {
			duccLogger.error(methodName, null, e);
		}
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}	
	
	private String normalize(DuccId duccId) {
		return duccId.getFriendly()+"";
	}
	
	private void handleDuccRequest(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleDuccRequest";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		duccLogger.debug(methodName, null,request.toString());
		duccLogger.debug(methodName, null,"getRequestURI():"+request.getRequestURI());
		String reqURI = request.getRequestURI()+"";
		if(reqURI.startsWith(duccContext)) {
			response.setContentType("text/html;charset=utf-8");
			response.setStatus(HttpServletResponse.SC_OK);
			baseRequest.setHandled(true);
			if(reqURI.startsWith(duccVersion)) {
				handleDuccServletVersion(target, baseRequest, request, response);
			}
			else if(reqURI.startsWith(duccAuthenticationStatus)) {
				handleDuccServletAuthenticationStatus(target, baseRequest, request, response);
			}
			else if(reqURI.startsWith(duccAuthenticatorVersion)) {
				handleDuccServletAuthenticatorVersion(target, baseRequest, request, response);
			}
			else if(reqURI.startsWith(duccLoginLink)) {
				handleDuccServletLoginLink(target, baseRequest, request, response);
			}
			else if(reqURI.startsWith(duccUserLogout)) {
				duccLogger.info(methodName, null,"getRequestURI():"+request.getRequestURI());
				handleDuccServletLogout(target, baseRequest, request, response);
			}
			else if(reqURI.startsWith(duccUserLogin)) {
				duccLogger.info(methodName, null,"getRequestURI():"+request.getRequestURI());
				handleDuccServletLogin(target, baseRequest, request, response);
			}
			else if(reqURI.startsWith(duccJobsData)) {
				handleDuccServletJobsData(target, baseRequest, request, response);
			}
			else if(reqURI.startsWith(duccJobIdData)) {
				handleDuccServletJobIdData(target, baseRequest, request, response);
			}
			else if(reqURI.startsWith(duccJobWorkitemsCountData)) {
				handleDuccServletJobWorkitemsCountData(target, baseRequest, request, response);
			}
			else if(reqURI.startsWith(duccJobProcessesData)) {
				handleDuccServletJobProcessesData(target, baseRequest, request, response);
			}
			else if(reqURI.startsWith(duccJobWorkitemsData)) {
				handleDuccServletJobWorkitemsData(target, baseRequest, request, response);
			}
			else if(reqURI.startsWith(duccJobPerformanceData)) {
				handleDuccServletJobPerformanceData(target, baseRequest, request, response);
			}
			else if(reqURI.startsWith(duccJobSpecificationData)) {
				handleDuccServletJobSpecificationData(target, baseRequest, request, response);
			}
			else if(reqURI.startsWith(duccJobInitializationFailData)) {
				handleDuccServletJobInitializationFailData(target, baseRequest, request, response);
			}
			else if(reqURI.startsWith(duccJobRuntimeFailData)) {
				handleDuccServletJobRuntimeFailData(target, baseRequest, request, response);
			}
			else if(reqURI.startsWith(duccReservationsData)) {
				handleDuccServletReservationsData(target, baseRequest, request, response);
			}
			else if(reqURI.startsWith(jsonServicesDefinitionsData)) {
				handleServletJsonServicesDefinitionsData(target, baseRequest, request, response);
			}
			else if(reqURI.startsWith(duccServicesDeploymentsData)) {
				handleDuccServletServicesDeploymentsData(target, baseRequest, request, response);
			}
			else if(reqURI.startsWith(duccServiceProcessesData)) {
				handleDuccServletServiceProcessesData(target, baseRequest, request, response);
			}
			else if(reqURI.startsWith(duccServiceSpecificationData)) {
				handleDuccServletServiceSpecificationData(target, baseRequest, request, response);
			}
			else if(reqURI.startsWith(jsonMachinesData)) {
				handleServletJsonMachinesData(target, baseRequest, request, response);
			}
			else if(reqURI.startsWith(duccSystemAdminAdminData)) {
				handleDuccServletSystemAdminAdminData(target, baseRequest, request, response);
			}
			else if(reqURI.startsWith(duccSystemAdminControlData)) {
				handleDuccServletSystemAdminControlData(target, baseRequest, request, response);
			}
			else if(reqURI.startsWith(duccSystemJobsControl)) {
				handleDuccServletSystemJobsControl(target, baseRequest, request, response);
			}
			else if(reqURI.startsWith(jsonSystemClassesData)) {
				handleServletJsonSystemClassesData(target, baseRequest, request, response);
			}
			else if(reqURI.startsWith(jsonSystemDaemonsData)) {
				handleServletJsonSystemDaemonsData(target, baseRequest, request, response);
			}
			else if(reqURI.startsWith(duccClusterName)) {
				handleDuccServletClusterName(target, baseRequest, request, response);
			}
			else if(reqURI.startsWith(duccTimeStamp)) {
				handleDuccServletTimeStamp(target, baseRequest, request, response);
			}
			else if(reqURI.startsWith(duccJobSubmit)) {
				duccLogger.info(methodName, null,"getRequestURI():"+request.getRequestURI());
				handleDuccServletJobSubmit(target, baseRequest, request, response);
			}
			else if(reqURI.startsWith(duccJobCancel)) {
				duccLogger.info(methodName, null,"getRequestURI():"+request.getRequestURI());
				handleDuccServletJobCancel(target, baseRequest, request, response);
			}
			else if(reqURI.startsWith(duccReservationSubmit)) {
				duccLogger.info(methodName, null,"getRequestURI():"+request.getRequestURI());
				handleDuccServletReservationSubmit(target, baseRequest, request, response);
			}
			else if(reqURI.startsWith(duccReservationCancel)) {
				duccLogger.info(methodName, null,"getRequestURI():"+request.getRequestURI());
				handleDuccServletReservationCancel(target, baseRequest, request, response);
			}
			else if(reqURI.startsWith(duccReservationSchedulingClasses)) {
				handleDuccServletReservationSchedulingClasses(target, baseRequest, request, response);
			}
			else if(reqURI.startsWith(duccReservationInstanceMemorySizes)) {
				handleDuccServletReservationInstanceMemorySizes(target, baseRequest, request, response);
			}
			else if(reqURI.startsWith(duccReservationNumberOfInstances)) {
				handleDuccServletReservationNumberOfInstances(target, baseRequest, request, response);
			}
			else if(reqURI.startsWith(duccReservationInstanceMemoryUnits)) {
				handleDuccServletReservationInstanceMemoryUnits(target, baseRequest, request, response);
			}
			else if(reqURI.startsWith(duccJobSubmitButton)) {
				handleDuccServletJobSubmitButton(target, baseRequest, request, response);
			}
			else if(reqURI.startsWith(duccReservationSubmitButton)) {
				handleDuccServletReservationSubmitButton(target, baseRequest, request, response);
			}
			else if(reqURI.startsWith(duccJobSubmitForm)) {
				handleDuccServletJobSubmitForm(target, baseRequest, request, response);
			}
			else if(reqURI.startsWith(duccLogData)) {
				handleDuccServletLogData(target, baseRequest, request, response);
			}
			else if(reqURI.startsWith(duccjConsoleLink)) {
				handleDuccServletjConsoleLink(target, baseRequest, request, response);
			}
			else if(reqURI.startsWith(duccMachinesData)) {
				handleDuccServletMachinesData(target, baseRequest, request, response);
			}
			else if(reqURI.startsWith(duccSystemClassesData)) {
				handleDuccServletSystemClassesData(target, baseRequest, request, response);
			}
			else if(reqURI.startsWith(duccSystemDaemonsData)) {
				handleDuccServletSystemDaemonsData(target, baseRequest, request, response);
			}
		}
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}
	
	public void handle(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException {
		String methodName = "handle";
		try{ 
			handleDuccRequest(target, baseRequest, request, response);
		}
		catch(Throwable t) {
			DuccId jobid = null;
			duccLogger.info(methodName, jobid, "", t.getMessage(), t);
			duccLogger.error(methodName, null, t);
		}
	}
	
}
