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

import java.io.File;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.uima.ducc.common.NodeIdentity;
import org.apache.uima.ducc.common.boot.DuccDaemonRuntimeProperties.DaemonName;
import org.apache.uima.ducc.common.internationalization.Messages;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.TimeStamp;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.orchestrator.authentication.DuccWebAdministrators;
import org.apache.uima.ducc.transport.event.common.IDuccPerWorkItemStatistics;
import org.apache.uima.ducc.transport.event.common.IDuccProcess;
import org.apache.uima.ducc.transport.event.common.IDuccProcessMap;
import org.apache.uima.ducc.transport.event.common.IDuccSchedulingInfo;
import org.apache.uima.ducc.transport.event.common.IDuccUnits.MemoryUnits;
import org.apache.uima.ducc.transport.event.common.IDuccWork;
import org.apache.uima.ducc.transport.event.common.IDuccWorkJob;
import org.apache.uima.ducc.ws.DuccMachinesData;
import org.eclipse.jetty.server.handler.AbstractHandler;

public abstract class DuccAbstractHandler extends AbstractHandler {
	
	private static DuccLogger duccLogger = DuccLoggerComponents.getWsLogger(DuccHandler.class.getName());
	private static Messages messages = Messages.getInstance();
	
	private DuccId jobid = null;
	
	public static DuccWebAdministrators duccWebAdministrators = DuccWebAdministrators.getInstance();
	public static DuccWebSessionManager duccWebSessionManager = DuccWebSessionManager.getInstance();
	
	public final String duccContext = "/ducc-servlet";
	
	public final String duccLogData			  = duccContext+"/log-data";
	
	public final String duccContextJsonFormat = duccContext+"/json-format";
	public final String duccContextUser       = duccContext+"/user";
	public final String duccContextLegacy     = duccContext+"/legacy";
	public final String duccContextProxy      = duccContext+"/proxy";
	
	public final String duccjConsoleLink	  = duccContext+"/jconsole-link.jnlp";
	
	public final int maximumRecordsJobs = 4096;
	public final int defaultRecordsJobs = 16;
	public final int maximumRecordsReservations = 4096;
	public final int defaultRecordsReservations = 8;
	public final int maximumRecordsServices = 4096;
	public final int defaultRecordsServices = 12;

	public final String defaultStyleDate = DuccWebUtil.valueStyleDateLong;
	
	public String dir_home = System.getenv("DUCC_HOME");
	public String dir_resources = "resources";

	protected boolean terminateEnabled = true;
	
	public static final String valueStateTypeAll = "all";
	public static final String valueStateTypeActive = "active";
	public static final String valueStateTypeInactive = "inactive";
	public static final String valueStateTypeDefault = valueStateTypeAll;
	
	public enum RequestStateType {
		Active,
		Inactive,
		All
	}
	
	public static final RequestStateType requestStateTypeDefault = RequestStateType.All;
	
	public String quote(String string) {
		return "\""+string+"\"";
	}
	
	public String normalize(DuccId duccId) {
		return duccId.getFriendly()+"";
	}
	
	public String stringNormalize(String value,String defaultValue) {
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
	
	public String getTimeStamp(DateStyle dateStyle, String date) {
		String location = "getTimeStamp";
		StringBuffer sb = new StringBuffer();
		if(date != null) {
			sb.append(date);
			if(date.trim().length() > 0) {
				try {
					switch(dateStyle) {
					case Long:
						break;
					case Medium:
						String day = sb.substring(sb.length()-4);
						sb.delete(0, 5);
						sb.delete(sb.lastIndexOf(":"), sb.length());
						sb.append(day);
						break;
					case Short:
						sb.delete(0, 5);
						sb.delete(sb.lastIndexOf(":"), sb.length());
						break;
					}
				}
				catch(Exception e) {
					duccLogger.error(location, jobid, dateStyle, date, e);
				}
			}
		}
		return sb.toString();
	}
	
	public String getTimeStamp(HttpServletRequest request, DuccId jobId, String millis) {
		return getTimeStamp(getDateStyle(request),getTimeStamp(jobId, millis));
	}
	
	private String getTimeStamp(DuccId jobId, String millis) {
		String methodName = "getTimeStamp";
		String retVal = "";
		try {
			retVal = TimeStamp.simpleFormat(millis);
		}
		catch(Throwable t) {
			duccLogger.debug(methodName, jobId, "millis:"+millis);
		}
		return retVal;
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
	
	public boolean isAuthorized(HttpServletRequest request, String resourceOwnerUserid) {
		String methodName = "isAuthorized";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		boolean retVal = false;
		try {
			String text = "";
			boolean authenticated = duccWebSessionManager.isAuthentic(request);
			String userId = duccWebSessionManager.getUserId(request);
			if(authenticated) {
				if(match(resourceOwnerUserid,userId)) {
					text = "user "+userId+" is resource owner";
					retVal = true;
				}
				else {
					RequestRole requestRole = getRole(request);
					switch(requestRole) {
					case User:
						text = "user "+userId+" is not resource owner "+resourceOwnerUserid;
						break;
					case Administrator:
						if(duccWebAdministrators.isAdministrator(userId)) {
							text = "user "+userId+" is administrator";
							retVal = true;
						}
						else {
							text = "user "+userId+" is not administrator ";
						}
						break;
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
	
	protected boolean isAuthenticated(HttpServletRequest request, HttpServletResponse response) {
		String methodName = "isAuthenticated";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		boolean authenticated = false;
		try {
			authenticated = duccWebSessionManager.isAuthentic(request);
		}
		catch(Exception e) {
			duccLogger.error(methodName, null, e);
		}
		duccLogger.trace(methodName, null, messages.fetch("exit"));
		return authenticated;
	}
	
	public boolean isIncludeUser(List<String> users, String user) {
		boolean retVal = true;
		if(users != null) {
			if(user != null) { 
				if(!users.isEmpty()) {
					if(!user.contains(user)) {
						retVal = false;
					}
				}
			}
		}
		return retVal;
	}
	
	private ArrayList<String> getUsers(String usersString) {
		ArrayList<String> userRecords = new ArrayList<String>();
		try {
			String[] users = usersString.split("\\s+");
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
	
	public ArrayList<String> getJobsUsers(HttpServletRequest request) {
		String cookie = DuccWebUtil.getCookie(request,DuccWebUtil.cookieJobsUsers);
		return getUsers(cookie);
	}
	
	public ArrayList<String> getReservationsUsers(HttpServletRequest request) {
		String cookie = DuccWebUtil.getCookie(request,DuccWebUtil.cookieReservationsUsers);
		return getUsers(cookie);
	}
	
	public ArrayList<String> getServicesUsers(HttpServletRequest request) {
		String cookie = DuccWebUtil.getCookie(request,DuccWebUtil.cookieServicesUsers);
		return getUsers(cookie);
	}
	
	public String getProcessMemorySize(DuccId id, String type, String size, MemoryUnits units) {
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
	
	public RequestStateType getStateTypeParameter(HttpServletRequest request) {
		RequestStateType requestStateType = requestStateTypeDefault;
		try {
			String stateType = request.getParameter("stateType");
			if(stateType != null) {
				stateType = stateType.trim();
				if(stateType.equals(valueStateTypeAll)) {
					requestStateType = RequestStateType.All;
				}
				else if(stateType.equals(valueStateTypeActive)) {
					requestStateType = RequestStateType.Active;
				}
				else if(stateType.equals(valueStateTypeInactive)) {
					requestStateType = RequestStateType.Inactive;
				}
			}
		}
		catch(Exception e) {
		}
		return requestStateType;
	}
	
	public int getReservationsMaxRecordsParameter(HttpServletRequest request) {
		int maxRecords = defaultRecordsReservations;
		try {
			String sMaxRecords = request.getParameter("maxRecords");
			int iMaxRecords= Integer.parseInt(sMaxRecords);
			if(iMaxRecords <= maximumRecordsReservations) {
				if(iMaxRecords > 0) {
					maxRecords = iMaxRecords;
				}
			}
		}
		catch(Exception e) {
		}
		return maxRecords;
	}
	
	public long getMillisMIA(DaemonName daemonName) {
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
			String rm_ratio = "1";
			try {
				String ratio = properties.getProperty("ducc.rm.state.publish.ratio");
				if(ratio != null) {
					rm_ratio = ratio;
				}
			}
			catch(Exception e) {
			}
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
	
	public String getPropertiesValue(Properties properties, String key, String defaultValue) {
		String retVal = defaultValue;
		if(properties != null) {
			String value = properties.getProperty(key);
			if(value != null) {
				retVal = properties.getProperty(key);
			}
		}
		return retVal;
	}
	
	public enum DateStyle { Long, Medium, Short };
	
	public DateStyle getDateStyle(HttpServletRequest request) {
		DateStyle dateStyle = DateStyle.Long;
		try {
			String cookie = DuccWebUtil.getCookie(request,DuccWebUtil.cookieStyleDate);
			if(cookie.equals(DuccWebUtil.valueStyleDateLong)) {
				dateStyle = DateStyle.Long;
			}
			else if(cookie.equals(DuccWebUtil.valueStyleDateMedium)) {
				dateStyle = DateStyle.Medium;
			}
			else if(cookie.equals(DuccWebUtil.valueStyleDateShort)) {
				dateStyle = DateStyle.Short;
			}
		}
		catch(Exception e) {
		}
		return dateStyle;
	}
	
	public enum FilterUsersStyle { Include, IncludePlusActive, Exclude, ExcludePlusActive };
	
	public FilterUsersStyle getFilterUsersStyle(HttpServletRequest request) {
		FilterUsersStyle filterUsersStyle = FilterUsersStyle.Include;
		try {
			String cookie = DuccWebUtil.getCookie(request,DuccWebUtil.cookieStyleFilterUsers);
			if(cookie.equals(DuccWebUtil.valueStyleFilterUsersInclude)) {
				filterUsersStyle = FilterUsersStyle.Include;;
			}
			else if(cookie.equals(DuccWebUtil.valueStyleFilterUsersIncludePlusActive)) {
				filterUsersStyle = FilterUsersStyle.IncludePlusActive;
			}
			else if(cookie.equals(DuccWebUtil.valueStyleFilterUsersExclude)) {
				filterUsersStyle = FilterUsersStyle.Exclude;
			}
			else if(cookie.equals(DuccWebUtil.valueStyleFilterUsersExcludePlusActive)) {
				filterUsersStyle = FilterUsersStyle.ExcludePlusActive;
			}
		}
		catch(Exception e) {
		}
		return filterUsersStyle;
	}
	
	public enum RequestRole { Administrator, User};
	
	public RequestRole getRole(HttpServletRequest request) {
		RequestRole role = RequestRole.User;
		try {
			String cookie = DuccWebUtil.getCookie(request,DuccWebUtil.cookieRole);
			if(cookie.equals(DuccWebUtil.valueRoleAdministrator)) {
				role = RequestRole.Administrator;;
			}
			/*
			else if(cookie.equals(DuccWebUtil.valueRoleUser)) {
				role = RequestRole.User;
			}
			*/
		}
		catch(Exception e) {
		}
		return role;
	}
	
	public int getJobsMax(HttpServletRequest request) {
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

	public int getReservationsMax(HttpServletRequest request) {
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

	public int getServicesMax(HttpServletRequest request) {
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
	
	public String getValue(Properties properties, String key, String defaultValue) {
		String retVal = defaultValue;
		if(properties != null) {
			if(key != null) {
				retVal = properties.getProperty(key, defaultValue);
			}
		}
		return retVal;
	}
	
	public ArrayList<String> getSwappingMachines(IDuccWorkJob job) {
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

	public String getCompletionOrProjection(HttpServletRequest request, IDuccWorkJob job) {
		String methodName = "getCompletionOrProjection";
		String retVal = "";
		try {
			String tVal = job.getStandardInfo().getDateOfCompletion();
			duccLogger.trace(methodName, null, tVal);
			retVal = getTimeStamp(request,job.getDuccId(),tVal);
		}
		catch(Exception e) {
			duccLogger.trace(methodName, null, "no worries", e);
		}
		catch(Throwable t) {
			duccLogger.trace(methodName, null, "no worries", t);
		}
		try {
			if(retVal.trim().length() == 0) {
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

	public String getDisabled(HttpServletRequest request, IDuccWork duccWork) {
		String resourceOwnerUserId = duccWork.getStandardInfo().getUser();
		String disabled = "disabled=\"disabled\"";
		if(isAuthorized(request, resourceOwnerUserId)) {
			disabled = "";
		}
		return disabled;
	}

	public String buildjConsoleLink(String service) {
		String location = "buildjConsoleLink";
		String href = "<a href=\""+duccjConsoleLink+"?"+"service="+service+"\" onclick=\"var newWin = window.open(this.href,'child','height=800,width=1200,scrollbars');  newWin.focus(); return false;\">"+service+"</a>";
		duccLogger.trace(location, null, href);
		return href;
	}
	
	public String buildErrorLink(IDuccWorkJob job) {
		return(buildErrorLink(job,null));
	}
	
	public String buildErrorLink(IDuccWorkJob job, String name) {
		String retVal = job.getSchedulingInfo().getWorkItemsError();
		if(!retVal.equals("0")) {
			String errorCount = retVal;
			if(name == null) {
				name = errorCount;
			}
			String logsjobdir = job.getUserLogsDir()+job.getDuccId().getFriendly()+File.separator;
			String logfile = "jd.err.log";
			String href = "<a href=\""+duccLogData+"?"+"fname="+logsjobdir+logfile+"\" onclick=\"var newWin = window.open(this.href,'child','height=800,width=1200,scrollbars');  newWin.focus(); return false;\">"+name+"</a>";
			retVal = href;
		}
		return retVal;
	}
	
	public String buildInitializeFailuresLink(IDuccWorkJob job) {
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

	public String buildRuntimeFailuresLink(IDuccWorkJob job) {
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
	
	public String trGet(int counter) {
		if((counter % 2) > 0) {
			return "<tr class=\"ducc-row-odd\">";
		}
		else {
			return "<tr class=\"ducc-row-even\">";
		}
	}
	
}
