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
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.uima.ducc.common.NodeConfiguration;
import org.apache.uima.ducc.common.NodeIdentity;
import org.apache.uima.ducc.common.SizeBytes;
import org.apache.uima.ducc.common.boot.DuccDaemonRuntimeProperties;
import org.apache.uima.ducc.common.internationalization.Messages;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccPropertiesResolver;
import org.apache.uima.ducc.common.utils.SystemPropertyResolver;
import org.apache.uima.ducc.common.utils.TimeStamp;
import org.apache.uima.ducc.common.utils.Utils;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.orchestrator.authentication.DuccWebAdministrators;
import org.apache.uima.ducc.transport.event.common.IDuccCompletionType.JobCompletionType;
import org.apache.uima.ducc.transport.event.common.IDuccPerWorkItemStatistics;
import org.apache.uima.ducc.transport.event.common.IDuccProcess;
import org.apache.uima.ducc.transport.event.common.IDuccProcessMap;
import org.apache.uima.ducc.transport.event.common.IDuccSchedulingInfo;
import org.apache.uima.ducc.transport.event.common.IDuccWork;
import org.apache.uima.ducc.transport.event.common.IDuccWorkJob;
import org.apache.uima.ducc.transport.event.common.IDuccWorkReservation;
import org.apache.uima.ducc.transport.event.common.IRationale;
import org.apache.uima.ducc.ws.DuccDataHelper;
import org.apache.uima.ducc.ws.DuccMachinesData;
import org.apache.uima.ducc.ws.registry.IServicesRegistry;
import org.apache.uima.ducc.ws.registry.ServicesRegistry;
import org.apache.uima.ducc.ws.registry.sort.IServiceAdapter;
import org.apache.uima.ducc.ws.server.DuccCookies.DateStyle;
import org.apache.uima.ducc.ws.server.IWebMonitor.MonitorType;
import org.apache.uima.ducc.ws.utils.FormatHelper;
import org.apache.uima.ducc.ws.utils.FormatHelper.Precision;
import org.apache.uima.ducc.ws.utils.alien.EffectiveUser;
import org.apache.uima.ducc.ws.utils.HandlersHelper;
import org.eclipse.jetty.server.handler.AbstractHandler;

public abstract class DuccAbstractHandler extends AbstractHandler {

	private static DuccLogger duccLogger = DuccLogger.getLogger(DuccAbstractHandler.class);
	private static Messages messages = Messages.getInstance();

	private DuccId jobid = null;

	public static DuccWebAdministrators duccWebAdministrators = DuccWebAdministrators.getInstance();
	public static DuccWebSessionManager duccWebSessionManager = DuccWebSessionManager.getInstance();

	public final String notAvailable = Helper.notAvailable;
	public final String inc = "INC";

	public final String duccUimaInitializationReport		  = "uima-initialization-report.html";

	public final String duccContext = "/ducc-servlet";

	public final String duccLogData			  = duccContext+"/log-data";

	public final String duccJpInitSummary	  = duccContext+"/uima-initialization-report-summary";
	public final String duccJpInitData		  = duccContext+"/uima-initialization-report-data";

	public final String duccContextJsonFormat = duccContext+"/json-format";
	public final String duccContextUser       = duccContext+"/user";
	public final String duccContextClassic    = duccContext+"/classic";
	public final String duccContextProxy      = duccContext+"/proxy";
	public final String duccContextViz        = duccContext+"/viz";

	public final String duccjConsoleLink	  = duccContext+"/jconsole-link.jnlp";

	public final int maximumRecordsJobs = 4096;
	public final int defaultRecordsJobs = 16;
	public final int maximumRecordsReservations = 4096;
	public final int defaultRecordsReservations = 8;
	public final int maximumRecordsServices = 4096;
	public final int defaultRecordsServices = 12;

	public String dir_home = Utils.findDuccHome();
	public String dir_resources = "resources";

	protected boolean terminateEnabled = true;
	protected boolean buttonsEnabled = true;

	public static final String valueStateTypeAll = "all";
	public static final String valueStateTypeActive = "active";
	public static final String valueStateTypeInactive = "inactive";
	public static final String valueStateTypeDefault = valueStateTypeAll;

	protected String root_dir = null;
	protected String jconsole_wrapper_signed_jar = null;

	protected DuccWebServer duccWebServer = null;

	public void init(DuccWebServer duccWebServer) {
		this.duccWebServer = duccWebServer;
		root_dir = duccWebServer.getRootDir();
		jconsole_wrapper_signed_jar = root_dir+File.separator+"lib"+File.separator+"webstart"+File.separator+"jconsole-wrapper-signed.jar";
	}

	public DuccWebServer getDuccWebServer() {
		return duccWebServer;
	}

	public enum RequestStateType {
		Active,
		Inactive,
		All
	}

	public static final RequestStateType requestStateTypeDefault = RequestStateType.All;

	public boolean isIgnorable(Throwable t) {
		boolean retVal = false;
		try {
			String rcm = ExceptionUtils.getMessage(t).trim();
			if(rcm.endsWith("java.io.IOException: Broken pipe")) {
				retVal = true;
			}
		}
		catch(Throwable throwable) {
		}
		return retVal;
	}

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

	public String getShortDescription(String description) {
		String retVal = null;
		if(description != null) {
			int index = description.lastIndexOf('/');
			if(index > 0) {
				retVal = description.substring(index);
			}
		}
		return retVal;
	}

	private DateFormat format = new SimpleDateFormat("yyyy.MM.dd HH:mm:ss E", Locale.ENGLISH);

	public String toDash(String value) {
		String retVal = "-";
		if(value != null) {
			if(value.trim().length() > 0) {
				retVal = value;
			}
		}
		return retVal;
	}
	
	public String getTimeStamp(DateStyle dateStyle, long tod) {
		String methodName = "";
		Date date = new Date(tod);
		String sDate = format.format(date);
		duccLogger.trace(methodName, null, "fm="+sDate);
		String retVal = getTimeStamp(dateStyle,  sDate);
		duccLogger.trace(methodName, null, "ts="+retVal);
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

	public String getWebServerHostIP() {
		Properties properties = DuccDaemonRuntimeProperties.getInstance().get(DuccDaemonRuntimeProperties.DaemonName.Webserver);
		String retVal = getPropertiesValue(properties,DuccDaemonRuntimeProperties.keyNodeIpAddress,"");
		return retVal;
	}

	public String getWebServerHostName() {
		Properties properties = DuccDaemonRuntimeProperties.getInstance().get(DuccDaemonRuntimeProperties.DaemonName.Webserver);
		String retVal = getPropertiesValue(properties,DuccDaemonRuntimeProperties.keyNodeName,"");
		return retVal;
	}

	public String useWS(String wsValue, String sofa) {
		String retVal = sofa;
		if(wsValue != null) {
			if(sofa != null) {
				if(wsValue.startsWith(sofa)) {
					retVal = wsValue;
				}
			}
		}
		return retVal;
	}

	public String useWS(String v0, String v1, String v2) {
		String retVal = "";
		if(v0 != null) {
			if(v1 != null) {
				if(v0.equals(v1)) {
					retVal = v2;
				}
			}
		}
		return retVal;
	}

	public String getTimeStamp(HttpServletRequest request, DuccId jobId, String millis) {
		return getTimeStamp(DuccCookies.getDateStyle(request),getTimeStamp(jobId, millis));
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

	protected boolean isAdministrator(HttpServletRequest request, HttpServletResponse response) {
		String methodName = "isAdministrator";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		boolean administrator = false;
		try {
			DuccCookies.RequestRole requestRole = DuccCookies.getRole(request);
			switch(requestRole) {
			case Administrator:
				administrator = true;
				break;
			default:
				break;
			}
		}
		catch(Exception e) {
			duccLogger.error(methodName, null, e);
		}
		duccLogger.trace(methodName, null, messages.fetch("exit"));
		return administrator;
	}

	public boolean isIncludeUser(List<String> users, String user) {
		boolean retVal = true;
		if(users != null) {
			if(user != null) {
				if(!users.isEmpty()) {
					if(!users.contains(user)) {
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
		String cookie = DuccCookies.getCookie(request,DuccCookies.cookieJobsUsers);
		return getUsers(cookie);
	}

	public ArrayList<String> getReservationsUsers(HttpServletRequest request) {
		String cookie = DuccCookies.getCookie(request,DuccCookies.cookieReservationsUsers);
		return getUsers(cookie);
	}

	public ArrayList<String> getServicesUsers(HttpServletRequest request) {
		String cookie = DuccCookies.getCookie(request,DuccCookies.cookieServicesUsers);
		return getUsers(cookie);
	}

	public String getProcessMemorySize(DuccId id, SizeBytes size) {
		String methodName = "getProcessMemorySize";
		String retVal = "?";
		try {
			double dSize = size.getGBytes();
			DecimalFormat formatter = new DecimalFormat("###0");
			retVal = formatter.format(dSize);
		}
		catch(Exception e) {
			duccLogger.trace(methodName, id, e);
		}
		return retVal;
	}

	/*
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
			DecimalFormat formatter = new DecimalFormat("###0");
			retVal = formatter.format(dSize);
		}
		catch(Exception e) {
			duccLogger.trace(methodName, id, messages.fetchLabel("type")+type+" "+messages.fetchLabel("size")+size, e);
		}
		return retVal;
	}
	*/

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

	public int getJobsMax(HttpServletRequest request) {
		int maxRecords = defaultRecordsJobs;
		try {
			String cookie = DuccCookies.getCookie(request,DuccCookies.cookieJobsMax);
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
			String cookie = DuccCookies.getCookie(request,DuccCookies.cookieReservationsMax);
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
			String cookie = DuccCookies.getCookie(request,DuccCookies.cookieServicesMax);
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
		return retVal.trim();
	}

	public String getDeployments(ServicesRegistry servicesRegistry, Properties propertiesMeta) {
		String deployments = "0";
		if(propertiesMeta != null) {
			if(propertiesMeta.containsKey(IServicesRegistry.implementors)) {
                // UIMA-4258, use common implementors parser
                String[] implementors = DuccDataHelper.parseImplementors(propertiesMeta);
				deployments = ""+implementors.length;
			}
		}
		return deployments;
	}

	public ArrayList<String> getSwappingMachines(IDuccWorkJob job) {
		ArrayList<String> retVal = new ArrayList<String>();
		DuccMachinesData.getInstance();
		IDuccProcessMap map = job.getProcessMap();
		for(DuccId duccId : map.keySet()) {
			IDuccProcess jp = map.get(duccId);
			switch(jp.getProcessState()) {
			case Starting:
			case Started:
			case Initializing:
			case Running:
				NodeIdentity nodeId = jp.getNodeIdentity();
				if(nodeId != null) {
					String ip = nodeId.getIp();
					if(DuccMachinesData.getInstance().isMachineSwapping(ip)) {
						if(!retVal.contains(nodeId.getCanonicalName())) {
							retVal.add(nodeId.getCanonicalName());
						}
					}
				}
				break;
			default:
				break;
			}
		}
		return retVal;
	}

	public String getCompletion(HttpServletRequest request, IDuccWorkJob job) {
		String methodName = "getCompletion";
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
		return retVal;
	}

	public String getCompletion(HttpServletRequest request, IDuccWorkReservation reservation) {
		String methodName = "getCompletion";
		String retVal = "";
		try {
			String tVal = reservation.getStandardInfo().getDateOfCompletion();
			duccLogger.trace(methodName, null, tVal);
			retVal = getTimeStamp(request,reservation.getDuccId(),tVal);
		}
		catch(Exception e) {
			duccLogger.trace(methodName, null, "no worries", e);
		}
		catch(Throwable t) {
			duccLogger.trace(methodName, null, "no worries", t);
		}
		return retVal;
	}

	public String getDuration(HttpServletRequest request, IDuccWork dw, Precision precision) {
		String methodName = "getDuration";
		String retVal = "";
		try {
			String v2 = dw.getStandardInfo().getDateOfCompletion();
			String v1 = dw.getStandardInfo().getDateOfSubmission();
			duccLogger.trace(methodName, null, "v2:"+v2+" v1:"+v1);
			retVal = Helper.getDuration(dw.getDuccId(),v2,v1,precision);
		}
		catch(Exception e) {
			duccLogger.trace(methodName, null, "no worries", e);
		}
		catch(Throwable t) {
			duccLogger.trace(methodName, null, "no worries", t);
		}
		return retVal;
	}

	public String getDuration(HttpServletRequest request, IDuccWork dw, long now, Precision precision) {
		String methodName = "getDuration";
		String retVal = "";
		try {
			String v2 = ""+now;
			String v1 = dw.getStandardInfo().getDateOfSubmission();
			duccLogger.trace(methodName, null, "v2:"+v2+" v1:"+v1);
			retVal = Helper.getDuration(dw.getDuccId(),v2,v1,precision);
		}
		catch(Exception e) {
			duccLogger.trace(methodName, null, "no worries", e);
		}
		catch(Throwable t) {
			duccLogger.trace(methodName, null, "no worries", t);
		}
		return retVal;
	}

	public String getProjection(HttpServletRequest request, IDuccWorkJob job, Precision precision) {
		String methodName = "getProjection";
		String retVal = "";
		try {
			IDuccSchedulingInfo schedulingInfo = job.getSchedulingInfo();
			IDuccPerWorkItemStatistics perWorkItemStatistics = schedulingInfo.getPerWorkItemStatistics();
			if (perWorkItemStatistics == null) {
				return "";
			}
			int total = schedulingInfo.getIntWorkItemsTotal();
			int completed = schedulingInfo.getIntWorkItemsCompleted();
			int error = schedulingInfo.getIntWorkItemsError();
			int remainingWorkItems = total - (completed + error);
			if(remainingWorkItems > 0) {
				int usableProcessCount = job.getProcessMap().getUsableProcessCount();
				if(usableProcessCount > 0) {
					if(completed > 0) {
						int threadsPerProcess = schedulingInfo.getIntThreadsPerProcess();
						int totalThreads = usableProcessCount * threadsPerProcess;
						double remainingIterations = remainingWorkItems / totalThreads;
						double avgMillis = perWorkItemStatistics.getMean();
						double leastOperatingMillis = job.getWiMillisOperatingLeast();
						double mostCompletedMillis = job.getWiMillisCompletedMost();
						double projectedTime = (avgMillis * remainingIterations) + (mostCompletedMillis - leastOperatingMillis);
						duccLogger.trace(methodName, job.getDuccId(), "avgMillis:"+avgMillis+" "+"remainingIterations:"+remainingIterations+" "+"mostCompleteMillis:"+mostCompletedMillis+" "+"leastOperatingMillis:"+leastOperatingMillis);
						if(projectedTime > 0) {
							long millis = Math.round(projectedTime);
							if(millis > 1000) {
								String projection = FormatHelper.duration(millis,precision);
								String health = "class=\"health_yellow\"";
								String title = "title=\"Time (ddd:hh:mm:ss) until projected completion\"";
								retVal = "+"+"<span "+health+" "+title+"><i>"+projection+"</i></span>";
								retVal = " {"+retVal+"}";
							}
						}
						else {
							long millis = Math.round(0-projectedTime);
							if(millis > 1000) {
								String projection = FormatHelper.duration(millis,precision);
								String health = "class=\"health_purple\"";
								String title = "title=\"Time (ddd:hh:mm:ss) past-due projected completion\"";
								retVal = "-"+"<span "+health+" "+title+"><i>"+projection+"</i></span>";
								retVal = " {"+retVal+"}";
							}
						}
					}
				}
			}
		}
		catch(Throwable t) {
			duccLogger.trace(methodName, null, t);
		}
		return retVal;
	}

	public double getAvgMillisPerWorkItem(HttpServletRequest request, IDuccWorkJob job) {
		double avgMillis = 0;
		IDuccSchedulingInfo schedulingInfo = job.getSchedulingInfo();
		IDuccPerWorkItemStatistics perWorkItemStatistics = schedulingInfo.getPerWorkItemStatistics();
		if (perWorkItemStatistics != null) {
			avgMillis = perWorkItemStatistics.getMean();
		}
		return avgMillis;
	}

	public String decorateDuration(HttpServletRequest request, IDuccWorkJob job, String duration, Precision precision) {
		String location = "decorateDuration";
		String retVal = duration;
		DuccId duccId = job.getDuccId();
		try {
			StringBuffer title = new StringBuffer();
			double avgMillisPerWorkItem = getAvgMillisPerWorkItem(request, job);
			if(avgMillisPerWorkItem > 0) {
				if(avgMillisPerWorkItem < 500) {
					avgMillisPerWorkItem = 500;
				}
			}
			int iAvgMillisPerWorkItem = (int)avgMillisPerWorkItem;
			if(iAvgMillisPerWorkItem > 0) {
				if(title.length() > 0) {
					title.append("; ");
				}
				title.append("Time (ddd:hh:mm:ss) elapsed for job, average processing time per work item="+FormatHelper.duration(iAvgMillisPerWorkItem,precision));
			}
			String cVal = getCompletion(request,job);
			if(cVal != null) {
				if(cVal.length() > 0) {
					if(title.length() > 0) {
						title.append("; ");
					}
					title.append("End="+cVal);
				}
			}
			if(title.length() > 0) {
				retVal = "<span "+"title=\""+title+"\""+">"+duration+"</span>";
			}
		}
		catch(Exception e) {
			duccLogger.error(location, duccId, e);
		}
		return retVal;
	}

	public String decorateDuration(HttpServletRequest request, IDuccWorkReservation reservation, String duration) {
		String retVal = duration;
		String cVal = getCompletion(request,reservation);
		if(cVal != null) {
			if(cVal.length() > 0) {
				String title = "title=\""+"End="+cVal+"\"";
				retVal = "<span "+title+">"+duration+"</span>";
			}
		}
		return retVal;
	}

	public String getDisabledWithHover(HttpServletRequest request, IDuccWork duccWork) {
		String resourceOwnerUserId = duccWork.getStandardInfo().getUser();
		return getDisabledWithHover(request, resourceOwnerUserId);
	}

	public String getDisabledWithHover(HttpServletRequest request, String resourceOwnerUserId, IServiceAdapter service) {
		String location = "getDisabledWithHover";
		String disabled = "disabled=\"disabled\"";
		String hover = "";
		try {
			String userId = duccWebSessionManager.getUserId(request);
			Properties meta = service.getMeta();
			String serviceId = meta.getProperty(IServicesRegistry.numeric_id);
			if(HandlersHelper.isLoggedIn(request)) {
				if(HandlersHelper.isServiceController(userId, serviceId)) {
					disabled = "";
				}
				else {
					hover = " title=\""+DuccConstants.hintPreferencesNotAdministrator+"\"";
				}
			}
			else {
				hover = " title=\""+DuccConstants.hintLogin+"\"";
			}
		}
		catch(Exception e) {
			duccLogger.trace(location, null, e);
		}
		String retVal = disabled+hover;
		return retVal;
	}
	
	private String getDisabledWithHover(HttpServletRequest request, String resourceOwnerUserId) {
		String disabled = "disabled=\"disabled\"";
		String hover = "";
		HandlersHelper.AuthorizationStatus authorizationStatus = HandlersHelper.getAuthorizationStatus(request, resourceOwnerUserId);
		switch(authorizationStatus) {
		case LoggedInOwner:
			disabled = "";
			break;
		case LoggedInAdministrator:
			disabled = "";
			break;
		case LoggedInNotOwner:
			hover = " title=\""+DuccConstants.hintPreferencesRoleAdministrator+"\"";
			break;
		case LoggedInNotAdministrator:
			hover = " title=\""+DuccConstants.hintPreferencesNotAdministrator+"\"";
			break;
		case NotLoggedIn:
			hover = " title=\""+DuccConstants.hintLogin+"\"";
			break;
		default:
			break;
		}
		return disabled+hover;
	}

	public String buildjConsoleLink(String service) {
		String location = "buildjConsoleLink";
		String retVal = service;
		if(jconsole_wrapper_signed_jar != null) {
			File file = new File(jconsole_wrapper_signed_jar);
			if(file.exists()) {
				retVal = "<a href=\""+duccjConsoleLink+"?"+"service="+service+"\" onclick=\"var newWin = window.open(this.href,'child','height=800,width=1200,scrollbars');  newWin.focus(); return false;\">"+service+"</a>";
			}
		}
		duccLogger.trace(location, null, retVal);
		return retVal;
	}

	public String buildErrorLink(EffectiveUser eu, IDuccWorkJob job) {
		return(buildErrorLink(eu,job,null));
	}

	public String buildErrorLink(EffectiveUser eu, IDuccWorkJob job, String name) {
		String retVal = job.getSchedulingInfo().getWorkItemsError();
		if(!retVal.equals("0")) {
			String errorCount = retVal;
			if(name == null) {
				name = errorCount;
			}
			String logsjobdir = job.getUserLogDir();
			String logfile = "jd.err.log";
			String file_name= logsjobdir+logfile;
			String _window_file_pager = "_window_file_pager";
			String url = Helper.getFilePagerUrl(eu, file_name);
			String href = "<a href=\""+url+"\" onclick=\"var newWin = window.open(this.href,'"+_window_file_pager+"','height=800,width=1200,scrollbars');  newWin.focus(); return false;\">"+name+"</a>";
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

	public String evaluateServices(IDuccWorkJob job, ServicesRegistry servicesRegistry) {
		StringBuffer sb = new StringBuffer();
		String[] serviceDependencies = job.getServiceDependencies();
		if(serviceDependencies == null) {
			sb.append("<span class=\"health_neutral\" >");
			sb.append("0");
			sb.append("</span>");
		}
		else if(job.isCompleted()){
			sb.append("<span class=\"health_neutral\" >");
			sb.append(serviceDependencies.length);
			sb.append("</span>");
		}
		else {
			StringBuffer down = new StringBuffer();
			StringBuffer title = new StringBuffer();
			for(String serviceName : serviceDependencies) {
				if(title.length() > 0) {
					title.append(",");
				}
				title.append(serviceName);
				if(!job.isFinished()) {
					String status = servicesRegistry.getServiceState(serviceName);
					if(!status.equalsIgnoreCase(IServicesRegistry.constant_OK)) {
						if(down.length() != 0) {
							down.append("<br>");
						}
						down.append("<span class=\"health_red\" >");
						down.append(serviceName);
						down.append("=");
						down.append(status);
						down.append("</span>");
					}
				}
			}
			if(down.length() != 0) {
				sb.append(down);
			}
			else {
				if(title.length() > 0) {
					sb.append("<span class=\"health_green\" title=\""+title+"\">");
					sb.append(serviceDependencies.length);
					sb.append("</span>");
				}
				else {
					sb.append("<span class=\"health_green\" >");
					sb.append(serviceDependencies.length);
					sb.append("</span>");
				}
			}
		}
		return sb.toString();
	}

	/*
	 * Format the value of a possibly long string that may contain a classpath
	 * Put space-delimited tokens and the elements of a classpath on separate lines,
	 */
    public String formatValue(String value, boolean formatCp) {
        String[] tokens = value.split(" +");
        StringBuffer sb = new StringBuffer();
        sb.append("<div>");
        for (String item : tokens) {
            if (formatCp) {
                item = item.replace(":", ":<br>");
                //sb.append(item.replace(":", ":<br>")).append("<br>");
                formatCp = false;
            } //else {
                sb.append(item).append("<br>");
                if (item.equals("-cp") || item.equals("-classpath")) {
                    formatCp = true;
                }
            //}
        }
        sb.append("</div>");
        return sb.toString();
    }

    public String tableize(String stringList) {
    	String retVal = stringList;
    	if(retVal != null) {
    		String[] list = stringList.split("\\s+");
    		retVal = tableize(list);
    	}
	    return retVal;
    }
    
    public String tableize(String[] list) {
    	StringBuffer sb = new StringBuffer();
    	if(list != null) {
    		sb.append("<table>");
    		for(String item : list) {
    			sb.append("<tr>");
    			sb.append("<td>");
    			sb.append(item);
    			sb.append("</td>");
    			sb.append("</tr>");
    		}
    		sb.append("</table>");
    	}
    	return sb.toString();
    }
    
	protected String getMonitor(DuccId duccId, MonitorType monitorType) {
		return getMonitor(duccId, monitorType, false);
	}

	protected String getMonitor(DuccId duccId, MonitorType monitorType, boolean multi) {
		String location = "getMonitor";
		StringBuffer sb = new StringBuffer();
		DuccWebMonitor duccWebMonitor = DuccWebMonitor.getInstance();
		Long expiry = duccWebMonitor.getExpiry(monitorType, duccId);
		if(!duccWebMonitor.isAutoCancelEnabled()) {
			if(expiry != null) {
				String text = "webserver not primary";
				sb.append("<span class=\"health_neutral\" title=\""+text+"\">");
				sb.append("MonitorRequested");
				sb.append("</span>");
			}
		}
		else if(expiry != null) {
			if(multi) {
				sb.append(" ");
			}
			long expiryWarnTime = 3;
			Properties properties = DuccWebProperties.get();
			String key = "ducc.ws.job.automatic.cancel.minutes";
			if(properties.containsKey(key)) {
				String value = properties.getProperty(key);
				try {
					long time = Long.parseLong(value)/2;
					if(time > 0) {
						expiryWarnTime = time;
					}
				}
				catch(Exception e) {
					duccLogger.debug(location, duccId, e);
				}
			}
			String base = "submitter's client monitor contact ";
			if(expiry > expiryWarnTime) {
				String hover = base+"established";
				sb.append("<span class=\"health_green\" title=\""+hover+"\">");
				sb.append("MonitorActive");
			}
			else {
				String type = monitorType.getText();
				String minutes = "less than 1 minute";
				if(expiry > 1) {
					minutes = expiry+" minutes";
				}
				String hover = base+"lost, "+type+" will be canceled in "+minutes;
				sb.append("<span class=\"health_red\" title=\""+hover+"\">");
				sb.append("MonitorWarning");
			}
			sb.append("</span>");
		}
		else if(duccWebMonitor.isCanceled(MonitorType.Job, duccId)) {
			sb.append("<span class=\"health_red\" >");
			sb.append("CancelPending...");
			sb.append("</span>");
		}
		return sb.toString();
	}

	protected StringBuffer getReason(IDuccWorkJob job, MonitorType monitorType) {
		StringBuffer sb = new StringBuffer();
		try {
			if(job != null) {
				DuccId duccId = job.getDuccId();
				sb = new StringBuffer();
				if(job.isOperational()) {
					switch(job.getJobState()) {
					case WaitingForResources:
						String rmReason = job.getRmReason();
						if(rmReason != null) {
							sb.append("<span>");
							sb.append(rmReason);
							sb.append("</span>");
						}
						break;
					default:
						String monitor = getMonitor(duccId, monitorType);
						if(monitor.length() > 0) {
							sb.append(monitor);
						}
						break;
					}
				}
				else if(job.isCompleted()) {
					JobCompletionType jobCompletionType = job.getCompletionType();
					switch(jobCompletionType) {
					case EndOfJob:
						try {
							if(job.getDriver().getProcessMap().getAbnormalDeallocationCount() > 0) {
								jobCompletionType = JobCompletionType.DriverProcessFailed;
							}
							else {
								int total = job.getSchedulingInfo().getIntWorkItemsTotal();
								int done = job.getSchedulingInfo().getIntWorkItemsCompleted();
								int error = job.getSchedulingInfo().getIntWorkItemsError();
								if(total != (done+error)) {
									jobCompletionType = JobCompletionType.Premature;
								}
							}
						}
						catch(Exception e) {
						}
						sb.append("<span>");
						break;
					case Undefined:
						sb.append("<span>");
						break;
					default:
						IRationale rationale = job.getCompletionRationale();
						if(rationale != null) {
							if(rationale.isUnspecified()) {
								sb.append("<span>");
							}
							else {
								sb.append("<span title="+rationale.getTextQuoted()+">");
							}
						}
						else {
							sb.append("<span>");
						}
						break;
					}
					sb.append(jobCompletionType);
					sb.append("</span>");
				}
			}
		}
		catch(Exception e) {
			sb.append(e.getMessage());
		}
		return sb;
	}

	protected NodeConfiguration getNodeConfiguration() {
		String methodName = "getNodeConfiguration";
		NodeConfiguration nc = null;
		try {
			String class_definitions = SystemPropertyResolver.getStringProperty(DuccPropertiesResolver.ducc_rm_class_definitions, "scheduler.classes");
			String user_registry = SystemPropertyResolver.getStringProperty(DuccPropertiesResolver.ducc_rm_user_registry, "ducc.users");
			class_definitions = System.getProperty("DUCC_HOME") + "/resources/" + class_definitions;
			nc = new NodeConfiguration(class_definitions, null, user_registry, duccLogger);        // UIMA-4142 make the config global
			nc.readConfiguration();
		}
		catch(Exception e) {
			duccLogger.error(methodName, jobid, e);
		}
		return nc;
	}

	protected int getQuantum(NodeConfiguration nc, String class_name) {
		String methodName = "getQuantum";
		int quantum = SystemPropertyResolver.getIntProperty("ducc.rm.share.quantum", 0);
		try {
			quantum = nc.getQuantumForClass(class_name);
		}
		catch(Exception e) {
			duccLogger.error(methodName, jobid, e);
		}
		return quantum;
	}

}
