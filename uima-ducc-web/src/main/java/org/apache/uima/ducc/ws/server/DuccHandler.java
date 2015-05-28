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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.uima.ducc.cli.ws.json.MachineFacts;
import org.apache.uima.ducc.cli.ws.json.MachineFactsList;
import org.apache.uima.ducc.common.CancelReasons.CancelReason;
import org.apache.uima.ducc.common.NodeConfiguration;
import org.apache.uima.ducc.common.boot.DuccDaemonRuntimeProperties;
import org.apache.uima.ducc.common.boot.DuccDaemonRuntimeProperties.DaemonName;
import org.apache.uima.ducc.common.internationalization.Messages;
import org.apache.uima.ducc.common.jd.files.IWorkItemState;
import org.apache.uima.ducc.common.jd.files.IWorkItemState.State;
import org.apache.uima.ducc.common.jd.files.perf.PerformanceMetricsSummaryItem;
import org.apache.uima.ducc.common.jd.files.perf.PerformanceMetricsSummaryMap;
import org.apache.uima.ducc.common.jd.files.perf.PerformanceSummary;
import org.apache.uima.ducc.common.jd.files.perf.UimaStatistic;
import org.apache.uima.ducc.common.jd.files.workitem.WorkItemStateReader;
import org.apache.uima.ducc.common.system.SystemState;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.DuccProperties;
import org.apache.uima.ducc.common.utils.DuccPropertiesResolver;
import org.apache.uima.ducc.common.utils.DuccSchedulerClasses;
import org.apache.uima.ducc.common.utils.IDuccLoggerComponents;
import org.apache.uima.ducc.common.utils.SynchronizedSimpleDateFormat;
import org.apache.uima.ducc.common.utils.TimeStamp;
import org.apache.uima.ducc.common.utils.Version;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.orchestrator.authentication.DuccWebAdministrators;
import org.apache.uima.ducc.transport.Constants;
import org.apache.uima.ducc.transport.agent.IUimaPipelineAEComponent;
import org.apache.uima.ducc.transport.event.ProcessInfo;
import org.apache.uima.ducc.transport.event.cli.SpecificationProperties;
import org.apache.uima.ducc.transport.event.common.DuccWorkJob;
import org.apache.uima.ducc.transport.event.common.IDuccProcess;
import org.apache.uima.ducc.transport.event.common.IDuccProcess.ReasonForStoppingProcess;
import org.apache.uima.ducc.transport.event.common.IDuccProcessMap;
import org.apache.uima.ducc.transport.event.common.IDuccProcessWorkItems;
import org.apache.uima.ducc.transport.event.common.IDuccStandardInfo;
import org.apache.uima.ducc.transport.event.common.IDuccState.JobState;
import org.apache.uima.ducc.transport.event.common.IDuccTypes.DuccType;
import org.apache.uima.ducc.transport.event.common.IDuccWorkJob;
import org.apache.uima.ducc.transport.event.common.IDuccWorkMap;
import org.apache.uima.ducc.transport.event.common.IDuccWorkReservation;
import org.apache.uima.ducc.transport.event.common.IProcessState.ProcessState;
import org.apache.uima.ducc.transport.event.common.IResourceState.ProcessDeallocationType;
import org.apache.uima.ducc.transport.event.common.TimeWindow;
import org.apache.uima.ducc.ws.DuccDaemonsData;
import org.apache.uima.ducc.ws.DuccData;
import org.apache.uima.ducc.ws.DuccDataHelper;
import org.apache.uima.ducc.ws.DuccMachinesData;
import org.apache.uima.ducc.ws.JobProcessInfo;
import org.apache.uima.ducc.ws.MachineInfo;
import org.apache.uima.ducc.ws.MachineSummaryInfo;
import org.apache.uima.ducc.ws.authentication.DuccAsUser;
import org.apache.uima.ducc.ws.authentication.DuccAuthenticator;
import org.apache.uima.ducc.ws.broker.BrokerHelper;
import org.apache.uima.ducc.ws.broker.BrokerHelper.BrokerAttribute;
import org.apache.uima.ducc.ws.registry.IServicesRegistry;
import org.apache.uima.ducc.ws.registry.ServiceInterpreter;
import org.apache.uima.ducc.ws.registry.ServiceInterpreter.StartState;
import org.apache.uima.ducc.ws.registry.ServicesRegistry;
import org.apache.uima.ducc.ws.registry.ServicesRegistryMapPayload;
import org.apache.uima.ducc.ws.registry.sort.ServicesSortCache;
import org.apache.uima.ducc.ws.server.IWebMonitor.MonitorType;
import org.apache.uima.ducc.ws.sort.JobDetailsProcesses;
import org.apache.uima.ducc.ws.utils.FormatHelper;
import org.apache.uima.ducc.ws.utils.FormatHelper.Precision;
import org.apache.uima.ducc.ws.utils.HandlersHelper;
import org.apache.uima.ducc.ws.utils.LinuxSignals;
import org.apache.uima.ducc.ws.utils.LinuxSignals.Signal;
import org.eclipse.jetty.server.Request;

public class DuccHandler extends DuccAbstractHandler {
	
	private static String component = IDuccLoggerComponents.abbrv_webServer;
	
	private static DuccLogger duccLogger = DuccLoggerComponents.getWsLogger(DuccHandler.class.getName());
	private static Messages messages = Messages.getInstance();
	private static DuccId jobid = null;

	private enum DetailsType { Job, Reservation, Service };
	private enum ShareType { JD, MR, SPC, SPU, UIMA };
	private enum LogType { POP, UIMA };
	
	private DuccAuthenticator duccAuthenticator = DuccAuthenticator.getInstance();
	
	private String duccVersion						= duccContext+"/version";
	
	private String duccLoginLink					= duccContext+"/login-link";
	private String duccLogoutLink					= duccContext+"/logout-link";
	private String duccAuthenticationStatus 		= duccContext+"/authentication-status";
	private String duccAuthenticatorVersion 		= duccContext+"/authenticator-version";
	private String duccAuthenticatorPasswordChecked	= duccContext+"/authenticator-password-checked";
	
	private String duccFileContents 				= duccContext+"/file-contents";
	
	private String duccJobIdData					= duccContext+"/job-id-data";
	private String duccJobWorkitemsCountData		= duccContext+"/job-workitems-count-data";
	private String duccJobProcessesData    			= duccContext+"/job-processes-data";
	private String duccJobWorkitemsData				= duccContext+"/job-workitems-data";
	private String duccJobPerformanceData			= duccContext+"/job-performance-data";
	private String duccJobSpecificationData 		= duccContext+"/job-specification-data";
	private String duccJobFilesData 				= duccContext+"/job-files-data";
	private String duccJobInitializationFailData	= duccContext+"/job-initialization-fail-data";
	private String duccJobRuntimeFailData			= duccContext+"/job-runtime-fail-data";
	
	private String duccReservationProcessesData    	= duccContext+"/reservation-processes-data";
	private String duccReservationSpecificationData = duccContext+"/reservation-specification-data";
	private String duccReservationFilesData 		= duccContext+"/reservation-files-data";
	
	private String duccServiceDeploymentsData    	= duccContext+"/service-deployments-data";
	private String duccServiceRegistryData 			= duccContext+"/service-registry-data";
	private String duccServiceFilesData 			= duccContext+"/service-files-data";
	private String duccServiceHistoryData 			= duccContext+"/service-history-data";
	private String duccServiceSummaryData			= duccContext+"/service-summary-data";
	
	private String duccBrokerSummaryData			= duccContext+"/broker-summary-data";
	
	private String duccSystemAdminAdminData 		= duccContext+"/system-admin-admin-data";
	private String duccSystemAdminControlData 		= duccContext+"/system-admin-control-data";
	private String duccSystemJobsControl			= duccContext+"/jobs-control-request";
	
	private String duccClusterName 					= duccContext+"/cluster-name";
	private String duccClusterUtilization 			= duccContext+"/cluster-utilization";
	private String duccTimeStamp   					= duccContext+"/timestamp";
	private String duccJobSubmit   					= duccContext+"/job-submit-request";
	private String duccJobCancel   					= duccContext+"/job-cancel-request";
	private String duccReservationSubmit    		= duccContext+"/reservation-submit-request";
	private String duccReservationCancel    		= duccContext+"/reservation-cancel-request";
	private String duccServiceSubmit    			= duccContext+"/service-submit-request";
	private String duccServiceCancel    			= duccContext+"/service-cancel-request";
	private String duccServiceEnable  				= duccContext+"/service-enable-request";
	private String duccServiceStart   				= duccContext+"/service-start-request";
	private String duccServiceStop   				= duccContext+"/service-stop-request";
	
	private String duccServiceUpdate   				= duccContext+"/service-update-request";
	
	private String duccReleaseShares   				= duccContext+"/release-shares-request";
	
	private String jsonMachinesData 				= duccContext+"/json-machines-data";
	private String jsonSystemClassesData 			= duccContext+"/json-system-classes-data";
	private String jsonSystemDaemonsData 			= duccContext+"/json-system-daemons-data";

	//private String duccJobSubmitForm	    		= duccContext+"/job-submit-form";
	
	private String duccJobSubmitButton    			= duccContext+"/job-get-submit-button";
	private String duccReservationFormButton  		= duccContext+"/reservation-get-form-button";
	private String duccReservationSubmitButton  	= duccContext+"/reservation-get-submit-button";
	private String duccServiceUpdateFormButton  	= duccContext+"/service-update-get-form-button";
	
	private String duccReservationSchedulingClasses     = duccContext+"/reservation-scheduling-classes";
	private String duccReservationInstanceMemorySizes   = duccContext+"/reservation-memory-sizes";
	private String duccReservationInstanceMemoryUnits   = duccContext+"/reservation-memory-units";
	
	protected String headProvider = "Provider";
	
	protected String providerUser = "user";
	protected String providerFile = "file";
	protected String providerSystem = "";
	protected String providerUnknown = null;
	
	public DuccHandler(DuccWebServer duccWebServer) {
		super.init(duccWebServer);
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
		String value = DuccPropertiesResolver.getInstance().getProperty(DuccPropertiesResolver.ducc_ws_login_enabled);
		Boolean result = new Boolean(value);
		if(!result) {
			String href = "<span title=\"System is configured to disallow logins\" stylen=\"font-size:8pt;\" disabled>Login</span>";
			sb.append(href);
		}
		else {
			boolean userAuth = isAuthenticated(request,response);
	        if (userAuth) {
	        	sb.append("<span class=\"status_on\">");
	        	sb.append("Logged-in");
	        	sb.append("<span>");
	        }
	        else {
	    		String link = "https://"+request.getServerName()+":"+getDuccWebServer().getPortSsl()+"/";
	    		String href = "<a href=\""+link+"login.html\" onclick=\"var newWin = window.open(this.href,'child','height=600,width=475,scrollbars');  newWin.focus(); return false;\">Login</a>";
	    		sb.append(href);
	        }
		}
		response.getWriter().println(sb);
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}	
	
	private void handleDuccServletLogoutLink(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletLogoutLink";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		boolean userAuth = isAuthenticated(request,response);
        if (userAuth) {
    		String link = "https://"+request.getServerName()+":"+getDuccWebServer().getPortSsl()+"/";
    		String href = "<a href=\""+link+"logout.html\" onclick=\"var newWin = window.open(this.href,'child','height=600,width=475,scrollbars');  newWin.focus(); return false;\">Logout</a>";
    		sb.append(href);
        }
        else {
        	sb.append("<span class=\"status_off\">");
        	sb.append("Logged-out");
        	sb.append("<span>");
        }
		response.getWriter().println(sb);
		duccLogger.trace(methodName, null, messages.fetch("exit"));
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
        	sb.append("logged-in");
        	sb.append("<span>");
        }
        else {
        	sb.append("<span class=\"status_off\">");
        	sb.append("logged-out");
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
		sb.append(duccAuthenticator.getVersion());
		response.getWriter().println(sb);
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}	
	
	private void handleDuccServletduccAuthenticatorPasswordChecked(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletduccAuthenticatorPasswordChecked";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		if(duccAuthenticator.isPasswordChecked()) {
			sb.append("<input type=\"password\" name=\"password\"/>");
		}
		else {
			sb.append("<input name=\"password\" value=\"not used\" disabled=disabled title=\"Authenticator does not check password\"/>");
		}
		response.getWriter().println(sb);
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}	
	
	/*
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
	*/
	
	private String buildLogFileName(IDuccWorkJob job, IDuccProcess process, ShareType type) {
		String retVal = "";
		if(process != null) {
			switch(type) {
			case UIMA:
				retVal = job.getDuccId().getFriendly()+"-"+LogType.UIMA.name()+"-"+process.getNodeIdentity().getName()+"-"+process.getPID()+".log";
				break;
			case MR:
				retVal = job.getDuccId().getFriendly()+"-"+LogType.POP.name()+"-"+process.getNodeIdentity().getName()+"-"+process.getPID()+".log";
				break;
			case SPU:
				retVal = job.getDuccId().getFriendly()+"-"+LogType.UIMA.name()+"-"+process.getNodeIdentity().getName()+"-"+process.getPID()+".log";
				break;
			case SPC:
				retVal = job.getDuccId().getFriendly()+"-"+LogType.POP.name()+"-"+process.getNodeIdentity().getName()+"-"+process.getPID()+".log";
				break;
			case JD:
				retVal = "jd.out.log";
				// <UIMA-3802>
				// {jobid}-JD-{node}-{PID}.log
				String node = process.getNodeIdentity().getName();
				String pid = process.getPID();
				retVal = job.getDuccId()+"-"+"JD"+"-"+node+"-"+pid+".log";
				// </UIMA-3802>
				break;
			}
		}
		return retVal;
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
		String retVal = "0";
		try {
			File file = new File(fileName);
			double size = file.length();
			size = size / Constants.MB;
			retVal = sizeFormatter.format(size);
		}
		catch(Exception e) {
			duccLogger.warn(location,jobid,e);
		}
		return retVal;
	}
	
	private String getId(IDuccWorkJob job, IDuccProcess process) {
		StringBuffer sb = new StringBuffer();
		sb.append(job.getDuccId().getFriendly());
		sb.append(".");
		if(process != null) {
			sb.append(process.getDuccId().getFriendly());
		}
		else {
			sb.append("pending");
		}
		return sb.toString();
	}
	
	private String getLog(IDuccWorkJob job, IDuccProcess process, String href) {
		StringBuffer sb = new StringBuffer();
		if(process != null) {
			String pid = process.getPID();
			if(pid != null) {
				sb.append(href);
			}
		}
		return sb.toString();
	}
	
	private String getPid(IDuccWorkJob job, IDuccProcess process) {
		StringBuffer sb = new StringBuffer();
		if(process != null) {
			String pid = process.getPID();
			if(pid != null) {
				sb.append(pid);
			}
		}
		return sb.toString();
	}
	
	private String getStateScheduler(IDuccWorkJob job, IDuccProcess process) {
		StringBuffer sb = new StringBuffer();
		if(process != null) {
			sb.append(process.getResourceState());
		}
		return sb.toString();
	}
	
	private String getRmReason(IDuccWorkJob job) {
		StringBuffer sb = new StringBuffer();
		String rmReason = job.getRmReason();
		if(rmReason != null) {
			sb.append("<span>");
			sb.append(rmReason);
			sb.append("</span>");
		}
		return sb.toString();
	}
	
	private String getProcessReason(IDuccProcess process) {
		StringBuffer sb = new StringBuffer();
		if(process != null) {
			switch(process.getProcessState()) {
			case Starting:
			case Initializing:
			case Running:
				break;
			default:
				ProcessDeallocationType deallocationType = process.getProcessDeallocationType();
				switch(deallocationType) {
				case Undefined:
					break;
				default:
					sb.append(process.getProcessDeallocationType());
					break;
				}
				break;
			}
		}
		return sb.toString();
	}
	private String getReasonScheduler(IDuccWorkJob job, IDuccProcess process) {
		StringBuffer sb = new StringBuffer();
		if(job.isOperational()) {
			switch(job.getJobState()) {
			case WaitingForResources:
				sb.append(getRmReason(job));
				break;
			default:
				sb.append(getProcessReason(process));
				break;
			}
		}
		else {
			sb.append(getProcessReason(process));
		}
		return sb.toString();
	}
	
	private String getStateAgent(IDuccWorkJob job, IDuccProcess process) {
		StringBuffer sb = new StringBuffer();
		if(process != null) {
			ProcessState ps = process.getProcessState();
			switch(ps) {
			case Undefined:
				break;
			default:
				sb.append(ps);
				break;
			}
		}
		return sb.toString();
	}
	
	private String getReasonAgent(IDuccWorkJob job, IDuccProcess process) {
		StringBuffer sb = new StringBuffer();
		if(process != null) {
			String agentReason = process.getReasonForStoppingProcess();
			if(agentReason != null) {
				if(agentReason.equalsIgnoreCase(ReasonForStoppingProcess.KilledByDucc.toString())) {
					agentReason = "<div title=\""+ReasonForStoppingProcess.KilledByDucc.toString()+"\">Discontinued</div>";
				}
				else if(agentReason.equalsIgnoreCase(ReasonForStoppingProcess.Other.toString())) {
					agentReason = "<div title=\""+ReasonForStoppingProcess.Other.toString()+"\">Discontinued</div>";
				}
				sb.append(agentReason);
			}
		}
		return sb.toString();
	}
	
	private String getExit(IDuccWorkJob job, IDuccProcess process) {
		StringBuffer sb = new StringBuffer();
		if(process != null) {
			boolean suppressExitCode = false;
			if(!suppressExitCode) {
				switch(process.getProcessState()) {
				case Stopped:
				case Failed:
				case FailedInitialization:
				case InitializationTimeout:
				case Killed:
					int code = process.getProcessExitCode();
					if(LinuxSignals.isSignal(code)) {
						Signal signal = LinuxSignals.lookup(code);
						if(signal != null) {
							sb.append(signal.name()+"("+signal.number()+")");
						}
						else {
							sb.append("UnknownSignal"+"("+LinuxSignals.getValue(code)+")");
						}
					}
					else {
						sb.append("ExitCode"+"="+code);
					}
					break;
				default:
					break;
				}
			}
		}
		return sb.toString();
	}
	
	private String getTimeInit(IDuccWorkJob job, IDuccProcess process, ShareType sType) {
		String location = "getTimeInit";
		StringBuffer sb = new StringBuffer();
		if(process != null) {
			switch(sType) {
			case MR:
				break;
			default:
				StringBuffer loadme = new StringBuffer();
				String initTime = "00";
				String isp0 = "<span>";
				String isp1 = "</span>";
				try {
					TimeWindow t = (TimeWindow) process.getTimeWindowInit();
					if(t != null) {
						long now = System.currentTimeMillis();
						String tS = t.getStart(""+now);
						String tE = t.getEnd(""+now);
						initTime = getDuration(jobid,tE,tS,Precision.Whole);
						if(t.isEstimated()) {
							isp0 = "<span title=\"estimated\" class=\"health_green\">";
						}
						else {
							isp0 = "<span class=\"health_black\">";
						}
					}
					boolean cluetips_disabled = true;
					if(cluetips_disabled) {
						if(!initTime.equals("00")) {
							String p_idJob = pname_idJob+"="+job.getDuccId().getFriendly();
							String p_idPro = pname_idPro+"="+process.getDuccId().getFriendly();
							initTime = "<a href=\""+duccUimaInitializationReport+"?"+p_idJob+"&"+p_idPro+"\" onclick=\"var newWin = window.open(this.href,'child','height=600,width=475,scrollbars');  newWin.focus(); return false;\">"+initTime+"</a>";
							loadme.append("");
						}
					}
					else {
						List<IUimaPipelineAEComponent> upcList = process.getUimaPipelineComponents();
						if(upcList != null) {
							if(!upcList.isEmpty()) {
								String id = ""+process.getDuccId().getFriendly();
								initTime = "<a class=\"classLoad\" title=\""+id+"\" href=\"#loadme"+id+"\" rel=\"#loadme"+id+"\">"+initTime+"</a>";
								loadme.append("<div id=\"loadme"+id+"\">");
								loadme.append("<table>");
								loadme.append("<tr>");
								String ch1 = "Name";
								String ch2 = "State";
								String ch3 = "Time";
								loadme.append("<td>"+"<b>"+ch1+"</b>");
								loadme.append("<td>"+"<b>"+ch2+"</b>");
								loadme.append("<td>"+"<b>"+ch3+"</b>");
								Iterator<IUimaPipelineAEComponent> upcIterator = upcList.iterator();
								while(upcIterator.hasNext()) {
									IUimaPipelineAEComponent upc = upcIterator.next();
									String iName = upc.getAeName();
									String iState = upc.getAeState().toString();
									String iTime = FormatHelper.duration(upc.getInitializationTime(),Precision.Whole);
									loadme.append("<tr>");
									loadme.append("<td>"+iName);
									loadme.append("<td>"+iState);
									loadme.append("<td>"+iTime);
								}
								loadme.append("</table>");
								loadme.append("</div>");
							}
						}
					}
				}
				catch(Exception e) {
					duccLogger.trace(location, jobid, "no worries", e);
				}
				catch(Throwable t) {
					duccLogger.trace(location, jobid, "no worries", t);
				}
				sb.append(isp0);
				sb.append(loadme);
				sb.append(initTime);
				sb.append(isp1);		
				break;
			}
		}
		return sb.toString();
	}
	
	private String getTimeRun(IDuccWorkJob job, IDuccProcess process, ShareType sType) {
		String location = "getTimeRun";
		StringBuffer sb = new StringBuffer();
		if(process != null) {
			String runTime = "00";
			String rsp0 = "<span>";
			String rsp1 = "</span>";
			// <UIMA-3351>
			boolean useTimeRun = true;
			switch(sType) {
			case SPC:
				break;
			case SPU:
				break;
			case MR:
				break;
			case JD:
				break;	
			case UIMA:
				if(!process.isAssignedWork()) {
					useTimeRun = false;
				}
				break;	
			default:
				break;
			}
			// </UIMA-3351>
			if(useTimeRun) {
				try {
					TimeWindow t = (TimeWindow) process.getTimeWindowRun();
					if(t != null) {
						long now = System.currentTimeMillis();
						String tS = t.getStart(""+now);
						String tE = t.getEnd(""+now);
						runTime = getDuration(jobid,tE,tS,Precision.Whole);
						if(t.isEstimated()) {
							rsp0 = "<span title=\"estimated\" class=\"health_green\">";
						}
						else {
							rsp0 = "<span class=\"health_black\">";
						}
					}
				}
				catch(Exception e) {
					duccLogger.trace(location, jobid, "no worries", e);
				}
				catch(Throwable t) {
					duccLogger.trace(location, jobid, "no worries", t);
				}
			}
			sb.append(rsp0);
			sb.append(runTime);
			sb.append(rsp1);
		}
		return sb.toString();
	}
	
	private SynchronizedSimpleDateFormat dateFormat = new SynchronizedSimpleDateFormat("HH:mm:ss");
	
	private String getTimeGC(IDuccWorkJob job, IDuccProcess process, ShareType sType) {
		StringBuffer sb = new StringBuffer();
		if(process != null) {
			switch(sType) {
			case MR:
				break;
			default:
				long timeGC = 0;
				try {
					timeGC = process.getGarbageCollectionStats().getCollectionTime();
				}
				catch(Exception e) {
				}
				dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
				String displayGC = dateFormat.format(new Date(timeGC));
				displayGC = chomp("00:", displayGC);
				sb.append(displayGC);
				break;
			}
		}
		return sb.toString();
	}
	
	private String getPgIn(IDuccWorkJob job, IDuccProcess process, ShareType sType) {
		StringBuffer sb = new StringBuffer();
		if(process != null) {
			switch(sType) {
			case MR:
				break;
			default:
				long faults = 0;
				try {
					faults = process.getMajorFaults();
				}
				catch(Exception e) {
				}
				sb.append(faults);
				break;
			}
		}
		return sb.toString();
	}
	
	private DecimalFormat formatter = new DecimalFormat("##0.0");
	
	private String getSwap(IDuccWorkJob job, IDuccProcess process, ShareType sType) {
		StringBuffer sb = new StringBuffer();
		if(process != null) {
			switch(sType) {
			case MR:
				break;
			default:
				if(!process.isActive()) {
					double swap = process.getSwapUsageMax();
					swap = swap/Constants.GB;
					String displaySwap = formatter.format(swap);
					sb.append(displaySwap);
				}
				else {
					double swap = process.getSwapUsage();
					swap = swap/Constants.GB;
					String displaySwap = formatter.format(swap);
					double swapMax = process.getSwapUsageMax();
					swapMax = swapMax/Constants.GB;
					String displaySwapMax = formatter.format(swapMax);
					sb.append("<span title=\"max="+displaySwapMax+"\" align=\"right\" "+">");
					sb.append(displaySwap);
					sb.append("</span>");
				}
				break;
			}
		}
		return sb.toString();
	}
	
	private String getPctCPU(IDuccWorkJob job, IDuccProcess process) {
		StringBuffer sb = new StringBuffer();
		if(process != null) {
			String runTime = ""+process.getCpuTime();
			double pctCPU_overall = 0;
			double pctCPU_current = 0;
			String displayCPU = formatter.format(pctCPU_overall);
			if(process.getDataVersion() < 1) {
				boolean rt = false;
				if(runTime != null) {
					if(runTime.contains(":")) {
						rt = true;
					}
					else {
						try {
							long value = Long.parseLong(runTime);
							if(value > 0) {
								rt = true;
							}
						}
						catch(Exception e) {
						}
					}
				}
				try {
					if(rt) {
						long msecsCPU = process.getCpuTime()*1000;
						long msecsRun = process.getTimeWindowRun().getElapsedMillis();
						switch(process.getProcessState()) {
						case Running:
							long msecsInit = process.getTimeWindowInit().getElapsedMillis();
							msecsRun = msecsRun - msecsInit;
							break;
						}
						double secsCPU = (msecsCPU*1.0)/1000.0;
						double secsRun = (msecsRun*1.0)/1000.0;
						double timeCPU = secsCPU;
						double timeRun = secsRun;
						pctCPU_overall = 100*(timeCPU/timeRun);
						if(!Double.isNaN(pctCPU_overall)) {
							StringBuffer tb = new StringBuffer();
							String fmtsecsCPU = formatter.format(secsCPU);
							String fmtsecsRun = formatter.format(secsRun);
							String title = "title="+"\""+"seconds"+" "+"CPU:"+fmtsecsCPU+" "+"run:"+fmtsecsRun+"\"";
							tb.append("<span "+title+">");
							String fmtPctCPU = formatter.format(pctCPU_overall);
							tb.append(fmtPctCPU);
							tb.append("</span>");
							displayCPU = tb.toString();
						}
					}
				}
				catch(Exception e) {
				}
			}
			else {
				StringBuffer tb = new StringBuffer();
				pctCPU_overall = process.getCpuTime();
				pctCPU_current = process.getCurrentCPU();
				switch(process.getProcessState()) {
				case Running:
					String title = "title="+"\"lifetime: "+formatter.format(pctCPU_overall)+"\"";
					tb.append("<span "+title+" class=\"health_green\">");
					tb.append(formatter.format(pctCPU_current));
					tb.append("</span>");
					break;
				default:
					tb.append("<span>");
					tb.append(formatter.format(pctCPU_overall));
					tb.append("</span>");
					break;
				}
				displayCPU = tb.toString();
			}
			sb.append(displayCPU);
		}
		return sb.toString();
	}
	
	private String getRSS(IDuccWorkJob job, IDuccProcess process) {
		StringBuffer sb = new StringBuffer();
		if(process != null) {
			if(process.isComplete()) {
				double rss = process.getResidentMemoryMax();
				rss = rss/Constants.GB;
				String displayRss = formatter.format(rss);
				sb.append(displayRss);
			}
			else {
				double rss = process.getResidentMemory();
				rss = rss/Constants.GB;
				String displayRss = formatter.format(rss);
				double rssMax = process.getResidentMemoryMax();
				rssMax = rssMax/Constants.GB;
				String displayRssMax = formatter.format(rssMax);
				sb.append("<span title=\"max="+displayRssMax+"\" align=\"right\" "+">");
				sb.append(displayRss);
				sb.append("</span>");
			}
		}
		return sb.toString();
	}
	
	private String getJConsole(IDuccWorkJob job, IDuccProcess process, ShareType sType) {
		StringBuffer sb = new StringBuffer();
		if(process != null) {
			switch(process.getProcessState()) {
			case Initializing:
			case Running:
				String jmxUrl = process.getProcessJmxUrl();
				if(jmxUrl != null) {
					String link = buildjConsoleLink(jmxUrl);
					sb.append(link);
				}
				break;
			default:
				break;
			}
		}
		return sb.toString();
	}
	
	private String getFilePagerUrl(String user, String file_name) {
		AlienTextFile atf = new AlienTextFile(user, file_name);
		int pages = atf.getPageCount();
		String parms = "?"+"fname="+file_name+"&"+"pages="+pages;
		String url=duccFilePager+parms;
		return url;
	}
	
	String pname_idJob = "idJob";
	String pname_idPro = "idPro";
	
	private void buildJobProcessListEntry(StringBuffer pb, DuccWorkJob job, IDuccProcess process, DetailsType dType, ShareType sType, int counter) {
		StringBuffer rb = new StringBuffer();
		int COLS = 26;
		switch(sType) {
		case SPC:
		case SPU:
			COLS++;
			break;
		default:
			break;
		}
		StringBuffer[] cbList = new StringBuffer[COLS];
		for(int i=0; i < COLS; i++) {
			cbList[i] = new StringBuffer();
		}
		String logsjobdir = job.getUserLogsDir()+job.getDuccId().getFriendly()+File.separator;
		String logfile = buildLogFileName(job, process, sType);
		
		String user = job.getStandardInfo().getUser();
		String file_name = logsjobdir+logfile;
		
		String url = getFilePagerUrl(user, file_name);
		String href = "<a href=\""+url+"\" onclick=\"var newWin = window.open(this.href,'child','height=800,width=1200,scrollbars');  newWin.focus(); return false;\">"+logfile+"</a>";
		String tr = trGet(counter);
		rb.append(tr);
		int index = -1;
		// Id
		index++; // jp.00
		cbList[index].append("<td align=\"right\">");
		String id = "";
		switch(sType) {
		case SPC:
			id = getId(job,process);
			break;
		case SPU:
			id = getId(job,process);
			break;
		case MR:
			id = getId(job,process);
			break;
		default:
			id = ""+process.getDuccId().getFriendly();
			break;
		}
		cbList[index].append(id);
		logAppend(index,"id",id);
		cbList[index].append("</td>");
		// State
		switch(sType) {
		case SPC:
		case SPU:
			index++; // jp.00.1
			cbList[index].append("<td>");
			String state = job.getJobState().toString();
			cbList[index].append(state);
			logAppend(index,"state",state);
			cbList[index].append("</td>");
			break;
		default:
			break;
		}
		// Services
		switch(sType) {
		case SPC:
		case SPU:
			ServicesRegistry servicesRegistry = ServicesRegistry.getInstance();
			index++; // jp.00.2
			cbList[index].append("<td valign=\"bottom\" align=\"right\">");
			String services = evaluateServices(job,servicesRegistry);
			cbList[index].append(services);
			logAppend(index,"services",services);
			cbList[index].append("</td>");
			break;
		default:
			break;
		}
		// Log
		index++; // jp.01
		cbList[index].append("<td>");
		String log = getLog(job, process, href);
		cbList[index].append(log);
		logAppend(index,"log",log);
		cbList[index].append("</td>");
		// Log Size (in MB)
		index++; // jp.02
		cbList[index].append("<td align=\"right\">");
		String fileSize = getFileSize(logsjobdir+logfile);
		cbList[index].append(fileSize);
		logAppend(index,"fileSize",fileSize);
		cbList[index].append("</td>");
		// Hostname
		index++; // jp.03
		cbList[index].append("<td>");
		String hostname = "";
		if(process != null) {
			hostname = process.getNodeIdentity().getName();
		}
		cbList[index].append(hostname);
		logAppend(index,"hostname",hostname);
		cbList[index].append("</td>");
		// PID
		index++; // jp.04
		cbList[index].append("<td align=\"right\">");
		String pid = getPid(job,process);
		cbList[index].append(pid);
		logAppend(index,"pid",pid);
		cbList[index].append("</td>");
		// State:scheduler
		index++; // jp.05
		cbList[index].append("<td>");
		String stateScheduler = getStateScheduler(job,process);
		cbList[index].append(stateScheduler);
		logAppend(index,"stateScheduler",stateScheduler);
		cbList[index].append("</td>");
		// Reason:scheduler
		index++; // jp.06
		cbList[index].append("<td>");
		String reasonScheduler = getReasonScheduler(job,process);
		cbList[index].append(reasonScheduler);
		logAppend(index,"reasonScheduler",reasonScheduler);
		cbList[index].append("</td>");
		// State:agent
		index++; // jp.07
		cbList[index].append("<td>");
		String stateAgent = getStateAgent(job,process);
		cbList[index].append(stateAgent);
		logAppend(index,"stateAgent",stateAgent);
		cbList[index].append("</td>");
		// Reason:agent
		index++; // jp.08
		cbList[index].append("<td>");
		String reasonAgent = getReasonAgent(job,process);
		cbList[index].append(reasonAgent);
		logAppend(index,"reasonAgent",reasonAgent);
		cbList[index].append("</td>");
		// Exit
		index++; // jp.09
		cbList[index].append("<td>");
		String exit = getExit(job,process);
		cbList[index].append(exit);
		logAppend(index,"exit",exit);
		cbList[index].append("</td>");	
		// Time:init
		switch(sType) {
		case MR:
			break;
		default:
			index++; // jp.10
			cbList[index].append("<td align=\"right\">");
			String timeInit = getTimeInit(job,process,sType);
			cbList[index].append(timeInit);
			logAppend(index,"timeInit",timeInit);
			cbList[index].append("</td>");	
			break;
		}
		// Time:run
		index++; // jp.11
		cbList[index].append("<td align=\"right\">");
		String timeRun = getTimeRun(job,process,sType);
		cbList[index].append(timeRun);
		logAppend(index,"timeRun",timeRun);
		cbList[index].append("</td>");	
		// Time:GC
		switch(sType) {
		case MR:
			break;
		default:
			index++; // jp.12
			cbList[index].append("<td align=\"right\">");
			String timeGC = getTimeGC(job,process,sType);
			cbList[index].append(timeGC);
			logAppend(index,"timeGC",timeGC);
			cbList[index].append("</td>");	
			break;
		}
		// PgIn
		switch(sType) {
		case MR:
			break;
		default:
			index++; // jp.13
			cbList[index].append("<td align=\"right\">");
			String pgin = getPgIn(job,process,sType);
			cbList[index].append(pgin);
			logAppend(index,"pgin",pgin);
			cbList[index].append("</td>");	
			break;
		}
		// Swap
		switch(sType) {
		case MR:
			break;
		default:
			index++; // jp.14
			cbList[index].append("<td align=\"right\">");
			String swap = getSwap(job,process,sType);
			cbList[index].append(swap);
			logAppend(index,"swap",swap);
			cbList[index].append("</td>");
			break;
		}
		// %cpu
		index++; // jp.15
		cbList[index].append("<td align=\"right\">");
		String pctCPU = getPctCPU(job,process);
		cbList[index].append(pctCPU);
		logAppend(index,"%cpu",pctCPU);
		cbList[index].append("</td>");	
		// rss
		index++; // jp.16
		cbList[index].append("<td align=\"right\">");
		String rss = getRSS(job,process);
		cbList[index].append(rss);
		logAppend(index,"rss",rss);
		cbList[index].append("</td>");	
		// other
		switch(sType) {
		case SPC:
			break;
		case SPU:
			break;
		case MR:
			break;
		default:
			// Time:avg
			index++; // jp.17
			String timeAvg = "";
			IDuccProcessWorkItems pwi = process.getProcessWorkItems();
			cbList[index].append("<td align=\"right\">");
			switch(sType) {
			case JD:
				if(pwi != null) {
					timeAvg = ""+(job.getWiMillisAvg()/1000);
				}
				break;
			default:
				if(pwi != null) {
					timeAvg = ""+pwi.getSecsAvg();
				}
				break;
			}
			cbList[index].append(timeAvg);
			logAppend(index,"timeAvg",timeAvg);
			cbList[index].append("</td>");
			// Time:max
			index++; // jp.18
			cbList[index].append("<td align=\"right\">");
			String timeMax = "";
			if(pwi != null) {
				timeMax = ""+pwi.getSecsMax();
			}
			cbList[index].append(timeMax);
			logAppend(index,"timeMax",timeMax);
			cbList[index].append("</td>");
			// Time:min
			index++; // jp.19
			cbList[index].append("<td align=\"right\">");
			String timeMin = "";
			if(pwi != null) {
				timeMin = ""+pwi.getSecsMin();
			}
			cbList[index].append(timeMin);
			logAppend(index,"timeMin",timeMin);
			cbList[index].append("</td>");
			// Done
			index++; // jp.20
			cbList[index].append("<td align=\"right\">");
			String done = "";
			if(pwi != null) {
				done = ""+pwi.getCountDone();
			}
			cbList[index].append(done);
			logAppend(index,"done",done);
			cbList[index].append("</td>");
			// Error
			index++; // jp.21
			cbList[index].append("<td align=\"right\">");
			String error = "";
			if(pwi != null) {
				error = ""+pwi.getCountError();
			}
			cbList[index].append(error);
			logAppend(index,"error",error);
			cbList[index].append("</td>");
			// Dispatch
			switch(dType) {
			case Job:
				index++; // jp.22
				cbList[index].append("<td align=\"right\">");
				String dispatch = "";
				if(pwi != null) {
					if(job.isCompleted()) {
						dispatch = "0";
					}
					else {
						dispatch = ""+pwi.getCountDispatch();
					}
				}
				cbList[index].append(dispatch);
				logAppend(index,"dispatch",dispatch);
				cbList[index].append("</td>");
				break;
			default:
				break;
			}
			// Retry
			index++; // jp.23
			cbList[index].append("<td align=\"right\">");
			String retry = "";
			if(pwi != null) {
				retry = ""+pwi.getCountRetry();
			}
			cbList[index].append(retry);
			logAppend(index,"retry",retry);
			cbList[index].append("</td>");
			// Preempt
			index++; // jp.24
			cbList[index].append("<td align=\"right\">");
			String preempt = "";
			if(pwi != null) {
				preempt = ""+pwi.getCountPreempt();
			}
			cbList[index].append(preempt);
			logAppend(index,"exit",exit);
			cbList[index].append("</td>");
			break;
		}
		// Jconsole:Url
		switch(sType) {
		case MR:
			break;
		default:
			index++; // jp.24
			cbList[index].append("<td>");
			String jConsole = getJConsole(job,process,sType);
			cbList[index].append(jConsole);
			logAppend(index,"jConsole",jConsole);
			cbList[index].append("</td>");	
			break;
		}
		// ResponseBuffer
		for(int i=0; i < COLS; i++) {
			rb.append(cbList[i]);
		}
		rb.append("</tr>");
		pb.append(rb.toString());
		// additional job driver related log files
		switch(sType) {
		case JD:
			String errfile = "jd.err.log";
			// jd.err.log
			if(fileExists(logsjobdir+errfile)) {
				rb = new StringBuffer();
				cbList = new StringBuffer[COLS];
				for(int i=0; i < COLS; i++) {
					cbList[i] = new StringBuffer();
					cbList[i].append("<td>");
					cbList[i].append("</td>");
				}
				// Id
				index = 0;
				// Log
				index = 1;
				String jd_url = getFilePagerUrl(user, logsjobdir+errfile);
				String href2 = "<a href=\""+jd_url+"\" onclick=\"var newWin = window.open(this.href,'child','height=800,width=1200,scrollbars');  newWin.focus(); return false;\">"+errfile+"</a>";
				cbList[index] = new StringBuffer();
				cbList[index].append("<td>");
				cbList[index].append(href2);
				cbList[index].append("</td>");
				// Size
				index = 2;
				cbList[index] = new StringBuffer();
				cbList[index].append("<td align=\"right\">");
				cbList[index].append(getFileSize(logsjobdir+errfile));
				cbList[index].append("</td>");
				// row
				rb.append(tr);
				for(int i=0; i < COLS; i++) {
					rb.append(cbList[i]);
				}
				rb.append("</tr>");
				pb.append(rb.toString());
			}
			break;
		}
	}

	private void logAppend(int index, String name, String value) {
		String location = "";
		duccLogger.debug(location, jobid, "index:"+index+" "+""+name+"="+"\'"+value+"\'");
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
	
	private void thSep(StringBuffer sb) {
		sb.append("<th>");
		sb.append("&nbsp");
		sb.append("&nbsp");
		sb.append("</th>");
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
		sb.append("Id: ");
		String jobId = request.getParameter("id");
		sb.append(jobId);
		sb.append("</th>");
		thSep(sb);
		//
		IDuccWorkJob job = findJob(jobId);
		// job state
		sb.append("<th title=\"The current state of this job\">");
		sb.append("State: ");
		if(job != null) {
			Object stateObject = job.getStateObject();
			if(stateObject != null) {
				String state = stateObject.toString();
				sb.append(state);
			}
			else {
				String state = JobState.Undefined.name();
				sb.append(state);
				duccLogger.warn(methodName, job.getDuccId(), state);
			}
			sb.append("</th>");
			thSep(sb);
			// job reason
			if(job.isCompleted()) {
				sb.append("<th title=\"The reason for the final state of this job, normally EndOfJob\">");
				sb.append("Reason: ");
				String reason = getReason(job, MonitorType.Job).toString();
				sb.append(reason);
				thSep(sb);
			}
			// workitems
			String jobWorkitemsCount = "?";
			if(job != null) {
				jobWorkitemsCount = job.getSchedulingInfo().getWorkItemsTotal();
			}
			sb.append("<th title=\"The total number of work items for this job\">");
			sb.append("Workitems: ");
			sb.append(jobWorkitemsCount);
			sb.append("</th>");
			thSep(sb);
			// done
			sb.append("<th title=\"The number of work items that completed successfully\">");
			sb.append("Done: ");
			String done = "0";
			try {
				done = ""+job.getSchedulingInfo().getIntWorkItemsCompleted();
			}
			catch(Exception e) {
			}
			sb.append(done);
			sb.append("</th>");
			thSep(sb);
			// error & lost
			int eCount = 0;
			try {
				eCount = job.getSchedulingInfo().getIntWorkItemsError();
			}
			catch(Exception e) {
			}
			String error = ""+eCount;
			sb.append("<th title=\"The number of work items that failed to complete successfully\">");
			sb.append("Error: ");
			sb.append(error);
			sb.append("</th>");
			// extended info live jobs
			thSep(sb);
			JobState jobState = JobState.Undefined;
			try {
				jobState = job.getJobState();
			}
			catch(Exception e) {
			}
			switch(jobState) {
			case Completed:
			case Undefined:
				break;
			default:
				int dispatch = 0;
				int unassigned = job.getSchedulingInfo().getCasQueuedMap().size();
				try {
					dispatch = Integer.parseInt(job.getSchedulingInfo().getWorkItemsDispatched())-unassigned;
				}
				catch(Exception e) {
				}
				// dispatch
				sb.append("<th title=\"The number of work items currently dispatched\">");
				sb.append("Dispatch: ");
				sb.append(dispatch);
				sb.append("</th>");
				thSep(sb);
				break;
			}
		}
		else {
			String state = "NotFound";
			sb.append(state);
			duccLogger.warn(methodName, jobid, jobId);
		}
		sb.append("</table>");
		response.getWriter().println(sb);
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}
	
	private IDuccWorkJob findJob(String jobno) {
		IDuccWorkJob job = null;
		IDuccWorkMap duccWorkMap = DuccData.getInstance().get();
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
		IDuccWorkMap duccWorkMap = DuccData.getInstance().get();
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
			int counter = 1;
			while(iterator.hasNext()) {
				DuccId processId = iterator.next();
				IDuccProcess process = job.getDriver().getProcessMap().get(processId);
				StringBuffer bb = new StringBuffer();
				buildJobProcessListEntry(bb, job, process, DetailsType.Job, ShareType.JD, counter);
				if(bb.length() > 0) {
					sb.append(bb.toString());
					counter++;
				}
			}
			TreeMap<JobDetailsProcesses,JobDetailsProcesses> map = new TreeMap<JobDetailsProcesses,JobDetailsProcesses>();
			iterator = job.getProcessMap().keySet().iterator();
			while(iterator.hasNext()) {
				DuccId processId = iterator.next();
				IDuccProcess process = job.getProcessMap().get(processId);
				JobDetailsProcesses jdp = new JobDetailsProcesses(process);
				map.put(jdp, jdp);
			}
			Iterator<JobDetailsProcesses> sortedIterator = map.keySet().iterator();
			while(sortedIterator.hasNext()) {
				JobDetailsProcesses jdp = sortedIterator.next();
				IDuccProcess process = jdp.getProcess();
				StringBuffer bb = new StringBuffer();
				buildJobProcessListEntry(bb, job, process, DetailsType.Job, ShareType.UIMA, counter);
				if(bb.length() > 0) {
					sb.append(bb.toString());
					counter++;
				}
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
		IDuccWorkMap duccWorkMap = DuccData.getInstance().get();
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
	
	private DuccWorkJob getManagedReservation(String reservationNo) {
		DuccWorkJob managedReservation = null;
		IDuccWorkMap duccWorkMap = DuccData.getInstance().get();
		if(duccWorkMap.getServiceKeySet().size()> 0) {
			Iterator<DuccId> iterator = null;
			iterator = duccWorkMap.getServiceKeySet().iterator();
			while(iterator.hasNext()) {
				DuccId jobId = iterator.next();
				String fid = ""+jobId.getFriendly();
				if(reservationNo.equals(fid)) {
					managedReservation = (DuccWorkJob) duccWorkMap.findDuccWork(jobId);
					break;
				}
			}
		}
		return managedReservation;
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
				long now = System.currentTimeMillis();
				String directory = job.getLogDirectory()+jobNo;
				String userId = duccWebSessionManager.getUserId(request);
				long wiVersion = job.getWiVersion();
				WorkItemStateReader workItemStateReader = new WorkItemStateReader(component, directory, userId, wiVersion);
				ConcurrentSkipListMap<Long,IWorkItemState> map = workItemStateReader.getMap();
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
					String ptime;
					String itime;
					int counter = 0;
			    	for (Entry<IWorkItemState, IWorkItemState> entry : sortedMap.entrySet()) {
			    		StringBuffer row = new StringBuffer();
			    		IWorkItemState wis = entry.getValue();
					    row.append(trGet(counter++));
			    		if(counter > DuccConstants.workItemsDisplayMax) {
			    			// SeqNo
							row.append("<td align=\"right\">");
							row.append("*****");
							// Id
							row.append("<td align=\"right\">");
							row.append("*****");
							// Status
							row.append("<td align=\"right\">");
							row.append("display");
							// Queuing Time (sec)
							row.append("<td align=\"right\">");
							row.append("limit");
							// Processing Time (sec)
							row.append("<td align=\"right\">");
							row.append("reached");
							// Investment Time (sec)
							row.append("<td align=\"right\">");
							row.append("reached");
							// Node (IP)
							row.append("<td align=\"right\">");
							row.append("*****");
							// Node (Name)
							row.append("<td align=\"right\">");
							row.append("*****");
							// PID
							row.append("<td align=\"right\">");
							row.append("*****");
							sb.append(row);
			    			duccLogger.warn(methodName, job.getDuccId(), "work items display max:"+DuccConstants.workItemsDisplayMax);
			    			break;
			    		}
			    		// SeqNo
						row.append("<td align=\"right\">");
						row.append(wis.getSeqNo());
						// Id
						row.append("<td align=\"right\">");
						row.append(wis.getWiId());
						// Status
						row.append("<td align=\"right\">");
						
						State state = wis.getState();
						StringBuffer status = new StringBuffer();
						switch(state) {
						case lost:
							//status = row.append("<span title=\"Work Item was queued but never dequeued. (This is most likely a DUCC framework issue.)\" >");
							status.append("<span title=\"Work Item was queued but never dequeued.\" >");
							status.append(state);
							status.append("</span>");
							break;
						default:
							status.append(state);
							break;
						}
						row.append(status);
						// Queuing Time (sec)
						time = getAdjustedTime(wis.getMillisOverhead(), job);
						time = time/1000;
						row.append("<td align=\"right\">");
						row.append(formatter.format(time));
						// Processing Time (sec)
						time = getAdjustedTime(wis.getMillisProcessing(now), job);
						time = time/1000;
						ptime = formatter.format(time);
						row.append("<td align=\"right\">");
						switch(state) {
						case start:
						case queued:
						case operating:
							row.append("<span title=\"estimated\" class=\"health_green\">");
							break;
						default:
							row.append("<span class=\"health_black\">");
							break;
						}
						row.append(ptime);
						row.append("</span>");
						// Investment Time (sec)
						time = getAdjustedTime(wis.getMillisInvestment(now), job);
						time = time/1000;
						itime = formatter.format(time);
						row.append("<td align=\"right\">");
						String ispan = "<span class=\"health_black\">";
						if(time > 0) {
							if(!itime.equals(ptime)) {
								ispan = "<span title=\"investment reset\" class=\"health_red\">";
							}
						}
						row.append(ispan);
						row.append(itime);
						row.append("</span>");
						// Node (IP)
						row.append("<td>");
						String node = wis.getNode();
						if(node != null) {
							row.append(node);
						}
						// Node (Name)
						row.append("<td>");
						if(node != null) {
							String hostName = machinesData.getNameForIp(node);
							if(hostName != null) {
								row.append(hostName);
							}
						}
						// PID
						row.append("<td>");
						String pid = wis.getPid();
						if(pid != null) {
							row.append(pid);
						}
						sb.append(row);
						duccLogger.trace(methodName, null, "**"+counter+"**"+" "+row);
			    	}
			    }
			}
			catch(Exception e) {
				duccLogger.warn(methodName, null, e.getMessage());
				duccLogger.debug(methodName, null, e);
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
				String directory = job.getLogDirectory()+jobNo;
				String userId = duccWebSessionManager.getUserId(request);
				long wiVersion = job.getWiVersion();
				WorkItemStateReader workItemStateReader = new WorkItemStateReader(component, directory, userId, wiVersion);
				PerformanceSummary performanceSummary = new PerformanceSummary(job.getLogDirectory()+jobNo);
			    PerformanceMetricsSummaryMap performanceMetricsSummaryMap = performanceSummary.readSummary(userId);
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
			    	sb.append("<table class=\"sortable\">");
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
				    	PerformanceMetricsSummaryItem item = entry.getValue();
				    	String shortname = item.getDisplayName();
				    	long anTime = item.getAnalysisTime();
				    	long anMinTime = item.getAnalysisTimeMin();
				    	long anMaxTime = item.getAnalysisTimeMax();
				    	analysisTime += anTime;
				    	UimaStatistic stat = new UimaStatistic(shortname, entry.getKey(), anTime, anMinTime, anMaxTime);
				    	uimaStats.add(stat);
				    }
				    Collections.sort(uimaStats);
				    int numstats = uimaStats.size();
				    DecimalFormat formatter = new DecimalFormat("##0.0");
				    // pass 1
				    double time_total = 0;
				    for (int i = 0; i < numstats; ++i) {
						time_total += (uimaStats.get(i).getAnalysisTime());
					}
				    int counter = 0;
				    sb.append(trGet(counter++));
				    // Totals
					sb.append("<td>");
					sb.append("<i><b>Summary</b></i>");
					long ltime = 0;
					// Total
					sb.append("<td align=\"right\">");
					ltime = (long)time_total;
					sb.append(FormatHelper.duration(ltime,Precision.Tenths));
					// % of Total
					sb.append("<td align=\"right\">");
					sb.append(formatter.format(100));
					// Avg
					sb.append("<td align=\"right\">");
					sb.append("<span class=\"health_purple\" title=\"average processing time per completed work item\">");
					long avgMillis = 0;
					if(casCount > 0) {
						avgMillis = (long) (analysisTime  / (1.0 * casCount));
					}
					try {
						//ltime = job.getWiMillisAvg();
						ltime = avgMillis;
					}
					catch(Exception e) {
						ltime = (long)workItemStateReader.getMin();
					}
					sb.append(FormatHelper.duration(ltime,Precision.Tenths));
					sb.append("</span>");
					// Min
					sb.append("<td align=\"right\">");
					sb.append("<span class=\"health_purple\" title=\"minimum processing time for any completed work item\">");
					try {
						ltime = job.getWiMillisMin();
					}
					catch(Exception e) {
						ltime = (long)workItemStateReader.getMin();
					}
					sb.append(FormatHelper.duration(ltime,Precision.Tenths));
					sb.append("</span>");
					// Max
					sb.append("<td align=\"right\">");
					sb.append("<span class=\"health_purple\" title=\"maximum processing time for any completed work item\">");
					try {
						ltime = job.getWiMillisMax();
					}
					catch(Exception e) {
						ltime = (long)workItemStateReader.getMin();
					}
					sb.append(FormatHelper.duration(ltime,Precision.Tenths));
					sb.append("</span>");
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
						sb.append(FormatHelper.duration(ltime,Precision.Tenths));
						// % of Total
						sb.append("<td align=\"right\">");
						double dtime = (time/time_total)*100;
						sb.append(formatter.format(dtime));
						// Avg
						sb.append("<td align=\"right\">");
						time = time/casCount;
						ltime = (long)time;
						sb.append(FormatHelper.duration(ltime,Precision.Tenths));
						// Min
						sb.append("<td align=\"right\">");
						time = uimaStats.get(i).getAnalysisMinTime();
						ltime = (long)time;
						sb.append(FormatHelper.duration(ltime,Precision.Tenths));
						// Max
						sb.append("<td align=\"right\">");
						time = uimaStats.get(i).getAnalysisMaxTime();
						ltime = (long)time;
						sb.append(FormatHelper.duration(ltime,Precision.Tenths));
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
		putJobSpecEntry(properties, providerUnknown, key, value, sb, counter);
	}
	
	private void putJobSpecEntry(Properties properties, String provider, String key, String value, StringBuffer sb, int counter) {
		if(value != null) {
			sb.append(trGet(counter));
			if(provider != null) {
				sb.append("<td>");
				sb.append(provider);
			}
			sb.append("<td>");
			sb.append(key);
			sb.append("</td>");
			sb.append("<td>");
			sb.append(value);
			sb.append("</td>");
			sb.append("</tr>");
		}
	}
	
	private boolean isProvided(Properties usProperties, Properties fsProperties) {
		if(usProperties != null) {
			return true;
		}
		if(fsProperties != null) {
			return true;
		}
		return false;	
	}
	
	private String getProvider(String key, Properties usProperties, Properties fsProperties) {
		if(isProvided(usProperties, fsProperties)) {
			if(usProperties != null) {
				if(usProperties.containsKey(key)) {
					return providerUser;
				}
			}
			if(fsProperties != null) {
				if(fsProperties.containsKey(key)) {
					return providerFile;
				}
			}
			return providerSystem;
		}
		else {
			return providerUnknown;
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
				String userId = duccWebSessionManager.getUserId(request);
				Properties usProperties = DuccFile.getUserSpecifiedProperties(job, userId);
				Properties fsProperties = DuccFile.getFileSpecifiedProperties(job, userId);
				Properties properties = DuccFile.getJobProperties(job, userId);
				TreeMap<String,String> map = new TreeMap<String,String>();
				Enumeration<?> enumeration = properties.keys();
				while(enumeration.hasMoreElements()) {
					String key = (String)enumeration.nextElement();
					map.put(key, key);
				}
				Iterator<String> iterator = map.keySet().iterator();
				sb.append("<table id=\"specification_table\" class=\"sortable\">");
				sb.append("<tr class=\"ducc-head\">");
				if(isProvided(usProperties, fsProperties)) {
					sb.append("<th title=\"system provided if blank\">");
					sb.append(headProvider);
				}
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
					String provider = getProvider(key, usProperties, fsProperties);
					if(key.endsWith("classpath")) {
						value = formatClasspath(value);
						String show = "<div class=\"hidedata\"><input type=\"submit\" name=\"showcp\" value=\"Show\" id=\"showbutton"+i+"\"/></div>";
						String hide = "<div class=\"showdata\"><input type=\"submit\" name=\"hidecp\" value=\"Hide\" id=\"hidebutton"+i+"\"/>"+" "+value+"</div>";
						value = show+hide;
						i++;
					}
					putJobSpecEntry(properties, provider, key, value, sb, counter++);
				}
				sb.append("</table>");
				sb.append("<br>");
				sb.append("<br>");
			}
			catch(Throwable t) {
				duccLogger.warn(methodName, null, t);
				sb = new StringBuffer();
				sb.append("no data");
			}
		}
		response.getWriter().println(sb);
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}

	private void handleDuccServletJobFilesData(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletJobFilesData";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		String jobNo = request.getParameter("id");
		DuccWorkJob job = getJob(jobNo);
		if(job != null) {
			try {
				String userId = duccWebSessionManager.getUserId(request);
				TreeMap<String, File> map = DuccFile.getFilesInLogDirectory(job, userId);
				Set<String> keys = map.keySet();
				int counter = 0;
				for(String key : keys) {
					File file = map.get(key);
					StringBuffer row = new StringBuffer();
					//
					String tr = trGet(counter);
					sb.append(tr);
					/*
					// date
					row.append("<td>");
					row.append("<span title=\""+DuccConstants.hintPreferencesDateStyle+"\">");
					String date = getTimeStamp(request,job.getDuccId(), ""+file.lastModified());
					row.append(date);
					row.append("</span>");
					row.append("</td>");
					*/
					// name
					row.append("<td>");
					String url = getFilePagerUrl(userId, file.getAbsolutePath());
					String href = "<a href=\""+url+"\" onclick=\"var newWin = window.open(this.href,'child','height=800,width=1200,scrollbars');  newWin.focus(); return false;\">"+file.getName()+"</a>";
					row.append(href);
					row.append("</td>");
					// size
					row.append("<td>");
					row.append(getFileSize(file.getAbsolutePath()));
					row.append("</td>");
					//
					row.append("</tr>");
					sb.append(row);
					counter++;
				}
			}
			catch(Throwable t) {
				duccLogger.warn(methodName, null, t);
				sb = new StringBuffer();
				sb.append("no data");
			}
		}
		response.getWriter().println(sb);
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}
		
	private void handleDuccServletReservationFilesData(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletReservationFilesData";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		String reservationNo = request.getParameter("id");
		DuccWorkJob reservation = getManagedReservation(reservationNo);
		if(reservation != null) {
			try {
				String userId = duccWebSessionManager.getUserId(request);
				TreeMap<String, File> map = DuccFile.getFilesInLogDirectory(reservation, userId);
				Set<String> keys = map.keySet();
				int counter = 0;
				for(String key : keys) {
					File file = map.get(key);
					StringBuffer row = new StringBuffer();
					//
					String tr = trGet(counter);
					sb.append(tr);
					/*
					// date
					row.append("<td>");
					row.append("<span title=\""+DuccConstants.hintPreferencesDateStyle+"\">");
					String date = getTimeStamp(request,job.getDuccId(), ""+file.lastModified());
					row.append(date);
					row.append("</span>");
					row.append("</td>");
					*/
					// name
					row.append("<td>");
					String url = getFilePagerUrl(userId, file.getAbsolutePath());
					String href = "<a href=\""+url+"\" onclick=\"var newWin = window.open(this.href,'child','height=800,width=1200,scrollbars');  newWin.focus(); return false;\">"+file.getName()+"</a>";
					row.append(href);
					row.append("</td>");
					// size
					row.append("<td>");
					row.append(getFileSize(file.getAbsolutePath()));
					row.append("</td>");
					//
					row.append("</tr>");
					sb.append(row);
					counter++;
				}
			}
			catch(Throwable t) {
				duccLogger.warn(methodName, null, t);
				sb = new StringBuffer();
				sb.append("no data");
			}
		}
		response.getWriter().println(sb);
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}
	
	private void buildServiceFilesListEntry(Request baseRequest,HttpServletRequest request, StringBuffer sb, DuccWorkJob job, IDuccProcess process, ShareType type, int counter) {
		if(job != null) {
			try {
				String userId = duccWebSessionManager.getUserId(request);
				TreeMap<String, File> map = DuccFile.getFilesInLogDirectory(job, userId);
				Set<String> keys = map.keySet();
				for(String key : keys) {
					File file = map.get(key);
					StringBuffer row = new StringBuffer();
					//
					String tr = trGet(counter);
					sb.append(tr);
					/*
					// date
					row.append("<td>");
					row.append("<span title=\""+DuccConstants.hintPreferencesDateStyle+"\">");
					String date = getTimeStamp(request,job.getDuccId(), ""+file.lastModified());
					row.append(date);
					row.append("</span>");
					row.append("</td>");
					*/
					// id
					row.append("<td>");
					row.append(job.getId()+"."+process.getDuccId());
					row.append("</td>");
					// name
					row.append("<td>");
					String url = getFilePagerUrl(userId, file.getAbsolutePath());
					String href = "<a href=\""+url+"\" onclick=\"var newWin = window.open(this.href,'child','height=800,width=1200,scrollbars');  newWin.focus(); return false;\">"+file.getName()+"</a>";
					row.append(href);
					row.append("</td>");
					// size
					row.append("<td>");
					row.append(getFileSize(file.getAbsolutePath()));
					row.append("</td>");
					//
					row.append("</tr>");
					sb.append(row);
					counter++;
				}
			}
			catch(Throwable t) {
				// no worries
			}
		}
	}
	
	private void handleDuccServletServiceFilesData(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletServiceFilesData";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		
		try {
			String name = request.getParameter("name");
			ServicesRegistry servicesRegistry = ServicesRegistry.getInstance();
			ServicesRegistryMapPayload payload = servicesRegistry.findService(name);
			Properties properties;
			properties = payload.meta;

            // UIMA-4258, use common implementors parser
			ArrayList<String> implementors = DuccDataHelper.parseServiceIdsAsList(properties);
			
			DuccWorkJob service = null;
			IDuccWorkMap duccWorkMap = DuccData.getInstance().get();
			if(duccWorkMap.getServiceKeySet().size()> 0) {
				Iterator<DuccId> iterator = null;
				iterator = duccWorkMap.getServiceKeySet().iterator();
				int counter = 0;
				ShareType type = ShareType.SPU;
				String service_type = properties.getProperty(IServicesRegistry.service_type);
				if(service_type != null) {
					if(service_type.equalsIgnoreCase(IServicesRegistry.service_type_CUSTOM)) {
						type = ShareType.SPC;
					}
				}
				while(iterator.hasNext()) {
					DuccId serviceId = iterator.next();
					String fid = ""+serviceId.getFriendly();
					if(implementors.contains(fid)) {
						service = (DuccWorkJob) duccWorkMap.findDuccWork(serviceId);
						IDuccProcessMap map = service.getProcessMap();
						for(DuccId key : map.keySet()) {
							IDuccProcess process = map.get(key);
							buildServiceFilesListEntry(baseRequest,request,sb, service, process, type, ++counter);
						}
					}
				}
			}
		}
		catch(Throwable t) {
			// no worries
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
	
	private void handleDuccServletServiceHistoryData(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletServiceHistoryData";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		
		try {
			String name = request.getParameter("name");
			duccLogger.debug(methodName, null, name);
			String userId = duccWebSessionManager.getUserId(request);
			ServicesRegistry servicesRegistry = ServicesRegistry.getInstance();
			ServicesRegistryMapPayload payload = servicesRegistry.findService(name);
			Properties properties;
			properties = payload.meta;
			String numeric_id = properties.getProperty(IServicesRegistry.numeric_id);
			properties = payload.svc;
			String log_directory = properties.getProperty(IServicesRegistry.log_directory);
			File serviceDirectory = new File(log_directory);
			if(serviceDirectory.isDirectory()) {
				int counter = 0;
				TreeMap<String,File> map = new TreeMap<String,File>();
				File[] files = serviceDirectory.listFiles();
				for(File file : files) {
					if(file.isDirectory()) {
						map.put(file.getName(),file);
					}
				}
				for(String historyDirectoryName : map.descendingKeySet()) {
					File historyDirectoryFile = map.get(historyDirectoryName);
					File[] historyFiles = historyDirectoryFile.listFiles();
					for(File historyFile : historyFiles) {
						StringBuffer row = new StringBuffer();
						String tr = trGet(counter);
						row.append(tr);
						// id
						row.append("<td>");
						row.append(numeric_id+"."+historyDirectoryFile.getName());
						row.append("</td>");
						// name
						row.append("<td>");
						
						String url = getFilePagerUrl(userId, historyFile.getAbsolutePath());
						String href = "<a href=\""+url+"\" onclick=\"var newWin = window.open(this.href,'child','height=800,width=1200,scrollbars');  newWin.focus(); return false;\">"+historyFile.getName()+"</a>";
						row.append(href);
						row.append("</td>");
						// size
						row.append("<td>");
						row.append(getFileSize(historyFile.getAbsolutePath()));
						row.append("</td>");
						// date
						row.append("<td>");
						row.append(getTimeStamp(request, jobid, ""+historyFile.lastModified()));
						row.append("</td>");
						//
						row.append("</tr>");
						sb.append(row);
						counter++;
					}
				}
			}
			duccLogger.debug(methodName, null, log_directory);
		}
		catch(Throwable t) {
			// no worries
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
	
	private void handleDuccServletJobInitializationFailData(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletJobSpecificationData";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		String userId = duccWebSessionManager.getUserId(request);
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
					switch(DuccCookies.getDisplayStyle(request)) {
					case Textual:
						break;
					case Visual:
						String key = "cap.large";
						String capFile = DuccWebServerHelper.getImageFileName(key);
						if(capFile != null) {
							if(job.getSchedulingInfo().getLongSharesMax() < 0) {
								data.append("<tr>");
								data.append("<td>");
								sb.append("<span title=\"capped at current number of running processes due to excessive initialization failures\">");
								sb.append("<img src=\""+capFile+"\">");
								sb.append("</span>");
							}
						}
						break;
					default:
						break;
					}
					while(processIterator.hasNext()) {
						data.append("<tr>");
						data.append("<td>");
						DuccId processId = processIterator.next();
						IDuccProcess process = processMap.get(processId);
						String logsjobdir = job.getUserLogsDir()+job.getDuccId().getFriendly()+File.separator;
						String logfile = buildLogFileName(job, process, ShareType.UIMA);
						String link = logfile;
						String reason = process.getReasonForStoppingProcess();
						if(reason != null) {
							if(reason.trim() != "") {
								link = logfile+":"+reason;
							}
						}
						String url = getFilePagerUrl(userId, logsjobdir+logfile);
						String href = "<a href=\""+url+"\" onclick=\"var newWin = window.open(this.href,'child','height=800,width=1200,scrollbars');  newWin.focus(); return false;\">"+link+"</a>";
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
		String userId = duccWebSessionManager.getUserId(request);
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
						String logsjobdir = job.getUserLogsDir()+job.getDuccId().getFriendly()+File.separator;
						String logfile = buildLogFileName(job, process, ShareType.UIMA);
						String link = logfile;
						String reason = process.getReasonForStoppingProcess();
						if(reason != null) {
							if(reason.trim() != "") {
								link = logfile+":"+reason;
							}
						}
						String url = getFilePagerUrl(userId, logsjobdir+logfile);
						String href = "<a href=\""+url+"\" onclick=\"var newWin = window.open(this.href,'child','height=800,width=1200,scrollbars');  newWin.focus(); return false;\">"+link+"</a>";
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
	
	private void buildServiceProcessListEntry(StringBuffer sb, DuccWorkJob job, IDuccProcess process, DetailsType dType, ShareType sType, int counter) {
		buildJobProcessListEntry(sb, job, process, dType, sType, counter);
	}
	
	private void handleDuccServletReservationProcessesData(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletReservationProcessesData";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		String reservationNo = request.getParameter("id");
		
		IDuccWorkMap duccWorkMap = DuccData.getInstance().get();
		DuccWorkJob managedReservation = null;
		if(duccWorkMap.getServiceKeySet().size()> 0) {
			Iterator<DuccId> iterator = null;
			iterator = duccWorkMap.getServiceKeySet().iterator();
			while(iterator.hasNext()) {
				DuccId jobId = iterator.next();
				String fid = ""+jobId.getFriendly();
				if(reservationNo.equals(fid)) {
					managedReservation = (DuccWorkJob) duccWorkMap.findDuccWork(jobId);
					break;
				}
			}
		}
		if(managedReservation != null) {
			Iterator<DuccId> iterator = null;
			int counter = 0;
			iterator = managedReservation.getProcessMap().keySet().iterator();
			while(iterator.hasNext()) {
				DuccId processId = iterator.next();
				IDuccProcess process = managedReservation.getProcessMap().get(processId);
				buildServiceProcessListEntry(sb, managedReservation, process, DetailsType.Reservation, ShareType.MR, ++counter);
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
	
	private void handleDuccServletReservationSpecificationData(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletReservationSpecificationData";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		String reservationNo = request.getParameter("id");
		
		DuccWorkJob managedReservation = getManagedReservation(reservationNo);
		if(managedReservation != null) {
			try {
				String userId = duccWebSessionManager.getUserId(request);
				Properties usProperties = DuccFile.getUserSpecifiedProperties(managedReservation, userId);
				Properties fsProperties = DuccFile.getFileSpecifiedProperties(managedReservation, userId);
				Properties properties = DuccFile.getManagedReservationProperties(managedReservation, userId);
				TreeMap<String,String> map = new TreeMap<String,String>();
				Enumeration<?> enumeration = properties.keys();
				while(enumeration.hasMoreElements()) {
					String key = (String)enumeration.nextElement();
					map.put(key, key);
				}
				Iterator<String> iterator = map.keySet().iterator();
				sb.append("<table id=\"specification_table\" class=\"sortable\">");
				sb.append("<tr class=\"ducc-head\">");
				if(isProvided(usProperties, fsProperties)) {
					sb.append("<th title=\"system provided if blank\">");
					sb.append(headProvider);
				}
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
						value = formatClasspath(value);
						String show = "<div class=\"hidedata\"><input type=\"submit\" name=\"showcp\" value=\"Show\" id=\"showbutton"+i+"\"/></div>";
						String hide = "<div class=\"showdata\"><input type=\"submit\" name=\"hidecp\" value=\"Hide\" id=\"hidebutton"+i+"\"/>"+" "+value+"</div>";
						value = show+hide;
						i++;
					}
					String provider = getProvider(key, usProperties, fsProperties);
					putJobSpecEntry(properties, provider, key, value, sb, counter++);
				}
				sb.append("</table>");
				sb.append("<br>");
				sb.append("<br>");
			}
			catch(Throwable t) {
				duccLogger.warn(methodName, null, t);
				sb = new StringBuffer();
				sb.append("no data");
			}
		}
		
		response.getWriter().println(sb);
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}
	
	private void handleDuccServletServiceDeploymentsData(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletServiceDeploymentsData";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		
		try {
			String name = request.getParameter("name");
			ServicesRegistry servicesRegistry = ServicesRegistry.getInstance();
			ServicesRegistryMapPayload payload = servicesRegistry.findService(name);
			Properties properties;
			properties = payload.meta;
			
            // UIMA-4258, use common implementors parser
			ArrayList<String> implementors = DuccDataHelper.parseServiceIdsAsList(properties);
			
			IDuccWorkMap duccWorkMap = DuccData.getInstance().get();
			List<DuccWorkJob> servicesList = duccWorkMap.getServices(implementors);
			int counter = 0;
			ShareType type = ShareType.SPU;
			String service_type = properties.getProperty(IServicesRegistry.service_type);
			if(service_type != null) {
				if(service_type.equalsIgnoreCase(IServicesRegistry.service_type_CUSTOM)) {
					type = ShareType.SPC;
				}
			}
			for(DuccWorkJob service : servicesList) {
				IDuccProcessMap map = service.getProcessMap();
				if(map.isEmpty()) {
					buildServiceProcessListEntry(sb, service, null, DetailsType.Service, type, ++counter);
				}
				else {
					for(DuccId key : map.keySet()) {
						IDuccProcess process = map.get(key);
						buildServiceProcessListEntry(sb, service, process, DetailsType.Service, type, ++counter);
					}
				}
			}
		}
		catch(Throwable t) {
			duccLogger.trace(methodName, jobid, t);
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
	
	private void handleDuccServletServiceRegistryData(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletServiceRegistryData";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		try {
			String name = request.getParameter("name");
			ServicesRegistry servicesRegistry = ServicesRegistry.getInstance();
			ServicesRegistryMapPayload payload = servicesRegistry.findService(name);
			String hint = getLoginRefreshHint(request, response);
			String enable_or_disable = getEnabledOrDisabled(request, response);;
			sb.append("<table>");
			sb.append("<tr class=\"ducc-head\">");
			sb.append("<th>");
			sb.append("Key");
			sb.append("</th>");
			sb.append("<th>");
			sb.append("Value");
			sb.append("</th>");
			sb.append("</tr>");
			Properties properties;
			if(payload != null) {
				properties = payload.meta;
				String resourceOwnerUserId = properties.getProperty(IServicesRegistry.user).trim();
				if(!HandlersHelper.isUserAuthorized(request,resourceOwnerUserId)) {
					if(hint.length() == 0) {
						HandlersHelper.AuthorizationStatus authorizationStatus = HandlersHelper.getAuthorizationStatus(request, resourceOwnerUserId);
						switch(authorizationStatus) {
						case LoggedInOwner:
						case LoggedInAdministrator:
							break;
						case LoggedInNotOwner:
						case LoggedInNotAdministrator:
							enable_or_disable = "disabled=\"disabled\"";
							String userid = DuccWebSessionManager.getInstance().getUserId(request);
							boolean administrator = DuccWebAdministrators.getInstance().isAdministrator(userid);
							if(administrator) {
								hint = "title=\""+DuccConstants.hintPreferencesRoleAdministrator+"\"";
							}
							else {
								hint = "title=\""+DuccConstants.hintNotAuthorized+"\"";
							}
							break;
						case NotLoggedIn:
							break;
						default:
							break;
						}
					}
				}
				String prefix;
				TreeMap<String,String> map;
				Enumeration<?> enumeration;
				Iterator<String> iterator;
				int i = 0;
				int counter = 0;
				//
				prefix = "svc.";
				properties = payload.svc;
				map = new TreeMap<String,String>();
				enumeration = properties.keys();
				while(enumeration.hasMoreElements()) {
					String key = (String)enumeration.nextElement();
					map.put(key, key);
				}
				iterator = map.keySet().iterator();
				while(iterator.hasNext()) {
					String key = iterator.next();
					String value = properties.getProperty(key);
					if(key.endsWith("classpath")) {
						value = formatClasspath(value);
						String show = "<div class=\"hidedata\"><input type=\"submit\" name=\"showcp\" value=\"Show\" id=\"showbutton"+i+"\"/></div>";
						String hide = "<div class=\"showdata\"><input type=\"submit\" name=\"hidecp\" value=\"Hide\" id=\"hidebutton"+i+"\"/>"+" "+value+"</div>";
						value = show+hide;
						i++;
					}
					putJobSpecEntry(properties, prefix+key, value, sb, counter++);
				}
				//
				prefix = "meta.";
				properties = payload.meta;
				map = new TreeMap<String,String>();
				enumeration = properties.keys();
				while(enumeration.hasMoreElements()) {
					String key = (String)enumeration.nextElement();
					map.put(key, key);
				}
				iterator = map.keySet().iterator();
				while(iterator.hasNext()) {
					String key = iterator.next();
					String value = properties.getProperty(key);
					if(key.endsWith("classpath")) {
						value = formatClasspath(value);
						String show = "<div class=\"hidedata\"><input type=\"submit\" name=\"showcp\" value=\"Show\" id=\"showbutton"+i+"\"/></div>";
						String hide = "<div class=\"showdata\"><input type=\"submit\" name=\"hidecp\" value=\"Hide\" id=\"hidebutton"+i+"\"/>"+" "+value+"</div>";
						value = show+hide;
						i++;
					}
					key = key.trim();
					// autostart
					if(key.equalsIgnoreCase(IServicesRegistry.autostart)) {
						if(value != null) {
							value = value.trim();
							if(value.equalsIgnoreCase(IServicesRegistry.constant_true)) {
								StringBuffer replacement = new StringBuffer();
								replacement.append("<select id=\"autostart\""+enable_or_disable+" "+hint+">");
								replacement.append("<option value=\"true\"  selected=\"selected\">true</option>");
								replacement.append("<option value=\"false\"                      >false</option>");
								replacement.append("</select>");
								value = replacement.toString();
							}
							else if(value.equalsIgnoreCase(IServicesRegistry.constant_false)) {
								StringBuffer replacement = new StringBuffer();
								replacement.append("<select id=\"autostart\""+enable_or_disable+" "+hint+">");
								replacement.append("<option value=\"false\"  selected=\"selected\">false</option>");
								replacement.append("<option value=\"true\"                        >true</option>");
								replacement.append("</select>");
								value = replacement.toString();
							}
						}
					}
					// instances
					if(key.equalsIgnoreCase(IServicesRegistry.instances)) {
						if(value != null) {
							value = value.trim();
							StringBuffer replacement = new StringBuffer();
							replacement.append("<span id=\"instances_area\">");
							replacement.append("<input type=\"text\" size=\"5\" id=\"instances\" value=\""+value+"\""+enable_or_disable+" "+hint+">");
							replacement.append("</span>");
							value = replacement.toString();
						}
					}
					putJobSpecEntry(properties, prefix+key, value, sb, counter++);
				}
				sb.append("</table>");
				sb.append("<br>");
				sb.append("<br>");
			}
			else {
				sb.append("<tr>");
				sb.append("<td>");
				sb.append("not found");
				sb.append("</td>");
				sb.append("<td>");
				sb.append("</td>");
				sb.append("</tr>");
			}
		}
		catch(Exception e) {
			duccLogger.warn(methodName, null, e);
			sb = new StringBuffer();
			sb.append("no data");
		}

		response.getWriter().println(sb);
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}

	private void handleDuccServletServiceSummaryData(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletServiceSummaryData";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		
		sb.append("<table>");
		sb.append("<tr>");
		
		String id = "?";
		String name = request.getParameter("name");
		String instances = "?";
		String deployments = "?";
		StartState startState = StartState.Unknown;
		boolean disabled = false;
		String disableReason = "";
		
		try {
			ServicesRegistry servicesRegistry = ServicesRegistry.getInstance();
			ServicesRegistryMapPayload payload = servicesRegistry.findService(name);
			if(payload != null) {
				Properties meta = payload.meta;
				Properties svc = payload.svc;
				ServiceInterpreter si = new ServiceInterpreter(svc, meta);
				id = ""+si.getId();
				instances = ""+si.getInstances();
				deployments = ""+si.getDeployments();
				startState = si.getStartState();
				disabled = si.isDisabled();
				disableReason = si.getDisableReason();
			}
		}
		catch(Exception e) {
			duccLogger.error(methodName, jobid, e);
		}
		
		// serviceid
		sb.append("<th title=\"The system assigned id for this service\">");
		sb.append("Id: ");
		sb.append(id);
		sb.append("&nbsp");
		// name
		sb.append("<th title=\"The name for this service\">");
		sb.append("Name: ");
		sb.append(name);
		sb.append("&nbsp");
		// instances
		sb.append("<th title=\"The configured number of instances for this service\">");
		sb.append("Instances: ");
		sb.append(instances);
		sb.append("&nbsp");
		// deployments
		sb.append("<th title=\"The number of active deployments for this service\">");
		sb.append("Deployments: ");
		sb.append(deployments);
		sb.append("&nbsp");
		// start-mode
		sb.append("<th>");
		sb.append("StartState: ");
		sb.append(startState.name());
		sb.append("&nbsp");
		// disabled
		if(disabled) {
			sb.append("<th title=\""+disableReason+"\">");
			sb.append("StartControl: ");
			sb.append("disabled");
			sb.append("&nbsp");
		}
		
		sb.append("</table>");
		
		response.getWriter().println(sb);
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}
	
	private void handleDuccServletBrokerSummaryData(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletBrokerSummaryData";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		
		int MB = 1024 * 1024;
		
		String brokerHost = "?";
		String brokerPort = "?";
		
		String brokerVersion = "?";
		String uptime = "?";
		
		Long memoryUsed = new Long(0);
		Long memoryMax = new Long(0);
		
		int threadsLive = 0;
		int threadsPeak = 0;
		
		double systemLoadAverage = 0;
		
		try {
			BrokerHelper brokerHelper = BrokerHelper.getInstance();
			systemLoadAverage = brokerHelper.getSystemLoadAverage();
			threadsLive = brokerHelper.getThreadsLive();
			threadsPeak = brokerHelper.getThreadsPeak();
			memoryMax = brokerHelper.getMemoryMax();
			memoryUsed = brokerHelper.getMemoryUsed();
			uptime = brokerHelper.getAttribute(BrokerAttribute.Uptime);
			brokerVersion = brokerHelper.getAttribute(BrokerAttribute.BrokerVersion);
			brokerPort = ""+brokerHelper.getPort();
			brokerHost = brokerHelper.getHost();
		}
		catch(Exception e) {
			duccLogger.error(methodName, jobid, e);
		}
		
		sb.append("<table>");
		
		//
		
		StringBuffer row1 = new StringBuffer();
		StringBuffer row2 = new StringBuffer();
		StringBuffer row3 = new StringBuffer();
		
		row1.append("<tr>");
		row2.append("<tr>");
		row3.append("<tr>");
		
		String thl = "<th align=\"left\"  style=\"font-family: monospace;\">";
		String thr = "<th align=\"right\" style=\"font-family: monospace;\">";
		
		// Host
		row1.append(thl);
		row1.append("Host: ");
		row1.append(thl);
		row1.append(brokerHost);
		row1.append("&nbsp");
		// Post
		row2.append(thl);
		row2.append("Port: ");
		row2.append(thl);
		row2.append(brokerPort);
		row2.append("&nbsp");
		// 
		row3.append(thl);
		row3.append("");
		row3.append(thl);
		row3.append("");
		row3.append("&nbsp");
		
		// BrokerVersion
		row1.append(thl);
		row1.append("BrokerVersion: ");
		row1.append(thl);
		row1.append(brokerVersion);
		row1.append("&nbsp");
		// Uptime
		row2.append(thl);
		row2.append("Uptime: ");
		row2.append(thl);
		row2.append(uptime);
		row2.append("&nbsp");
		// 
		row3.append(thl);
		row3.append("");
		row3.append(thl);
		row3.append("");
		row3.append("&nbsp");
		
		// MemoryUsed
		row1.append(thl);
		row1.append("MemoryUsed(MB): ");
		row1.append(thr);
		row1.append(memoryUsed/MB);
		row1.append("&nbsp");
		// MemoryMax
		row2.append(thl);
		row2.append("MemoryMax(MB): ");
		row2.append(thr);
		row2.append(memoryMax/MB);
		row2.append("&nbsp");
		// 
		row3.append(thl);
		row3.append("");
		row3.append(thl);
		row3.append("");
		row3.append("&nbsp");
				
		// ThreadsLive
		row1.append(thl);
		row1.append("ThreadsLive: ");
		row1.append(thr);
		row1.append(threadsLive);
		row1.append("&nbsp");
		// ThreadsPeak
		row2.append(thl);
		row2.append("ThreadsPeak: ");
		row2.append(thr);
		row2.append(threadsPeak);
		row2.append("&nbsp");
		// 
		row3.append(thl);
		row3.append("");
		row3.append(thl);
		row3.append("");
		row3.append("&nbsp");
		
		// System Load Average
		row1.append(thl);
		row1.append("SystemLoadAverage: ");
		row1.append(thr);
		row1.append(systemLoadAverage);
		row1.append("&nbsp");
		//
		row2.append(thl);
		row2.append("");
		row2.append(thr);
		row2.append("");
		row2.append("&nbsp");
		// 
		row3.append(thl);
		row3.append("");
		row3.append(thl);
		row3.append("");
		row3.append("&nbsp");	
		
		//
		
		sb.append(row1);
		sb.append(row2);
		sb.append(row3);
		
		sb.append("</table>");
		
		response.getWriter().println(sb);
		duccLogger.trace(methodName, null, messages.fetch("exit"));
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
		long swapInuse = 0;
		long swapFree = 0;
		long alienPids = 0;
		long sharesTotal = 0;
		long sharesInuse = 0;
		while(iterator.hasNext()) {
			MachineInfo machineInfo = iterator.next();
			try {
				memTotal += Long.parseLong(machineInfo.getMemTotal());
			}
			catch(Exception e) {};
			try {swapInuse += Long.parseLong(machineInfo.getSwapInuse());
			}
			catch(Exception e) {};
			try {swapFree += Long.parseLong(machineInfo.getSwapFree());
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
		sb.append(quote(""+swapInuse));
		sb.append(",");
		sb.append(quote(""+swapFree));
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
			sb.append(quote(machineInfo.getSwapInuse()));
			sb.append(",");
			sb.append(quote(machineInfo.getSwapFree()));
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
		boolean authorized = HandlersHelper.isUserAuthorized(request,null);
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
		boolean authorized = HandlersHelper.isUserAuthorized(request,null);
		if(authorized) {
			String userId = duccWebSessionManager.getUserId(request);
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
	
    /**
     * @Deprecated
     */
	private void handleServletJsonSystemClassesData(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws Exception
	{
		String methodName = "handleJsonServletSystemClassesData";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		sb.append("{ ");
		sb.append("\"aaData\": [ ");
		
		DuccSchedulerClasses schedulerClasses = new DuccSchedulerClasses();
        Map<String, DuccProperties> clmap = schedulerClasses.getClasses();

		boolean first = true;
		if( clmap != null ) {
            DuccProperties[] class_set = clmap.values().toArray(new DuccProperties[clmap.size()]);
            Arrays.sort(class_set, new NodeConfiguration.ClassSorter());

			for( DuccProperties cl : class_set ) {
				String class_name = cl.getProperty("name");
				if(first) {
					first = false;
				}
				else {
					sb.append(",");
				}
				sb.append("[");
				sb.append(quote(class_name));
				sb.append(",");

                String policy = cl.getProperty("policy");
				sb.append(quote(policy));
				sb.append(",");
				sb.append(quote(cl.getStringProperty("weight", "-")));
				sb.append(",");
				sb.append(quote(cl.getProperty("priority")));
				// cap is either absolute or proportional.  if proprotional, it ends with '%'.  It's always
                // either-or so at least one of these columns will have N/A
				String val = cl.getStringProperty("cap", "0");
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

                if ( policy.equals("FAIR_SHARE") ) {
                    val = cl.getStringProperty("initialization-cap", System.getProperty("ducc.rm.initialization.cap"));
                    if ( val == null ) {
                        val = "2";
                    }
                    sb.append(",");
                    sb.append(quote(val));
                    boolean bval = cl.getBooleanProperty("expand-by-doubling", true);
                    Boolean b = new Boolean(bval);
                    sb.append(",");
                    sb.append(quote(b.toString()));
                    val = cl.getStringProperty("use-prediction", System.getProperty("ducc.rm.prediction"));
                    if ( val == null ) {
                        val = "true";
                    }
                    sb.append(",");
                    sb.append(quote(val));
                    val = cl.getStringProperty("prediction-fudge", System.getProperty("ducc.rm.prediction.fudge"));
                    if ( val == null ) {
                        val = "10000";
                    }
                    sb.append(",");
                    sb.append(quote(val));
                } else {
                    sb.append(",-,-,-,-"); 
                }

                // max for reserve in in machines.  For fixed is in processes.  No max on fair-share. So slightly
                // ugly code here.
                if ( policy.equals("RESERVE") ) {
                    val = cl.getProperty("max-machines");
                    if( val == null || val.equals("0")) {
                        val = "-";
                    }
                } else if ( policy.equals("FIXED_SHARE") ) {
                    val = cl.getStringProperty("max-properties");
                    if( val == null || val.equals("0")) {
                        val = "-";
                    }
                } else {
					val = "-";
                }

				sb.append(",");
				sb.append(quote(val));
				val = cl.getStringProperty("nodepool");
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
			sb.append(quote(getTimeStamp(DuccCookies.getDateStyle(request),getPropertiesValue(properties,DuccDaemonRuntimeProperties.keyBootTime,""))));
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
			try {
				heartmaxTOD = getTimeStamp(DuccCookies.getDateStyle(request),heartmaxTOD);
			}
			catch(Exception e) {
			}
			sb.append(quote(heartmaxTOD));
			sb.append(",");
			String jmxUrl = getPropertiesValue(properties,DuccDaemonRuntimeProperties.keyJmxUrl,"");
			if(jmxUrl != null) {
				sb.append(quote(buildjSonjConsoleLink(jmxUrl)));
			}
			sb.append("]");
		}
		// <Agents>
		String cookie = DuccCookies.getCookie(request,DuccCookies.cookieAgents);
		if(cookie.equals(DuccCookies.valueAgentsShow)) {
			duccLogger.trace(methodName, jobid, "== show: "+cookie);
			ConcurrentSkipListMap<String,MachineInfo> machines = DuccMachinesData.getInstance().getMachines();
			Iterator<String> iterator = machines.keySet().iterator();
			while(iterator.hasNext()) {
				String key = iterator.next();
				MachineInfo machineInfo = machines.get(key);
				Properties properties = DuccDaemonRuntimeProperties.getInstance().getAgent(machineInfo.getName());
				if(first) {
					first = false;
				}
				else {
					sb.append(",");
				}
				sb.append("[");
				// Status
				String status;
				String machineStatus = machineInfo.getStatus();
				if(machineStatus.equals("down")) {
					//status.append("<span class=\"health_red\""+">");
					status = machineStatus;
					//status.append("</span>");
				}
				else if(machineStatus.equals("up")) {
					//status.append("<span class=\"health_green\""+">");
					status = machineStatus;
					//status.append("</span>");
				}
				else {
					status = "unknown";
				}
				sb.append(quote(status));
				// Daemon Name
				sb.append(",");
				String daemonName = "Agent";
				sb.append(quote(daemonName));
				// Boot Time
				sb.append(",");
				String bootTime = getTimeStamp(DuccCookies.getDateStyle(request),getPropertiesValue(properties,DuccDaemonRuntimeProperties.keyBootTime,""));
				sb.append(quote(bootTime));
				// Host IP
				sb.append(",");
				String hostIP = getPropertiesValue(properties,DuccDaemonRuntimeProperties.keyNodeIpAddress,"");
				sb.append(quote(hostIP));
				// Host Name
				sb.append(",");
				String hostName = machineInfo.getName();
				sb.append(quote(hostName));
				// PID
				sb.append(",");
				String pid = getPropertiesValue(properties,DuccDaemonRuntimeProperties.keyPid,"");
				sb.append(quote(pid));
				// Publication Size (last)
				sb.append(",");
				String publicationSizeLast = machineInfo.getPublicationSizeLast();
				sb.append(quote(publicationSizeLast));
				// Publication Size (max)
				sb.append(",");
				String publicationSizeMax = machineInfo.getPublicationSizeMax();
				sb.append(quote(publicationSizeMax));
				// Heartbeat (last)
				sb.append(",");
				String heartbeatLast = machineInfo.getHeartbeatLast();
				sb.append(quote(heartbeatLast));
				// Heartbeat (max)
				sb.append(",");
				String fmtHeartbeatMax = "";
				long heartbeatMax = machineInfo.getHeartbeatMax();
				if(heartbeatMax > 0) {
					fmtHeartbeatMax += heartbeatMax;
				}
				sb.append(quote(fmtHeartbeatMax));
				// Heartbeat (max) TOD
				sb.append(",");
				String fmtHeartbeatMaxTOD = "";
				long heartbeatMaxTOD = machineInfo.getHeartbeatMaxTOD();
				if(heartbeatMaxTOD > 0) {
					fmtHeartbeatMaxTOD = TimeStamp.simpleFormat(""+heartbeatMaxTOD);
					try {
						fmtHeartbeatMaxTOD = getTimeStamp(DuccCookies.getDateStyle(request),fmtHeartbeatMaxTOD);
					}
					catch(Exception e) {
					}
				}
				sb.append(quote(fmtHeartbeatMaxTOD));
				// JConsole URL
				sb.append(",");
				String fmtJmxUrl = "";
				String jmxUrl = getPropertiesValue(properties,DuccDaemonRuntimeProperties.keyJmxUrl,"");
				if(jmxUrl != null) {
					fmtJmxUrl = buildjSonjConsoleLink(jmxUrl);
				}
				sb.append(quote(fmtJmxUrl));
				sb.append("]");
			}
		}
		else {
			duccLogger.trace(methodName, jobid, "!= show: "+cookie);
		}
		// </Agents>
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
		IDuccWorkMap duccWorkMap = DuccData.getInstance().get();
		if(duccWorkMap.size()> 0) {
			sb.append("<span title=\"home="+dir_home+"\">");
			sb.append(getDuccWebServer().getClusterName());
			sb.append("</span>");
		}
		response.getWriter().println(sb);
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}	
	
	private void handleDuccServletClusterUtilization(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletClusterUtilization";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		
		MachineSummaryInfo msi = DuccMachinesData.getInstance().getTotals();
		
		DecimalFormat percentageFormatter = new DecimalFormat("##0.0");
		
		String utilization = "0%";
		if(msi.sharesTotal > 0) {
			double percentage = (((1.0) * msi.sharesInuse) / ((1.0) * msi.sharesTotal)) * 100.0;
			utilization = percentageFormatter.format(percentage)+"%";
		}
		
		sb.append(utilization);
		response.getWriter().println(sb);
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}	
	
	private void handleDuccServletTimeStamp(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletTimeStamp";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		DuccId jobid = null;
		StringBuffer sb = new StringBuffer(getTimeStamp(request,jobid,DuccData.getInstance().getPublished()));
		response.getWriter().println(sb);
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}	
	
	private void handleDuccServletReservationSchedulingClasses(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws Exception
	{
		String methodName = "handleDuccServletReservationSchedulingClasses";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		sb.append("<select id=\"scheduling_class\">");
		DuccSchedulerClasses schedulerClasses = new DuccSchedulerClasses();
		String[] class_array = schedulerClasses.getReserveClasses();
		String defaultName = schedulerClasses.getReserveClassDefaultName();
		for(int i=0; i<class_array.length; i++) {
			String name = class_array[i];
			if(name.equals(defaultName)) {
				sb.append("<option value=\""+name+"\" selected=\"selected\">"+name+"</option>");
			}
			else {
				sb.append("<option value=\""+name+"\">"+name+"</option>");
			}
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
		List<Long> machineSizes = DuccMachinesData.getInstance().getMachineSizes();
		for(Long machineSize : machineSizes) {
			sb.append("<option value=\""+machineSize+"\">");
		}
		response.getWriter().println(sb);
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}	
	
	private void handleDuccServletReservationInstanceMemoryUnits(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletReservationInstanceMemoryUnits";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		sb.append("<select id=\"memory_units\">");
		sb.append("<option value=\"GB\" selected=\"selected\">GB</option>");
		sb.append("</select>");
		response.getWriter().println(sb);
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}	
	
	private void handleDuccServletReservationFormButton(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletReservationFormButton";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		String button = "<button style=\"font-size:8pt; background-color:green; color:ffffff;\" onclick=\"var newWin = window.open('submit.reservation.html','child','height=550,width=550,scrollbars'); newWin.focus(); return false;\">Request<br>Reservation</button>";
		String value = DuccPropertiesResolver.getInstance().getProperty(DuccPropertiesResolver.ducc_orchestrator_unmanaged_reservations_accepted);
		Boolean result = new Boolean(value);
		if(!result) {
			button = "<button title=\"System is configured to disallow reservations\" style=\"font-size:8pt;\" disabled>Request<br>Reservation</button>";
		}
		else if(!isAuthenticated(request,response)) {
			button = "<button title=\"Login to enable\" style=\"font-size:8pt;\" disabled>Request<br>Reservation</button>";
		}
		sb.append(button);
		response.getWriter().println(sb);
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}
	
	private String getLoginRefreshHint(HttpServletRequest request,HttpServletResponse response) {
		String retVal = "";
		DuccCookies.RefreshMode refreshMode = DuccCookies.getRefreshMode(request);
		if(!isAuthenticated(request,response)) {
			switch(refreshMode) {
			default:
			case Automatic:
				retVal = "title=\""+DuccConstants.hintLoginAndManual+"\"";
				break;
			case Manual:
				retVal = "title=\""+DuccConstants.hintLogin+"\"";
				break;
			}
		}
		else {
			switch(refreshMode) {
			default:
			case Automatic:
				retVal = "title=\""+DuccConstants.hintManual+"\"";
				break;
			case Manual:
				break;
			}
		}
		return retVal;
	}
	
	private String getEnabledOrDisabled(HttpServletRequest request,HttpServletResponse response) {
		String retVal = "";
		DuccCookies.RefreshMode refreshMode = DuccCookies.getRefreshMode(request);
		if(!isAuthenticated(request,response)) {
			switch(refreshMode) {
			default:
			case Automatic:
				retVal = "disabled=\"disabled\"";
				break;
			case Manual:
				retVal = "disabled=\"disabled\"";
				break;
			}
		}
		else {
			switch(refreshMode) {
			default:
			case Automatic:
				retVal = "disabled=\"disabled\"";
				break;
			case Manual:
				break;
			}
		}
		return retVal;
	}
	
	private void handleDuccServletServiceUpdateFormButton(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletServiceUpdateFormButton";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		String name = request.getParameter("name");
		StringBuffer sb = new StringBuffer();
		String hint = getLoginRefreshHint(request, response);
		String enable_or_disable = getEnabledOrDisabled(request, response);
		String button = "<button id=\"update_button\" "+hint+" onclick=\"ducc_update_service('"+name+"')\" style=\"font-size:8pt; background-color:green; color:ffffff;\">Update</button>";
		if(enable_or_disable.length() > 0) {
			button = "<button id=\"update_button\" "+enable_or_disable+" "+hint+" style=\"font-size:8pt;\">Update</button>";
		}
		sb.append(button);
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
		String button = "<input id=\"submit_button\" type=\"button\" onclick=\"ducc_submit_reservation()\" value=\"Submit\" "+disabled+"/>";
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
	
	private void handleDuccServletFileContents(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletFileContents";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		String fname = request.getParameter("fname");
		String page = request.getParameter("page");
		StringBuffer sb = new StringBuffer();
		String userId = duccWebSessionManager.getUserId(request);	
		String newline = "\n";
		String colon = ":";
		try {
			String user = userId;
			String file_name = fname;
			AlienTextFile atf = new AlienTextFile(user, file_name);
			int pageCount = atf.getPageCount();
			int pageNo = 0;
			try {
				pageNo = Integer.parseInt(page);
			}
			catch(Exception e) {
			}
			if(pageNo == 0) {
				pageNo = pageCount;
			}
			pageNo = pageNo - 1;
			if(pageNo < 0) {
				pageNo = 0;
			}
			String prepend = "";
			String chunk = atf.getPage(pageNo);
			String postpend = "";
			if(pageNo > 0) {
				String previous = atf.getPage(pageNo-1);
				if(previous.contains(newline)) {
					String[] lines = previous.split(newline);
					int index = lines.length - 1;
					prepend = lines[index];
				}
				else if(previous.contains(colon)) {
					String[] lines = previous.split(colon);
					int index = lines.length - 1;
					prepend = lines[index];
				}
			}
			if(pageNo < (pageCount - 1)) {
				String next = atf.getPage(pageNo+1);
				if(next.contains(newline)) {
					String[] lines = next.split(newline);
					int index = 0;
					postpend = lines[index];
				}
				if(next.contains(colon)) {
					String[] lines = next.split(colon);
					int index = 0;
					postpend = lines[index];
				}
			}
			String aggregate = prepend + chunk + postpend;

			/*
			if(fname.endsWith(".xml")) {
				aggregate = aggregate.replace("<", "&lt");
				aggregate = aggregate.replace(">", "&gt");
			}
			*/
			
			//if(!aggregate.trim().contains("\n")) {
			//	if(aggregate.trim().contains(":")) {
			//		String[] lines = aggregate.trim().split(":");
			//		aggregate = "";
			//		for(String line : lines) {
			//			aggregate += line+"\n";
			//		}
			//	}
			//}
			sb.append("<pre>");
			sb.append(aggregate);
			sb.append("</pre>");
		}
		catch(Exception e) {
			duccLogger.error(methodName, jobid, e);
			sb = new StringBuffer();
			sb.append("Error processing file");
		}
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
		InputStreamReader isr = null;
		BufferedReader br = null;
		try {
			String userId = duccWebSessionManager.getUserId(request);
			isr = DuccFile.getInputStreamReader(fname, userId);
			br = new BufferedReader(isr);
			String logLine;
			while ((logLine = br.readLine()) != null)   {
					sb.append(logLine+"<br>");
			}
		}
		catch(FileNotFoundException e) {
			sb.append("File not found");
		}
		catch(Throwable t) {
			sb.append("Error accessing file");
		}
		finally {
			try {
				br.close();
			}
			catch(Throwable t) {
			}
			try {
				isr.close();
			}
			catch(Throwable t) {
			}
		}
		sb.append("</body>");
		sb.append("</html>");
		response.getWriter().println(sb);
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}	
	
	private void handleDuccServletJpInitSummary(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletJpInitSummary";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		String idJob = request.getParameter(pname_idJob);
		String idPro = request.getParameter(pname_idPro);
		StringBuffer sb = new StringBuffer();
		
		sb.append("<b>");
		sb.append("Id[job]:");
		sb.append(" ");
		sb.append(idJob);
		sb.append(" ");
		sb.append("Id[process]:");
		sb.append(" ");
		sb.append(idPro);
		sb.append("</b>");
		
		response.getWriter().println(sb);
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}	
	
	private void handleDuccServletJpInitData(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletJpInitData";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		String idJob = request.getParameter(pname_idJob);
		String idPro = request.getParameter(pname_idPro);
		StringBuffer sb = new StringBuffer();
		
		IDuccWorkMap duccWorkMap = DuccData.getInstance().get();
		DuccWorkJob job = null;
		if(duccWorkMap.getJobKeySet().size()> 0) {
			Iterator<DuccId> iterator = null;
			iterator = duccWorkMap.getJobKeySet().iterator();
			while(iterator.hasNext()) {
				DuccId jobId = iterator.next();
				String fid = ""+jobId.getFriendly();
				if(idJob.equals(fid)) {
					job = (DuccWorkJob) duccWorkMap.findDuccWork(jobId);
					break;
				}
			}
		}
		if(job != null) {
			IDuccProcess process = job.getProcess(idPro);
			if(process != null) {
				List<IUimaPipelineAEComponent> upcList = process.getUimaPipelineComponents();
				if(upcList != null) {
					if(!upcList.isEmpty()) {
						Iterator<IUimaPipelineAEComponent> upcIterator = upcList.iterator();
						while(upcIterator.hasNext()) {
							IUimaPipelineAEComponent upc = upcIterator.next();
							String iName = upc.getAeName();
							String iState = upc.getAeState().toString();
							String iTime = FormatHelper.duration(upc.getInitializationTime(),Precision.Whole);
							sb.append("<tr>");
							sb.append("<td>"+iName);
							sb.append("<td>"+iState);
							sb.append("<td align=\"right\">"+iTime);
						}
					}
				}
			}
		}
		if(sb.length() == 0) {
			sb.append("<tr>");
			sb.append("<td>"+"no data");
			sb.append("<td>");
			sb.append("<td>");
		}
		
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
		sb.append("    <jar href=\"jconsole-wrapper-signed.jar\" main=\"true\"/>");
		sb.append("  </resources>");
		sb.append("  <application-desc main-class=\"org.apache.uima.ducc.ws.jconsole.JConsoleWrapper\">");
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
			IDuccWorkMap duccWorkMap = duccData.get();
			String text;
			String result;
			IDuccWorkJob duccWorkJob = (IDuccWorkJob) duccWorkMap.findDuccWork(DuccType.Job, value);
			if(duccWorkJob != null) {
				String resourceOwnerUserId = duccWorkJob.getStandardInfo().getUser().trim();
				if(HandlersHelper.isUserAuthorized(request,resourceOwnerUserId)) {
					String arg1 = "-"+name;
					String arg2 = value;
					String arg3 = "--"+SpecificationProperties.key_reason;
					String reason = CancelReason.TerminateButtonPressed.getText();
			   		String arg4 = "\""+reason+"\"";
					String userId = duccWebSessionManager.getUserId(request);
					String cp = System.getProperty("java.class.path");
					String java = "/bin/java";
					String jclass = "org.apache.uima.ducc.cli.DuccJobCancel";
					String jhome = System.getProperty("java.home");
					DuccCookies.RequestRole requestRole = DuccCookies.getRole(request);
					switch(requestRole) {
					case Administrator:
						String arg5 = "--"+SpecificationProperties.key_role_administrator;
						String[] arglistAdministrator = { "-u", userId, "--", jhome+java, "-cp", cp, jclass, arg1, arg2, arg3, arg4, arg5 };
						result = DuccAsUser.duckling(userId, arglistAdministrator);
						response.getWriter().println(result);
						break;
					case User:
					default:
						String[] arglistUser = { "-u", userId, "--", jhome+java, "-cp", cp, jclass, arg1, arg2, arg3, arg4 };
						result = DuccAsUser.duckling(userId, arglistUser);
						response.getWriter().println(result);
						break;	
					}
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
			String memory_size = request.getParameter("memory_size");
			duccLogger.debug(methodName, null, "memory_size:"+memory_size);
			String memory_units = request.getParameter("memory_units");
			duccLogger.debug(methodName, null, "memory_units:"+memory_units);
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
			if(memory_size != null) {
				arg3 = "--memory_size";
				if(memory_units != null) {
					arg4 = memory_size+memory_units;
				}
				else {
					arg4 = memory_size;
				}
			}
			String arg7 = "";
			String arg8 = "";
			if(description != null) {
				arg7 = "--description";
				arg8 = description;
			}
			try {
				String userId = duccWebSessionManager.getUserId(request);
				String cp = System.getProperty("java.class.path");
				String java = "/bin/java";
				String jclass = "org.apache.uima.ducc.cli.DuccReservationSubmit";
				String jhome = System.getProperty("java.home");
				String[] arglist = { "-u", userId, "--", jhome+java, "-cp", cp, jclass, arg1, arg2, arg3, arg4, /*arg5, arg6,*/ arg7, arg8 };
				String result = DuccAsUser.duckling(userId, arglist);
				response.getWriter().println(result);
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
			IDuccWorkMap duccWorkMap = duccData.get();
			String text;
			String result;
			IDuccWorkReservation duccWorkReservation = (IDuccWorkReservation) duccWorkMap.findDuccWork(DuccType.Reservation, value);
			if(duccWorkReservation != null) {
				String resourceOwnerUserId = duccWorkReservation.getStandardInfo().getUser().trim();
				if(HandlersHelper.isUserAuthorized(request,resourceOwnerUserId)) {
					String arg1 = "-"+name;
					String arg2 = value;
					String userId = duccWebSessionManager.getUserId(request);
					String cp = System.getProperty("java.class.path");
					String java = "/bin/java";
					String jclass = "org.apache.uima.ducc.cli.DuccReservationCancel";
					String jhome = System.getProperty("java.home");
					DuccCookies.RequestRole requestRole = DuccCookies.getRole(request);
					switch(requestRole) {
					case Administrator:
						String arg3 = "--"+SpecificationProperties.key_role_administrator;
						String[] arglistAdministrator = { "-u", userId, "--", jhome+java, "-cp", cp, jclass, arg1, arg2, arg3 };
						result = DuccAsUser.duckling(userId, arglistAdministrator);
						response.getWriter().println(result);
						break;
					case User:
					default:
						String[] arglistUser = { "-u", userId, "--", jhome+java, "-cp", cp, jclass, arg1, arg2 };
						result = DuccAsUser.duckling(userId, arglistUser);
						response.getWriter().println(result);
						break;	
					}
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

	private void handleDuccServletServiceSubmit(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletServiceSubmit";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		if(isAuthenticated(request,response)) {
			//TODO
		}
		else {
			duccLogger.warn(methodName, null, messages.fetch("user not authenticated"));
		}
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}
	
	private void handleDuccServletServiceCancel(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletServiceCancel";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		if(isAuthenticated(request,response)) {
			try {
				String name = "id";
				String value = request.getParameter(name).trim();
				duccLogger.info(methodName, null, messages.fetchLabel("cancel")+value);
				DuccData duccData = DuccData.getInstance();
				IDuccWorkMap duccWorkMap = duccData.get();
				String text;
				String result;
				IDuccWorkJob duccWorkJob = (IDuccWorkJob) duccWorkMap.findDuccWork(DuccType.Service, value);
				if(duccWorkJob != null) {
					String resourceOwnerUserId = duccWorkJob.getStandardInfo().getUser().trim();
					if(HandlersHelper.isUserAuthorized(request,resourceOwnerUserId)) {
						String arg1 = "-"+name;
						String arg2 = value;
						String userId = duccWebSessionManager.getUserId(request);
						String cp = System.getProperty("java.class.path");
						String java = "/bin/java";
						String jclass = "org.apache.uima.ducc.cli.DuccServiceCancel";
						String jhome = System.getProperty("java.home");
						DuccCookies.RequestRole requestRole = DuccCookies.getRole(request);
						switch(requestRole) {
						case Administrator:
							String arg3 = "--"+SpecificationProperties.key_role_administrator;
							String[] arglistAdministrator = { "-u", userId, "--", jhome+java, "-cp", cp, jclass, arg1, arg2, arg3 };
							result = DuccAsUser.duckling(userId, arglistAdministrator);
							response.getWriter().println(result);
							break;
						case User:
						default:
							String[] arglistUser = { "-u", userId, "--", jhome+java, "-cp", cp, jclass, arg1, arg2 };
							result = DuccAsUser.duckling(userId, arglistUser);
							response.getWriter().println(result);
							break;	
						}
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
		}
		else {
			duccLogger.warn(methodName, null, messages.fetch("user not authenticated"));
		}
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}
	
	private void duccServletServiceCommand(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response, String command, ArrayList<String> parms) 
	{
		String methodName = "duccServletServiceCommand";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		try {
			String name = "id";
			String value = request.getParameter(name).trim();
			duccLogger.info(methodName, null, command+" "+messages.fetchLabel("name:")+value);
			String text;
			String result;
			name = value.trim();
			ServicesRegistry servicesRegistry = ServicesRegistry.getInstance();
			ServicesRegistryMapPayload payload = servicesRegistry.findService(name);
			if(payload != null) {
				Properties properties = payload.meta;
				String id = properties.getProperty(IServicesRegistry.numeric_id);
				String resourceOwnerUserId = servicesRegistry.findServiceUser(id);
				if(resourceOwnerUserId != null) {
					if(HandlersHelper.isUserAuthorized(request,resourceOwnerUserId)) {
						String arg1 = "--"+command;
						String arg2 = id;
						String userId = duccWebSessionManager.getUserId(request);
						String cp = System.getProperty("java.class.path");
						String java = "/bin/java";
						String jclass = "org.apache.uima.ducc.cli.DuccServiceApi";
						String jhome = System.getProperty("java.home");
						DuccCookies.RequestRole requestRole = DuccCookies.getRole(request);
						ArrayList<String> arglist = new ArrayList<String>();
						arglist.add("-u");
						arglist.add(userId);
						arglist.add("--");
						arglist.add(jhome+java);
						arglist.add("-cp");
						arglist.add(cp);
						arglist.add(jclass);
						switch(requestRole) {
						case Administrator:
							String arg0 = "--"+SpecificationProperties.key_role_administrator;
							arglist.add(arg0);
							break;
						default:
							break;
						}
						arglist.add(arg1);
						arglist.add(arg2);
						for(String parm : parms) {
							arglist.add(parm);
						}
						String[] arglistUser = arglist.toArray(new String[0]);
						result = DuccAsUser.duckling(userId, arglistUser);
						response.getWriter().println(result);
					}
				}
				else {
					text = "name "+value+" not found";
					duccLogger.debug(methodName, null, messages.fetch(text));
				}
			}
			else {
				result = text = "name "+value+" not found";
				duccLogger.debug(methodName, null, messages.fetch(text));
			}
		}
		catch(Exception e) {
			duccLogger.error(methodName, null, e);
		}
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}
	
	private String duccServletServiceCommand(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response, String command) 
	{
		String methodName = "duccServletServiceCommand";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		String result = null;
		try {
			String name = "id";
			String value = request.getParameter(name).trim();
			duccLogger.info(methodName, null, command+" "+messages.fetchLabel("id:")+value);
			String text;
			String id = value.trim();
			ServicesRegistry servicesRegistry = ServicesRegistry.getInstance();
			String resourceOwnerUserId = servicesRegistry.findServiceUser(id);
			if(resourceOwnerUserId != null) {
				if(HandlersHelper.isUserAuthorized(request,resourceOwnerUserId)) {
					String arg1 = "--"+command;
					String arg2 = id;
					String userId = duccWebSessionManager.getUserId(request);
					String cp = System.getProperty("java.class.path");
					String java = "/bin/java";
					String jclass = "org.apache.uima.ducc.cli.DuccServiceApi";
					String jhome = System.getProperty("java.home");
					DuccCookies.RequestRole requestRole = DuccCookies.getRole(request);
					switch(requestRole) {
					case Administrator:
						String arg3 = "--"+SpecificationProperties.key_role_administrator;
						String[] arglistAdministrator = { "-u", userId, "--", jhome+java, "-cp", cp, jclass, arg1, arg2, arg3 };
						result = DuccAsUser.duckling(userId, arglistAdministrator);
						response.getWriter().println(result);
						break;
					case User:
					default:
						String[] arglistUser = { "-u", userId, "--", jhome+java, "-cp", cp, jclass, arg1, arg2 };
						result = DuccAsUser.duckling(userId, arglistUser);
						response.getWriter().println(result);
						break;	
					}
				}
			}
			else {
				text = "id "+value+" not found";
				duccLogger.debug(methodName, null, messages.fetch(text));
			}
		}
		catch(Exception e) {
			duccLogger.error(methodName, null, e);
		}
		duccLogger.trace(methodName, null, messages.fetch("exit"));
		return result;
	}
	
	private void handleDuccServletServiceEnable(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletServiceEnable";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		
		String result = duccServletServiceCommand(target,baseRequest,request,response,"enable");
		
		boolean updateCache = true;
		
		if(updateCache) {
			if(result != null) {
				if(result.contains("success")) {
					String name = "id";
					String id = request.getParameter(name).trim();
					Integer sid = Integer.valueOf(id);
					ServicesSortCache.getInstance().setEnabled(sid);
				}
			}
		}
		
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}
	
	private void handleDuccServletServiceStart(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletServiceStart";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		
		duccServletServiceCommand(target,baseRequest,request,response,"start");
		
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}
	
	private void handleDuccServletServiceStop(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletServiceStop";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		
		duccServletServiceCommand(target,baseRequest,request,response,"stop");
		
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}			
	
	private void handleDuccServletServiceUpdate(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletServiceUpdate";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		
		String instances = request.getParameter("instances");
		String autostart = request.getParameter("autostart");
		
		ArrayList<String> parms = new ArrayList<String>();
		parms.add("--instances");
		parms.add(instances);
		parms.add("--autostart");
		parms.add(autostart);
		
		duccServletServiceCommand(target,baseRequest,request,response,"modify",parms);
		
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}		
	
	private void handleDuccServletReleaseShares(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletReleaseShares";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		
		String result = "";
		
		if(!isAuthenticated(request,response)) {
			result = "error: not authenticated";
			response.getWriter().println(result);
			duccLogger.debug(methodName, null, result);
		}
		else if(!isAdministrator(request,response)) {
			result = "error: not administrator";
			response.getWriter().println(result);
			duccLogger.debug(methodName, null, result);
		}
		else {
			String node = request.getParameter("node");
			String type = request.getParameter("type");
			duccLogger.info(methodName, null, "node:"+node+" "+"type:"+type);
			
			if(node != null) {
				node = node.trim();
			}
			else {
				node = "";
			}
			
			if(type != null) {
				type = type.trim();
			}
			else {
				type = "";
			}
			
			if(node.length() < 1) {
				result = "error: node missing";
				response.getWriter().println(result);
			}
			else if(type.length() < 1) {
				result = "error: type missing";
				response.getWriter().println(result);
			}
			else {
				if(type.equalsIgnoreCase("jobs")) {
					ArrayList<String> nodeList = new ArrayList<String>();
					MachineFactsList mfl = DuccMachinesData.getInstance().getMachineFactsList();
					Iterator<MachineFacts> mfIterator = mfl.iterator();
					while(mfIterator.hasNext()) {
						MachineFacts mf = mfIterator.next();
						if(mf.status != null) {
							if(mf.status != "up") {
								if(mf.name != null) {
									if(node.equals("*")) {
										nodeList.add(mf.name);
									}
									else if(node.equals(mf.name)) {
										nodeList.add(mf.name);
									}
								}
								
							}
						}
					}
					ArrayList<JobProcessInfo> list = DuccDataHelper.getInstance().getJobProcessIds(nodeList);
					if(list.isEmpty()) {
						result = "info: no job processes found on node";
						response.getWriter().println(result);
					}
					else {
						Iterator<JobProcessInfo> iterator = list.iterator();
						String userId = duccWebSessionManager.getUserId(request);
						String cp = System.getProperty("java.class.path");
						String java = "/bin/java";
						String jclass = "org.apache.uima.ducc.cli.DuccJobCancel";
						String jhome = System.getProperty("java.home");
						while(iterator.hasNext()) {
							JobProcessInfo jpi = iterator.next();
							String arg1 = "--id";
							String arg2 = ""+jpi.jobId.getFriendly();
							String arg3 = "--dpid";
							String arg4 = ""+jpi.procid.getFriendly();
							String arg5 = "--"+SpecificationProperties.key_role_administrator;
							String arg6 = "--"+SpecificationProperties.key_reason;
							String arg7 = "\"administrator "+userId+" released shares for this machine\"";
							String[] arglistAdministrator = { "-u", userId, "--", jhome+java, "-cp", cp, jclass, arg1, arg2, arg3, arg4, arg5, arg6, arg7 };
							result = DuccAsUser.duckling(userId, arglistAdministrator);
							response.getWriter().println(result);
						}
					}
				}
				else {
					result = "error: type invalid";
					response.getWriter().println(result);
				}
			}
		}
		
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}		
	
	private void handleDuccRequest(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws Exception
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
				//DuccWebUtil.noCache(response);
			}
			else if(reqURI.startsWith(duccAuthenticationStatus)) {
				handleDuccServletAuthenticationStatus(target, baseRequest, request, response);
				DuccWebUtil.noCache(response);
			}
			else if(reqURI.startsWith(duccAuthenticatorVersion)) {
				handleDuccServletAuthenticatorVersion(target, baseRequest, request, response);
				//DuccWebUtil.noCache(response);
			}
			else if(reqURI.startsWith(duccAuthenticatorPasswordChecked)) {
				handleDuccServletduccAuthenticatorPasswordChecked(target, baseRequest, request, response);
				//DuccWebUtil.noCache(response);
			}
			else if(reqURI.startsWith(duccLoginLink)) {
				handleDuccServletLoginLink(target, baseRequest, request, response);
			}
			else if(reqURI.startsWith(duccLogoutLink)) {
				handleDuccServletLogoutLink(target, baseRequest, request, response);
			}
			else if(reqURI.startsWith(duccJobIdData)) {
				handleDuccServletJobIdData(target, baseRequest, request, response);
				DuccWebUtil.noCache(response);
			}
			else if(reqURI.startsWith(duccJobWorkitemsCountData)) {
				handleDuccServletJobWorkitemsCountData(target, baseRequest, request, response);
				DuccWebUtil.noCache(response);
			}
			else if(reqURI.startsWith(duccJobProcessesData)) {
				handleDuccServletJobProcessesData(target, baseRequest, request, response);
				DuccWebUtil.noCache(response);
			}
			else if(reqURI.startsWith(duccJobWorkitemsData)) {
				handleDuccServletJobWorkitemsData(target, baseRequest, request, response);
				DuccWebUtil.noCache(response);
			}
			else if(reqURI.startsWith(duccJobPerformanceData)) {
				handleDuccServletJobPerformanceData(target, baseRequest, request, response);
			}
			else if(reqURI.startsWith(duccJobSpecificationData)) {
				handleDuccServletJobSpecificationData(target, baseRequest, request, response);
				DuccWebUtil.noCache(response);
			}
			else if(reqURI.startsWith(duccJobFilesData)) {
				handleDuccServletJobFilesData(target, baseRequest, request, response);
				DuccWebUtil.noCache(response);
			}
			else if(reqURI.startsWith(duccReservationFilesData)) {
				handleDuccServletReservationFilesData(target, baseRequest, request, response);
				DuccWebUtil.noCache(response);
			}
			else if(reqURI.startsWith(duccServiceFilesData)) {
				handleDuccServletServiceFilesData(target, baseRequest, request, response);
				DuccWebUtil.noCache(response);
			}
			else if(reqURI.startsWith(duccServiceHistoryData)) {
				handleDuccServletServiceHistoryData(target, baseRequest, request, response);
				DuccWebUtil.noCache(response);
			}
			else if(reqURI.startsWith(duccJobInitializationFailData)) {
				handleDuccServletJobInitializationFailData(target, baseRequest, request, response);
				DuccWebUtil.noCache(response);
			}
			else if(reqURI.startsWith(duccJobRuntimeFailData)) {
				handleDuccServletJobRuntimeFailData(target, baseRequest, request, response);
				DuccWebUtil.noCache(response);
			}
			else if(reqURI.startsWith(duccReservationProcessesData)) {
				handleDuccServletReservationProcessesData(target, baseRequest, request, response);
				DuccWebUtil.noCache(response);
			}
			else if(reqURI.startsWith(duccReservationSpecificationData)) {
				handleDuccServletReservationSpecificationData(target, baseRequest, request, response);
				DuccWebUtil.noCache(response);
			}
			else if(reqURI.startsWith(duccServiceDeploymentsData)) {
				handleDuccServletServiceDeploymentsData(target, baseRequest, request, response);
				DuccWebUtil.noCache(response);
			}
			else if(reqURI.startsWith(duccServiceRegistryData)) {
				handleDuccServletServiceRegistryData(target, baseRequest, request, response);
			}
			else if(reqURI.startsWith(duccServiceSummaryData)) {
				handleDuccServletServiceSummaryData(target, baseRequest, request, response);
			}
			else if(reqURI.startsWith(duccBrokerSummaryData)) {
				handleDuccServletBrokerSummaryData(target, baseRequest, request, response);
			}
			else if(reqURI.startsWith(jsonMachinesData)) {
				handleServletJsonMachinesData(target, baseRequest, request, response);
				DuccWebUtil.noCache(response);
			}
			else if(reqURI.startsWith(duccSystemAdminAdminData)) {
				handleDuccServletSystemAdminAdminData(target, baseRequest, request, response);
				DuccWebUtil.noCache(response);
			}
			else if(reqURI.startsWith(duccSystemAdminControlData)) {
				handleDuccServletSystemAdminControlData(target, baseRequest, request, response);
				DuccWebUtil.noCache(response);
			}
			else if(reqURI.startsWith(duccSystemJobsControl)) {
				handleDuccServletSystemJobsControl(target, baseRequest, request, response);
				DuccWebUtil.noCache(response);
			}
			else if(reqURI.startsWith(jsonSystemClassesData)) {
				handleServletJsonSystemClassesData(target, baseRequest, request, response);
				DuccWebUtil.noCache(response);
			}
			else if(reqURI.startsWith(jsonSystemDaemonsData)) {
				handleServletJsonSystemDaemonsData(target, baseRequest, request, response);
				DuccWebUtil.noCache(response);
			}
			else if(reqURI.startsWith(duccClusterName)) {
				handleDuccServletClusterName(target, baseRequest, request, response);
				//DuccWebUtil.noCache(response);
			}
			else if(reqURI.startsWith(duccClusterUtilization)) {
				handleDuccServletClusterUtilization(target, baseRequest, request, response);
				DuccWebUtil.noCache(response);
			}
			else if(reqURI.startsWith(duccTimeStamp)) {
				handleDuccServletTimeStamp(target, baseRequest, request, response);
				DuccWebUtil.noCache(response);
			}
			else if(reqURI.startsWith(duccJobSubmit)) {
				duccLogger.info(methodName, null,"getRequestURI():"+request.getRequestURI());
				handleDuccServletJobSubmit(target, baseRequest, request, response);
				DuccWebUtil.noCache(response);
			}
			else if(reqURI.startsWith(duccJobCancel)) {
				duccLogger.info(methodName, null,"getRequestURI():"+request.getRequestURI());
				handleDuccServletJobCancel(target, baseRequest, request, response);
				DuccWebUtil.noCache(response);
			}
			else if(reqURI.startsWith(duccReservationSubmit)) {
				duccLogger.info(methodName, null,"getRequestURI():"+request.getRequestURI());
				handleDuccServletReservationSubmit(target, baseRequest, request, response);
				DuccWebUtil.noCache(response);
			}
			else if(reqURI.startsWith(duccReservationCancel)) {
				duccLogger.info(methodName, null,"getRequestURI():"+request.getRequestURI());
				handleDuccServletReservationCancel(target, baseRequest, request, response);
				DuccWebUtil.noCache(response);
			}
			else if(reqURI.startsWith(duccServiceSubmit)) {
				duccLogger.info(methodName, null,"getRequestURI():"+request.getRequestURI());
				handleDuccServletServiceSubmit(target, baseRequest, request, response);
				DuccWebUtil.noCache(response);
			}
			else if(reqURI.startsWith(duccServiceCancel)) {
				duccLogger.info(methodName, null,"getRequestURI():"+request.getRequestURI());
				handleDuccServletServiceCancel(target, baseRequest, request, response);
				DuccWebUtil.noCache(response);
			}
			else if(reqURI.startsWith(duccServiceEnable)) {
				duccLogger.info(methodName, null,"getRequestURI():"+request.getRequestURI());
				handleDuccServletServiceEnable(target, baseRequest, request, response);
				DuccWebUtil.noCache(response);
			}			
			else if(reqURI.startsWith(duccServiceStart)) {
				duccLogger.info(methodName, null,"getRequestURI():"+request.getRequestURI());
				handleDuccServletServiceStart(target, baseRequest, request, response);
				DuccWebUtil.noCache(response);
			}
			else if(reqURI.startsWith(duccServiceStop)) {
				duccLogger.info(methodName, null,"getRequestURI():"+request.getRequestURI());
				handleDuccServletServiceStop(target, baseRequest, request, response);
				DuccWebUtil.noCache(response);
			}
			else if(reqURI.startsWith(duccServiceUpdate)) {
				duccLogger.info(methodName, null,"getRequestURI():"+request.getRequestURI());
				handleDuccServletServiceUpdate(target, baseRequest, request, response);
				DuccWebUtil.noCache(response);
			}
			else if(reqURI.startsWith(duccReleaseShares)) {
				duccLogger.info(methodName, null,"getRequestURI():"+request.getRequestURI());
				handleDuccServletReleaseShares(target, baseRequest, request, response);
				DuccWebUtil.noCache(response);
			}
			else if(reqURI.startsWith(duccReservationSchedulingClasses)) {
				handleDuccServletReservationSchedulingClasses(target, baseRequest, request, response);
				DuccWebUtil.noCache(response);
			}
			else if(reqURI.startsWith(duccReservationInstanceMemorySizes)) {
				handleDuccServletReservationInstanceMemorySizes(target, baseRequest, request, response);
				DuccWebUtil.noCache(response);
			}
			else if(reqURI.startsWith(duccReservationInstanceMemoryUnits)) {
				handleDuccServletReservationInstanceMemoryUnits(target, baseRequest, request, response);
				DuccWebUtil.noCache(response);
			}
			else if(reqURI.startsWith(duccJobSubmitButton)) {
				handleDuccServletJobSubmitButton(target, baseRequest, request, response);
				DuccWebUtil.noCache(response);
			}
			else if(reqURI.startsWith(duccReservationSubmitButton)) {
				handleDuccServletReservationSubmitButton(target, baseRequest, request, response);
				DuccWebUtil.noCache(response);
			}
			else if(reqURI.startsWith(duccReservationFormButton)) {
				handleDuccServletReservationFormButton(target, baseRequest, request, response);
				DuccWebUtil.noCache(response);
			}
			else if(reqURI.startsWith(duccServiceUpdateFormButton)) {
				handleDuccServletServiceUpdateFormButton(target, baseRequest, request, response);
				DuccWebUtil.noCache(response);
			}
			/*
			else if(reqURI.startsWith(duccJobSubmitForm)) {
				handleDuccServletJobSubmitForm(target, baseRequest, request, response);
				DuccWebUtil.noCache(response);
			}
			*/
			else if(reqURI.startsWith(duccLogData)) {
				handleDuccServletLogData(target, baseRequest, request, response);
				DuccWebUtil.noCache(response);
			}
			else if(reqURI.startsWith(duccFileContents)) {
				handleDuccServletFileContents(target, baseRequest, request, response);
				DuccWebUtil.noCache(response);
			}
			else if(reqURI.startsWith(duccJpInitSummary)) {
				handleDuccServletJpInitSummary(target, baseRequest, request, response);
				DuccWebUtil.noCache(response);
			}
			else if(reqURI.startsWith(duccJpInitData)) {
				handleDuccServletJpInitData(target, baseRequest, request, response);
				DuccWebUtil.noCache(response);
			}
			else if(reqURI.startsWith(duccjConsoleLink)) {
				handleDuccServletjConsoleLink(target, baseRequest, request, response);
				DuccWebUtil.noCache(response);
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
