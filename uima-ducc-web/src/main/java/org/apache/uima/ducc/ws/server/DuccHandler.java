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
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListMap;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.uima.ducc.common.CancelReasons.CancelReason;
import org.apache.uima.ducc.common.NodeConfiguration;
import org.apache.uima.ducc.common.SizeBytes;
import org.apache.uima.ducc.common.SizeBytes.Type;
import org.apache.uima.ducc.common.boot.DuccDaemonRuntimeProperties;
import org.apache.uima.ducc.common.boot.DuccDaemonRuntimeProperties.DaemonName;
import org.apache.uima.ducc.common.head.IDuccHead;
import org.apache.uima.ducc.common.internationalization.Messages;
import org.apache.uima.ducc.common.jd.files.IWorkItemState;
import org.apache.uima.ducc.common.jd.files.IWorkItemState.State;
import org.apache.uima.ducc.common.jd.files.perf.PerformanceMetricsSummaryItem;
import org.apache.uima.ducc.common.jd.files.perf.PerformanceMetricsSummaryMap;
import org.apache.uima.ducc.common.jd.files.perf.PerformanceSummary;
import org.apache.uima.ducc.common.jd.files.perf.UimaStatistic;
import org.apache.uima.ducc.common.system.SystemState;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccProperties;
import org.apache.uima.ducc.common.utils.DuccPropertiesHelper;
import org.apache.uima.ducc.common.utils.DuccPropertiesResolver;
import org.apache.uima.ducc.common.utils.DuccSchedulerClasses;
import org.apache.uima.ducc.common.utils.IDuccLoggerComponents;
import org.apache.uima.ducc.common.utils.InetHelper;
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
import org.apache.uima.ducc.transport.event.common.IDuccSchedulingInfo;
import org.apache.uima.ducc.transport.event.common.IDuccStandardInfo;
import org.apache.uima.ducc.transport.event.common.IDuccState.JobState;
import org.apache.uima.ducc.transport.event.common.IDuccTypes.DuccType;
import org.apache.uima.ducc.transport.event.common.IDuccWork;
import org.apache.uima.ducc.transport.event.common.IDuccWorkJob;
import org.apache.uima.ducc.transport.event.common.IDuccWorkMap;
import org.apache.uima.ducc.transport.event.common.IProcessState.ProcessState;
import org.apache.uima.ducc.ws.DuccDaemonsData;
import org.apache.uima.ducc.ws.DuccData;
import org.apache.uima.ducc.ws.DuccDataHelper;
import org.apache.uima.ducc.ws.DuccHead;
import org.apache.uima.ducc.ws.DuccMachinesData;
import org.apache.uima.ducc.ws.MachineInfo;
import org.apache.uima.ducc.ws.authentication.DuccAsUser;
import org.apache.uima.ducc.ws.authentication.DuccAuthenticator;
import org.apache.uima.ducc.ws.helper.BrokerHelper;
import org.apache.uima.ducc.ws.helper.DatabaseHelper;
import org.apache.uima.ducc.ws.helper.DiagnosticsHelper;
import org.apache.uima.ducc.ws.registry.IServicesRegistry;
import org.apache.uima.ducc.ws.registry.ServiceInterpreter;
import org.apache.uima.ducc.ws.registry.ServiceInterpreter.StartState;
import org.apache.uima.ducc.ws.registry.ServicesRegistry;
import org.apache.uima.ducc.ws.registry.ServicesRegistryMapPayload;
import org.apache.uima.ducc.ws.registry.sort.IServiceAdapter;
import org.apache.uima.ducc.ws.registry.sort.ServicesSortCache;
import org.apache.uima.ducc.ws.server.Helper.AllocationType;
import org.apache.uima.ducc.ws.server.HelperSpecifications.PType;
import org.apache.uima.ducc.ws.server.IWebMonitor.MonitorType;
import org.apache.uima.ducc.ws.sort.JobDetailsProcesses;
import org.apache.uima.ducc.ws.types.NodeId;
import org.apache.uima.ducc.ws.utils.FormatHelper;
import org.apache.uima.ducc.ws.utils.FormatHelper.Precision;
import org.apache.uima.ducc.ws.utils.HandlersHelper;
import org.apache.uima.ducc.ws.utils.HandlersHelper.DataAccessPermission;
import org.apache.uima.ducc.ws.utils.alien.AlienWorkItemStateReader;
import org.apache.uima.ducc.ws.utils.alien.EffectiveUser;
import org.apache.uima.ducc.ws.utils.alien.FileInfo;
import org.apache.uima.ducc.ws.utils.alien.FileInfoKey;
import org.apache.uima.ducc.ws.utils.alien.OsProxy;
import org.eclipse.jetty.server.Request;

public class DuccHandler extends DuccAbstractHandler {

	private static String component = IDuccLoggerComponents.abbrv_webServer;

	private static DuccLogger duccLogger = DuccLogger.getLogger(DuccHandler.class);
	private static Messages messages = Messages.getInstance();
	private static DuccId jobid = null;
	
	private static IDuccHead dh = DuccHead.getInstance();
	
	// These keys may have large values and be displayed with Show/Hide buttons.
	// ducc.js must be updated if more than 4 are needed (services may have 4)
	private final String[] n = {"classpath", "service_ping_classpath", "process_executable_args", "process_jvm_args", "environment"};
	private final Set<String> hideableKeys = new HashSet<String>(Arrays.asList(n));

	private enum DetailsType { Job, Reservation, Service };

	private HelperSpecifications helperSpecifications = HelperSpecifications.getInstance();
	private DuccAuthenticator duccAuthenticator = DuccAuthenticator.getInstance();

	private String duccVersion						= duccContext+"/version";
	private String duccHome							= duccContext+"/home";
	private String duccHostname						= duccContext+"/hostname";

	private String duccLoginLink					= duccContext+"/login-link";
	private String duccLogoutLink					= duccContext+"/logout-link";
	private String duccAuthenticationStatus 		= duccContext+"/authentication-status";
	private String duccAuthenticatorVersion 		= duccContext+"/authenticator-version";
	private String duccAuthenticatorNotes	 		= duccContext+"/authenticator-notes";
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

	private String duccServicesRecordsCeiling    	= duccContext+"/services-records-ceiling";

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
	private String duccClusterReliableLabel			= duccContext+"/cluster-reliable-label";
	private String duccClusterReliableStatus		= duccContext+"/cluster-reliable-status";
	private String duccTimeStamp   					= duccContext+"/timestamp";
	private String duccAlerts   					= duccContext+"/alerts";
	private String duccBannerMessage   				= duccContext+"/banner-message";
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

	private String jsonMachinesData 				= duccContext+"/json-machines-data";
	private String jsonSystemClassesData 			= duccContext+"/json-system-classes-data";
	private String jsonSystemDaemonsData 			= duccContext+"/json-system-daemons-data";

	//private String duccJobSubmitForm	    		= duccContext+"/job-submit-form";

	private String duccJobSubmitButton    			= duccContext+"/job-get-submit-button";
	private String duccReservationFormButton  		= duccContext+"/reservation-get-form-button";
	private String duccReservationSubmitButton  	= duccContext+"/reservation-get-submit-button";
	private String duccServiceUpdateFormButton  	= duccContext+"/service-update-get-form-button";

	private String duccReservationSchedulingClasses     = duccContext+"/reservation-scheduling-classes";
	private String duccReservationInstanceMemoryUnits   = duccContext+"/reservation-memory-units";

	private String _window_login_logout = "_window_login_logout";
	private String _window_file_pager = "_window_file_pager";
	private String _window_reservation_request = "_window_reservation_request";
	private String _window_jconsole = "_window_jconsole";

	private long boottime = System.currentTimeMillis();
	private long maxTimeToBoot = 2*60*1000;
	
	public DuccHandler(DuccWebServer duccWebServer) {
		super.init(duccWebServer);
	}
	
	public String getUserIdFromRequest(HttpServletRequest request) {
		String retVal = duccWebSessionManager.getUserId(request);
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
	    		String href = "<a href=\""+link+"login.html\" onclick=\"var newWin = window.open(this.href,'"+_window_login_logout+"','height=600,width=550,scrollbars');  newWin.focus(); return false;\">Login</a>";
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
    		String href = "<a href=\""+link+"logout.html\" onclick=\"var newWin = window.open(this.href,'"+_window_login_logout+"','height=600,width=550,scrollbars');  newWin.focus(); return false;\">Logout</a>";
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

	private void handleDuccServletHome(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response)
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletHome";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		sb.append(dir_home);
		response.getWriter().println(sb);
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}
	
	private void handleDuccServletHostname(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response)
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletHostname";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		try {
			Process p = Runtime.getRuntime().exec("hostname");
			BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
			String hostname = "";
			while ((hostname = stdInput.readLine()) != null) {
				sb.append(hostname);
			}
		}
		catch(Exception e) {
			duccLogger.error(methodName, null, e);
		}
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

	private void handleDuccServletAuthenticatorNotes(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response)
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletAuthenticatorNotes";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		String uid = DuccCookies.getLoginUid(request);
		String notes = duccAuthenticator.getNotes(uid);
		if(notes != null) {
			sb.append(notes);
		}
		duccLogger.debug(methodName, jobid, "uid:"+uid+" "+"notes:"+notes);
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

	private String buildLogFileName(IDuccWorkJob job, IDuccProcess process, AllocationType type) {
		return Helper.getLogFileName(job, process, type);
	}

	private DecimalFormat sizeFormatter = new DecimalFormat("##0.00");

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

	private String normalizeFileSize(long fileSize) {
		return Helper.normalize(fileSize);
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
		return Helper.getPid(job, process);
	}

	private String getStateScheduler(IDuccWorkJob job, IDuccProcess process) {
		return Helper.getSchedulerState(job, process);
	}

	private String getReasonScheduler(IDuccWorkJob job, IDuccProcess process) {
		return Helper.getSchedulerReason(job, process);
	}


	private boolean isIdleJobProcess(IDuccWorkJob job, IDuccProcess process, AllocationType type) {
		return Helper.isIdleJobProcess(job, process, type);
	}
	
	private String getStateAgent(IDuccWorkJob job, IDuccProcess process) {
		return Helper.getAgentState(job, process);
	}

	private String getReasonAgent(IDuccWorkJob job, IDuccProcess process) {
		StringBuffer sb = new StringBuffer();
		if(process != null) {
			String agentReason = process.getExtendedReasonForStoppingProcess();
			if(agentReason == null) {
				agentReason = process.getReasonForStoppingProcess();
			}
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
		return Helper.getExit(job, process);
	}

	private String getTimeInit(IDuccWorkJob job, IDuccProcess process, AllocationType type) {
		return Helper.getTimeInit(job, process, type);
	}

	private boolean isTimeInitEstimated(IDuccWorkJob job, IDuccProcess process, AllocationType type) {
		return Helper.isTimeInitEstimated(job, process, type);
	}
	
	private String getTimeRun(IDuccWorkJob job, IDuccProcess process, AllocationType type) {
		return Helper.getTimeRun(job, process, type);
	}

	private boolean isTimeRunEstimated(IDuccWorkJob job, IDuccProcess process, AllocationType type) {
		return Helper.isTimeRunEstimated(job, process, type);
	}
	
	private String getTimeGC(IDuccWorkJob job, IDuccProcess process, AllocationType type) {
		return Helper.getTimeGC(job, process, type);
	}

	private String getPgIn(IDuccWorkJob job, IDuccProcess process, AllocationType sType) {
		return Helper.getPgIn(job, process, sType);
	}

	private DecimalFormat formatter = new DecimalFormat("##0.0");

	private String getSwap(IDuccWorkJob job, IDuccProcess process, AllocationType sType) {
		StringBuffer sb = new StringBuffer();
		boolean swapping = Helper.isSwapping(job, process, sType);
		String swap = Helper.getSwap(job, process, sType);
		String swapMax = Helper.getSwapMax(job, process, sType);
		if(swapMax != null) {
			sb.append("<span title=\"max="+swapMax+"\" align=\"right\" "+">");
		}
		if(swapping) {
			sb.append("<span class=\"health_red\""+">");
		}
		else {
			sb.append("<span class=\"health_black\""+">");
		}
		sb.append(swap);
		sb.append("</span>");
		if(swapMax != null) {
			sb.append("</span>");
		}
		return sb.toString();
	}
	
	// legacy
	private String getPctCpuV0(IDuccWorkJob job, IDuccProcess process, AllocationType sType) {
		String retVal = "";
		boolean rt = false;
		double pctCPU_overall = 0;
		String runTime = ""+process.getCpuTime();
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
				default:
					break;
				}
				double secsCPU = (msecsCPU*1.0)/1000.0;
				double secsRun = (msecsRun*1.0)/1000.0;
				double timeCPU = secsCPU;
				double timeRun = secsRun;
				pctCPU_overall = 100*(timeCPU/timeRun);
				if(!Double.isNaN(pctCPU_overall)) {
					StringBuffer sb = new StringBuffer();
					String fmtsecsCPU = formatter.format(secsCPU);
					String fmtsecsRun = formatter.format(secsRun);
					String title = "title="+"\""+"seconds"+" "+"CPU:"+fmtsecsCPU+" "+"run:"+fmtsecsRun+"\"";
					sb.append("<span "+title+">");
					String fmtPctCPU = formatter.format(pctCPU_overall);
					sb.append(fmtPctCPU);
					sb.append("</span>");
					retVal = sb.toString();
				}
			}
		}
		catch(Exception e) {
		}
		return retVal;
	}

	private String getPctCpuV1(IDuccWorkJob job, IDuccProcess process, AllocationType sType) {
		String fmtCPU_overall = Helper.getPctCpuOverall(job, process, sType);
		String fmtCPU_current = Helper.getPctCpuCurrent(job, process,sType);
		StringBuffer sb = new StringBuffer();
		ProcessState ps = process.getProcessState();
		switch(ps) {
		case Starting:
		case Started:
		case Initializing:
		case Running:
			String title = "title="+"\"lifetime: "+fmtCPU_overall+"\"";
			sb.append("<span "+title+" class=\"health_green\">");
			sb.append(fmtCPU_current);
			sb.append("</span>");
			break;
		default:
			sb.append("<span>");
			sb.append(fmtCPU_overall);
			sb.append("</span>");
			break;
		}
		String retVal = sb.toString();
		return retVal;
	}

	private String getPctCpu(IDuccWorkJob job, IDuccProcess process, AllocationType sType) {
		String location = "getPctCpu";
		String retVal = "";
		if(process != null) {
			try {
				if(process.getDataVersion() < 1) {
					retVal = getPctCpuV0(job, process, sType);
				}
				else {
					retVal = getPctCpuV1(job, process, sType);
				}
			}
			catch(Exception e) {
				duccLogger.error(location, jobid, e);
			}
		}
		return retVal;
	}

	private String getRSS(IDuccWorkJob job, IDuccProcess process, AllocationType sType) {
		StringBuffer sb = new StringBuffer();
		String rss = Helper.getRss(job, process, sType);
		String rssMax = Helper.getRssMax(job, process, sType);
		if(rssMax != null) {
			sb.append("<span title=\"max="+rssMax+"\" align=\"right\" "+">");
			sb.append(rss);
			sb.append("</span>");
		}
		else {
			sb.append(rss);
		}
		return sb.toString();
	}

	private String getJConsole(IDuccWorkJob job, IDuccProcess process, AllocationType sType) {
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

	String pname_idJob = "idJob";
	String pname_idPro = "idPro";

	private long getLogFileSize(String filename, Map<String, FileInfo> fileInfoMap) {
		return Helper.getFileSize(filename, fileInfoMap);
	}

	private void buildJobProcessListEntry(EffectiveUser eu, StringBuffer pb, DuccWorkJob job, IDuccProcess process, DetailsType dType, AllocationType sType, int counter, Map<String, FileInfo> fileInfoMap) {
		StringBuffer rb = new StringBuffer();
		int COLS = 26;
		switch(sType) {
		case SPC:
		case SPU:
			COLS++;	// Services
			COLS++;	// Memory
			break;
		default:
			break;
		}
		StringBuffer[] cbList = new StringBuffer[COLS];
		for(int i=0; i < COLS; i++) {
			cbList[i] = new StringBuffer();
		}
		String logsjobdir = job.getUserLogDir();
		String logfile = buildLogFileName(job, process, sType);

		String file_name = logsjobdir+logfile;

		String url = Helper.getFilePagerUrl(eu, file_name);
		String href = "<a href=\""+url+"\" onclick=\"var newWin = window.open(this.href,'"+_window_file_pager+"','height=800,width=1200,scrollbars');  newWin.focus(); return false;\">"+logfile+"</a>";
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
		String fileSize = Helper.getLogFileSize(job, process, logfile, fileInfoMap);
		cbList[index].append(fileSize);
		logAppend(index,"fileSize",fileSize);
		cbList[index].append("</td>");
		// Hostname
		index++; // jp.03
		cbList[index].append("<td>");
		String hostname = Helper.getHostname(job, process);
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
		// Memory
		switch(sType) {
		case SPC:
		case SPU:
			index++; // jp.05
			cbList[index].append("<td align=\"right\">");
			DuccId duccId = job.getDuccId();
			IDuccSchedulingInfo si;
			SizeBytes sizeBytes;
			String requested;
			String actual;
			si = job.getSchedulingInfo();
			sizeBytes = new SizeBytes(SizeBytes.Type.Bytes, si.getMemorySizeAllocatedInBytes());
			actual = getProcessMemorySize(duccId,sizeBytes);
			sizeBytes = new SizeBytes(si.getMemoryUnits().name(), Long.parseLong(si.getMemorySizeRequested()));
			requested = getProcessMemorySize(duccId,sizeBytes);
			cbList[index].append("<span title=\""+"requested: "+requested+"\">");
			cbList[index].append(actual);
			cbList[index].append("</span>");
			logAppend(index,"actual",actual);
			logAppend(index,"requested",requested);
			break;
		default:
			break;
		}
		// State:scheduler
		index++; // jp.06
		cbList[index].append("<td>");
		String stateScheduler = getStateScheduler(job,process);
		cbList[index].append(stateScheduler);
		logAppend(index,"stateScheduler",stateScheduler);
		cbList[index].append("</td>");
		// Reason:scheduler
		index++; // jp.07
		cbList[index].append("<td>");
		String reasonScheduler = getReasonScheduler(job,process);
		cbList[index].append(reasonScheduler);
		logAppend(index,"reasonScheduler",reasonScheduler);
		cbList[index].append("</td>");
		// State:agent
		index++; // jp.08
		cbList[index].append("<td>");
		String stateAgent = getStateAgent(job,process);
		if(isIdleJobProcess(job,process,sType)) {
			stateAgent = process.getProcessState()+":Idle";
		}
		cbList[index].append(stateAgent);
		logAppend(index,"stateAgent",stateAgent);
		cbList[index].append("</td>");
		// Reason:agent
		index++; // jp.09
		cbList[index].append("<td>");
		String reasonAgent = getReasonAgent(job,process);
		cbList[index].append(reasonAgent);
		logAppend(index,"reasonAgent",reasonAgent);
		cbList[index].append("</td>");
		// Exit
		index++; // jp.10
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
			index++; // jp.11
			cbList[index].append("<td align=\"right\">");
			String timeInitPrefix = "<span class=\"health_black\">";
			if(isTimeInitEstimated(job, process, sType)) {
				timeInitPrefix = "<span class=\"health_green\">";
			}
			String timeInitBody = getTimeInit(job,process,sType);
			String timeInitSuffix = "</span>";
			String timeInit = timeInitPrefix+timeInitBody+timeInitSuffix;
			cbList[index].append(timeInit);
			logAppend(index,"timeInit",timeInit);
			cbList[index].append("</td>");
			break;
		}
		// Time:run
		index++; // jp.12
		cbList[index].append("<td align=\"right\">");
		String timeRunPrefix = "<span class=\"health_black\">";
		if(isTimeRunEstimated(job, process, sType)) {
			timeRunPrefix = "<span class=\"health_green\">";
		}
		String timeRunBody = getTimeRun(job,process,sType);
		String timeRunSuffix = "</span>";
		String timeRun = timeRunPrefix+timeRunBody+timeRunSuffix;
		cbList[index].append(timeRun);
		logAppend(index,"timeRun",timeRun);
		cbList[index].append("</td>");
		// Time:GC
		switch(sType) {
		case MR:
			break;
		default:
			index++; // jp.13
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
		default:
			index++; // jp.14
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
		default:
			index++; // jp.15
			cbList[index].append("<td align=\"right\">");
			String swap = getSwap(job,process,sType);
			cbList[index].append(swap);
			logAppend(index,"swap",swap);
			cbList[index].append("</td>");
			break;
		}
		// %cpu
		index++; // jp.16
		cbList[index].append("<td align=\"right\">");
		String pctCPU = getPctCpu(job,process,sType);
		cbList[index].append(pctCPU);
		logAppend(index,"%cpu",pctCPU);
		cbList[index].append("</td>");
		// rss
		index++; // jp.17
		cbList[index].append("<td align=\"right\">");
		String rss = getRSS(job,process,sType);
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
			index++; // jp.18
			String timeAvg = "";
			cbList[index].append("<td align=\"right\">");
			timeAvg = Helper.getWiTimeAvg(job, process, sType);
			cbList[index].append(timeAvg);
			logAppend(index,"timeAvg",timeAvg);
			cbList[index].append("</td>");
			// Time:max
			index++; // jp.19
			cbList[index].append("<td align=\"right\">");
			String timeMax = Helper.getWiTimeMax(job, process, sType);
			cbList[index].append(timeMax);
			logAppend(index,"timeMax",timeMax);
			cbList[index].append("</td>");
			// Time:min
			index++; // jp.20
			cbList[index].append("<td align=\"right\">");
			String timeMin = Helper.getWiTimeMin(job, process, sType);
			cbList[index].append(timeMin);
			logAppend(index,"timeMin",timeMin);
			cbList[index].append("</td>");
			// Done
			index++; // jp.21
			cbList[index].append("<td align=\"right\">");
			String done = Helper.getWiDone(job, process, sType);
			cbList[index].append(done);
			logAppend(index,"done",done);
			cbList[index].append("</td>");
			// Error
			index++; // jp.22
			cbList[index].append("<td align=\"right\">");
			String error = Helper.getWiError(job, process, sType);
			cbList[index].append(error);
			logAppend(index,"error",error);
			cbList[index].append("</td>");
			// Dispatch
			switch(dType) {
			case Job:
				index++; // jp.23
				cbList[index].append("<td align=\"right\">");
				String dispatch = Helper.getWiDispatch(job, process, sType);
				cbList[index].append(dispatch);
				logAppend(index,"dispatch",dispatch);
				cbList[index].append("</td>");
				break;
			default:
				break;
			}
			// Retry
			index++; // jp.24
			cbList[index].append("<td align=\"right\">");
			String retry = Helper.getWiRetry(job, process, sType);
			cbList[index].append(retry);
			logAppend(index,"retry",retry);
			cbList[index].append("</td>");
			// Preempt
			index++; // jp.25
			cbList[index].append("<td align=\"right\">");
			String preempt = Helper.getWiPreempt(job, process, sType);
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
			index++; // jp.26
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
				String jd_url = Helper.getFilePagerUrl(eu, logsjobdir+errfile);
				String href2 = "<a href=\""+jd_url+"\" onclick=\"var newWin = window.open(this.href,'"+_window_file_pager+"','height=800,width=1200,scrollbars');  newWin.focus(); return false;\">"+errfile+"</a>";
				cbList[index] = new StringBuffer();
				cbList[index].append("<td>");
				cbList[index].append(href2);
				cbList[index].append("</td>");
				// Size
				index = 2;
				cbList[index] = new StringBuffer();
				cbList[index].append("<td align=\"right\">");
				cbList[index].append(normalizeFileSize(getLogFileSize(errfile, fileInfoMap)));
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
		default:
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
		String methodName = "findJob";
		IDuccWorkJob job = null;
		try {
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
		}
		catch(Exception e) {
			duccLogger.trace(methodName, null, "jobno="+jobno, e);
		}
		return job;
	}

	private Map<String, FileInfo> getFileInfoMap(EffectiveUser eu, String directory) {
		return Helper.getFileInfoMap(eu, directory);
	}

	private void handleDuccServletJobProcessesData(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response)
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletJobProcessesData";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		EffectiveUser eu = EffectiveUser.create(request);
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
			String directory = job.getLogDirectory()+File.separator+job.getId();
			Map<String, FileInfo> fileInfoMap = getFileInfoMap(eu, directory);
			Iterator<DuccId> iterator = null;
			iterator = job.getDriver().getProcessMap().keySet().iterator();
			int counter = 1;
			while(iterator.hasNext()) {
				DuccId processId = iterator.next();
				IDuccProcess process = job.getDriver().getProcessMap().get(processId);
				StringBuffer bb = new StringBuffer();
				buildJobProcessListEntry(eu, bb, job, process, DetailsType.Job, AllocationType.JD, counter, fileInfoMap);
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
				buildJobProcessListEntry(eu, bb, job, process, DetailsType.Job, AllocationType.UIMA, counter, fileInfoMap);
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
		return Helper.getJob(jobNo);
	}

	private DuccWorkJob getManagedReservation(String reservationNo) {
		return Helper.getManagedReservation(reservationNo);
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
				String directory = job.getLogDirectory()+jobNo;
				EffectiveUser eu = EffectiveUser.create(request);
				long wiVersion = job.getWiVersion();
				AlienWorkItemStateReader workItemStateReader = new AlienWorkItemStateReader(eu, component, directory, wiVersion);
				ConcurrentSkipListMap<Long,IWorkItemState> map = workItemStateReader.getMap();
			    if (map == null) {
			    	sb.append(eu.isLoggedin() ? "(data missing or unreadable)" : "(not visible - try logging in)");
			    } else if (map.size() == 0) {
			    	sb.append("(no work-items processed)");
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
							// Start Time 
							row.append("<td align=\"right\">");
							row.append("limit");
							// Queuing Time (sec)
							row.append("<td align=\"right\">");
							row.append("reached");
							// Processing Time (sec)
							row.append("<td align=\"right\">");
							row.append("*****");
							// Investment Time (sec)
							row.append("<td align=\"right\">");
							row.append("*****");
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
			    		long refTime = DuccHandlerUtils.getReferenceTime(job,wis.getNode(),wis.getPid());
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
						// Start Time 
						String fmt_startTime = "";
						IDuccProcess jd = job.getDriverProcess();
						if(jd != null) {
							long jdStartTime = Helper.getTimeStart(jd);
							if(jdStartTime >= 0) {
								long wiStartTime = wis.getMillisAtStart();
								if(wiStartTime > jdStartTime) {
									double elapsedTime = wiStartTime - jdStartTime;
									elapsedTime = elapsedTime/1000;
									fmt_startTime = formatter.format(elapsedTime);
								}
							}
						}
						row.append("<td align=\"right\">");
						row.append(fmt_startTime);
						// Queuing Time (sec)
						time = getAdjustedTime(wis.getMillisOverhead(refTime), job);
						time = time/1000;
						row.append("<td align=\"right\">");
						row.append(formatter.format(time));
						// Processing Time (sec)
						time = getAdjustedTime(wis.getMillisProcessing(refTime), job);
						time = time/1000;
						ptime = formatter.format(time);
						row.append("<td align=\"right\">");
						switch(state) {
						case start:
						case queued:
						case operating:
							row.append("<span class=\"health_green\">");
							break;
						default:
							row.append("<span class=\"health_black\">");
							break;
						}
						row.append(ptime);
						row.append("</span>");
						// Investment Time (sec)
						time = getAdjustedTime(wis.getMillisInvestment(refTime), job);
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
				//sb = new StringBuffer();
				sb.append("no accessible data ("+e+")");
			}
		}
		else {
			sb.append("no accessible data (no job?)");
		}

		response.getWriter().println(sb);
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}

	private void dumpMap(PerformanceMetricsSummaryMap map) {
		String location = "dumpMap";
		if(map != null) {
			if(map.size() == 0) {
				duccLogger.debug(location, jobid, "map=empty");
			}
			else {
				duccLogger.debug(location, jobid, "map.size="+map.size());
				for (Entry<String, PerformanceMetricsSummaryItem> entry : map.entrySet()) {
					PerformanceMetricsSummaryItem item = entry.getValue();
					duccLogger.debug(location, jobid, "displayName=", item.getDisplayName(), "analysisTime=", item.getAnalysisTime(), "analysisTimeMin=", item.getAnalysisTimeMin(), "analysisTimeMax=", item.getAnalysisTimeMax(), "analysisTasks=", item.getAnalysisTasks());
				}
			}
		}
		else {
			duccLogger.debug(location, jobid, "map=null");
		}
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
				EffectiveUser eu = EffectiveUser.create(request);
				PerformanceSummary performanceSummary = new PerformanceSummary(job.getLogDirectory()+jobNo);
			    PerformanceMetricsSummaryMap performanceMetricsSummaryMap = performanceSummary.readSummary(eu.get());
			    dumpMap(performanceMetricsSummaryMap);
			    if (performanceMetricsSummaryMap == null) {
			    	sb.append(eu.isLoggedin() ? "(data missing or unreadable)" : "(not visible - try logging in)");
			    } else if (performanceMetricsSummaryMap.size() == 0) {
			        sb.append("(no performance metrics)");
			    }
			    else {
			    	int casCount  = performanceMetricsSummaryMap.casCount();
					ArrayList <UimaStatistic> uimaStats = new ArrayList<UimaStatistic>();
				    uimaStats.clear();
				    long analysisTime = 0;
				    PerformanceMetricsSummaryItem summaryValues = null;
				    for (Entry<String, PerformanceMetricsSummaryItem> entry : performanceMetricsSummaryMap.entrySet()) {
				    	PerformanceMetricsSummaryItem item = entry.getValue();
				    	// UIMA-4641 Totals are passed as if a delegate with an empty name
				    	if (entry.getKey().isEmpty()) {
				    	    summaryValues = item;
				    	    continue;
				    	}
				    	String shortname = item.getDisplayName();
				    	long anTime = item.getAnalysisTime();
				    	long anMinTime = item.getAnalysisTimeMin();
				    	long anMaxTime = item.getAnalysisTimeMax();
				    	long anTasks = item.getAnalysisTasks();
				    	analysisTime += anTime;
				    	UimaStatistic stat = new UimaStatistic(shortname, entry.getKey(), anTime, anMinTime, anMaxTime, anTasks);
				    	uimaStats.add(stat);
				    }
				    Collections.sort(uimaStats);
				    int numstats = uimaStats.size();
				    DecimalFormat formatter = new DecimalFormat("##0.0");
				    // pass 1
				    int counter = 0;
				    sb.append(trGet(counter++));
				    // Totals
					sb.append("<td>");
					sb.append("<i><b>Summary</b></i>");
					long ltime = 0;
					// Total
					sb.append("<td align=\"right\">");
					if (summaryValues != null) {
					    analysisTime = summaryValues.getAnalysisTime();
					}
					sb.append(FormatHelper.duration(analysisTime,Precision.Tenths));
					// % of Total
					sb.append("<td align=\"right\">");
					sb.append(formatter.format(100));
					// Avg
					sb.append("<td align=\"right\">");
					sb.append("<span class=\"health_purple\" title=\"average processing time per completed work item\">");
					long avgMillis = 0;
					if(casCount > 0) {
						avgMillis = analysisTime  / casCount;    // No need to round up as will display only 10ths
					}
					sb.append(FormatHelper.duration(avgMillis,Precision.Tenths));
					sb.append("</span>");
					// Min
					sb.append("<td align=\"right\">");
					sb.append("<span class=\"health_purple\" title=\"minimum processing time for any completed work item\">");
					if (summaryValues != null) {
					    ltime = summaryValues.getAnalysisTimeMin();
					} else {
						ltime = job.getWiMillisMin();
					}
					sb.append(FormatHelper.duration(ltime,Precision.Tenths));
					sb.append("</span>");
					// Max
					sb.append("<td align=\"right\">");
					sb.append("<span class=\"health_purple\" title=\"maximum processing time for any completed work item\">");
					if (summaryValues != null) {
					    ltime = summaryValues.getAnalysisTimeMax();
					} else {
						ltime = job.getWiMillisMax();
					}
					sb.append(FormatHelper.duration(ltime,Precision.Tenths));
					sb.append("</span>");
					// Tasks
					sb.append("<td align=\"right\">");
					sb.append("<span class=\"health_purple\" title=\"number of tasks per completed work item\">");
					sb.append(""+"N/A");
					sb.append("</span>");
				    // pass 2
				    for (int i = 0; i < numstats; ++i) {
				    	sb.append(trGet(counter++));
				    	String title = "title="+"\""+uimaStats.get(i).getLongName()+"\"";
						sb.append("<td "+title+">");
						sb.append(uimaStats.get(i).getShortName());
						double time;
						// Total
						sb.append("<td align=\"right\">");
						time = uimaStats.get(i).getAnalysisTime();
						ltime = (long)time;
						sb.append(FormatHelper.duration(ltime,Precision.Tenths));
						// % of Total
						sb.append("<td align=\"right\">");
						double dtime = (time/analysisTime)*100;
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
						// Tasks
						sb.append("<td align=\"right\">");
						long lnumTasks = (long)(uimaStats.get(i).getAnalysisTasks());
						sb.append(""+lnumTasks);
					}
			    }
			}
			catch(Exception e) {
				duccLogger.warn(methodName, null, e);
				//sb = new StringBuffer();
				sb.append("no accessible data ("+e+")");
			}
		}
		else {
			sb.append("no accessible data (no job?)");
		}

		response.getWriter().println(sb);
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}

	private void putJobSpecEntry(Properties properties, String provider, String key, String value, StringBuffer sb, int counter) {
		if(value != null) {
            sb.append(trGet(counter));
            // Sort "user" before the system key "" with:  <td sorttable_customkey="false|true"\>user</td>
            sb.append("<td sorttable_customkey=\"" + provider.isEmpty() + "\">");
            sb.append(provider);
            sb.append("</td>");
			sb.append("<td>");
			sb.append(key);
			sb.append("</td>");
			sb.append("<td>");
			sb.append(value);
			sb.append("</td>");
			sb.append("</tr>");
		}
	}

	/**
	 * Format job & reservation & service specification files
	 * If no properties provided then the files may be inaccessible or missing
	 *
	 * @param request - servlet request
	 * @param response - generated response
	 * @param usProperties - properties specified by the user
	 * @param properties - meta properties (for services), or user + default properties (for jobs & APs)
	 * @param buttonHint - for services says how to enable modification of autostart and instances values
	 * @throws IOException
	 */
    private void processSpecificationData(HttpServletRequest request, HttpServletResponse response,
            Properties usProperties, Properties properties, String buttonHint) throws IOException {
        String methodName = "ProcessSpecificationData";
        if (usProperties == null || properties == null) {
            String msg = isAuthenticated(request, response) ? "(data missing or unreadable)" : "(not visible - try logging in)";
            response.getWriter().println(msg);
            duccLogger.warn(methodName, null, request.getParameter("id") + " failed: " + msg);
            return;
        }
        StringBuffer sb = new StringBuffer();
        // Create a sorted list of all properties
        TreeSet<String> list = new TreeSet<String>(properties.stringPropertyNames());
        if (buttonHint != null) {
            list.addAll(usProperties.stringPropertyNames());  // Include the service user values as well as the meta ones
            // Move autostart to the user properties where it should have been all along
            if (!usProperties.contains("autostart")) {
                Object val = properties.remove("autostart");   // Should never be null ??
                if (val != null) {
                    usProperties.put("autostart", val);
                } else {
                    duccLogger.warn(methodName, null, "Service "+properties.getProperty("numeric_id")+" has no autostart setting?");
                }
            }
        }
        int i = 0;
        int counter = 0;
        for (String key : list) {
            // Determine the origin of the property, user vs. default or meta
            String provider = "user";
            String value = usProperties.getProperty(key);
            if (value == null) {
                value = properties.getProperty(key);
                provider = "";
            }
            // Format certain values that can be long and attach a Hide/Show button if would span many lines
            if (hideableKeys.contains(key)) {
                value = formatValue(value, key.endsWith("classpath"));
                if (value.length() > 500) {
                    String show = "<div class=\"hidedata" + i + "\"><input type=\"submit\" name=\"showcp" + i
                            + "\" value=\"Show\" id=\"showbutton" + i + "\"/></div>";
                    String hide = "<div class=\"showdata" + i + "\"><input type=\"submit\" name=\"hidecp" + i
                            + "\" value=\"Hide\" id=\"hidebutton" + i + "\"/>" + " " + value + "</div>";
                    value = show + hide;
                    i++;
                }
            }
            // Add a button for modifying the autostart value
            if (key.equalsIgnoreCase(IServicesRegistry.autostart)) {
                if (value != null) {
                    value = value.trim();
                    String eulav = value.equals("true") ? "false" : "true"; // Reverse value
                    StringBuffer replacement = new StringBuffer();
                    replacement.append("<select id=\"autostart\"" + buttonHint + ">");
                    replacement.append("<option value=\"" + value + "\"  selected=\"selected\">" + value + "</option>");
                    replacement.append("<option value=\"" + eulav + "\"                       >" + eulav + "</option>");
                    replacement.append("</select>");
                    value = replacement.toString();
                }
            }
            // Add a button for modifying the instances value
            if (key.equalsIgnoreCase(IServicesRegistry.instances)) {
                if (value != null) {
                    value = value.trim();
                    StringBuffer replacement = new StringBuffer();
                    replacement.append("<span id=\"instances_area\">");
                    replacement.append("<input type=\"text\" size=\"5\" id=\"instances\" value=\"" + value + "\"" + buttonHint + ">");
                    replacement.append("</span>");
                    value = replacement.toString();
                }
            }
            putJobSpecEntry(properties, provider, key, value, sb, counter++);
        }

        response.getWriter().println(sb);
    }

	private void handleDuccServletJobSpecificationData(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response)
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletJobSpecificationData";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		String jobNo = request.getParameter("id");
		Map<String,Properties> map = null;
		DuccWorkJob dwj = getJob(jobNo);
		if(dwj != null) {
			String resOwner = dwj.getStandardInfo().getUser();
			EffectiveUser eu = EffectiveUser.create(request);
			String reqUser = eu.get();
			if(HandlersHelper.isResourceAuthorized(resOwner, reqUser)) {
				map = helperSpecifications.getJobSpecificationProperties(dwj, eu);
			}
		}
		if(map != null) {
			Properties propertiesUser = map.get(PType.user.name());
			Properties propertiesSystem = map.get(PType.all.name());
			int usize = -1;
			if(propertiesUser != null) {
				usize = propertiesUser.size();
			}
			duccLogger.debug(methodName, jobid, "user="+usize);
			int ssize = -1;
			if(propertiesSystem != null) {
				ssize = propertiesSystem.size();
			}
			duccLogger.debug(methodName, jobid, "system="+ssize);
			processSpecificationData(request, response, propertiesUser, propertiesSystem, null);
		}
		else {
			String msg = "(data not found)";
            response.getWriter().println(msg);
            duccLogger.warn(methodName, null, request.getParameter("id") + " failed: " + msg);
		}
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
				EffectiveUser eu = EffectiveUser.create(request);
				String directory = job.getUserLogDir();
				Map<String, FileInfo> map = OsProxy.getFilesInDirectory(eu, directory);
				Set<String> keys = map.keySet();
				int counter = 0;
				for(String key : keys) {
					FileInfo fileInfo = map.get(key);
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
					String url = Helper.getFilePagerUrl(eu, fileInfo.getName());
					String href = "<a href=\""+url+"\" onclick=\"var newWin = window.open(this.href,'"+_window_file_pager+"','height=800,width=1200,scrollbars');  newWin.focus(); return false;\">"+fileInfo.getShortName()+"</a>";
					row.append(href);
					row.append("</td>");
					// size
					row.append("<td>");
					double size = (fileInfo.getLength()*1.0)/Constants.MB;
					row.append(sizeFormatter.format(size));
					row.append("</td>");
					//
					row.append("</tr>");
					sb.append(row);
					counter++;
				}
				if (counter == 0) {
				    sb.append(eu.isLoggedin() ? "(data missing or unreadable)" : "(not visible - try logging in)");
				}
			}
			catch(Throwable t) {
				duccLogger.warn(methodName, null, t);
				//sb = new StringBuffer();
				sb.append("no accessible data ("+t+")");
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
				EffectiveUser eu = EffectiveUser.create(request);
				String directory = reservation.getUserLogDir();
				Map<String, FileInfo> map = OsProxy.getFilesInDirectory(eu, directory);
				Set<String> keys = map.keySet();
				int counter = 0;
				for(String key : keys) {
					FileInfo fileInfo = map.get(key);
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
					String url = Helper.getFilePagerUrl(eu, fileInfo.getName());
					String href = "<a href=\""+url+"\" onclick=\"var newWin = window.open(this.href,'"+_window_file_pager+"','height=800,width=1200,scrollbars');  newWin.focus(); return false;\">"+fileInfo.getShortName()+"</a>";
					row.append(href);
					row.append("</td>");
					// size
					row.append("<td>");
					double size = (fileInfo.getLength()*1.0)/Constants.MB;
					row.append(sizeFormatter.format(size));
					row.append("</td>");
					//
					row.append("</tr>");
					sb.append(row);
					counter++;
				}
		        if (counter == 0) {
		            sb.append(eu.isLoggedin() ? "(no files found)" : "(not visible - try logging in)");
		        }
			}
			catch(Throwable t) {
				duccLogger.warn(methodName, null, t);
				sb.append("no accessible data ("+t+")");
			}
		}
		response.getWriter().println(sb);
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}

	private void buildServiceFilesListEntry(Request baseRequest,HttpServletRequest request, StringBuffer sb, DuccWorkJob job, IDuccProcess process, AllocationType type, int counter, Map<String, FileInfo> map) {
		EffectiveUser eu = EffectiveUser.create(request);
		if(job != null) {
			try {
				Set<String> keys = map.keySet();
				for(String key : keys) {
					FileInfo fileInfo = map.get(key);
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
					String url = Helper.getFilePagerUrl(eu, fileInfo.getName());
					String href = "<a href=\""+url+"\" onclick=\"var newWin = window.open(this.href,'"+_window_file_pager+"','height=800,width=1200,scrollbars');  newWin.focus(); return false;\">"+fileInfo.getShortName()+"</a>";
					row.append(href);
					row.append("</td>");
					// size
					row.append("<td>");
					double size = (fileInfo.getLength()*1.0)/Constants.MB;
					row.append(sizeFormatter.format(size));
					row.append("</td>");
					// date
					row.append("<td>");
					String date = fileInfo.getDate();
					String time = fileInfo.getTime();
					String dow = fileInfo.getDOW();
					row.append(date+" "+time+" "+dow);
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
			ArrayList<String> implementors = DuccDataHelper.parseImplementorsAsList(properties);

			DuccWorkJob service = null;
			IDuccWorkMap duccWorkMap = DuccData.getInstance().get();
			if(duccWorkMap.getServiceKeySet().size()> 0) {
				Iterator<DuccId> iterator = null;
				iterator = duccWorkMap.getServiceKeySet().iterator();
				int counter = 0;
				AllocationType type = AllocationType.SPU;
				String service_type = properties.getProperty(IServicesRegistry.service_type);
				if(service_type != null) {
					if(service_type.equalsIgnoreCase(IServicesRegistry.service_type_CUSTOM)) {
						type = AllocationType.SPC;
					}
				}
				EffectiveUser eu = EffectiveUser.create(request);
				while(iterator.hasNext()) {
					DuccId serviceId = iterator.next();
					String fid = ""+serviceId.getFriendly();
					if(implementors.contains(fid)) {
						service = (DuccWorkJob) duccWorkMap.findDuccWork(serviceId);
						IDuccProcessMap map = service.getProcessMap();
						String directory = service.getUserLogDir();
						Map<String, FileInfo> fmap = OsProxy.getFilesInDirectory(eu, directory);
						for(DuccId key : map.keySet()) {
							IDuccProcess process = map.get(key);
							buildServiceFilesListEntry(baseRequest,request,sb, service, process, type, ++counter, fmap);
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
			EffectiveUser eu = EffectiveUser.create(request);
			ServicesRegistry servicesRegistry = ServicesRegistry.getInstance();
			ServicesRegistryMapPayload payload = servicesRegistry.findService(name);
			Properties properties;
			properties = payload.meta;
			String numeric_id = properties.getProperty(IServicesRegistry.numeric_id);
			properties = payload.svc;
			String log_directory = properties.getProperty(IServicesRegistry.log_directory);

			Map<String, FileInfo> pmap = OsProxy.getFilesInDirectory(eu, log_directory, true);
			Set<String> keys = pmap.keySet();

			long sequence = 0;
			TreeMap<FileInfoKey,FileInfo> map = new TreeMap<FileInfoKey,FileInfo>();
			for(String key : keys) {
				sequence++;
				FileInfo fileInfo = pmap.get(key);
				FileInfoKey fik = new FileInfoKey(fileInfo.getTOD(), sequence);
				map.put(fik, fileInfo);
			}
			Set<FileInfoKey> sortkeys = map.descendingKeySet();

			int counter = 0;
			for(FileInfoKey key : sortkeys) {
				FileInfo fileInfo = map.get(key);

				StringBuffer row = new StringBuffer();
				String tr = trGet(counter);
				row.append(tr);
				// id
				row.append("<td>");
				row.append(numeric_id+fileInfo.getRelDir());
				row.append("</td>");
				// name
				row.append("<td>");

				String url = Helper.getFilePagerUrl(fileInfo.getName(), fileInfo.getPageCount());
				String href = "<a href=\""+url+"\" onclick=\"var newWin = window.open(this.href,'"+_window_file_pager+"','height=800,width=1200,scrollbars');  newWin.focus(); return false;\">"+fileInfo.getShortName()+"</a>";
				row.append(href);
				row.append("</td>");
				// size
				row.append("<td>");
				double size = (fileInfo.getLength()*1.0)/Constants.MB;
				row.append(sizeFormatter.format(size));
				row.append("</td>");
				// date
				row.append("<td>");
				String date = fileInfo.getDate();
				String time = fileInfo.getTime();
				String dow = fileInfo.getDOW();
				row.append(date+" "+time+" "+dow);
				row.append("</td>");
				//
				row.append("</tr>");
				sb.append(row);
				counter++;
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
		String methodName = "handleDuccServletJobInitializationFailData";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		EffectiveUser eu = EffectiveUser.create(request);
		StringBuffer sb = new StringBuffer();
		String jobNo = request.getParameter("id");
		DuccWorkJob job = getJob(jobNo);
		StringBuffer data = new StringBuffer();
		data.append("no accessible data");
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
							if(job.getSchedulingInfo().getLongProcessesMax() < 0) {
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
						String logsjobdir = job.getUserLogDir();
						String logfile = buildLogFileName(job, process, AllocationType.UIMA);
						String link = logfile;
						String reason = process.getReasonForStoppingProcess();
						if(reason != null) {
							if(reason.trim().length() > 0) {
								link = logfile+":"+reason;
							}
						}
						String url = Helper.getFilePagerUrl(eu, logsjobdir+logfile);
						String href = "<a href=\""+url+"\" onclick=\"var newWin = window.open(this.href,'"+_window_file_pager+"','height=800,width=1200,scrollbars');  newWin.focus(); return false;\">"+link+"</a>";
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
		String methodName = "handleDuccServletJobRuntimeFailData";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		EffectiveUser eu = EffectiveUser.create(request);
		StringBuffer sb = new StringBuffer();
		String jobNo = request.getParameter("id");
		DuccWorkJob job = getJob(jobNo);
		StringBuffer data = new StringBuffer();
		data.append("no accessible data");
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
						String logsjobdir = job.getUserLogDir();
						String logfile = buildLogFileName(job, process, AllocationType.UIMA);
						String link = logfile;
						String reason = process.getReasonForStoppingProcess();
						if(reason != null) {
							if(reason.trim().length() > 0) {
								link = logfile+":"+reason;
							}
						}
						String url = Helper.getFilePagerUrl(eu, logsjobdir+logfile);
						String href = "<a href=\""+url+"\" onclick=\"var newWin = window.open(this.href,'"+_window_file_pager+"','height=800,width=1200,scrollbars');  newWin.focus(); return false;\">"+link+"</a>";
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

	private void buildServiceProcessListEntry(EffectiveUser eu, StringBuffer sb, DuccWorkJob job, IDuccProcess process, DetailsType dType, AllocationType sType, int counter, Map<String, FileInfo> fileInfoMap) {
		buildJobProcessListEntry(eu, sb, job, process, dType, sType, counter, fileInfoMap);
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
			EffectiveUser eu = EffectiveUser.create(request);
			String directory = managedReservation.getUserLogDir();
			Map<String, FileInfo> fileInfoMap = OsProxy.getFilesInDirectory(eu, directory);
			Iterator<DuccId> iterator = null;
			int counter = 0;
			iterator = managedReservation.getProcessMap().keySet().iterator();
			while(iterator.hasNext()) {
				DuccId processId = iterator.next();
				IDuccProcess process = managedReservation.getProcessMap().get(processId);
				buildServiceProcessListEntry(eu, sb, managedReservation, process, DetailsType.Reservation, AllocationType.MR, ++counter, fileInfoMap);
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
		String mrNo = request.getParameter("id");
		DuccWorkJob dwj = getManagedReservation(mrNo);
		String resOwner = dwj.getStandardInfo().getUser();
		EffectiveUser eu = EffectiveUser.create(request);
		String reqUser = eu.get();
		Map<String,Properties> map = null;
		if(HandlersHelper.isResourceAuthorized(resOwner, reqUser)) {
			map = helperSpecifications.getManagedReservationSpecificationProperties(dwj, eu);
		}
		if(map != null) {
			Properties propertiesUser = map.get(PType.user.name());
			Properties propertiesSystem = map.get(PType.all.name());
			int usize = -1;
			if(propertiesUser != null) {
				usize = propertiesUser.size();
			}
			duccLogger.debug(methodName, jobid, "user="+usize);
			int ssize = -1;
			if(propertiesSystem != null) {
				ssize = propertiesSystem.size();
			}
			duccLogger.debug(methodName, jobid, "system="+ssize);
			processSpecificationData(request, response, propertiesUser, propertiesSystem, null);
		}
		else {
			String msg = "(data not found)";
            response.getWriter().println(msg);
            duccLogger.warn(methodName, null, request.getParameter("id") + " failed: " + msg);
		}
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}

	private void handleDuccServletServicesRecordsCeilingData(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response)
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletServicesRecordsCeilingData";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		try {
			int counter = 0;  // force counter to be zero for isListable calculation
			int ceiling = 0;
			ServicesSortCache servicesSortCache = ServicesSortCache.getInstance();
			Collection<IServiceAdapter> servicesSortedCollection = servicesSortCache.getSortedCollection();
			if(!servicesSortedCollection.isEmpty()) {
				int maxRecords = getServicesMax(request);
				ArrayList<String> users = getServicesUsers(request);
				for(IServiceAdapter service : servicesSortedCollection) {
					boolean list = DuccWebUtil.isListable(request, users, maxRecords, counter, service);
					if(!list) {
						continue;
					}
					ceiling++;
				}
			}
			sb.append(""+ceiling);
		}
		catch(Throwable t) {
			duccLogger.trace(methodName, jobid, t);
		}
		if(sb.length() == 0) {
			sb.append("0");
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
			List<String> implementors_current = DuccDataHelper.parseImplementorsAsList(properties);
			List<String> implementors_defunct = DuccDataHelper.parseWorkInstancesAsList(properties);

			IDuccWorkMap duccWorkMap = DuccData.getInstance().get();
			for(int i=0; i<2; i++) {
				Map<Long,DuccWorkJob> servicesMap = null;
				switch(i) {
				case 0:
					servicesMap = duccWorkMap.getServicesMap(implementors_current);
					break;
				case 1:
					servicesMap = duccWorkMap.getServicesMap(implementors_defunct);
					break;
				}

				Map<Long, DuccWorkJob> inverseServicesMap = new TreeMap<Long,DuccWorkJob>();
				for(Entry<Long, DuccWorkJob> entry : servicesMap.entrySet()) {
					inverseServicesMap.put(0-entry.getKey(), entry.getValue());
				}

				int counter = 0;
				AllocationType type = AllocationType.SPU;
				String service_type = properties.getProperty(IServicesRegistry.service_type);
				if(service_type != null) {
					if(service_type.equalsIgnoreCase(IServicesRegistry.service_type_CUSTOM)) {
						type = AllocationType.SPC;
					}
				}
				EffectiveUser eu = EffectiveUser.create(request);
				for(Entry<Long, DuccWorkJob> entry : inverseServicesMap.entrySet()) {
					DuccWorkJob service = entry.getValue();
					String directory = service.getLogDirectory()+File.separator+service.getId();
					Map<String, FileInfo> fileInfoMap = getFileInfoMap(eu, directory);
					IDuccProcessMap map = service.getProcessMap();
					if(map.isEmpty()) {
						buildServiceProcessListEntry(eu, sb, service, null, DetailsType.Service, type, ++counter, fileInfoMap);
					}
					else {
						for(DuccId key : map.keySet()) {
							IDuccProcess process = map.get(key);
							buildServiceProcessListEntry(eu, sb, service, process, DetailsType.Service, type, ++counter, fileInfoMap);
						}
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
			DataAccessPermission dap = HandlersHelper.getServiceAuthorization(baseRequest);
			switch(dap) {
				case Read:
				case Write:
					String name = request.getParameter("name");
					ServicesRegistry servicesRegistry = ServicesRegistry.getInstance();
					ServicesRegistryMapPayload payload = servicesRegistry.findService(name);
					String hint = getLoginRefreshHint(request, response);
					String enable_or_disable = getEnabledOrDisabled(request, response);;
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
						hint = enable_or_disable + " " + hint;
					    processSpecificationData(request, response, payload.svc, payload.meta, hint);

					} else {
						sb.append("<tr>");
						sb.append("<td>");
						sb.append("not found");
						sb.append("</td>");
						sb.append("<td>");
						sb.append("</td>");
						sb.append("</tr>");
					}
					break;
				case None:
				default:
					sb.append("<tr>");
					sb.append("<td>");
					sb.append("not authorized");
					sb.append("</td>");
					sb.append("<td>");
					sb.append("</td>");
					sb.append("</tr>");
					break;
			}

		}
		catch(Exception e) {
			duccLogger.warn(methodName, null, e);
			sb.append("no accessible data ("+e+")");
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
			uptime = brokerHelper.getBrokerUptime();
			brokerVersion = brokerHelper.getBrokerVersion();
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
		Map<MachineInfo,NodeId> sortedMachines = instance.getSortedMachines();
		Iterator<MachineInfo> iterator;
		// pass 1
		iterator = sortedMachines.keySet().iterator();
		long memTotal = 0;
		long swapInuse = 0;
		long swapFree = 0;
		long alienPids = 0;
		while(iterator.hasNext()) {
			MachineInfo machineInfo = iterator.next();
			try {
				memTotal += Long.parseLong(machineInfo.getMemTotal());
			}
			catch(Exception e) {
				duccLogger.trace(methodName, jobid, e);
			};
			try {swapInuse += Long.parseLong(machineInfo.getSwapInuse());
			}
			catch(Exception e) {
				duccLogger.trace(methodName, jobid, e);
			};
			try {swapFree += Long.parseLong(machineInfo.getSwapFree());
			}
			catch(Exception e) {
				duccLogger.trace(methodName, jobid, e);
			};
			try {
				List<ProcessInfo> alienPidsList = machineInfo.getAlienPids();
				if(alienPidsList != null) {
					alienPids += alienPidsList.size();
				}
			}
			catch(Exception e) {
				duccLogger.trace(methodName, jobid, e);
			};
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
			String userId = getUserIdFromRequest(request);
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

		DuccSchedulerClasses schedulerClasses = DuccSchedulerClasses.getInstance();
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
		String href = "<a href=\\\""+duccjConsoleLink+"?"+"service="+service+"\\\" onclick=\\\"var newWin = window.open(this.href,'"+_window_jconsole+"','height=800,width=1200,scrollbars');  newWin.focus(); return false;\\\">"+service+"</a>";
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
			case Broker:
				boolean brokerAlive = brokerHelper.isAlive();
				if(brokerAlive) {
					status = "up";
				}
				else {
					status = "down";
				}
				heartbeat = "0";
				heartmax = "0";
				break;
			case Database:
				if(databaseHelper.isAlive()) {
					status = "up";
				}
				else {
					status = "down";
				}
				heartbeat = "0";
				heartmax = "0";
				break;
			default:
				status = "unknown";
				String hb = DuccDaemonsData.getInstance().getHeartbeat(daemonName);
				try {
					Long.parseLong(hb);
					heartbeat = hb;
					long timeout = DuccWebProperties.get_ducc_ws_monitored_daemon_down_millis_expiry()/1000;
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
			Map<MachineInfo,NodeId> machines = DuccMachinesData.getInstance().getMachines();
			Iterator<MachineInfo> iterator = machines.keySet().iterator();
			while(iterator.hasNext()) {
				MachineInfo machineInfo = iterator.next();
				Properties properties = DuccDaemonRuntimeProperties.getInstance().getAgent(machineInfo.getShortName());
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
			sb.append("<span>");
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

		DecimalFormat percentageFormatter = new DecimalFormat("##0.0");

		String utilization = "0%";

		SizeBytes sbReserve = Helper.getSummaryReserve();
		long memReserve = sbReserve.getGBytes();

		long bytesInuseJobs = DuccData.getInstance().getLive().getMemoryInuseJobs();
		long bytesInuseServices = DuccData.getInstance().getLive().getMemoryInuseServices();
		long bytesInuseReservations = DuccData.getInstance().getLive().getMemoryInuseReservations();
		
		SizeBytes sbInuseJobs = new SizeBytes(Type.Bytes, bytesInuseJobs);
		SizeBytes sbInuseServices = new SizeBytes(Type.Bytes, bytesInuseServices);
		SizeBytes sbInuseReservations = new SizeBytes(Type.Bytes, bytesInuseReservations);
		
		long memInuseJobs = sbInuseJobs.getGBytes();
		long memInuseServices = sbInuseServices.getGBytes();
		long memInuseReservations = sbInuseReservations.getGBytes();
		long memInuseAll = memInuseJobs+memInuseServices+memInuseReservations;
		
		duccLogger.trace(methodName, jobid, "Jobs:"+memInuseJobs+" "+"Services:"+memInuseServices+" "+"Reservations:"+memInuseReservations);
		duccLogger.trace(methodName, jobid, "Inuse:"+memInuseAll+" "+"Reserve:"+memReserve);
		
		long sumInuse = DuccData.getInstance().getLive().getMemoryInuse();

		SizeBytes sbInuse = new SizeBytes(Type.Bytes, sumInuse);
		long memInuse = sbInuse.getGBytes();

		if(memReserve > 0) {
			double percentage = (((1.0) * memInuse) / ((1.0) * memReserve)) * 100.0;
			utilization = percentageFormatter.format(percentage)+"%";
		}

		sb.append(utilization);
		response.getWriter().println(sb);
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}

	private void handleDuccServletClusterReliableStatus(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response)
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletClusterReliableStatus";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		String status = dh.get_ducc_head_mode();
		String head = "head="+DuccPropertiesHelper.getDuccHead();
		String local = "local="+InetHelper.getHostName();;
		String hover = head+' '+local;
		if(dh.is_ducc_head_master()) {
			String text = "<span title=\""+hover+"\">"+status+"</span>";
			sb.append(text);
		}
		else {
			String text = "<span>"+status+"</span>";
			sb.append(text);
		}
		response.getWriter().println(sb);
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}
	
	private void handleDuccServletClusterReliableLabel(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response)
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletClusterReliableLabel";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		String label = "reliable:";
		if(dh.is_ducc_head_backup()) {
			String hover = "Click to visit master";
			String text = "<span title=\""+hover+"\">"+label+"</span>";
			String host = DuccPropertiesHelper.getDuccHead();;
			String link = "http://"+host+":"+getDuccWebServer().getPort()+"/";
			String href = "<a href=\""+link+"\" target=\"_ducc_master\"  >"+text+"</a>";
			sb.append(href);
		}
		else {
			String text = "<span>"+label+"</span>";
			sb.append(text);
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
		StringBuffer sb = new StringBuffer(getTimeStamp(request,jobid,DuccData.getInstance().getPublished()));
		response.getWriter().println(sb);
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}

	private static BrokerHelper brokerHelper = BrokerHelper.getInstance();
	private static DatabaseHelper databaseHelper = DatabaseHelper.getInstance();

	private void addDownDaemon(StringBuffer sb, String name) {
		if(sb.length() == 0) {
			sb.append("ALERT - critical component(s) unresponsive: "+name);
		}
		else {
			sb.append(", "+name);
		}
	}

	private String cache_disk_info = "";
	private long cache_disk_info_TOD = 0;
	private long cache_disk_info_interval = 15*(60*1000);

	private void record_disk_info(String disk_info) {
		String methodName = "record_disk_info";
		if(disk_info != null) {
			long now= System.currentTimeMillis();
			if(disk_info.equals(cache_disk_info)) {
				long elapsed = now - cache_disk_info_TOD;
				if(elapsed > cache_disk_info_interval) {
					cache_disk_info_TOD = now;
					duccLogger.info(methodName, jobid, disk_info);
				}
			}
			else {
				cache_disk_info = disk_info;
				cache_disk_info_TOD = now;
				duccLogger.info(methodName, jobid, disk_info);
			}
		}
	}

	private void handleDuccServletAlerts(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response)
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletAlerts";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		String additionalInfo = null;
		daemons:
		for(DaemonName daemonName : DuccDaemonRuntimeProperties.daemonNames) {
			switch(daemonName) {
			case Database:
				String disk_info = DiagnosticsHelper.get_ducc_disk_info();
				record_disk_info(disk_info);
				if(databaseHelper.isDisabled()) {
					continue daemons;
				}
				if(!databaseHelper.isAlive()) {
					addDownDaemon(sb, daemonName.name());
					additionalInfo = DiagnosticsHelper.get_ducc_disk_info();
				}
				break;
			case Broker:
				if(!brokerHelper.isAlive()) {
					addDownDaemon(sb, daemonName.name());
				}
				break;
			case Orchestrator:
			case ProcessManager:
			case ResourceManager:
			case ServiceManager:
				long timeout = DuccWebProperties.get_ducc_ws_monitored_daemon_down_millis_expiry()/1000;
				if(timeout > 0) {
					try {
						long heartbeatLast = Long.parseLong(DuccDaemonsData.getInstance().getHeartbeat(daemonName));
						long overtime = timeout - heartbeatLast;
						if(overtime < 0) {
							addDownDaemon(sb, daemonName.name());
						}
					}
					catch(Exception e) {
						addDownDaemon(sb, daemonName.name());
					}
				}
				break;
			default:
				break;
			}
		}
		if(additionalInfo != null) {
			if(additionalInfo.trim().length() > 0) {
				sb.append("<br>");
				sb.append(additionalInfo);
			}
		}
		String text = sb.toString();
		
		if(sb.length() > 0) {
			long now = System.currentTimeMillis();
			long lifetime = now - boottime;
			if(lifetime < maxTimeToBoot) {
				long timeLeft = (maxTimeToBoot - lifetime)/1000;
				text = "Webserver booting...maximum time remaining for daemons to check-in: "+timeLeft+" seconds.";
				duccLogger.debug(methodName, null, text);
			}
		}
		response.getWriter().println(text);
		duccLogger.trace(methodName, null, messages.fetch("exit"));
	}

	private void handleDuccServletBannerMessage(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response)
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletBannerMessage";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		String key = DuccPropertiesResolver.ducc_ws_banner_message;
		String value = DuccPropertiesResolver.getInstance().getFileProperty(key);
		if(value != null) {
			String message = value.trim();
			if(message.length() > 0) {
				sb.append(message);
			}
		}
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
		DuccSchedulerClasses schedulerClasses = DuccSchedulerClasses.getInstance();
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
		String button = "<button style=\"font-size:8pt; background-color:green; color:ffffff;\" onclick=\"var newWin = window.open('submit.reservation.html','"+_window_reservation_request+"','height=600,width=550,scrollbars'); newWin.focus(); return false;\">Request<br>Reservation</button>";
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

	private String getDisabled() {
		String retVal = "disabled=\"disabled\"";
		return retVal;
	}

	private String getEnabledOrDisabled(HttpServletRequest request,HttpServletResponse response) {
		String retVal = "";
		DuccCookies.RefreshMode refreshMode = DuccCookies.getRefreshMode(request);
		if(!isAuthenticated(request,response)) {
			switch(refreshMode) {
			default:
			case Automatic:
				retVal = getDisabled();
				break;
			case Manual:
				retVal = getDisabled();
				break;
			}
		}
		else {
			switch(refreshMode) {
			default:
			case Automatic:
				retVal = getDisabled();
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

		String enable_or_disable = getDisabled();
		DataAccessPermission dap = HandlersHelper.getServiceAuthorization(baseRequest);
		switch(dap) {
			case Write:
				enable_or_disable = getEnabledOrDisabled(request, response);
				break;
			case Read:
			case None:
			default:
				break;
		}
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
		EffectiveUser eu = EffectiveUser.create(request);
		String newline = "\n";
		String colon = ":";
		try {
			String file_name = fname;
			AlienTextFile atf = new AlienTextFile(eu, file_name);
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
			EffectiveUser eu = EffectiveUser.create(request);
			isr = DuccFile.getInputStreamReader(eu, fname);
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
			sb.append("<td>"+"no accessible data (not logged in?)");
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
					String userId = getUserIdFromRequest(request);
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

			String arg5a = "--wait_for_completion";
			String arg5b = "false";
			String arg6a = "--cancel_on_interrupt";
			String arg6b = "false";

			String arg7 = "";
			String arg8 = "";
			if(description != null) {
				arg7 = "--description";
				arg8 = description;
			}
			try {
				String userId = getUserIdFromRequest(request);
				String cp = System.getProperty("java.class.path");
				String java = "/bin/java";
				String jclass = "org.apache.uima.ducc.cli.DuccReservationSubmit";
				String jhome = System.getProperty("java.home");
				String[] arglist = { "-u", userId, "--", jhome+java, "-cp", cp, jclass, arg1, arg2, arg3, arg4, arg5a, arg5b, arg6a, arg6b, arg7, arg8 };
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
			String text;
			String result;
			IDuccWork dw =duccData.getReservation(value);
			if(dw != null) {
				String resourceOwnerUserId = dw.getStandardInfo().getUser().trim();
				if(HandlersHelper.isUserAuthorized(request,resourceOwnerUserId)) {
					String arg1 = "-"+name;
					String arg2 = value;
					String userId = getUserIdFromRequest(request);
					String cp = System.getProperty("java.class.path");
					String java = "/bin/java";
					String jclass = "org.apache.uima.ducc.cli.DuccReservationCancel";
					if(dw instanceof IDuccWorkJob) {
						jclass = "org.apache.uima.ducc.cli.DuccManagedReservationCancel";
					}
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
				text = "reservation "+value+" not found";
				duccLogger.debug(methodName, null, messages.fetch(text));
				response.getWriter().println(text);
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
						String userId = getUserIdFromRequest(request);
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
						String userId = getUserIdFromRequest(request);
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
			String id = value.trim();
			String userId = getUserIdFromRequest(request);
			String serviceId = id;
			if(HandlersHelper.isLoggedIn(request)) {
				if(HandlersHelper.isServiceController(userId, serviceId)) {
					String arg1 = "--"+command;
					String arg2 = id;
					String cp = System.getProperty("java.class.path");
					String java = "/bin/java";
					String jclass = "org.apache.uima.ducc.cli.DuccServiceApi";
					String jhome = System.getProperty("java.home");
					DuccCookies.RequestRole requestRole = DuccCookies.getRole(request);
					switch(requestRole) {
					case Administrator:
						String arg3 = "--"+SpecificationProperties.key_role_administrator;
						String[] arglistAdministrator = { "-u", userId, "--", jhome+java, "-cp", cp, jclass, arg1, arg2, arg3 };
						duccLogger.debug(methodName, null, String.join(" ", arglistAdministrator));
						result = DuccAsUser.duckling(userId, arglistAdministrator);
						duccLogger.debug(methodName, null, result);
						response.getWriter().println(result);
						break;
					case User:
					default:
						String[] arglistUser = { "-u", userId, "--", jhome+java, "-cp", cp, jclass, arg1, arg2 };
						duccLogger.debug(methodName, null, String.join(" ", arglistUser));
						result = DuccAsUser.duckling(userId, arglistUser);
						duccLogger.debug(methodName, null, result);
						response.getWriter().println(result);
						break;
					}
				}
				else {
					duccLogger.debug(methodName, null, "not logged in");
				}
			}
			else {
				duccLogger.debug(methodName, null, "not service controller");
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
			else if(reqURI.startsWith(duccHome)) {
				handleDuccServletHome(target, baseRequest, request, response);
				//DuccWebUtil.noCache(response);
			}
			else if(reqURI.startsWith(duccHostname)) {
				handleDuccServletHostname(target, baseRequest, request, response);
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
			else if(reqURI.startsWith(duccAuthenticatorNotes)) {
				handleDuccServletAuthenticatorNotes(target, baseRequest, request, response);
				DuccWebUtil.noCache(response);
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
			else if(reqURI.startsWith(duccClusterReliableStatus)) {
				handleDuccServletClusterReliableStatus(target, baseRequest, request, response);
			}
			else if(reqURI.startsWith(duccClusterReliableLabel)) {
				handleDuccServletClusterReliableLabel(target, baseRequest, request, response);
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
			else if(reqURI.startsWith(duccServicesRecordsCeiling)) {
				handleDuccServletServicesRecordsCeilingData(target, baseRequest, request, response);
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
			else if(reqURI.startsWith(duccAlerts)) {
				handleDuccServletAlerts(target, baseRequest, request, response);
				DuccWebUtil.noCache(response);
			}
			else if(reqURI.startsWith(duccBannerMessage)) {
				handleDuccServletBannerMessage(target, baseRequest, request, response);
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
			else if(reqURI.startsWith(duccReservationSchedulingClasses)) {
				handleDuccServletReservationSchedulingClasses(target, baseRequest, request, response);
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
