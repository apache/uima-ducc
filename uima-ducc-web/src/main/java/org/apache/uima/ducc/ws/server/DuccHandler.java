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
import java.util.ArrayList;
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

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;
import org.apache.uima.ducc.common.authentication.AuthenticationManager;
import org.apache.uima.ducc.common.authentication.IAuthenticationManager;
import org.apache.uima.ducc.common.boot.DuccDaemonRuntimeProperties;
import org.apache.uima.ducc.common.boot.DuccDaemonRuntimeProperties.DaemonName;
import org.apache.uima.ducc.common.internationalization.Messages;
import org.apache.uima.ducc.common.jd.files.IWorkItemState;
import org.apache.uima.ducc.common.jd.files.IWorkItemState.State;
import org.apache.uima.ducc.common.jd.files.WorkItemStateManager;
import org.apache.uima.ducc.common.system.SystemState;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.DuccProperties;
import org.apache.uima.ducc.common.utils.SynchronizedSimpleDateFormat;
import org.apache.uima.ducc.common.utils.TimeStamp;
import org.apache.uima.ducc.common.utils.Version;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.orchestrator.authentication.DuccWebAdministrators;
import org.apache.uima.ducc.transport.agent.IUimaPipelineAEComponent;
import org.apache.uima.ducc.transport.event.ProcessInfo;
import org.apache.uima.ducc.transport.event.cli.SpecificationProperties;
import org.apache.uima.ducc.transport.event.common.DuccWorkJob;
import org.apache.uima.ducc.transport.event.common.DuccWorkMap;
import org.apache.uima.ducc.transport.event.common.IDuccProcess;
import org.apache.uima.ducc.transport.event.common.IDuccProcessMap;
import org.apache.uima.ducc.transport.event.common.IDuccProcessWorkItems;
import org.apache.uima.ducc.transport.event.common.IDuccStandardInfo;
import org.apache.uima.ducc.transport.event.common.IDuccState.JobState;
import org.apache.uima.ducc.transport.event.common.IDuccTypes.DuccType;
import org.apache.uima.ducc.transport.event.common.IDuccWorkJob;
import org.apache.uima.ducc.transport.event.common.IDuccWorkReservation;
import org.apache.uima.ducc.transport.event.common.IResourceState.ProcessDeallocationType;
import org.apache.uima.ducc.transport.event.common.TimeWindow;
import org.apache.uima.ducc.transport.event.jd.PerformanceMetricsSummaryItem;
import org.apache.uima.ducc.transport.event.jd.PerformanceMetricsSummaryMap;
import org.apache.uima.ducc.transport.event.jd.PerformanceSummary;
import org.apache.uima.ducc.transport.event.jd.UimaStatistic;
import org.apache.uima.ducc.ws.DuccDaemonsData;
import org.apache.uima.ducc.ws.DuccData;
import org.apache.uima.ducc.ws.DuccMachinesData;
import org.apache.uima.ducc.ws.MachineInfo;
import org.apache.uima.ducc.ws.MachineSummaryInfo;
import org.apache.uima.ducc.ws.registry.IServicesRegistry;
import org.apache.uima.ducc.ws.registry.ServicesRegistry;
import org.apache.uima.ducc.ws.registry.ServicesRegistryMapPayload;
import org.apache.uima.ducc.ws.utils.FormatHelper;
import org.eclipse.jetty.server.Request;

public class DuccHandler extends DuccAbstractHandler {
	
	private static DuccLogger duccLogger = DuccLoggerComponents.getWsLogger(DuccHandler.class.getName());
	private static Messages messages = Messages.getInstance();
	private static DuccId jobid = null;

	private static IAuthenticationManager iAuthenticationManager = AuthenticationManager.getInstance();

	private String duccVersion						= duccContext+"/version";
	
	private String duccLoginLink					= duccContext+"/login-link";
	private String duccLogoutLink					= duccContext+"/logout-link";
	private String duccAuthenticationStatus 		= duccContext+"/authentication-status";
	private String duccAuthenticatorVersion 		= duccContext+"/authenticator-version";
	
	private String duccJobIdData					= duccContext+"/job-id-data";
	private String duccJobWorkitemsCountData		= duccContext+"/job-workitems-count-data";
	private String duccJobProcessesData    			= duccContext+"/job-processes-data";
	private String duccJobWorkitemsData				= duccContext+"/job-workitems-data";
	private String duccJobPerformanceData			= duccContext+"/job-performance-data";
	private String duccJobSpecificationData 		= duccContext+"/job-specification-data";
	private String duccJobInitializationFailData	= duccContext+"/job-initialization-fail-data";
	private String duccJobRuntimeFailData			= duccContext+"/job-runtime-fail-data";
	
	private String duccReservationProcessesData    	= duccContext+"/reservation-processes-data";
	private String duccReservationSpecificationData = duccContext+"/reservation-specification-data";
	
	private String duccServiceDeploymentsData    	= duccContext+"/service-deployments-data";
	private String duccServiceRegistryData 			= duccContext+"/service-registry-data";
	private String duccServiceSummaryData			= duccContext+"/service-summary-data";
	
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
	private String duccReservationInstanceMemorySizes   = duccContext+"/reservation-instance-memory-sizes";
	private String duccReservationInstanceMemoryUnits   = duccContext+"/reservation-instance-memory-units";
	private String duccReservationNumberOfInstances	    = duccContext+"/reservation-number-of-instances";

	private DuccWebServer duccWebServer = null;
	
	public DuccHandler(DuccWebServer duccWebServer) {
		this.duccWebServer = duccWebServer;
		initializeAuthenticator();
	}
	
	public DuccWebServer getDuccWebServer() {
		return duccWebServer;
	}
	
	public String getFileName() {
		return dir_home+File.separator+dir_resources+File.separator+getDuccWebServer().getClassDefinitionFile();
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
	
	/*
	 * non-authenticated
	 */
	
	private void handleDuccServletLoginLink(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletLoginLink";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
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
		sb.append(iAuthenticationManager.getVersion());
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
	
	private String buildLogFileName(IDuccWorkJob job, IDuccProcess process, String type) {
		String retVal = "";
		if(type == "UIMA") {
			String fType = type;
			retVal = job.getDuccId().getFriendly()+"-"+fType+"-"+process.getNodeIdentity().getName()+"-"+process.getPID()+".log";
		}
		if(type == "MR") {
			String fType = "POP";
			retVal = job.getDuccId().getFriendly()+"-"+fType+"-"+process.getNodeIdentity().getName()+"-"+process.getPID()+".log";
		}
		else if(type == "SP") {
			String fType = "UIMA";
			retVal = job.getDuccId().getFriendly()+"-"+fType+"-"+process.getNodeIdentity().getName()+"-"+process.getPID()+".log";
		}
		else if(type == "JD") {
			retVal = "jd.out.log";
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
		String location = "buildJobProcessListEntry";
		String logsjobdir = job.getUserLogsDir()+job.getDuccId().getFriendly()+File.separator;
		String logfile = buildLogFileName(job, process, type);
		String errfile = "jd.err.log";
		String href = "<a href=\""+duccLogData+"?"+"fname="+logsjobdir+logfile+"\" onclick=\"var newWin = window.open(this.href,'child','height=800,width=1200,scrollbars');  newWin.focus(); return false;\">"+logfile+"</a>";
		String tr = trGet(counter);
		sb.append(tr);
		String pid = process.getPID();
		// Id
		sb.append("<td align=\"right\">");
		/*
		long id = process.getDuccId().getFriendly();
		System.out.println(id);
		 */
		if(type.equals("SP")) {
			sb.append(job.getDuccId().getFriendly()+"."+process.getDuccId().getFriendly());
		}
		else if(type.equals("MR")) {
			sb.append(job.getDuccId().getFriendly()+"."+process.getDuccId().getFriendly());
		}
		else {
			sb.append(process.getDuccId().getFriendly());
		}
		sb.append("</td>");
		// Log
		sb.append("<td>");
		if(pid != null) {
			sb.append(href);
		}
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
		if(pid != null) {
			sb.append(pid);
		}
		else {
			sb.append("?");
		}
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
			/*
			NodeIdentity nodeId = jp.getNodeIdentity();
			if(nodeId != null) {
				String ip = nodeId.getIp();
				if(DuccMachinesData.getInstance().isMachineSwapping(ip)) {
					sb.append("<span class=\"health_red\">");
					sb.append("Swapping");
					sb.append("</span>");
				}
			}
			*/
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
		// Time:initializationTimeWindow t;
		long timeInitMillis = 0;
		if(type.equals("MR")) {
			// 
		}
		else {
			sb.append("<td align=\"right\">");
			List<IUimaPipelineAEComponent> upcList = jp.getUimaPipelineComponents();
			TimeWindow t;
			t = (TimeWindow) process.getTimeWindowInit();
			try {
				timeInitMillis = t.getElapsedMillis();
				duccLogger.debug(location, jobid, "init millis = "+timeInitMillis);
			}
			catch(Exception e) {
			}
			String initTime = "?";
			if(t != null) {
				initTime = t.getElapsed(job);
			}
			if((t != null) && (t.isEstimated())) {
				sb.append("<span title=\"estimated\" class=\"health_green\">");
			}
			else {
				sb.append("<span class=\"health_black\">");
			}
			if(initTime == null) {
				initTime = "0";
			}
			if(!job.isOperational()) {
				if(upcList == null) {
					initTime = "";
				}
				else {
					if(upcList.isEmpty()) {
						initTime = "";
					}
				}
			}
			initTime = chomp("00:", initTime);
			if(upcList != null) {
				if(!upcList.isEmpty()) {
					String id = ""+process.getDuccId().getFriendly();
					initTime = "<a class=\"classLoad\" title=\""+id+"\" href=\"#loadme"+id+"\" rel=\"#loadme"+id+"\">"+initTime+"</a>";
					StringBuffer loadme = new StringBuffer();
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
						String iTime = FormatHelper.duration(upc.getInitializationTime());
						loadme.append("<tr>");
						loadme.append("<td>"+iName);
						loadme.append("<td>"+iState);
						loadme.append("<td>"+iTime);
					}
					loadme.append("</table>");
					loadme.append("</div>");
					sb.append(loadme);
				}
			}
			sb.append(initTime);
			sb.append("</span>");
			sb.append("</td>");
		}
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
		TimeWindow t;
		t = (TimeWindow) process.getTimeWindowRun();
		long timeRunMillis = 0;
		try {
			timeRunMillis = t.getElapsedMillis();
			duccLogger.debug(location, jobid, "run millis = "+timeRunMillis);
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
		DecimalFormat formatter = new DecimalFormat("##0.0");
		if(type.equals("MR")) {
			// 
		}
		else {
			SynchronizedSimpleDateFormat dateFormat = new SynchronizedSimpleDateFormat("HH:mm:ss");
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
			/*
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
			sb.append("<td align=\"right\">");
			sb.append(formatter.format(pctGC));
			sb.append("</td>");
			*/
			// PgIn
			long faults = 0;
			try {
				faults = process.getMajorFaults();
			}
			catch(Exception e) {
			}
			sb.append("<td align=\"right\">");
			sb.append(faults);
			sb.append("</td>");
			// Swap
			if(process.isComplete()) {
				double swap = process.getSwapUsageMax();
				swap = swap/GB;
				String displaySwap = formatter.format(swap);
				sb.append("<td align=\"right\" "+">");
				sb.append(displaySwap);
				sb.append("</td>");
			}
			else {
				double swap = process.getSwapUsage();
				swap = swap/GB;
				String displaySwap = formatter.format(swap);
				double swapMax = process.getSwapUsageMax();
				swapMax = swapMax/GB;
				String displaySwapMax = formatter.format(swapMax);
				sb.append("<td title=\"max="+displaySwapMax+"\" align=\"right\" "+">");
				sb.append(displaySwap);
				sb.append("</td>");
			}
		}
		/*
		// Time:cpu
		sb.append("<td align=\"right\">");
		long timeCPU = process.getCpuTime();
		String fmtCPU = dateFormat.format(new Date(timeCPU));
		fmtCPU = chomp("00:", fmtCPU);
		sb.append(fmtCPU);
		sb.append("</td>");
		*/
		
		/*
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
		*/
		if(process.isComplete()) {
			double rss = process.getResidentMemoryMax();
			rss = rss/GB;
			String displayRss = formatter.format(rss);
			sb.append("<td align=\"right\" "+">");
			sb.append(displayRss);
			sb.append("</td>");
		}
		else {
			double rss = process.getResidentMemory();
			rss = rss/GB;
			String displayRss = formatter.format(rss);
			double rssMax = process.getResidentMemoryMax();
			rssMax = rssMax/GB;
			String displayRssMax = formatter.format(rssMax);
			sb.append("<td title=\"max="+displayRssMax+"\" align=\"right\" "+">");
			sb.append(displayRss);
			sb.append("</td>");
		}
		if(type.equals("SP")) {
			// 
		}
		else if(type.equals("MR")) {
			// 
		}
		else {
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
		}
		// Jconsole:Url
		if(type.equals("MR")) {
			// 
		}
		else {
			sb.append("<td>");
			switch(process.getProcessState()) {
			case Initializing:
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
		sb.append("Id: ");
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
		sb.append("Workitems: ");
		sb.append(jobWorkitemsCount);
		sb.append("&nbsp");
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
		sb.append("&nbsp");
		// error
		sb.append("<th title=\"The number of work items that failed to complete successfully\">");
		sb.append("Error: ");
		String error = "0";
		try {
			error = ""+job.getSchedulingInfo().getIntWorkItemsError();
		}
		catch(Exception e) {
		}
		sb.append(error);
		sb.append("&nbsp");
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
			// dispatch
			sb.append("<th title=\"The number of work items currently dispatched\">");
			sb.append("Dispatch: ");
			sb.append(job.getSchedulingInfo().getWorkItemsDispatched());
			sb.append("&nbsp");
			// unassigned
			sb.append("<th title=\"The number of work items currently dispatched for which acknowledgement is yet to be received\">");
			sb.append("Unassigned: ");
			sb.append(job.getSchedulingInfo().getCasQueuedMap().size());
			sb.append("&nbsp");
			// limbo
			sb.append("<th title=\"The number of work items pending re-dispatch to an alternate Job Process. Each of these work items is essentially stuck waiting for its previous JP to terminate.\">");
			sb.append("Limbo: ");
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
	
	private DuccWorkJob getManagedReservation(String reservationNo) {
		DuccWorkJob managedReservation = null;
		DuccWorkMap duccWorkMap = DuccData.getInstance().get();
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
			    		if(counter > DuccConstants.workItemsDisplayMax) {
			    			// SeqNo
							sb.append("<td align=\"right\">");
							sb.append("*****");
							// Id
							sb.append("<td align=\"right\">");
							sb.append("*****");
							// Status
							sb.append("<td align=\"right\">");
							sb.append("display");
							// Queuing Time (sec)
							sb.append("<td align=\"right\">");
							sb.append("limit");
							// Processing Time (sec)
							sb.append("<td align=\"right\">");
							sb.append("reached");
							// Node (IP)
							sb.append("<td align=\"right\">");
							sb.append("*****");
							// Node (Name)
							sb.append("<td align=\"right\">");
							sb.append("*****");
							// PID
							sb.append("<td align=\"right\">");
							sb.append("*****");
			    			duccLogger.warn(methodName, job.getDuccId(), "work items display max:"+DuccConstants.workItemsDisplayMax);
			    			break;
			    		}
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
				    //long analysisTime = 0;
				    for (Entry<String, PerformanceMetricsSummaryItem> entry : performanceMetricsSummaryMap.entrySet()) {
				    	String key = entry.getKey();
				    	int posName = key.lastIndexOf('=');
				    	long anTime = entry.getValue().getAnalysisTime();
				    	long anMinTime = entry.getValue().getAnalysisTimeMin();
				    	long anMaxTime = entry.getValue().getAnalysisTimeMax();
				    	//analysisTime += anTime;
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
					sb.append(FormatHelper.duration(ltime));
					// % of Total
					sb.append("<td align=\"right\">");
					sb.append(formatter.format(100));
					// Avg
					sb.append("<td align=\"right\">");
					ltime = (long)time_avg;
					sb.append(FormatHelper.duration(ltime));
					// Min
					sb.append("<td align=\"right\">");
					ltime = (long)time_min;
					sb.append(FormatHelper.duration(ltime));
					// Max
					sb.append("<td align=\"right\">");
					ltime = (long)time_max;
					sb.append(FormatHelper.duration(ltime));
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
						sb.append(FormatHelper.duration(ltime));
						// % of Total
						sb.append("<td align=\"right\">");
						double dtime = (time/time_total)*100;
						sb.append(formatter.format(dtime));
						// Avg
						sb.append("<td align=\"right\">");
						time = time/casCount;
						ltime = (long)time;
						sb.append(FormatHelper.duration(ltime));
						// Min
						sb.append("<td align=\"right\">");
						time = uimaStats.get(i).getAnalysisMinTime();
						ltime = (long)time;
						sb.append(FormatHelper.duration(ltime));
						// Max
						sb.append("<td align=\"right\">");
						time = uimaStats.get(i).getAnalysisMaxTime();
						ltime = (long)time;
						sb.append(FormatHelper.duration(ltime));
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
				Properties properties = DuccFile.getJobProperties(job);
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
						value = formatClasspath(value);
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
						String logsjobdir = job.getUserLogsDir()+job.getDuccId().getFriendly()+File.separator;
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
						String logsjobdir = job.getUserLogsDir()+job.getDuccId().getFriendly()+File.separator;
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
	
	private void buildServiceProcessListEntry(StringBuffer sb, DuccWorkJob job, IDuccProcess process, String type, int counter) {
		buildJobProcessListEntry(sb, job, process, type, counter);
	}
	
	private void handleDuccServletReservationProcessesData(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletReservationProcessesData";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		String reservationNo = request.getParameter("id");
		
		DuccWorkMap duccWorkMap = DuccData.getInstance().get();
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
			String type = "MR";
			iterator = managedReservation.getProcessMap().keySet().iterator();
			while(iterator.hasNext()) {
				DuccId processId = iterator.next();
				IDuccProcess process = managedReservation.getProcessMap().get(processId);
				buildServiceProcessListEntry(sb, managedReservation, process, type, ++counter);
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
				Properties properties = DuccFile.getManagedReservationProperties(managedReservation);
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
						value = formatClasspath(value);
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
	
	private void handleDuccServletServiceDeploymentsData(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletServiceDeploymentsData";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		
		String name = request.getParameter("name");
		ServicesRegistry servicesRegistry = new ServicesRegistry();
		ServicesRegistryMapPayload payload = servicesRegistry.findService(name);
		Properties properties;
		properties = payload.meta;
		
		ArrayList<String> implementors = servicesRegistry.getArrayList(properties.getProperty(IServicesRegistry.implementors));
		
		DuccWorkJob service = null;
		DuccWorkMap duccWorkMap = DuccData.getInstance().get();
		if(duccWorkMap.getServiceKeySet().size()> 0) {
			Iterator<DuccId> iterator = null;
			iterator = duccWorkMap.getServiceKeySet().iterator();
			int counter = 0;
			String type = "SP";
			while(iterator.hasNext()) {
				DuccId serviceId = iterator.next();
				String fid = ""+serviceId.getFriendly();
				if(implementors.contains(fid)) {
					service = (DuccWorkJob) duccWorkMap.findDuccWork(serviceId);
					IDuccProcessMap map = service.getProcessMap();
					for(DuccId key : map.keySet()) {
						IDuccProcess process = map.get(key);
						buildServiceProcessListEntry(sb, service, process, type, ++counter);
					}
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
	
	private void handleDuccServletServiceRegistryData(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletServiceRegistryData";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		try {
			String name = request.getParameter("name");
			ServicesRegistry servicesRegistry = new ServicesRegistry();
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
				if(!isUserAuthorized(request,resourceOwnerUserId)) {
					if(hint.length() == 0) {
						AuthorizationStatus authorizationStatus = getAuthorizationStatus(request, resourceOwnerUserId);
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
		
		String name = request.getParameter("name");
		ServicesRegistry servicesRegistry = new ServicesRegistry();
		ServicesRegistryMapPayload payload = servicesRegistry.findService(name);
		Properties properties;
		properties = payload.meta;
		// serviceid
		sb.append("<th title=\"The system assigned id for this service\">");
		sb.append("Id: ");
		sb.append(properties.getProperty(IServicesRegistry.numeric_id, "?"));
		sb.append("&nbsp");
		// name
		sb.append("<th title=\"The name for this service\">");
		sb.append("Name: ");
		sb.append(name);
		sb.append("&nbsp");
		// autostart
		sb.append("<th title=\"The configured autostart value for this service\">");
		sb.append("Autostart: ");
		sb.append(properties.getProperty(IServicesRegistry.autostart, "?"));
		sb.append("&nbsp");
		// instances
		sb.append("<th title=\"The configured number of instances for this service\">");
		sb.append("Instances: ");
		sb.append(properties.getProperty(IServicesRegistry.instances, "?"));
		sb.append("&nbsp");
		
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
		boolean authorized = isUserAuthorized(request,null);
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
		boolean authorized = isUserAuthorized(request,null);
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
		DuccWorkMap duccWorkMap = DuccData.getInstance().get();
		if(duccWorkMap.size()> 0) {
			sb.append(getDuccWebServer().getClusterName());
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
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletReservationSchedulingCLasses";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		sb.append("<select id=\"scheduling_class\">");
		DuccWebSchedulerClasses schedulerClasses = new DuccWebSchedulerClasses(getFileName());
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
		sb.append("<select id=\"instance_memory_size\">");
		int shareSize = DuccConstants.defaultShareSize;
		try {
			shareSize = Integer.parseInt(DuccWebProperties.getProperty(DuccWebProperties.key_ducc_rm_share_quantum, DuccWebProperties.val_ducc_rm_share_quantum));
		}
		catch(Throwable t) {
			duccLogger.warn(methodName, jobid, t);
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
	
	private void handleDuccServletReservationFormButton(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleDuccServletReservationFormButton";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		String button = "<button style=\"font-size:8pt; background-color:green; color:ffffff;\" onclick=\"var newWin = window.open('submit.reservation.html','child','height=550,width=550,scrollbars'); newWin.focus(); return false;\">Request<br>Reservation</button>";
		if(!isAuthenticated(request,response)) {
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
			String result;
			IDuccWorkJob duccWorkJob = (IDuccWorkJob) duccWorkMap.findDuccWork(DuccType.Job, value);
			if(duccWorkJob != null) {
				String resourceOwnerUserId = duccWorkJob.getStandardInfo().getUser().trim();
				if(isUserAuthorized(request,resourceOwnerUserId)) {
					String arg1 = "-"+name;
					String arg2 = value;
					String userId = duccWebSessionManager.getUserId(request);
					String cp = System.getProperty("java.class.path");
					String java = "/bin/java";
					String jclass = "org.apache.uima.ducc.cli.DuccJobCancel";
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
				String userId = duccWebSessionManager.getUserId(request);
				String cp = System.getProperty("java.class.path");
				String java = "/bin/java";
				String jclass = "org.apache.uima.ducc.cli.DuccReservationSubmit";
				String jhome = System.getProperty("java.home");
				String[] arglist = { "-u", userId, "--", jhome+java, "-cp", cp, jclass, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8 };
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
			DuccWorkMap duccWorkMap = duccData.get();
			String text;
			String result;
			IDuccWorkReservation duccWorkReservation = (IDuccWorkReservation) duccWorkMap.findDuccWork(DuccType.Reservation, value);
			if(duccWorkReservation != null) {
				String resourceOwnerUserId = duccWorkReservation.getStandardInfo().getUser().trim();
				if(isUserAuthorized(request,resourceOwnerUserId)) {
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
				DuccWorkMap duccWorkMap = duccData.get();
				String text;
				String result;
				IDuccWorkJob duccWorkJob = (IDuccWorkJob) duccWorkMap.findDuccWork(DuccType.Service, value);
				if(duccWorkJob != null) {
					String resourceOwnerUserId = duccWorkJob.getStandardInfo().getUser().trim();
					if(isUserAuthorized(request,resourceOwnerUserId)) {
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
			ServicesRegistry servicesRegistry = new ServicesRegistry();
			ServicesRegistryMapPayload payload = servicesRegistry.findService(name);
			if(payload != null) {
				Properties properties = payload.meta;
				String id = properties.getProperty(IServicesRegistry.numeric_id);
				String resourceOwnerUserId = servicesRegistry.findServiceUser(id);
				if(resourceOwnerUserId != null) {
					if(isUserAuthorized(request,resourceOwnerUserId)) {
						String arg1 = "--"+command;
						String arg2 = id;
						String userId = duccWebSessionManager.getUserId(request);
						String cp = System.getProperty("java.class.path");
						String java = "/bin/java";
						String jclass = "org.apache.uima.ducc.cli.DuccServiceApi";
						String jhome = System.getProperty("java.home");
						DuccCookies.RequestRole requestRole = DuccCookies.getRole(request);
						switch(requestRole) {
						/*
						case Administrator:
							String arg3 = "--"+SpecificationProperties.key_role_administrator;
							String[] arglistAdministrator = { "-u", userId, "--", jhome+java, "-cp", cp, jclass, arg1, arg2, arg3 };
							result = DuccAsUser.duckling(userId, arglistAdministrator);
							response.getWriter().println(result);
							break;
						case User:
						*/
						default:
							ArrayList<String> arglist = new ArrayList<String>();
							arglist.add("-u");
							arglist.add(userId);
							arglist.add("--");
							arglist.add(jhome+java);
							arglist.add("-cp");
							arglist.add(cp);
							arglist.add(jclass);
							arglist.add(arg1);
							arglist.add(arg2);
							for(String parm : parms) {
								arglist.add(parm);
							}
							String[] arglistUser = arglist.toArray(new String[0]);
							result = DuccAsUser.duckling(userId, arglistUser);
							response.getWriter().println(result);
							break;	
						}
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
	
	private void duccServletServiceCommand(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response, String command) 
	{
		String methodName = "duccServletServiceCommand";
		duccLogger.trace(methodName, null, messages.fetch("enter"));
		try {
			String name = "id";
			String value = request.getParameter(name).trim();
			duccLogger.info(methodName, null, command+" "+messages.fetchLabel("id:")+value);
			String text;
			String result;
			String id = value.trim();
			ServicesRegistry servicesRegistry = new ServicesRegistry();
			String resourceOwnerUserId = servicesRegistry.findServiceUser(id);
			if(resourceOwnerUserId != null) {
				if(isUserAuthorized(request,resourceOwnerUserId)) {
					String arg1 = "--"+command;
					String arg2 = id;
					String userId = duccWebSessionManager.getUserId(request);
					String cp = System.getProperty("java.class.path");
					String java = "/bin/java";
					String jclass = "org.apache.uima.ducc.cli.DuccServiceApi";
					String jhome = System.getProperty("java.home");
					DuccCookies.RequestRole requestRole = DuccCookies.getRole(request);
					switch(requestRole) {
					/*
					case Administrator:
						String arg3 = "--"+SpecificationProperties.key_role_administrator;
						String[] arglistAdministrator = { "-u", userId, "--", jhome+java, "-cp", cp, jclass, arg1, arg2, arg3 };
						result = DuccAsUser.duckling(userId, arglistAdministrator);
						response.getWriter().println(result);
						break;
					case User:
					*/
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
			else if(reqURI.startsWith(duccReservationInstanceMemorySizes)) {
				handleDuccServletReservationInstanceMemorySizes(target, baseRequest, request, response);
				DuccWebUtil.noCache(response);
			}
			else if(reqURI.startsWith(duccReservationNumberOfInstances)) {
				handleDuccServletReservationNumberOfInstances(target, baseRequest, request, response);
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
