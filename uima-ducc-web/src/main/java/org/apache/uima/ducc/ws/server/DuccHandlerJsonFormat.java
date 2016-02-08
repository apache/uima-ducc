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
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.uima.ducc.cli.ws.json.MachineFacts;
import org.apache.uima.ducc.cli.ws.json.MachineFactsList;
import org.apache.uima.ducc.cli.ws.json.NodePidList;
import org.apache.uima.ducc.cli.ws.json.ReservationFacts;
import org.apache.uima.ducc.cli.ws.json.ReservationFactsList;
import org.apache.uima.ducc.common.IDuccEnv;
import org.apache.uima.ducc.common.NodeConfiguration;
import org.apache.uima.ducc.common.SizeBytes;
import org.apache.uima.ducc.common.boot.DuccDaemonRuntimeProperties;
import org.apache.uima.ducc.common.boot.DuccDaemonRuntimeProperties.DaemonName;
import org.apache.uima.ducc.common.internationalization.Messages;
import org.apache.uima.ducc.common.utils.ComponentHelper;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.DuccProperties;
import org.apache.uima.ducc.common.utils.DuccPropertiesResolver;
import org.apache.uima.ducc.common.utils.DuccSchedulerClasses;
import org.apache.uima.ducc.common.utils.TimeStamp;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.orchestrator.jd.scheduler.JdReservation;
import org.apache.uima.ducc.transport.Constants;
import org.apache.uima.ducc.transport.event.common.DuccWorkJob;
import org.apache.uima.ducc.transport.event.common.DuccWorkReservation;
import org.apache.uima.ducc.transport.event.common.IDuccPerWorkItemStatistics;
import org.apache.uima.ducc.transport.event.common.IDuccProcess;
import org.apache.uima.ducc.transport.event.common.IDuccReservation;
import org.apache.uima.ducc.transport.event.common.IDuccReservationMap;
import org.apache.uima.ducc.transport.event.common.IDuccSchedulingInfo;
import org.apache.uima.ducc.transport.event.common.IDuccState.ReservationState;
import org.apache.uima.ducc.transport.event.common.IDuccWork;
import org.apache.uima.ducc.transport.event.common.IDuccWorkJob;
import org.apache.uima.ducc.transport.event.common.IRationale;
import org.apache.uima.ducc.transport.event.common.JdReservationBean;
import org.apache.uima.ducc.ws.DuccDaemonsData;
import org.apache.uima.ducc.ws.DuccData;
import org.apache.uima.ducc.ws.DuccMachinesData;
import org.apache.uima.ducc.ws.Info;
import org.apache.uima.ducc.ws.JobInfo;
import org.apache.uima.ducc.ws.MachineInfo;
import org.apache.uima.ducc.ws.ReservationInfo;
import org.apache.uima.ducc.ws.helper.BrokerHelper;
import org.apache.uima.ducc.ws.helper.BrokerHelper.FrameworkAttribute;
import org.apache.uima.ducc.ws.helper.BrokerHelper.JmxAttribute;
import org.apache.uima.ducc.ws.helper.DatabaseHelper;
import org.apache.uima.ducc.ws.registry.ServiceInterpreter.StartState;
import org.apache.uima.ducc.ws.registry.ServicesRegistry;
import org.apache.uima.ducc.ws.registry.sort.IServiceAdapter;
import org.apache.uima.ducc.ws.registry.sort.ServicesHelper;
import org.apache.uima.ducc.ws.registry.sort.ServicesSortCache;
import org.apache.uima.ducc.ws.server.DuccCookies.DisplayStyle;
import org.apache.uima.ducc.ws.server.IWebMonitor.MonitorType;
import org.apache.uima.ducc.ws.types.Ip;
import org.apache.uima.ducc.ws.types.NodeId;
import org.apache.uima.ducc.ws.types.UserId;
import org.apache.uima.ducc.ws.utils.FormatHelper.Precision;
import org.eclipse.jetty.server.Request;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

public class DuccHandlerJsonFormat extends DuccAbstractHandler {
	
	private static DuccLogger duccLogger = DuccLoggerComponents.getWsLogger(DuccHandlerJsonFormat.class.getName());
	private static Messages messages = Messages.getInstance();
	private static DuccId jobid = null;

	private static BrokerHelper brokerHelper = BrokerHelper.getInstance();
	private static DatabaseHelper databaseHelper = DatabaseHelper.getInstance();
	
	//private static PagingObserver pagingObserver = PagingObserver.getInstance();
	
	private final String jsonFormatJobsAaData					= duccContextJsonFormat+"-aaData-jobs";
	private final String jsonFormatReservationsAaData			= duccContextJsonFormat+"-aaData-reservations";
	private final String jsonFormatServicesAaData				= duccContextJsonFormat+"-aaData-services";
	private final String jsonFormatMachinesAaData				= duccContextJsonFormat+"-aaData-machines";
	private final String jsonFormatBrokerAaData					= duccContextJsonFormat+"-aaData-broker";
	private final String jsonFormatClassesAaData				= duccContextJsonFormat+"-aaData-classes";
	private final String jsonFormatDaemonsAaData				= duccContextJsonFormat+"-aaData-daemons";
	private final String jsonFormatDaemonsAaDataAll				= duccContextJsonFormat+"-aaData-daemons-all";
	
	private final String jsonFormatMachines 		= duccContextJsonFormat+"-machines";
	private final String jsonFormatReservations 	= duccContextJsonFormat+"-reservations";
	
	public DuccHandlerJsonFormat(DuccWebServer duccWebServer) {
		super.init(duccWebServer);
	}
	
	public String getFileName() {
		return dir_home+File.separator+dir_resources+File.separator+getDuccWebServer().getClassDefinitionFile();
	}
	
	private JsonArray buildJobRow(HttpServletRequest request, IDuccWorkJob job, DuccData duccData, long now, ServicesRegistry servicesRegistry) {
		JsonArray row = new JsonArray();
		StringBuffer sb;
		DuccId duccId = job.getDuccId();
		// Terminate
		sb = new StringBuffer();
		String id = normalize(duccId);
		sb.append("<span class=\"ducc-col-terminate\">");
		if(terminateEnabled) {
			if(!job.isFinished()) {
				sb.append("<input type=\"button\" onclick=\"ducc_confirm_terminate_job("+id+")\" value=\"Terminate\" "+getDisabledWithHover(request,job)+"/>");
			}
		}
		sb.append("</span>");
		row.add(new JsonPrimitive(sb.toString()));
		// Id
		sb = new StringBuffer();
		sb.append("<span>");
		sb.append("<a href=\"job.details.html?id="+id+"\">"+id+"</a>");
		sb.append("</span>");
		row.add(new JsonPrimitive(sb.toString()));
		// Start
		sb = new StringBuffer();
		sb.append("<span title=\""+DuccConstants.hintPreferencesDateStyle+"\">");
		sb.append(getTimeStamp(request,job.getDuccId(), job.getStandardInfo().getDateOfSubmission()));
		sb.append("</span>");
		row.add(new JsonPrimitive(sb.toString()));
		// Duration
		sb = new StringBuffer();
		if(job.isCompleted()) {
			String duration = getDuration(request,job,Precision.Whole);
			String decoratedDuration = decorateDuration(request,job, duration,Precision.Whole);
			sb.append("<span>");
			sb.append(decoratedDuration);
			sb.append("</span>");
		}
		else {
			String duration = getDuration(request,job,now,Precision.Whole);
			String decoratedDuration = decorateDuration(request,job,duration,Precision.Whole);
			sb.append("<span class=\"health_green\""+">");
			sb.append(decoratedDuration);
			sb.append("</span>");
			String projection = getProjection(request,job,Precision.Whole);
			sb.append(projection);
		}
		row.add(new JsonPrimitive(sb.toString()));
		// User
		sb = new StringBuffer();
		String title = "";
		String submitter = job.getStandardInfo().getSubmitter();
		if(submitter != null) {
			title = "title=\"submitter PID@host: "+submitter+"\" ";
		}
		sb.append("<span "+title+">");
		sb.append(job.getStandardInfo().getUser());
		sb.append("</span>");
		row.add(new JsonPrimitive(sb.toString()));
		// Class
		sb = new StringBuffer();
		String schedulingClass = stringNormalize(job.getSchedulingInfo().getSchedulingClass(),messages.fetch("default"));
		long debugPortDriver = job.getDebugPortDriver();
		long debugPortProcess = job.getDebugPortProcess();
		title = "debug ports:";
		if(debugPortDriver >= 0) {
			title = title + " driver="+debugPortDriver;
		}
		if(debugPortProcess >= 0) {
			title = title + " process="+debugPortProcess;
		}
		switch(DuccCookies.getDisplayStyle(request)) {
		case Textual:
		default:
			sb.append(schedulingClass);
			if((debugPortDriver >= 0) || (debugPortProcess >= 0)) {
				sb.append("<br>");
				if(job.isCompleted()) {
					sb.append("<span class=\"health_red\""+">");
				}
				else {
					sb.append("<span class=\"health_green\""+">");
				}
				sb.append("<div title=\""+title+"\">DEBUG</div>");
				sb.append("</span>");
			}
			break;
		case Visual:
			// Below
			String key = "bug";
			String bugFile = DuccWebServerHelper.getImageFileName(key);
			sb.append(schedulingClass);
			if((debugPortDriver >= 0) || (debugPortProcess >= 0)) {
				sb.append("<br>");
				if(job.isCompleted()) {
					sb.append("<span class=\"health_red\""+">");
				}
				else {
					sb.append("<span class=\"health_green\""+">");
				}
				if(bugFile != null) {
					sb.append("<div title=\""+title+"\"><img src=\""+bugFile+"\"></div>");
				}
				sb.append("</span>");
			}
			break;
		}	
		row.add(new JsonPrimitive(sb.toString()));
		// State
		sb = new StringBuffer();
		String state = job.getStateObject().toString();
		sb.append("<span>");
		if(duccData.isLive(duccId)) {
			if(job.isOperational()) {
				sb.append("<span class=\"active_state\">");
			}
			else {
				sb.append("<span class=\"completed_state\">");
			}
			sb.append(state);
			sb.append("</span>");
		}
		else {
			sb.append("<span class=\"historic_state\">");
			sb.append(state);
			sb.append("</span>");
		}
		sb.append("</span>");
		row.add(new JsonPrimitive(sb.toString()));
		// Reason
		sb = getReason(job,MonitorType.Job);
		row.add(new JsonPrimitive(sb.toString()));
		// Services
		sb = new StringBuffer();
		sb.append(evaluateServices(job,servicesRegistry));
		row.add(new JsonPrimitive(sb.toString()));
		// Processes
		sb = new StringBuffer();
		sb.append("<span>");
		if(duccData.isLive(duccId)) {
			sb.append(job.getProcessMap().getAliveProcessCount());
		}
		else {
			sb.append("0");
		}
		sb.append("</span>");
		row.add(new JsonPrimitive(sb.toString()));
		// Init Fails
		sb = new StringBuffer();
		long initFails = job.getProcessInitFailureCount();
		if(initFails > 0) {
			if(job.getSchedulingInfo().getLongProcessesMax() < 0) {
				DisplayStyle style = DuccCookies.getDisplayStyle(request);
				String key = "cap.small";
				String capFile = DuccWebServerHelper.getImageFileName(key);
				switch(style) {
					case Visual:
						if(capFile == null) {
							style = DisplayStyle.Textual;
						}
						break;
				}
				switch(style) {
				case Textual:
				default:
					sb.append(buildInitializeFailuresLink(job));
					sb.append("<span title=\"capped at current number of running processes due to excessive initialization failures\">");
					sb.append("<sup>");
					sb.append("<small>");
					sb.append("capped");
					sb.append("</small>");
					sb.append("<sup>");
					sb.append("</span>");
					sb.append("<br>");
					break;
				case Visual:
					sb.append("<span title=\"capped at current number of running processes due to excessive initialization failures\">");
					sb.append("<img src=\""+capFile+"\">");
					sb.append("</span>");
					sb.append("<br>");
					sb.append(buildInitializeFailuresLink(job));
					break;
				}
			}
			else {
				sb.append(buildInitializeFailuresLink(job));
			}
		}
		else {
			sb.append(""+initFails);
		}
		row.add(new JsonPrimitive(sb.toString()));
		// Run Fails
		sb = new StringBuffer();
		sb.append("<span>");
		sb.append(buildRuntimeFailuresLink(job));
		sb.append("</span>");
		row.add(new JsonPrimitive(sb.toString()));
		// Pgin
		sb = new StringBuffer();
		sb.append("<span>");
		long faults = 0;
		try {
			faults = job.getPgInCount();
		}
		catch(Exception e) {
		}
		double swapping = job.getSwapUsageGbMax();
		if((swapping * faults) > 0) {
			sb.append("<span class=\"health_red\""+">");
		}
		else {
			sb.append("<span class=\"health_black\""+">");
		}
		sb.append(faults);
		sb.append("</span>");
		sb.append("</span>");
		row.add(new JsonPrimitive(sb.toString()));
		// Swap
		DecimalFormat formatter = new DecimalFormat("###0.0");
		sb = new StringBuffer();
		sb.append("<span>");
		double swap = job.getSwapUsageGb();
		if(job.isCompleted()) {
			swap = job.getSwapUsageGbMax();
		}
		String displaySwapMax = formatter.format(swap);
		if(swap > 0) {
			sb.append("<span class=\"health_red\""+">");
		}
		else {
			sb.append("<span class=\"health_black\""+">");
		}
		sb.append(displaySwapMax);
		sb.append("</span>");
		sb.append("</span>");
		row.add(new JsonPrimitive(sb.toString()));
		// Memory
		sb = new StringBuffer();
		IDuccSchedulingInfo si;
		SizeBytes sizeBytes;
		String requested;
		String actual;
		si = job.getSchedulingInfo();
		sizeBytes = new SizeBytes(SizeBytes.Type.Bytes, si.getMemorySizeAllocatedInBytes());
		actual = getProcessMemorySize(duccId,sizeBytes);
		sizeBytes = new SizeBytes(si.getMemoryUnits().name(), Long.parseLong(si.getMemorySizeRequested()));
		requested = getProcessMemorySize(duccId,sizeBytes);
		sb.append("<span title=\""+"requested: "+requested+"\">");
		sb.append(actual);
		sb.append("</span>");
		row.add(new JsonPrimitive(sb.toString()));
		// Total
		sb = new StringBuffer();
		sb.append("<span>");
		sb.append(job.getSchedulingInfo().getWorkItemsTotal());
		sb.append("</span>");
		row.add(new JsonPrimitive(sb.toString()));
		// Done
		sb = new StringBuffer();
		sb.append("<span>");
		IDuccPerWorkItemStatistics perWorkItemStatistics = job.getSchedulingInfo().getPerWorkItemStatistics();
		String done = job.getSchedulingInfo().getWorkItemsCompleted();
		if (perWorkItemStatistics != null) {
			double max = Math.round(perWorkItemStatistics.getMax()/100.0)/10.0;
			double min = Math.round(perWorkItemStatistics.getMin()/100.0)/10.0;
			double avg = Math.round(perWorkItemStatistics.getMean()/100.0)/10.0;
			double dev = Math.round(perWorkItemStatistics.getStandardDeviation()/100.0)/10.0;
			done = "<span title=\""+"seconds-per-work-item "+"Max:"+max+" "+"Min:"+min+" "+"Avg:"+avg+" "+"StdDev:"+dev+"\""+">"+done+"</span>";
		}
		sb.append(done);
		sb.append("</span>");
		row.add(new JsonPrimitive(sb.toString()));
		// Error
		sb = new StringBuffer();
		sb.append("<span>");
		sb.append(buildErrorLink(job));
		sb.append("</span>");
		row.add(new JsonPrimitive(sb.toString()));
		// Dispatch
		sb = new StringBuffer();
		String d0 = "<span>";
		String d1 = "0";
		String d2 = "</span>";
		if(duccData.isLive(duccId)) {
			int dispatch = 0;
			int unassigned = job.getSchedulingInfo().getCasQueuedMap().size();
			try {
				dispatch = Integer.parseInt(job.getSchedulingInfo().getWorkItemsDispatched())-unassigned;
			}
			catch(Exception e) {
			}
			if(dispatch < 0) {
				d0 = "<span class=\"health_red\""+" title=\"unassigned location count: "+(0-dispatch)+"\">";
				//d1 = "0";
			}
			else {
				d1 = ""+dispatch;
			}
		}
		sb.append(d0);
		sb.append(d1);
		sb.append(d2);
		row.add(new JsonPrimitive(sb.toString()));
		// Retry
		sb = new StringBuffer();
		sb.append("<span>");
		sb.append(job.getSchedulingInfo().getWorkItemsRetry());
		sb.append("</span>");
		row.add(new JsonPrimitive(sb.toString()));
		// Preempt
		sb = new StringBuffer();
		sb.append("<span>");
		sb.append(job.getSchedulingInfo().getWorkItemsPreempt());
		sb.append("</span>");
		row.add(new JsonPrimitive(sb.toString()));
		// Description
		sb = new StringBuffer();
		String description = stringNormalize(job.getStandardInfo().getDescription(),messages.fetch("none"));
		switch(DuccCookies.getDescriptionStyle(request)) {
		case Long:
		default:
			sb.append("<span title=\""+DuccConstants.hintPreferencesDescriptionStyleShort+"\">");
			sb.append(description);
			sb.append("</span>");
			break;
		case Short:
			String shortDescription = getShortDescription(description);
			if(shortDescription == null) {
				sb.append("<span>");
				sb.append(description);
				sb.append("</span>");
			}
			else {
				sb.append("<span title=\""+description+"\">");
				sb.append(shortDescription);
				sb.append("</span>");
			}
			break;
		}
		row.add(new JsonPrimitive(sb.toString()));
		
		return row;
	}
	
	private void handleServletJsonFormatJobsAaData(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleServletJsonFormatJobsAaData";
		duccLogger.trace(methodName, jobid, messages.fetch("enter"));
		
		JsonObject jsonResponse = new JsonObject();
		JsonArray data = new JsonArray();
		
		ServicesRegistry servicesRegistry = ServicesRegistry.getInstance();
		
		long now = System.currentTimeMillis();
		
		int maxRecords = getJobsMax(request);
		ArrayList<String> users = getJobsUsers(request);
		DuccData duccData = DuccData.getInstance();
		ConcurrentSkipListMap<JobInfo,JobInfo> sortedJobs = duccData.getSortedJobs();
		if(sortedJobs.size()> 0) {
			Iterator<Entry<JobInfo, JobInfo>> iterator = sortedJobs.entrySet().iterator();
			int counter = 0;
			while(iterator.hasNext()) {
				JobInfo jobInfo = iterator.next().getValue();
				DuccWorkJob job = jobInfo.getJob();
				boolean list = DuccWebUtil.isListable(request, users, maxRecords, counter, job);
				if(list) {
					counter++;
					JsonArray row = buildJobRow(request, job, duccData, now, servicesRegistry);
					data.add(row);
				}
			}
		}
		else {
			JsonArray row = new JsonArray();
			if(DuccData.getInstance().isPublished()) {
				// Terminate
				row.add(new JsonPrimitive("no jobs"));
			}
			else {
				// Terminate
				row.add(new JsonPrimitive("no data"));
			}
			// Id
			row.add(new JsonPrimitive(""));
			// Start
			row.add(new JsonPrimitive(""));
			// Duration
			row.add(new JsonPrimitive(""));
			// User
			row.add(new JsonPrimitive(""));
			// Class
			row.add(new JsonPrimitive(""));
			// State
			row.add(new JsonPrimitive(""));
			// Reason
			row.add(new JsonPrimitive(""));
			// Services
			row.add(new JsonPrimitive(""));
			// Processes
			row.add(new JsonPrimitive(""));
			// Init Fails
			row.add(new JsonPrimitive(""));
			// Run Fails
			row.add(new JsonPrimitive(""));
			// Pgin
			row.add(new JsonPrimitive(""));
			// Swap
			row.add(new JsonPrimitive(""));
			// Size
			row.add(new JsonPrimitive(""));			
			// Total
			row.add(new JsonPrimitive(""));
			// Done
			row.add(new JsonPrimitive(""));
			// Error
			row.add(new JsonPrimitive(""));
			// Dispatch
			row.add(new JsonPrimitive(""));
			// Retry
			row.add(new JsonPrimitive(""));
			// Preempt
			row.add(new JsonPrimitive(""));
			// Description
			row.add(new JsonPrimitive(""));
			data.add(row);
		}
		
		jsonResponse.add("aaData", data);
		
		String json = jsonResponse.toString();
		duccLogger.debug(methodName, jobid, json);
		response.getWriter().println(json);
		response.setContentType("application/json");
		
		duccLogger.trace(methodName, jobid, messages.fetch("exit"));
	}
	
	
	private JsonArray buildReservationRow(HttpServletRequest request, IDuccWork duccwork, DuccData duccData, long now) {
		JsonArray row = new JsonArray();
		String reservationType = "Unmanaged";
		if(duccwork instanceof DuccWorkJob) {
			reservationType = "Managed";
		}
		StringBuffer sb;
		DuccId duccId = duccwork.getDuccId();
		// Terminate
		sb = new StringBuffer();
		String id = normalize(duccId);
		sb.append("<span class=\"ducc-col-terminate\">");
		if(terminateEnabled) {
			if(!duccwork.isCompleted()) {
				String disabled = getDisabledWithHover(request,duccwork);
				String user = duccwork.getStandardInfo().getUser();
				if(user != null) {
					DuccPropertiesResolver dpr = DuccPropertiesResolver.getInstance();
					String jdHostUser = dpr.getCachedProperty(DuccPropertiesResolver.ducc_jd_host_user);
					// We presume that user is sufficient to identify JD allocation
					if(user.equals(jdHostUser)) {
						disabled = "disabled=\"disabled\"";
					}
				}
				sb.append("<input type=\"button\" onclick=\"ducc_confirm_terminate_reservation("+id+")\" value=\"Terminate\" "+disabled+"/>");
			}
		}
		sb.append("</span>");
		row.add(new JsonPrimitive(sb.toString()));
		// Id
		sb = new StringBuffer();
		if(reservationType.equals("Managed")) {
			sb.append("<span>");
			sb.append("<a href=\"reservation.details.html?id="+id+"\">"+id+"</a>");
			sb.append("</span>");
		}
		else {
			sb.append("<span>");
			sb.append(id);
			sb.append("</span>");
		}
		row.add(new JsonPrimitive(sb.toString()));
		// Start
		sb = new StringBuffer();
		sb.append("<span title=\""+DuccConstants.hintPreferencesDateStyle+"\">");
		sb.append(getTimeStamp(request,duccwork.getDuccId(), duccwork.getStandardInfo().getDateOfSubmission()));
		sb.append("</span>");
		row.add(new JsonPrimitive(sb.toString()));
		// Duration
		sb = new StringBuffer();
		if(duccwork instanceof DuccWorkReservation) {
			DuccWorkReservation reservation = (DuccWorkReservation) duccwork;
			switch(reservation.getReservationState()) {
			case Completed:
				sb.append("<span>");
				String duration = getDuration(request,reservation,Precision.Whole);
				String decoratedDuration = decorateDuration(request,reservation,duration);
				sb.append(decoratedDuration);
				sb.append("</span>");
				break;
			default:
				sb.append("<span class=\"health_green\""+">");
				duration = getDuration(request,reservation,now,Precision.Whole);
				decoratedDuration = decorateDuration(request,reservation,duration);
				sb.append(decoratedDuration);
				sb.append("</span>");
				break;
			}
		}
		else if(duccwork instanceof DuccWorkJob) {
			DuccWorkJob job = (DuccWorkJob) duccwork;
			switch(job.getJobState()) {
			case Completed:
				sb.append("<span>");
				String duration = getDuration(request,job,Precision.Whole);
				String decoratedDuration = decorateDuration(request,job,duration,Precision.Whole);
				sb.append(decoratedDuration);
				sb.append("</span>");
				break;
			default:
				sb.append("<span class=\"health_green\""+">");
				duration = getDuration(request,job,now,Precision.Whole);
				decoratedDuration = decorateDuration(request,job,duration,Precision.Whole);
				sb.append(decoratedDuration);
				sb.append("</span>");
				break;
			}
		}
		row.add(new JsonPrimitive(sb.toString()));
		// User
		sb = new StringBuffer();
		String title = "";
		String submitter = duccwork.getStandardInfo().getSubmitter();
		if(submitter != null) {
			title = "title=\"submitter PID@host: "+submitter+"\"";
		}
		sb.append("<span "+title+">");
		UserId userId = new UserId(duccwork.getStandardInfo().getUser());
		sb.append(userId.toString());
		sb.append("</span>");
		row.add(new JsonPrimitive(sb.toString()));
		// Class
		row.add(new JsonPrimitive(stringNormalize(duccwork.getSchedulingInfo().getSchedulingClass(),messages.fetch("default"))));
		// Type
		sb = new StringBuffer();
		sb.append(reservationType);
		row.add(new JsonPrimitive(sb.toString()));
		// State
		sb = new StringBuffer();
		String state = duccwork.getStateObject().toString();
		sb.append("<span>");
		if(duccData.isLive(duccId)) {
			if(duccwork.isOperational()) {
				sb.append("<span class=\"active_state\">");
			}
			else {
				sb.append("<span class=\"completed_state\">");
			}
			sb.append(state);
			sb.append("</span>");
		}
		else {
			sb.append("<span class=\"historic_state\">");
			sb.append(state);
			sb.append("</span>");
		}
		sb.append("</span>");
		row.add(new JsonPrimitive(sb.toString()));
		// Reason
		sb = new StringBuffer();
		if(duccwork instanceof DuccWorkReservation) {
			DuccWorkReservation reservation = (DuccWorkReservation) duccwork;
			switch(reservation.getReservationState()) {
			case WaitingForResources:
				String rmReason = reservation.getRmReason();
				if(rmReason != null) {
					sb.append("<span>");
					sb.append(rmReason);
					sb.append("</span>");
				}
				break;
			case Assigned:
				List<JdReservationBean> list = reservation.getJdReservationBeanList();
				long inuse = 0;
				long total = 0;
				if(list != null) {
					for(JdReservationBean jdReservationBean : list) {
						JdReservation jdReservation = (JdReservation) jdReservationBean;
						inuse += jdReservation.getSlicesInuse();
						total += jdReservation.getSlicesTotal();
					}
					title = "title=\"the number of job driver allocations inuse for this reservation\"";
					sb.append("<span "+title+">");
					sb.append("inuse: "+inuse);
					sb.append("</span>");
					sb.append(" ");
					title = "title=\"the number of job driver allocations maximum capacity for this reservation\"";
					sb.append("<span "+title+">");
					sb.append("limit: "+total);
					sb.append("</span>");
				}
				break;
			default:
				switch(reservation.getCompletionType()) {
				case Undefined:
					break;
				case CanceledByUser:
				case CanceledByAdmin:
					try {
						String cancelUser = duccwork.getStandardInfo().getCancelUser();
						if(cancelUser != null) {
							sb.append("<span title=\"canceled by "+cancelUser+"\">");
							sb.append(duccwork.getCompletionTypeObject().toString());
							sb.append("</span>");
						}
						else {							
							IRationale rationale = reservation.getCompletionRationale();
							if(rationale != null) {
								sb.append("<span title="+rationale.getTextQuoted()+">");
								sb.append(duccwork.getCompletionTypeObject().toString());
								sb.append("</span>");
							}
							else {
								sb.append(duccwork.getCompletionTypeObject().toString());
							}
							
						}
					} 
					catch(Exception e) {
						IRationale rationale = reservation.getCompletionRationale();
						if(rationale != null) {
							sb.append("<span title="+rationale.getTextQuoted()+">");
							sb.append(duccwork.getCompletionTypeObject().toString());
							sb.append("</span>");
						}
						else {
							sb.append(duccwork.getCompletionTypeObject().toString());
						}
					}
					break;
				default:
					IRationale rationale = reservation.getCompletionRationale();
					if(rationale != null) {
						sb.append("<span title="+rationale.getTextQuoted()+">");
						sb.append(duccwork.getCompletionTypeObject().toString());
						sb.append("</span>");
					}
					else {
						sb.append(duccwork.getCompletionTypeObject().toString());
					}
					break;
				}
				break;
			}
		}
		else if(duccwork instanceof DuccWorkJob) {
			// Reason
			DuccWorkJob job = (DuccWorkJob) duccwork;
			sb = getReason(job,MonitorType.ManagedReservation);
		}
		row.add(new JsonPrimitive(sb.toString()));
		// Allocation
		/*
		sb = new StringBuffer();
		sb.append("<span>");
		sb.append(duccwork.getSchedulingInfo().getInstancesCount());
		sb.append("</span>");
		row.add(new JsonPrimitive(sb.toString()));
		*/
		// User Processes
		sb = new StringBuffer();
		TreeMap<String,Integer> nodeMap = new TreeMap<String,Integer>();
		if(duccwork instanceof DuccWorkReservation) {
			DuccWorkReservation reservation = (DuccWorkReservation) duccwork;
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
			
			boolean qualify = false;
			if(!nodeMap.isEmpty()) {
				if(nodeMap.keySet().size() > 1) {
					qualify = true;
				}
			}
			ArrayList<String> qualifiedPids = new ArrayList<String>();
			if(duccwork.isOperational()) {
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
		}
		else {
			DuccWorkJob job = (DuccWorkJob) duccwork;
			if(job.isOperational()) {
				sb.append(duccwork.getSchedulingInfo().getInstancesCount());
			}
			else {
				sb.append("0");
			}
			Iterator<DuccId> iterator = job.getProcessMap().keySet().iterator();
			while(iterator.hasNext()) {
				DuccId processId = iterator.next();
				IDuccProcess process = job.getProcessMap().get(processId);
				String node = process.getNodeIdentity().getName();
				nodeMap.put(node, 1);
			}
		}
		row.add(new JsonPrimitive(sb.toString()));
		// PgIn
		sb = new StringBuffer();
		sb.append("<span>");
		if(duccwork instanceof DuccWorkJob) {
			DuccWorkJob job = (DuccWorkJob) duccwork;
			long faults = 0;
			try {
				faults = job.getPgInCount();
			}
			catch(Exception e) {
			}
			double swapping = job.getSwapUsageGbMax();
			if((swapping * faults) > 0) {
				sb.append("<span class=\"health_red\""+">");
			}
			else {
				sb.append("<span class=\"health_black\""+">");
			}
			sb.append(faults);
			sb.append("</span>");
		}
		sb.append("</span>");
		row.add(new JsonPrimitive(sb.toString()));
		// Swap
		sb = new StringBuffer();
		String swap = "";
		double dswap = 0;
		if(duccwork instanceof DuccWorkJob) {
			DecimalFormat formatter = new DecimalFormat("###0.0");
			DuccWorkJob job = (DuccWorkJob) duccwork;
			dswap = job.getSwapUsageGb();
			if(job.isCompleted()) {
				dswap = job.getSwapUsageGbMax();
			}
			swap = formatter.format(dswap);
		}
		sb.append("<span>");
		if(dswap > 0) {
			sb.append("<span class=\"health_red\""+">");
		}
		else {
			sb.append("<span class=\"health_black\""+">");
		}
		sb.append(swap);
		sb.append("</span>");
		sb.append("</span>");
		row.add(new JsonPrimitive(sb.toString()));
		// Memory
		sb = new StringBuffer();
		IDuccSchedulingInfo si;
		SizeBytes sizeBytes;
		String requested;
		String actual;
		si = duccwork.getSchedulingInfo();
		sizeBytes = new SizeBytes(SizeBytes.Type.Bytes, si.getMemorySizeAllocatedInBytes());
		actual = getProcessMemorySize(duccId,sizeBytes);
		sizeBytes = new SizeBytes(si.getMemoryUnits().name(), Long.parseLong(si.getMemorySizeRequested()));
		requested = getProcessMemorySize(duccId,sizeBytes);
		sb.append("<span title=\""+"requested: "+requested+"\">");
		sb.append(actual);
		sb.append("</span>");
		row.add(new JsonPrimitive(sb.toString()));
		// List
		sb = new StringBuffer();
		sb.append("<span>");
		if(!nodeMap.isEmpty()) {
			boolean useList = false;
			if(nodeMap.size() > 1) {
				useList = true;
			}
			if(useList) {
				sb.append("<select>");
			}
			for (String node: nodeMap.keySet()) {
				String option = node;
				Integer count = nodeMap.get(node);
				if(count > 1) {
					option += " "+"["+count+"]";
				}
				if(useList) {
					sb.append("<option>");
				}
				sb.append(option);
				if(useList) {
					sb.append("</option>");
				}
			}
			if(useList) {
				sb.append("</select>");
			}
		}
		sb.append("</span>");
		row.add(new JsonPrimitive(sb.toString()));
		// Description
		sb = new StringBuffer();
		String description = stringNormalize(duccwork.getStandardInfo().getDescription(),messages.fetch("none"));
		switch(DuccCookies.getDescriptionStyle(request)) {
		case Long:
		default:
			sb.append("<span title=\""+DuccConstants.hintPreferencesDescriptionStyleShort+"\">");
			sb.append(description);
			sb.append("</span>");
			break;
		case Short:
			String shortDescription = getShortDescription(description);
			if(shortDescription == null) {
				sb.append("<span>");
				sb.append(description);
				sb.append("</span>");
			}
			else {
				sb.append("<span title=\""+description+"\">");
				sb.append(shortDescription);
				sb.append("</span>");
			}
			break;
		}
		row.add(new JsonPrimitive(sb.toString()));
		
		return row;
	}
	
	private void handleServletJsonFormatReservationsAaData(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleServletJsonFormatReservationsAaData";
		duccLogger.trace(methodName, jobid, messages.fetch("enter"));
		
		JsonObject jsonResponse = new JsonObject();
		JsonArray data = new JsonArray();

		int maxRecords = getReservationsMax(request);
		
		DuccData duccData = DuccData.getInstance();
		
		ConcurrentSkipListMap<Info,Info> sortedCombinedReservations = duccData.getSortedCombinedReservations();

		ArrayList<String> users = getReservationsUsers(request);
		
		long now = System.currentTimeMillis();
		
		if((sortedCombinedReservations.size() > 0)) {
			int counter = 0;
			Iterator<Entry<Info, Info>> iR = sortedCombinedReservations.entrySet().iterator();
			while(iR.hasNext()) {
				Info info = iR.next().getValue();
				IDuccWork dw = info.getDuccWork();
				boolean list = DuccWebUtil.isListable(request, users, maxRecords, counter, dw);
				if(list) {
					counter++;
					if(dw instanceof DuccWorkReservation) {
						DuccWorkReservation reservation = (DuccWorkReservation) dw;
						JsonArray row = buildReservationRow(request, reservation, duccData, now);
						data.add(row);
					}
					else if(dw instanceof DuccWorkJob) {
						DuccWorkJob job = (DuccWorkJob) dw;
						JsonArray row = buildReservationRow(request, job, duccData, now);
						data.add(row);
					}
					else {
						// huh?
					}
				}
			}
		}
		else {
			JsonArray row = new JsonArray();
			if(DuccData.getInstance().isPublished()) {
				// Terminate
				row.add(new JsonPrimitive("no reservations"));
			}
			else {
				// Terminate
				row.add(new JsonPrimitive("no data"));
			}
			// Id
			row.add(new JsonPrimitive(""));
			// Start
			row.add(new JsonPrimitive(""));
			// End
			row.add(new JsonPrimitive(""));
			// User
			row.add(new JsonPrimitive(""));
			// Class
			row.add(new JsonPrimitive(""));
			// Type
			row.add(new JsonPrimitive(""));
			// State
			row.add(new JsonPrimitive(""));
			// Reason
			row.add(new JsonPrimitive(""));
			// Allocation
			row.add(new JsonPrimitive(""));
			// User Processes
			row.add(new JsonPrimitive(""));
			// Size
			row.add(new JsonPrimitive(""));
			// List
			row.add(new JsonPrimitive(""));
			// Description
			row.add(new JsonPrimitive(""));
			data.add(row);
		}
		
		jsonResponse.add("aaData", data);
		
		String json = jsonResponse.toString();
		duccLogger.debug(methodName, jobid, json);
		response.getWriter().println(json);
		response.setContentType("application/json");
		
		duccLogger.trace(methodName, jobid, messages.fetch("exit"));
	}	
	
	private static DecimalFormat formatter = new DecimalFormat("##0.0");
	
	private void handleServletJsonFormatServicesAaData(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleServletJsonFormatServicesAaData";
		duccLogger.trace(methodName, jobid, messages.fetch("enter"));
		
		JsonObject jsonResponse = new JsonObject();
		JsonArray data = new JsonArray();

		ServicesSortCache servicesSortCache = ServicesSortCache.getInstance();
		Collection<IServiceAdapter> servicesSortedCollection = servicesSortCache.getSortedCollection();
		if(!servicesSortedCollection.isEmpty()) {
			StringBuffer col;
			for(IServiceAdapter service : servicesSortedCollection) {
				JsonArray row = new JsonArray();
				int sid = service.getId();
				String user = service.getUser();
				long deployments = service.getDeployments();
				long instances = service.getInstances();
				// Enable
				col = new StringBuffer();
				col.append("<span class=\"ducc-col-start\">");
				if(service.isRegistered()) {
					if(buttonsEnabled) {
						if(service.isDisabled()) {
							col.append("<input type=\"button\" onclick=\"ducc_confirm_service_enable("+sid+")\" value=\"Enable\" "+getDisabledWithHover(request,user)+"/>");
						}
					}
				}
				col.append("</span>");
				row.add(new JsonPrimitive(col.toString()));
				// Stop
				col = new StringBuffer();
				col.append("<span class=\"ducc-col-stop\">");
				if(service.isRegistered()) {
					if(buttonsEnabled) {
						if(service.isPingOnly()) {
							if(service.isPingActive()) {
								col.append("<input type=\"button\" onclick=\"ducc_confirm_service_stop("+sid+")\" value=\"Stop\" "+getDisabledWithHover(request,user)+"/>");
							}
						}
						else {
							if(deployments != 0) {
								col.append("<input type=\"button\" onclick=\"ducc_confirm_service_stop("+sid+")\" value=\"Stop\" "+getDisabledWithHover(request,user)+"/>");
							}
						}
					}
				}
				col.append("</span>");
				row.add(new JsonPrimitive(col.toString()));
				// Id
				col = new StringBuffer();
				String name = service.getName();
				col.append("<span>");
				String id = "<a href=\"service.details.html?name="+name+"\">"+sid+"</a>";
				col.append(""+id);
				col.append("</span>");
				row.add(new JsonPrimitive(col.toString()));
				// Name
				col = new StringBuffer();
				col.append("<span>");
				col.append(name);
				col.append("</span>");
				row.add(new JsonPrimitive(col.toString()));
				// State
				col = new StringBuffer();
				String state = service.getState();
				boolean alert = service.isAlert();
				boolean available = service.isStateAvailable();
				if(alert) {
					state += "+Alert";
				}
				String style = "class=\"health_black\";";
				if(alert) {
					style = "class=\"health_red\"";
				}
				else if(available) {
					style = "class=\"health_green\"";
				}
				String stateHover = ServicesHelper.getInstance().getStateHover(service);
				if(stateHover.length() > 0) {
					stateHover = "title="+"\""+stateHover+"\"";
				}
				col.append("<span "+style+" "+stateHover+">");
				col.append(state);
				col.append("</span>");
				row.add(new JsonPrimitive(col.toString()));
				// Last Use
				col = new StringBuffer();
				long lastUse = service.getLastUse();
				if(lastUse > 0) {
					col.append(getTimeStamp(request, jobid, ""+lastUse));
				}
				row.add(new JsonPrimitive(col.toString()));
				// Instances
				col = new StringBuffer();
				col.append(""+instances);
				row.add(new JsonPrimitive(col.toString()));
				// Deployments
				col = new StringBuffer();
				col.append(""+deployments);
				row.add(new JsonPrimitive(col.toString()));
				// Start-Mode
				col = new StringBuffer();
				StartState startState = service.getStartState();
				col.append("<span>");
				col.append(startState.name());
				if(service.isDisabled()) {
					col.append("<br>");
					String health = "class=\"health_red\"";
					String reason = "title=\""+service.getDisableReason()+"\"";
					col.append("<span "+health+" "+reason+">");
					col.append("Disabled");
					col.append("</span>");
				}
				col.append("</span>");
				row.add(new JsonPrimitive(col.toString()));
				// User
				col = new StringBuffer();
				col.append(""+user);
				row.add(new JsonPrimitive(col.toString()));
				// Class
				col = new StringBuffer();
				if(service.isPingOnly()) {
					String schedulingClass = ""+service.getSchedulingClass();
					col.append("<span title=\""+schedulingClass+"\">");
					String serviceType = "ping-only";
					col.append("<span>");
					col.append(serviceType);
				}
				else {
					String schedulingClass = service.getSchedulingClass();
					col.append(""+schedulingClass);
				}
				row.add(new JsonPrimitive(col.toString()));
				// Pgin
				col = new StringBuffer();
				col.append("<span>");
				long faults = 0;
				try {
					faults = service.getPgIn();
				}
				catch(Exception e) {
				}
				double swapping = service.getSwap();
				swapping = swapping/Constants.GB;
				if((swapping * faults) > 0) {
					col.append("<span class=\"health_red\""+">");
				}
				else {
					col.append("<span class=\"health_black\""+">");
				}
				col.append(faults);
				col.append("</span>");
				row.add(new JsonPrimitive(col.toString()));
				// Swap
				col = new StringBuffer();
				double rawSwap = service.getSwap();
				rawSwap = rawSwap/Constants.GB;
				String swap = formatter.format(rawSwap);
				double rawSwapMax = service.getSwapMax();
				rawSwapMax = rawSwapMax/Constants.GB;
				String swapMax = formatter.format(rawSwap);
				col.append("<span title=\"max="+swapMax+"\" align=\"right\" "+">");
				if(rawSwap > 0) {
					col.append("<span class=\"health_red\""+">");
				}
				else {
					col.append("<span class=\"health_black\""+">");
				}
				col.append(swap);
				col.append("</span>");
				col.append("</span>");
				row.add(new JsonPrimitive(col.toString()));
				// Size
				col = new StringBuffer();
				long size = service.getSize();
				if(size < 0) {
					size = 0;
				}
				col.append(size);
				row.add(new JsonPrimitive(col.toString()));
				// Jobs
				col = new StringBuffer();
				ArrayList<String> dependentJobs = service.getDependentJobs();
				int countDependentJobs = dependentJobs.size();
				String titleJobs = "";
				if(countDependentJobs > 0) {
					StringBuffer idList = new StringBuffer();
					for(String duccId : dependentJobs) {
						if(idList.length() > 0) {
							idList.append(",");
						}
						idList.append(duccId);
					}
					titleJobs = "dependent Job Id list: "+idList;
				}
				String jobs = "<span title=\""+titleJobs+"\">"+countDependentJobs+"</span>";
				col.append(jobs);
				row.add(new JsonPrimitive(col.toString()));
				// Services
				col = new StringBuffer();
				ArrayList<String> dependentServices = service.getDependentServices();
				int countDependentServices = dependentServices.size();
				String titleServices = "";
				if(countDependentServices > 0) {
					StringBuffer idList = new StringBuffer();
					for(String duccId : dependentServices) {
						if(idList.length() > 0) {
							idList.append(",");
						}
						idList.append(duccId);
					}
					titleServices = "dependent Service Name list: "+idList;
				}
				String services = "<span title=\""+titleServices+"\">"+countDependentServices+"</span>";
				col.append(services);
				row.add(new JsonPrimitive(col.toString()));
				// Reservations
				col = new StringBuffer();
				ArrayList<String> dependentReservations = service.getDependentReservations();
				int countDependentReservations = dependentReservations.size();
				String titleReservations = "";
				if(countDependentReservations > 0) {
					StringBuffer idList = new StringBuffer();
					for(String duccId : dependentReservations) {
						if(idList.length() > 0) {
							idList.append(",");
						}
						idList.append(duccId);
					}
					titleReservations = "dependent Reservation Id list: "+idList;
				}
				String reservations = "<span title=\""+titleReservations+"\">"+countDependentReservations+"</span>";
				col.append(reservations);
				row.add(new JsonPrimitive(col.toString()));
				// Description
				col = new StringBuffer();
				String description = service.getDescription();
				col.append(description);
				row.add(new JsonPrimitive(col.toString()));
				// Row
				data.add(row);
			}
		}
		else {
			JsonArray row = new JsonArray();
			// Start
			String text = "";
			if(DuccData.getInstance().isPublished()) {
				text = messages.fetch("no services");
			}
			else {
				text = messages.fetch("no data");
			}
			row.add(new JsonPrimitive(text));
			// Stop
			row.add(new JsonPrimitive(""));
			// Id
			row.add(new JsonPrimitive(""));
			// Name
			row.add(new JsonPrimitive(""));
			// Type
			row.add(new JsonPrimitive(""));
			// State
			row.add(new JsonPrimitive(""));
			// Pinging
			row.add(new JsonPrimitive(""));
			// Health
			row.add(new JsonPrimitive(""));
			// Instances
			row.add(new JsonPrimitive(""));
			// Deployments
			row.add(new JsonPrimitive(""));
			// User
			row.add(new JsonPrimitive(""));
			// Class
			row.add(new JsonPrimitive(""));
			// Pgin
			row.add(new JsonPrimitive(""));
			// Swap
			row.add(new JsonPrimitive(""));			
			// Size
			row.add(new JsonPrimitive(""));
			// Jobs
			row.add(new JsonPrimitive(""));
			// Services
			row.add(new JsonPrimitive(""));
			// Reservations
			row.add(new JsonPrimitive(""));
			// Description
			row.add(new JsonPrimitive(""));
			data.add(row);
		}
		
		jsonResponse.add("aaData", data);
		
		String json = jsonResponse.toString();
		duccLogger.debug(methodName, jobid, json);
		response.getWriter().println(json);
		response.setContentType("application/json");
		
		duccLogger.trace(methodName, jobid, messages.fetch("exit"));
	}		
	
	private void handleServletJsonFormatMachinesAaData(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleServletJsonFormatMachinesAaData";
		duccLogger.trace(methodName, jobid, messages.fetch("enter"));
		JsonObject jsonResponse = new JsonObject();
		JsonArray data = new JsonArray();
		String hover;
		int sumMemTotal = 0;
		int sumMemReserve = 0;
		int sumSwapInuse = 0;
		int sumSwapFree = 0;
		int sumAliens = 0;
		ListIterator<MachineFacts> listIterator;
		JsonArray row;
		StringBuffer sb;
		DuccMachinesData instance = DuccMachinesData.getInstance();
		MachineFactsList factsList = instance.getMachineFactsList();
		if(factsList.size() > 0) {
			// Total
			listIterator = factsList.listIterator();
			while(listIterator.hasNext()) {
				MachineFacts facts = listIterator.next();
				try {
					sumMemTotal += Integer.parseInt(facts.memTotal);
					sumMemReserve += Integer.parseInt(facts.memReserve);
					sumSwapInuse += Integer.parseInt(facts.swapInuse);
					sumSwapFree += Integer.parseInt(facts.swapFree);
					sumAliens += facts.aliens.size();
				}
				catch(Exception e) {
					duccLogger.trace(methodName, jobid, e);
				}
			}
			row = new JsonArray();
			// Status
			row.add(new JsonPrimitive("Total"));
			// IP
			row.add(new JsonPrimitive(""));
			// Name
			row.add(new JsonPrimitive(""));
			// Memory: usable
			hover = "title=\"total="+sumMemTotal+"\"";
			String sumMemReserveWithHover = "<span "+hover+" >"+sumMemReserve+"</span>";
			row.add(new JsonPrimitive(sumMemReserveWithHover));
			// Swap: inuse
			row.add(new JsonPrimitive(sumSwapInuse));
			// Swap: free
			row.add(new JsonPrimitive(sumSwapFree));
			// C-Groups
			row.add(new JsonPrimitive(""));
			// Alien PIDs
			row.add(new JsonPrimitive(sumAliens));
			// Heartbeat: last
			row.add(new JsonPrimitive(""));
			data.add(row);
			// Individual Machines
			listIterator = factsList.listIterator();
			while(listIterator.hasNext()) {
				MachineFacts facts = listIterator.next();
				row = new JsonArray();
				// Status
				sb = new StringBuffer();
				String status = facts.status;
				hover = "title=\""+facts.statusReason+"\"";
				if(status.equals("down")) {
					sb.append("<span "+hover+" class=\"health_red\""+">");
					sb.append(status);
					sb.append("</span>");
				}
				else if(status.equals("up")) {
					sb.append("<span "+hover+"class=\"health_green\""+">");
					sb.append(status);
					sb.append("</span>");
				}
				else {
					sb.append(status);
				}
				row.add(new JsonPrimitive(sb.toString()));
				// IP
				row.add(new JsonPrimitive(facts.ip));
				// Name
				row.add(new JsonPrimitive(facts.name));
				// Memory: usable
				if(!status.equals("defined")) {
					sb = new StringBuffer();
					sb.append("total="+facts.memTotal);
					if(facts.quantum != null) {
						if(facts.quantum.trim().length() > 0) {
							sb.append(" ");
							sb.append("quantum="+facts.quantum.trim());
						}
					}
					hover = "title=\""+sb.toString()+"\"";
					String memReserveWithHover = "<span "+hover+" >"+facts.memReserve+"</span>";
					row.add(new JsonPrimitive(memReserveWithHover));
				}
				else {
					row.add(new JsonPrimitive(""));
				}
				// Swap: inuse
				sb = new StringBuffer();
				String swapping = facts.swapInuse;
				if(swapping.equals("0")) {
					sb.append(swapping);
				}
				else {
					sb.append("<span class=\"health_red\">");
					sb.append(swapping);
					sb.append("</span>");
				}
				if(!status.equals("defined")) {
					row.add(new JsonPrimitive(sb.toString()));
				}
				else {
					row.add(new JsonPrimitive(""));
				}
				// Swap: free
				if(!status.equals("defined")) {
					row.add(new JsonPrimitive(facts.swapFree));
				}
				else {
					row.add(new JsonPrimitive(""));
				}
				// C-Groups
				boolean isCgroups = facts.cgroups;
				sb = new StringBuffer();
				if(status.equals("up")) {
					if(isCgroups) {
						sb.append("<span title=\""+"control groups active"+"\" class=\"health_black\""+">");
						sb.append("on");
						sb.append("</span>");
					}
					else {
						sb.append("<span title=\""+"control groups inactive"+"\" class=\"health_red\""+">");
						sb.append("off");
						sb.append("</span>");
					}
				}
				row.add(new JsonPrimitive(sb.toString()));
				// Alien PIDs
				sb = new StringBuffer();
				long aliens = facts.aliens.size();
				if(aliens == 0) {
					sb.append(aliens);
				}
				else {
					StringBuffer title = new StringBuffer();
					title.append("title=");
					title.append("\"");
					for(String pid : facts.aliens) {
						title.append(pid+" ");
					}
					title.append("\"");
					sb.append("<span class=\"health_red\" "+title+">");
					sb.append(aliens);
					sb.append("</span>");
				}
				if(!status.equals("defined")) {
					row.add(new JsonPrimitive(sb.toString()));
				}
				else {
					row.add(new JsonPrimitive(""));
				}
				// Heartbeat: last
				if(!status.equals("defined")) {
					row.add(new JsonPrimitive(facts.heartbeat));
				}
				else {
					row.add(new JsonPrimitive(""));
				}
				data.add(row);
			}
		}
		else {
			row = new JsonArray();
			// Release
			row.add(new JsonPrimitive(""));
			// Status
			row.add(new JsonPrimitive(""));
			// IP
			row.add(new JsonPrimitive(""));
			// Name
			row.add(new JsonPrimitive(""));
			// Reserve
			row.add(new JsonPrimitive(""));
			// Memory: total
			row.add(new JsonPrimitive(""));
			// Swap: inuse
			row.add(new JsonPrimitive(""));
			// Alien PIDs
			row.add(new JsonPrimitive(""));
			// Shares: total
			row.add(new JsonPrimitive(""));
			// Shares:inuse
			row.add(new JsonPrimitive(""));
			// Heartbeat: last
			row.add(new JsonPrimitive(""));
			data.add(row);
		}
		
		jsonResponse.add("aaData", data);
		
		String json = jsonResponse.toString();
		duccLogger.debug(methodName, jobid, json);
		response.getWriter().println(json);
		response.setContentType("application/json");
		
		duccLogger.trace(methodName, jobid, messages.fetch("exit"));
	}		
	
	private static DecimalFormat formatter3 = new DecimalFormat("##0.000");
	
	private void handleServletJsonFormatBrokerAaData(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws Exception
	{
		String methodName = "handleServletJsonFormatBrokerAaData";
		duccLogger.trace(methodName, jobid, messages.fetch("enter"));
		
		JsonObject jsonResponse = new JsonObject();
		JsonArray data = new JsonArray();
		JsonArray row;

		BrokerHelper brokerHelper = BrokerHelper.getInstance();

		JsonArray topics = new JsonArray();
		JsonArray queues = new JsonArray();

		Map<String, Map<String, String>> topicAttributes = brokerHelper.getEntityAttributes();
		
		if(topics.size() > 0) {
			for(Entry<String, Map<String, String>> entry : topicAttributes.entrySet()) {
				String topic = entry.getKey();
				String attrValue;
				Map<String, String> map = entry.getValue();
				row = new JsonArray();
				// Name
				row.add(new JsonPrimitive(topic));
				// Type
				String type = map.get(JmxAttribute.destinationType.name());
				row.add(new JsonPrimitive(type));
				// ConsumerCount
				attrValue = map.get(FrameworkAttribute.ConsumerCount.name());
				row.add(new JsonPrimitive(attrValue));
				// QueueSize
				attrValue = map.get(FrameworkAttribute.QueueSize.name());
				row.add(new JsonPrimitive(attrValue));
				// MaxEnqueueTime
				attrValue = map.get(FrameworkAttribute.MaxEnqueueTime.name());
				row.add(new JsonPrimitive(attrValue));
				// AverageEnqueueTime
				attrValue = map.get(FrameworkAttribute.AverageEnqueueTime.name());
				try {
					Double d = Double.valueOf(attrValue);
					attrValue = formatter3.format(d);
				}
				catch(Exception e) {
					
				}
				row.add(new JsonPrimitive(attrValue));
				// MemoryPercentUsage
				attrValue = map.get(FrameworkAttribute.MemoryPercentUsage.name());
				row.add(new JsonPrimitive(attrValue));
				// Row
				if(type.equals(map.get(FrameworkAttribute.QueueSize.name()))) {
					topics.add(row);
				}
				else {
					queues.add(row);
				}
			}
			data.addAll(topics);
		}
		
		jsonResponse.add("aaData", data);
		
		String json = jsonResponse.toString();
		duccLogger.debug(methodName, jobid, json);
		response.getWriter().println(json);
		response.setContentType("application/json");
		
		duccLogger.trace(methodName, jobid, messages.fetch("exit"));
	}	
	
	private void handleServletJsonFormatClassesAaData(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws Exception
	{
		String methodName = "handleServletJsonFormatClassesAaData";
		duccLogger.trace(methodName, jobid, messages.fetch("enter"));
		
		JsonObject jsonResponse = new JsonObject();
		JsonArray data = new JsonArray();
		JsonArray row;

		DuccSchedulerClasses schedulerClasses = DuccSchedulerClasses.getInstance();
        Map<String, DuccProperties> clmap = schedulerClasses.getClasses();
		        
		if( clmap != null ) {
            DuccProperties[] class_set = clmap.values().toArray(new DuccProperties[clmap.size()]);
            Arrays.sort(class_set, new NodeConfiguration.ClassSorter());            

			for( DuccProperties cl : class_set ) {
				row = new JsonArray();
				String class_name = cl.getProperty("name");
				// Name
				row.add(new JsonPrimitive(class_name));
				// Policy
                String policy = cl.getProperty("policy");
                row.add(new JsonPrimitive(policy));
                // Weight
                String weight = cl.getStringProperty("weight", "-");
                row.add(new JsonPrimitive(weight));
                // Priority
                String priority = cl.getProperty("priority");
                row.add(new JsonPrimitive(priority));
                
                // cap is either absolute or proportional.  if proprotional, it ends with '%'.  It's always
                // either-or so at least one of these columns will have N/A
                
                // Relative & Absolute Caps
				String val = cl.getStringProperty("cap", "0");
				if( (val == null) || val.equals("0") ) {
					row.add(new JsonPrimitive("-"));
					row.add(new JsonPrimitive("-"));
				} 
				else if ( val.endsWith("%") ) {
					row.add(new JsonPrimitive(val));
					row.add(new JsonPrimitive("-"));
                } 
				else {
					row.add(new JsonPrimitive("-"));
					row.add(new JsonPrimitive(val));
                }

                if ( policy.equals("FAIR_SHARE") ) {
                    // Initialization Cap
                    String defaultInitializationCap = "2";
                    val = cl.getStringProperty("initialization-cap",
                                               System.getProperty("ducc.rm.initialization.cap",defaultInitializationCap));
                    row.add(new JsonPrimitive(val));
                    
                    // Expand-by-Doubling
                    boolean bval = cl.getBooleanProperty("expand-by-doubling", true);
                    row.add(new JsonPrimitive(bval));
                    
                    // Use Prediction
                    String defaultUsePrediction = "true";
                    val = cl.getProperty("use-prediction",
                                         System.getProperty("ducc.rm.prediction",defaultUsePrediction));
                    row.add(new JsonPrimitive(val));
                    
                    // Prediction Fudge
                    String defaultPredictionFudge = "10000";
                    val = cl.getStringProperty("prediction-fudge",
                                               System.getProperty("ducc.rm.prediction.fudge",defaultPredictionFudge));
                    row.add(new JsonPrimitive(val));
                } else {
                    row.add(new JsonPrimitive("-"));
                    row.add(new JsonPrimitive("-"));
                    row.add(new JsonPrimitive("-"));
                    row.add(new JsonPrimitive("-"));
                }

                // max for reserve in in machines.  For fixed is in processes.  No max on fair-share. So slightly
                // ugly code here.
				
				// Max Allocation 
				
                if ( policy.equals("RESERVE") ) {
                    val = cl.getStringProperty("max-machines", "0");
                    if( val == null || val.equals("0")) {
                        val = "-";
                    }
                } else if ( policy.equals("FIXED_SHARE") ) {
                    val = cl.getStringProperty("max-processes", "0");
                    if( val == null || val.equals("0")) {
                        val = "-";
                    }
                } else {
					val = "-";
                }

				val = cl.getStringProperty("max-shares", "0");
				if( val == null || val.equals("0")) {
					val = "-";
				}
				row.add(new JsonPrimitive(val));

				// Nodepool
				val = cl.getProperty("nodepool");
				row.add(new JsonPrimitive(val));
				
				// Debug
				val = "-";
				if(schedulerClasses.isPreemptable(class_name)) {
					if(schedulerClasses.isPreemptable(class_name)) {
						String v1 = cl.getStringProperty("debug", "");
						if(!v1.equals("")) {
							val = v1;
						}
					}
				}
				row.add(new JsonPrimitive(val));
				
				// Row
				data.add(row);
			}
		}
		
		jsonResponse.add("aaData", data);
		
		String json = jsonResponse.toString();
		duccLogger.debug(methodName, jobid, json);
		response.getWriter().println(json);
		response.setContentType("application/json");
		
		duccLogger.trace(methodName, jobid, messages.fetch("exit"));
	}	
	
	private void handleServletJsonFormatDaemonsAaDataAll(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		boolean allDaemonsFlag = true;
		handleServletJsonFormatDaemonsAaData(target, baseRequest, request, response, allDaemonsFlag);
	}
	
	private void handleServletJsonFormatDaemonsAaData(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		boolean allDaemonsFlag = false;
		handleServletJsonFormatDaemonsAaData(target, baseRequest, request, response, allDaemonsFlag);
	}
	
	private void handleServletJsonFormatDaemonsAaData(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response, boolean allDaemonsFlag)
	throws IOException, ServletException
	{
		String methodName = "handleServletJsonFormatDaemonsAaData";
		duccLogger.trace(methodName, jobid, messages.fetch("enter"));
		
		JsonObject jsonResponse = new JsonObject();
		JsonArray data = new JsonArray();
		JsonArray row;
		
		DuccDaemonsData duccDaemonsData = DuccDaemonsData.getInstance();
		
		String wsHostIP = getWebServerHostIP();
		String wsHostName = getWebServerHostName();
		
		daemons:
		for(DaemonName daemonName : DuccDaemonRuntimeProperties.daemonNames) {
			row = new JsonArray();
			String status = "";
			String bootTime = "";
			String hostIP = "";
			String hostName = "";
			String pid = "";
			String pubSizeLast = "";
			String pubSizeMax = "";
			String heartbeatLast = "";
			String heartbeatMax = "";
			String heartbeatMaxTOD = "";
			String jmxUrl = null;
			Properties properties = DuccDaemonRuntimeProperties.getInstance().get(daemonName);
			switch(daemonName) {
			case Database:
				if(databaseHelper.isDisabled()) {
					continue daemons;
				}
			}
			switch(daemonName) {
			case Broker:
				if(brokerHelper.isAlive()) {
					status = DuccHandlerUtils.up();
				}
				else {
					status = DuccHandlerUtils.down();
				}
				bootTime = getTimeStamp(DuccCookies.getDateStyle(request),brokerHelper.getStartTime());
				hostName = useWS(wsHostName, brokerHelper.getHost());
				hostIP = useWS(wsHostName, hostName, wsHostIP);
				pid = ""+brokerHelper.getPID();
				pubSizeLast = "-";
				pubSizeMax = "-";
				heartbeatLast = "";
				heartbeatMax = "";
				heartbeatMaxTOD = "";
				jmxUrl = brokerHelper.getJmxUrl();
				break;
			case Database:
				if(databaseHelper.isAlive()) {
					status = DuccHandlerUtils.up();
				}
				else {
					status = DuccHandlerUtils.down();
				}
				bootTime = getTimeStamp(DuccCookies.getDateStyle(request),databaseHelper.getStartTime());
				hostName = useWS(wsHostName, databaseHelper.getHost());
				hostIP = useWS(wsHostName, hostName, wsHostIP);
				pid = ""+databaseHelper.getPID();
				pubSizeLast = "-";
				pubSizeMax = "-";
				heartbeatLast = "";
				heartbeatMax = "";
				heartbeatMaxTOD = "";
				jmxUrl = databaseHelper.getJmxUrl();
				break;
			case Webserver:
				status = DuccHandlerUtils.up();
				bootTime = getTimeStamp(DuccCookies.getDateStyle(request),getPropertiesValue(properties,DuccDaemonRuntimeProperties.keyBootTime,""));
				hostIP = getPropertiesValue(properties,DuccDaemonRuntimeProperties.keyNodeIpAddress,"");
				hostName = getPropertiesValue(properties,DuccDaemonRuntimeProperties.keyNodeName,"");
				pid = getPropertiesValue(properties,DuccDaemonRuntimeProperties.keyPid,"");
				pubSizeLast = "*";
				pubSizeMax = "*";
				heartbeatLast = "";
				heartbeatMax = "";
				heartbeatMaxTOD = "";
				jmxUrl = getPropertiesValue(properties,DuccDaemonRuntimeProperties.keyJmxUrl,"");
				break;
			default:
				status = DuccHandlerUtils.unknown();
				if(daemonName.equals(DaemonName.Orchestrator)) {
					if(ComponentHelper.isLocked(IDuccEnv.DUCC_STATE_DIR,"orchestrator")) {
						String filename = ComponentHelper.getLockFileName(IDuccEnv.DUCC_STATE_DIR,"orchestrator");
						String hover = "title=\""+ComponentHelper.getLockFileNameWithPath(IDuccEnv.DUCC_STATE_DIR,"orchestrator")+"\"";
						String fileNameWithHover = "<span "+hover+" >"+filename+"</span>";
						status += ", "+DuccHandlerUtils.warn("warning: ")+fileNameWithHover+" found.";
					}
				}
				bootTime = getTimeStamp(DuccCookies.getDateStyle(request),getPropertiesValue(properties,DuccDaemonRuntimeProperties.keyBootTime,""));
				hostIP = getPropertiesValue(properties,DuccDaemonRuntimeProperties.keyNodeIpAddress,"");
				hostName = getPropertiesValue(properties,DuccDaemonRuntimeProperties.keyNodeName,"");
				pid = getPropertiesValue(properties,DuccDaemonRuntimeProperties.keyPid,"");
				pubSizeLast = ""+duccDaemonsData.getEventSize(daemonName);
				pubSizeMax = ""+duccDaemonsData.getEventSizeMax(daemonName);
				heartbeatLast = DuccDaemonsData.getInstance().getHeartbeat(daemonName);
				long timeout = getMillisMIA(daemonName)/1000;
				if(timeout > 0) {
					try {
						long overtime = timeout - Long.parseLong(heartbeatLast);
						if(overtime < 0) {
							status = DuccHandlerUtils.down();
							if(daemonName.equals(DaemonName.Orchestrator)) {
								if(ComponentHelper.isLocked(IDuccEnv.DUCC_STATE_DIR,"orchestrator")) {
									String filename = ComponentHelper.getLockFileName(IDuccEnv.DUCC_STATE_DIR,"orchestrator");
									String hover = "title=\""+ComponentHelper.getLockFileNameWithPath(IDuccEnv.DUCC_STATE_DIR,"orchestrator")+"\"";
									String fileNameWithHover = "<span "+hover+" >"+filename+"</span>";
									status += ", "+DuccHandlerUtils.warn("warning: ")+fileNameWithHover+" found.";
								}
							}
						}
						else {
							status = DuccHandlerUtils.up();
							if(daemonName.equals(DaemonName.Orchestrator)) {
								int jdCount = DuccData.getInstance().getLive().getJobDriverNodeCount();
								if(jdCount == 0) {
									status = DuccHandlerUtils.up_provisional(", pending JD allocation");
								}
							}
						}
					}
					catch(Throwable t) {
					}
				}
				heartbeatMax = DuccDaemonsData.getInstance().getMaxHeartbeat(daemonName);
				heartbeatMaxTOD = TimeStamp.simpleFormat(DuccDaemonsData.getInstance().getMaxHeartbeatTOD(daemonName));
				try {
					heartbeatMaxTOD = getTimeStamp(DuccCookies.getDateStyle(request),heartbeatMaxTOD);
				}
				catch(Exception e) {
				}
				jmxUrl = getPropertiesValue(properties,DuccDaemonRuntimeProperties.keyJmxUrl,"");
				break;
			}
			// Status
			row.add(new JsonPrimitive(status));
			// Daemon Name
			row.add(new JsonPrimitive(daemonName.name()));
			// Boot Time
			row.add(new JsonPrimitive(bootTime));
			// Host IP
			row.add(new JsonPrimitive(hostIP));
			// Host Name
			row.add(new JsonPrimitive(hostName));
			// PID
			row.add(new JsonPrimitive(pid));
			// Publication Size (last)
			row.add(new JsonPrimitive(""+pubSizeLast));
			// Publication Size (max)
			row.add(new JsonPrimitive(""+pubSizeMax));
			// Heartbeat (last)
			row.add(new JsonPrimitive(""+heartbeatLast));
			// Heartbeat (max)
			row.add(new JsonPrimitive(""+heartbeatMax));
			// Heartbeat (max) TOD
			row.add(new JsonPrimitive(""+heartbeatMaxTOD));
			// JConsole URL
			String jmxUrlLink = "";
			if(jmxUrl != null) {
				jmxUrlLink = buildjConsoleLink(jmxUrl);
			}
			row.add(new JsonPrimitive(jmxUrlLink));
			//
			data.add(row);
		}

		// <Agents>
		
		boolean showAgents = allDaemonsFlag;
		if(!showAgents) {
			String cookie = DuccCookies.getCookie(request,DuccCookies.cookieAgents);
			duccLogger.trace(methodName, jobid, "== show: "+cookie);
			if(cookie.equals(DuccCookies.valueAgentsShow)) {
				showAgents = true;
			}
		}

		if(showAgents) {
			ConcurrentSkipListMap<Ip,MachineInfo> machines = DuccMachinesData.getInstance().getMachines();
			Iterator<Ip> iterator = machines.keySet().iterator();
			while(iterator.hasNext()) {
				row = new JsonArray();
				Ip key = iterator.next();
				MachineInfo machineInfo = machines.get(key);
				Properties properties = DuccDaemonRuntimeProperties.getInstance().getAgent(machineInfo.getName());
				// Status
				StringBuffer status = new StringBuffer();
				String machineStatus = machineInfo.getStatus();
				if(machineStatus.equals("down")) {
					//status.append("<span class=\"health_red\""+">");
					status.append(DuccHandlerUtils.down());
					//status.append("</span>");
				}
				else if(machineStatus.equals("up")) {
					//status.append("<span class=\"health_green\""+">");
					status.append(DuccHandlerUtils.up());
					//status.append("</span>");
				}
				else {
					status.append(DuccHandlerUtils.unknown());
				}
				row.add(new JsonPrimitive(status.toString()));
				// Daemon Name
				String daemonName = "Agent";
				row.add(new JsonPrimitive(daemonName));
				// Boot Time
				String bootTime = getTimeStamp(DuccCookies.getDateStyle(request),getPropertiesValue(properties,DuccDaemonRuntimeProperties.keyBootTime,""));
				row.add(new JsonPrimitive(bootTime));
				// Host IP
				String hostIP = getPropertiesValue(properties,DuccDaemonRuntimeProperties.keyNodeIpAddress,"");
				row.add(new JsonPrimitive(hostIP));
				// Host Name
				String hostName = machineInfo.getName();
				row.add(new JsonPrimitive(hostName));
				// PID
				String pid = getPropertiesValue(properties,DuccDaemonRuntimeProperties.keyPid,"");
				row.add(new JsonPrimitive(pid));
				// Publication Size (last)
				String publicationSizeLast = machineInfo.getPublicationSizeLast();
				row.add(new JsonPrimitive(publicationSizeLast));
				// Publication Size (max)
				String publicationSizeMax = machineInfo.getPublicationSizeMax();
				row.add(new JsonPrimitive(publicationSizeMax));
				// Heartbeat (last)
				String heartbeatLast = machineInfo.getHeartbeatLast();
				row.add(new JsonPrimitive(heartbeatLast));	
				// Heartbeat (max)
				long heartbeatMax = machineInfo.getHeartbeatMax();
				if(heartbeatMax > 0) {
					row.add(new JsonPrimitive(heartbeatMax));	
				}
				else {
					row.add(new JsonPrimitive(""));
				}
				// Heartbeat (max) TOD
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
				row.add(new JsonPrimitive(fmtHeartbeatMaxTOD));
				// JConsole URL
				String jmxUrl = getPropertiesValue(properties,DuccDaemonRuntimeProperties.keyJmxUrl,"");
				String jmxUrlLink = "";
				if(jmxUrl != null) {
					jmxUrlLink = buildjConsoleLink(jmxUrl);
				}
				row.add(new JsonPrimitive(jmxUrlLink));
				//
				data.add(row);
			}
		}

		// </Agents>
		
		jsonResponse.add("aaData", data);
		
		String json = jsonResponse.toString();
		duccLogger.debug(methodName, jobid, json);
		response.getWriter().println(json);
		response.setContentType("application/json");
		
		duccLogger.trace(methodName, jobid, messages.fetch("exit"));
	}	
	
	private void handleServletJsonFormatMachines(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleServletJsonFormatMachines";
		duccLogger.trace(methodName, jobid, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
	
		DuccMachinesData instance = DuccMachinesData.getInstance();
		
		MachineFactsList factsList = instance.getMachineFactsList();
		
		Gson gson = new Gson();
		String jSon = gson.toJson(factsList);
		sb.append(jSon);
		duccLogger.debug(methodName, jobid, sb);
		response.getWriter().println(sb);
		response.setContentType("application/json");
		duccLogger.trace(methodName, jobid, messages.fetch("exit"));
	}
	
	private void handleServletJsonFormatReservations(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleServletJsonFormatReservations";
		duccLogger.trace(methodName, jobid, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		ReservationFactsList factsList = new ReservationFactsList();
		
		int maxRecords = getReservationsMaxRecordsParameter(request);
		RequestStateType requestStateType = getStateTypeParameter(request);
		ArrayList<String> users = getReservationsUsers(request);
		DuccData duccData = DuccData.getInstance();
		ConcurrentSkipListMap<ReservationInfo,ReservationInfo> sortedReservations = duccData.getSortedReservations();
		if(sortedReservations.size()> 0) {
			Iterator<Entry<ReservationInfo, ReservationInfo>> iterator = sortedReservations.entrySet().iterator();
			int counter = 0;
			nextReservation:
			while(iterator.hasNext()) {
				ReservationInfo reservationInfo = iterator.next().getValue();
				DuccWorkReservation reservation = reservationInfo.getReservation();
				ReservationState reservationState = reservation.getReservationState();
				switch(requestStateType) {
				case All:
					break;
				case Active:
					switch(reservationState) {
					case Completed:
						continue nextReservation;
					default:
						break;
					}
					break;
				case Inactive:
					switch(reservationState) {
					case Completed:
						break;
					default:
						continue nextReservation;
					}
					break;
				}
				String reservationUser = reservation.getStandardInfo().getUser().trim();
				if(isIncludeUser(users,reservationUser)) {
					if(maxRecords > 0) {
						if (counter++ < maxRecords) {
							String id = reservation.getId(); 
							String start = getTimeStamp(request,reservation.getDuccId(), reservation.getStandardInfo().getDateOfSubmission());;
							String end = getTimeStamp(request,reservation.getDuccId(), reservation.getStandardInfo().getDateOfCompletion());
							String user = reservation.getStandardInfo().getUser();
							String rclass = reservation.getSchedulingInfo().getSchedulingClass();
							String state = reservation.getReservationState().toString();
							String reason = reservation.getCompletionType().toString();
							String allocation = reservation.getSchedulingInfo().getInstancesCount();
							List<NodePidList> userProcesses = new ArrayList<NodePidList>();
							List<String> list = new ArrayList<String>();
							if(!reservation.isCompleted()) {
								userProcesses = DuccMachinesData.getInstance().getUserProcesses(reservation.getUniqueNodes(),user);
								list = reservation.getNodes();
							}
							SizeBytes resSize = new SizeBytes(SizeBytes.Type.Bytes,reservation.getSchedulingInfo().getMemorySizeAllocatedInBytes());
							String size = ""+getProcessMemorySize(reservation.getDuccId(),resSize);
							String description = reservation.getStandardInfo().getDescription();
							ReservationFacts facts = new ReservationFacts(id,start,end,user,rclass,state,reason,allocation,userProcesses,size,list,description);
							factsList.add(facts);
						}
					}
				}
			}
		}
		
		Gson gson = new Gson();
		String jSon = gson.toJson(factsList);
		sb.append(jSon);
		duccLogger.debug(methodName, jobid, sb);
		response.getWriter().println(sb);
		response.setContentType("application/json");
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
	throws Exception
	{
		String methodName = "handleDuccRequest";
		duccLogger.trace(methodName, jobid, messages.fetch("enter"));
		duccLogger.debug(methodName, jobid,request.toString());
		duccLogger.debug(methodName, jobid,"getRequestURI():"+request.getRequestURI());
		String reqURI = request.getRequestURI()+"";
		if(reqURI.startsWith(jsonFormatJobsAaData)) {
			handleServletJsonFormatJobsAaData(target, baseRequest, request, response);
		}
		else if(reqURI.startsWith(jsonFormatReservationsAaData)) {
			handleServletJsonFormatReservationsAaData(target, baseRequest, request, response);
		}
		else if(reqURI.startsWith(jsonFormatServicesAaData)) {
			handleServletJsonFormatServicesAaData(target, baseRequest, request, response);
		}
		else if(reqURI.startsWith(jsonFormatMachinesAaData)) {
			handleServletJsonFormatMachinesAaData(target, baseRequest, request, response);
		}
		else if(reqURI.startsWith(jsonFormatBrokerAaData)) {
			handleServletJsonFormatBrokerAaData(target, baseRequest, request, response);
		}
		else if(reqURI.startsWith(jsonFormatClassesAaData)) {
			handleServletJsonFormatClassesAaData(target, baseRequest, request, response);
		}
		else if(reqURI.startsWith(jsonFormatDaemonsAaDataAll)) {
			handleServletJsonFormatDaemonsAaDataAll(target, baseRequest, request, response);
		}
		else if(reqURI.startsWith(jsonFormatDaemonsAaData)) {
			handleServletJsonFormatDaemonsAaData(target, baseRequest, request, response);
		}
		else if(reqURI.startsWith(jsonFormatMachines)) {
			handleServletJsonFormatMachines(target, baseRequest, request, response);
		}
		else if(reqURI.startsWith(jsonFormatReservations)) {
			handleServletJsonFormatReservations(target, baseRequest, request, response);
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
			if(reqURI.startsWith(duccContextJsonFormat)) {
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
