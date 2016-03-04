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
import org.apache.uima.ducc.common.ConvertSafely;
import org.apache.uima.ducc.common.IDuccEnv;
import org.apache.uima.ducc.common.NodeConfiguration;
import org.apache.uima.ducc.common.SizeBytes;
import org.apache.uima.ducc.common.SizeBytes.Type;
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
import org.apache.uima.ducc.transport.event.common.IDuccWork;
import org.apache.uima.ducc.transport.event.common.IDuccWorkJob;
import org.apache.uima.ducc.transport.event.common.IRationale;
import org.apache.uima.ducc.transport.event.common.JdReservationBean;
import org.apache.uima.ducc.ws.Distiller;
import org.apache.uima.ducc.ws.DuccDaemonsData;
import org.apache.uima.ducc.ws.DuccData;
import org.apache.uima.ducc.ws.DuccMachinesData;
import org.apache.uima.ducc.ws.Info;
import org.apache.uima.ducc.ws.JobInfo;
import org.apache.uima.ducc.ws.MachineInfo;
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

public class DuccHandlerClassic extends DuccAbstractHandler {
	
	private static DuccLogger duccLogger = DuccLoggerComponents.getWsLogger(DuccHandlerClassic.class.getName());
	private static Messages messages = Messages.getInstance();
	private static DuccId jobid = null;
	
	private static BrokerHelper brokerHelper = BrokerHelper.getInstance();
	private static DatabaseHelper databaseHelper = DatabaseHelper.getInstance();
	
	public final String classicJobs 				= duccContextClassic+"-jobs-data";
	public final String classicReservations 		= duccContextClassic+"-reservations-data";
	public final String classicServices			 	= duccContextClassic+"-services-data";
	public final String classicSystemClasses	 	= duccContextClassic+"-system-classes-data";
	public final String classicSystemDaemons	 	= duccContextClassic+"-system-daemons-data";
	public final String classicSystemMachines	 	= duccContextClassic+"-system-machines-data";
	public final String classicSystemBroker		 	= duccContextClassic+"-system-broker-data";
	
	public DuccHandlerClassic(DuccWebServer duccWebServer) {
		super.init(duccWebServer);
	}

	public String getFileName() {
		return dir_home+File.separator+dir_resources+File.separator+getDuccWebServer().getClassDefinitionFile();
	}
	
	private void buildJobsListEntry(HttpServletRequest request, StringBuffer sb, DuccId duccId, IDuccWorkJob job, DuccData duccData, long now, ServicesRegistry servicesRegistry) {
		String id = normalize(duccId);
		// Terminate
		sb.append("<td valign=\"bottom\" class=\"ducc-col-terminate\">");
		if(terminateEnabled) {
			if(!job.isFinished()) {
				sb.append("<input type=\"button\" onclick=\"ducc_confirm_terminate_job("+id+")\" value=\"Terminate\" "+getDisabledWithHover(request,job)+"/>");
			}
		}
		sb.append("</td>");
		// Id
		sb.append("<td valign=\"bottom\">");
		sb.append("<a href=\"job.details.html?id="+id+"\">"+id+"</a>");
		sb.append("</td>");
		// Start
		sb.append("<td valign=\"bottom\">");
		sb.append("<span title=\""+DuccConstants.hintPreferencesDateStyle+"\">");
		sb.append(getTimeStamp(request,job.getDuccId(), job.getStandardInfo().getDateOfSubmission()));
		sb.append("</span>");
		sb.append("</td>");
		// Duration
		sb.append("<td valign=\"bottom\" align=\"right\">");
		if(job.isCompleted()) {
			String duration = getDuration(request,job, Precision.Whole);
			String decoratedDuration = decorateDuration(request,job,duration,Precision.Whole);
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
		sb.append("</td>");
		// User
		String title = "";
		String submitter = job.getStandardInfo().getSubmitter();
		if(submitter != null) {
			title = "title=\"submitter PID@host: "+submitter+"\" ";
		}
		sb.append("<td "+title+"valign=\"bottom\">");
		sb.append(job.getStandardInfo().getUser());
		sb.append("</td>");
		// Class
		sb.append("<td valign=\"bottom\">");
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
		sb.append("</td>");
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
		String reason = getReason(job, MonitorType.Job).toString();
		sb.append("<td valign=\"bottom\" align=\"right\">");
		sb.append(reason);
		sb.append("</td>");
		// Services
		sb.append("<td valign=\"bottom\" align=\"right\">");
		sb.append(evaluateServices(job,servicesRegistry));
		sb.append("</td>");
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
		sb.append("</td>");		
		// Runtime Failures
		sb.append("<td valign=\"bottom\" align=\"right\">");
		sb.append(buildRuntimeFailuresLink(job));
		sb.append("</td>");
		// Pgin
		sb.append("<td valign=\"bottom\" align=\"right\">");
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
		sb.append("</td>");
		// Swap
		sb.append("<td valign=\"bottom\" align=\"right\">");
		String swapSizeDisplay = "";
		String swapSizeHover = "";
		title = "";
		double swapBytes = 0;
		swapBytes = DuccHandlerUtils.getSwapSizeBytes(job);
		swapSizeDisplay = DuccHandlerUtils.getSwapSizeDisplay(swapBytes);
		swapSizeHover = DuccHandlerUtils.getSwapSizeHover(swapBytes);
		title = "title="+"\""+swapSizeHover+"\"";
		if(swapBytes > 0) {
			sb.append("<span "+title+" "+"class=\"health_red\""+">");
		}
		else {
			sb.append("<span "+title+" "+"class=\"health_black\""+">");
		}
		sb.append(swapSizeDisplay);
		sb.append("</span>");
		sb.append("</td>");
		// Memory
		IDuccSchedulingInfo si;
		SizeBytes sizeBytes;
		String requested;
		String actual;
		si = job.getSchedulingInfo();
		sizeBytes = new SizeBytes(SizeBytes.Type.Bytes, si.getMemorySizeAllocatedInBytes());
		actual = getProcessMemorySize(duccId,sizeBytes);
		sizeBytes = new SizeBytes(si.getMemoryUnits().name(), Long.parseLong(si.getMemorySizeRequested()));
		requested = getProcessMemorySize(duccId,sizeBytes);
		sb.append("<td valign=\"bottom\" align=\"right\">");
		sb.append("<span title=\""+"requested: "+requested+"\">");
		sb.append(actual);
		sb.append("</span>");
		sb.append("</td>");
		// Total
		sb.append("<td valign=\"bottom\" align=\"right\">");
		sb.append(job.getSchedulingInfo().getWorkItemsTotal());
		sb.append("</td>");
		// Done
		sb.append("<td valign=\"bottom\" align=\"right\">");
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
		sb.append("</td>");
		// Error
		sb.append("<td valign=\"bottom\" align=\"right\">");
		sb.append(buildErrorLink(job));
		sb.append("</td>");
		// Dispatch
		sb.append("<td valign=\"bottom\" align=\"right\">");
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
		sb.append("</td>");
		sb.append("</tr>");
	}

	private void handleServletClassicJobs(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleServletClassicJobs";
		duccLogger.trace(methodName, jobid, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		
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
					sb.append(trGet(counter));
					buildJobsListEntry(request, sb, job.getDuccId(), job, duccData, now, servicesRegistry);
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
		
		duccLogger.debug(methodName, jobid, sb);
		response.getWriter().println(sb);
		duccLogger.trace(methodName, jobid, messages.fetch("exit"));
	}	
	
	private void buildReservationsListEntry(HttpServletRequest request, StringBuffer sb, DuccId duccId, IDuccWork duccwork, DuccData duccData, long now) {
		String id = normalize(duccId);
		String reservationType = "Unmanaged";
		if(duccwork instanceof DuccWorkJob) {
			reservationType = "Managed";
		}
		sb.append("<td class=\"ducc-col-terminate\">");
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
				if(duccwork instanceof DuccWorkReservation) {
					sb.append("<input type=\"button\" onclick=\"ducc_confirm_terminate_reservation("+id+")\" value=\"Terminate\" "+disabled+"/>");
				}
				else if(duccwork instanceof DuccWorkJob) {
					sb.append("<input type=\"button\" onclick=\"ducc_confirm_terminate_service("+id+")\" value=\"Terminate\" "+disabled+"/>");
				}
				else {
					//huh?
				}
			}
		}
		sb.append("</td>");
		// Id
		if(reservationType.equals("Managed")) {
			sb.append("<td valign=\"bottom\">");
			sb.append("<a href=\"reservation.details.html?id="+id+"\">"+id+"</a>");
			sb.append("</td>");
		}
		else {
			sb.append("<td>");
			sb.append(id);
			sb.append("</td>");
		}
		// Start
		sb.append("<td>");
		sb.append("<span title=\""+DuccConstants.hintPreferencesDateStyle+"\">");
		sb.append(getTimeStamp(request,duccwork.getDuccId(),duccwork.getStandardInfo().getDateOfSubmission()));
		sb.append("</span>");
		sb.append("</td>");
		// Duration
		sb.append("<td align=\"right\">");
		if(duccwork instanceof DuccWorkReservation) {
			DuccWorkReservation reservation = (DuccWorkReservation) duccwork;
			String duration;
			String decoratedDuration;
			switch(reservation.getReservationState()) {
			case Completed:
				sb.append("<span>");
				duration = getDuration(request,reservation,Precision.Whole);
				decoratedDuration = decorateDuration(request,reservation, duration);
				sb.append(decoratedDuration);
				sb.append("</span>");
				break;
			default:
				sb.append("<span class=\"health_green\""+">");
				duration = getDuration(request,reservation,now,Precision.Whole);
				decoratedDuration = decorateDuration(request,reservation, duration);
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
		sb.append("</td>");
		// User
		String title = "";
		String submitter = duccwork.getStandardInfo().getSubmitter();
		if(submitter != null) {
			title = "title=\"submitter PID@host: "+submitter+"\"";
		}
		sb.append("<td "+title+">");
		UserId userId = new UserId(duccwork.getStandardInfo().getUser());
		sb.append(userId.toString());
		sb.append("</td>");
		// Class
		sb.append("<td>");
		sb.append(stringNormalize(duccwork.getSchedulingInfo().getSchedulingClass(),messages.fetch("default")));
		sb.append("</td>");
		// Type
		sb.append("<td>");
		sb.append(reservationType);
		sb.append("</td>");
		// State
		sb.append("<td>");
		if(duccData.isLive(duccId)) {
			if(duccwork.isOperational()) {
				sb.append("<span class=\"active_state\">");
			}
			else {
				sb.append("<span class=\"completed_state\">");
			}
		}
		else {
			sb.append("<span class=\"historic_state\">");
		}
		sb.append(duccwork.getStateObject().toString());
		if(duccData.isLive(duccId)) {
			sb.append("</span>");
		}
		sb.append("</td>");
		// Reason
		if(duccwork instanceof DuccWorkReservation) {
			DuccWorkReservation reservation = (DuccWorkReservation) duccwork;
			sb.append("<td>");
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
			sb.append("</td>");
		}
		else if(duccwork instanceof DuccWorkJob) {
			DuccWorkJob job = (DuccWorkJob) duccwork;
			String reason = getReason(job, MonitorType.ManagedReservation).toString();
			sb.append("<td>");
			sb.append(reason);
			sb.append("</td>");
		}
		// Allocation
		/*
		sb.append("<td align=\"right\">");
		sb.append(duccwork.getSchedulingInfo().getInstancesCount());
		sb.append("</td>");
		*/
		// User Processes
		sb.append("<td align=\"right\">");
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
		sb.append("</td>");
		// PgIn
		sb.append("<td align=\"right\">");
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
		sb.append("</td>");
		// Swap
		sb.append("<td align=\"right\">");
		String swapSizeDisplay = "";
		String swapSizeHover = "";
		title = "";
		double swapBytes = 0;
		if(duccwork instanceof DuccWorkJob) {
			DuccWorkJob job = (DuccWorkJob) duccwork;
			swapBytes = DuccHandlerUtils.getSwapSizeBytes(job);
			swapSizeDisplay = DuccHandlerUtils.getSwapSizeDisplay(swapBytes);
			swapSizeHover = DuccHandlerUtils.getSwapSizeHover(swapBytes);
			title = "title="+"\""+swapSizeHover+"\"";
		}
		if(swapBytes > 0) {
			sb.append("<span "+title+" "+"class=\"health_red\""+">");
		}
		else {
			sb.append("<span "+title+" "+"class=\"health_black\""+">");
		}
		sb.append(swapSizeDisplay);
		sb.append("</span>");
		sb.append("</td>");
		// Memory
		IDuccSchedulingInfo si;
		SizeBytes sizeBytes;
		String requested;
		String actual;
		si = duccwork.getSchedulingInfo();
		sizeBytes = new SizeBytes(SizeBytes.Type.Bytes, si.getMemorySizeAllocatedInBytes());
		actual = getProcessMemorySize(duccId,sizeBytes);
		sizeBytes = new SizeBytes(si.getMemoryUnits().name(), Long.parseLong(si.getMemorySizeRequested()));
		requested = getProcessMemorySize(duccId,sizeBytes);
		sb.append("<td align=\"right\">");
		sb.append("<span title=\""+"requested: "+requested+"\">");
		sb.append(actual);
		sb.append("</span>");
		sb.append("</td>");
		// Host Names
		sb.append("<td>");
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
		sb.append("</td>");
		// Description
		sb.append("<td>");
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
		sb.append("</td>");
		sb.append("</tr>");
	}
	
	private void handleServletClassicReservations(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleServletClassicReservations";
		duccLogger.trace(methodName, jobid, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		
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
						sb.append(trGet(counter));
						buildReservationsListEntry(request, sb, reservation.getDuccId(), reservation, duccData, now);
					}
					else if(dw instanceof DuccWorkJob) {
						DuccWorkJob job = (DuccWorkJob) dw;
						sb.append(trGet(counter));
						buildReservationsListEntry(request, sb, job.getDuccId(), job, duccData, now);
					}
					else {
						// huh?
					}
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
		
		duccLogger.debug(methodName, jobid, sb);
		response.getWriter().println(sb);
		duccLogger.trace(methodName, jobid, messages.fetch("exit"));
	}	
	
	private void handleServletClassicServices(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleServletClassicServices";
		duccLogger.trace(methodName, jobid, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		
		ServicesSortCache servicesSortCache = ServicesSortCache.getInstance();
		Collection<IServiceAdapter> servicesSortedCollection = servicesSortCache.getSortedCollection();
		if(!servicesSortedCollection.isEmpty()) {
			int maxRecords = getServicesMax(request);
			ArrayList<String> users = getServicesUsers(request);
			int counter = 0;
			for(IServiceAdapter service : servicesSortedCollection) {
				boolean list = DuccWebUtil.isListable(request, users, maxRecords, counter, service);
				if(!list) {
					continue;
				}
				counter++;
				// Row Begin
				sb.append("<tr>");
				int sid = service.getId();
				String user = service.getUser();
				long deployments = service.getDeployments();
				long instances = service.getInstances();
				// Enable
				sb.append("<td valign=\"bottom\" class=\"ducc-col-start\">");
				if(service.isRegistered()) {
					if(buttonsEnabled) {
						if(service.isDisabled()) {
							sb.append("<input type=\"button\" onclick=\"ducc_confirm_service_enable("+sid+")\" value=\"Enable\" "+getDisabledWithHover(request,user)+"/>");
						}
					}
				}
				sb.append("</td>");
				// Stop
				sb.append("<td valign=\"bottom\" class=\"ducc-col-stop\">");
				if(service.isRegistered()) {
					if(buttonsEnabled) {
						if(service.isPingOnly()) {
							if(service.isPingActive()) {
								sb.append("<input type=\"button\" onclick=\"ducc_confirm_service_stop("+sid+")\" value=\"Stop\" "+getDisabledWithHover(request,user)+"/>");
							}
						}
						else {
							if(deployments != 0) {
								sb.append("<input type=\"button\" onclick=\"ducc_confirm_service_stop("+sid+")\" value=\"Stop\" "+getDisabledWithHover(request,user)+"/>");
							}
						}
					}
				}
				sb.append("</td>");
				// Id
				String name = service.getName();
				sb.append("<td align=\"right\">");
				String id = "<a href=\"service.details.html?name="+name+"\">"+sid+"</a>";
				sb.append(""+id);
				sb.append("</td>");
				// Name
				sb.append("<td>");
				sb.append(name);
				sb.append("</td>");
				// State
				sb.append("<td>");
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
				sb.append("<span "+style+" "+stateHover+">");
				sb.append(state);
				sb.append("</span>");
				sb.append("</td>");
				// Last Use
				sb.append("<td>");
				long lastUse = service.getLastUse();
				if(lastUse > 0) {
					sb.append(getTimeStamp(request, jobid, ""+lastUse));
				}
				sb.append("</td>");
				// Instances
				sb.append("<td align=\"right\">");
				sb.append(instances);
				sb.append("</td>");
				// Deployments
				sb.append("<td align=\"right\">");
				sb.append(deployments);
				sb.append("</td>");
				// Start-State
				StartState startState = service.getStartState();
				sb.append("<td align=\"right\">");
				sb.append("<span>");
				sb.append(startState.name());
				if(service.isDisabled()) {
					sb.append("<br>");
					String health = "class=\"health_red\"";
					String reason = "title=\""+service.getDisableReason()+"\"";
					sb.append("<span "+health+" "+reason+">");
					sb.append("Disabled");
					sb.append("</span>");
				}
				sb.append("</span>");
				sb.append("</td>");
				// User
				sb.append("<td>");
				sb.append(user);
				sb.append("</td>");
				// Share Class (or Type)
				sb.append("<td>");
				if(service.isPingOnly()) {
					String schedulingClass = service.getSchedulingClass();
					sb.append("<span title=\""+schedulingClass+"\">");
					String serviceType = "ping-only";
					sb.append("<span>");
					sb.append(serviceType);
				}
				else {
					String schedulingClass = service.getSchedulingClass();
					sb.append(schedulingClass);
				}
				sb.append("</td>");
				// PgIn
				sb.append("<td align=\"right\">");
				long faults = 0;
				try {
					faults = service.getPgIn();
				}
				catch(Exception e) {
				}
				double swapping = service.getSwap();
				swapping = swapping/Constants.GB;
				if((swapping * faults) > 0) {
					sb.append("<span class=\"health_red\""+">");
				}
				else {
					sb.append("<span class=\"health_black\""+">");
				}
				sb.append(faults);
				sb.append("</span>");
				sb.append("</td>");
				// Swap
				sb.append("<td align=\"right\">");
				String swapSizeDisplay = "";
				String swapSizeHover = "";
				String title = "";
				double swapBytes = 0;
				swapBytes = service.getSwap();
				swapSizeDisplay = DuccHandlerUtils.getSwapSizeDisplay(swapBytes);
				swapSizeHover = DuccHandlerUtils.getSwapSizeHover(swapBytes);
				title = "title="+"\""+swapSizeHover+"\"";
				if(swapBytes > 0) {
					sb.append("<span "+title+" "+"class=\"health_red\""+">");
				}
				else {
					sb.append("<span "+title+" "+"class=\"health_black\""+">");
				}
				sb.append(swapSizeDisplay);
				sb.append("</span>");
				sb.append("</td>");
				// Size
				sb.append("<td align=\"right\">");
				long size = service.getSize();
				if(size < 0) {
					size = 0;
				}
				sb.append(size);
				sb.append("</td>");
				// Jobs
				sb.append("<td align=\"right\">");
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
				sb.append(jobs);
				sb.append("</td>");
				// Services
				sb.append("<td align=\"right\">");
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
				sb.append(services);
				sb.append("</td>");
				// Reservations
				sb.append("<td align=\"right\">");
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
				sb.append(reservations);
				sb.append("</td>");
				// Description
				sb.append("<td>");
				String description = service.getDescription();
				sb.append(description);
				sb.append("</td>");
				// Row End
				sb.append("</tr>");
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
		
		duccLogger.debug(methodName, jobid, sb);
		response.getWriter().println(sb);
		duccLogger.trace(methodName, jobid, messages.fetch("exit"));
	}
	
	private void handleServletClassicSystemClasses(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws Exception
	{
		String methodName = "handleServletClassicSystemClasses";
		duccLogger.trace(methodName, jobid, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		String val = null;
        
		DuccSchedulerClasses schedulerClasses = DuccSchedulerClasses.getInstance();
        Map<String, DuccProperties> clmap = schedulerClasses.getClasses();
		if ( clmap != null ) {
            DuccProperties[] class_set = clmap.values().toArray(new DuccProperties[clmap.size()]);
            Arrays.sort(class_set, new NodeConfiguration.ClassSorter());
            int i = 0;

            for ( DuccProperties cl : class_set) {
            	
            	sb.append(trGet(i+1));
            	
            	// Name
				String class_name = cl.getProperty("name");
				sb.append("<td>");
				sb.append(class_name);
				sb.append("</td>");
				
				// Nodepool
				sb.append("<td align=\"right\">");
				val = cl.getProperty("nodepool");
                sb.append(val);
				sb.append("</td>");	
				
				// Policy
				sb.append("<td>");
                String policy = cl.getProperty("policy");
				sb.append(policy);
				sb.append("</td>");	
				
				// Weight
				sb.append("<td align=\"right\">");
				sb.append(cl.getStringProperty("weight", "-"));
				sb.append("</td>");	
				
				// Priority
				sb.append("<td align=\"right\">");
				sb.append(cl.getProperty("priority"));
				sb.append("</td>");	

				// Non-preemptable Class
				sb.append("<td align=\"right\">");
				val = "-";
				if(schedulerClasses.isPreemptable(class_name)) {
					String v1 = cl.getStringProperty("debug", "");
					if(!v1.equals("")) {
						val = v1;
					} 
				}
				sb.append(val);
				sb.append("</td>");	

				sb.append("</tr>");
			}
		}
		
		duccLogger.debug(methodName, jobid, sb);
		response.getWriter().println(sb);
		duccLogger.trace(methodName, jobid, messages.fetch("exit"));
	}		
	private void handleServletClassicSystemDaemons(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleServletClassicSystemDaemons";
		duccLogger.trace(methodName, jobid, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		
		String wsHostIP = getWebServerHostIP();
		String wsHostName = getWebServerHostName();
		
		DuccDaemonsData duccDaemonsData = DuccDaemonsData.getInstance();
		int counter = 0;
		daemons:
		for(DaemonName daemonName : DuccDaemonRuntimeProperties.daemonNames) {
			switch(daemonName) {
			case Database:
				if(databaseHelper.isDisabled()) {
					continue daemons;
				}
			}
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
			sb.append(trGet(counter));
			sb.append("<td>");
			sb.append(status);
			sb.append("</td>");	
			// Daemon Name
			sb.append("<td>");
			sb.append(daemonName);
			sb.append("</td>");
			// Boot Time
			sb.append("<td>");
			sb.append(bootTime);
			sb.append("</td>");
			// Host IP
			sb.append("<td>");
			sb.append(hostIP);
			sb.append("</td>");	
			// Host Name
			sb.append("<td>");
			sb.append(hostName);
			sb.append("</td>");
			// PID
			sb.append("<td>");
			sb.append(pid);
			sb.append("</td>");
			// Publication Size (last)
			sb.append("<td align=\"right\">");
			sb.append(""+pubSizeLast);
			sb.append("</td>");	
			// Publication Size (max)
			sb.append("<td align=\"right\">");
			sb.append(pubSizeMax);
			sb.append("</td>");	
			// Heartbeat (last)
			sb.append("<td align=\"right\">");
			sb.append(heartbeatLast);
			sb.append("</td>");	
			// Heartbeat (max)
			sb.append("<td align=\"right\">");
			sb.append(heartbeatMax);
			sb.append("</td>");
			// Heartbeat (max) TOD
			sb.append("<td>");
			sb.append(heartbeatMaxTOD);
			sb.append("</td>");
			// JConsole URL
			sb.append("<td>");
			if(jmxUrl != null) {
				sb.append(buildjConsoleLink(jmxUrl));
			}
			sb.append("</td>");	
			//
			sb.append("</tr>");
			counter++;
		}
		// <Agents>
		String cookie = DuccCookies.getCookie(request,DuccCookies.cookieAgents);
		if(cookie.equals(DuccCookies.valueAgentsShow)) {
			duccLogger.trace(methodName, jobid, "== show: "+cookie);
			
			ConcurrentSkipListMap<Ip,MachineInfo> machines = DuccMachinesData.getInstance().getMachines();
			Iterator<Ip> iterator = machines.keySet().iterator();
			while(iterator.hasNext()) {
				Ip key = iterator.next();
				MachineInfo machineInfo = machines.get(key);
				Properties properties = DuccDaemonRuntimeProperties.getInstance().getAgent(machineInfo.getName());
				sb.append(trGet(counter));
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
				sb.append("<td>");
				sb.append(status);
				sb.append("</td>");	
				sb.append("</td>");	
				// Daemon Name
				String daemonName = "Agent";
				sb.append("<td>");
				sb.append(daemonName);
				sb.append("</td>");	
				// Boot Time
				String bootTime = getTimeStamp(DuccCookies.getDateStyle(request),getPropertiesValue(properties,DuccDaemonRuntimeProperties.keyBootTime,""));
				sb.append("<td>");
				sb.append(bootTime);
				sb.append("</td>");
				// Host IP
				String hostIP = getPropertiesValue(properties,DuccDaemonRuntimeProperties.keyNodeIpAddress,"");
				sb.append("<td>");
				sb.append(hostIP);
				sb.append("</td>");	
				// Host Name
				String hostName = machineInfo.getName();
				sb.append("<td>");
				sb.append(hostName);
				sb.append("</td>");
				// PID
				String pid = getPropertiesValue(properties,DuccDaemonRuntimeProperties.keyPid,"");
				sb.append("<td>");
				sb.append(pid);
				sb.append("</td>");
				// Publication Size (last)
				String publicationSizeLast = machineInfo.getPublicationSizeLast();
				sb.append("<td align=\"right\">");
				sb.append(publicationSizeLast);
				sb.append("</td>");	
				// Publication Size (max)
				String publicationSizeMax = machineInfo.getPublicationSizeMax();
				sb.append("<td align=\"right\">");
				sb.append(publicationSizeMax);
				sb.append("</td>");	
				// Heartbeat (last)
				String heartbeatLast = machineInfo.getHeartbeatLast();
				sb.append("<td align=\"right\">");
				sb.append(heartbeatLast);
				sb.append("</td>");	
				// Heartbeat (max)
				long heartbeatMax = machineInfo.getHeartbeatMax();
				sb.append("<td align=\"right\">");
				if(heartbeatMax > 0) {
					sb.append(heartbeatMax);
				}
				sb.append("</td>");
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
				sb.append("<td>");
				sb.append(fmtHeartbeatMaxTOD);
				sb.append("</td>");
				// JConsole URL
				sb.append("<td>");
				String jmxUrl = getPropertiesValue(properties,DuccDaemonRuntimeProperties.keyJmxUrl,"");
				if(jmxUrl != null) {
					sb.append(buildjConsoleLink(jmxUrl));
				}
				sb.append("</td>");
				//
				sb.append("</tr>");
				counter++;
			}
		}
		else {
			duccLogger.trace(methodName, jobid, "!= show: "+cookie);
		}
		// </Agents>
		duccLogger.debug(methodName, jobid, sb);
		response.getWriter().println(sb);
		duccLogger.trace(methodName, jobid, messages.fetch("exit"));
	}
	
	private void handleServletClassicSystemMachines(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleServletClassicSystemMachines";
		duccLogger.trace(methodName, jobid, messages.fetch("enter"));
		int counter = 0;
		long sumMemTotal = 0;
		long sumMemFree = 0;
		long sumMemReserve = 0;
		double sumCPU = 0;
		long sumMachines = 0;
		long sumSwapInuse = 0;
		long sumSwapFree = 0;
		long sumAliens = 0;
		String hover;
		ListIterator<MachineFacts> listIterator;
		StringBuffer row;
		StringBuffer data = new StringBuffer();
		DuccMachinesData instance = DuccMachinesData.getInstance();
		MachineFactsList factsList = instance.getMachineFactsList();
		if(factsList.size() > 0) {
			// Total
			listIterator = factsList.listIterator();
			while(listIterator.hasNext()) {
				MachineFacts facts = listIterator.next();
				if(facts.status != null) {
					if(facts.status.equals("up")) {
						try {
							sumMemTotal += ConvertSafely.String2Long(facts.memTotal);
							sumMemReserve += ConvertSafely.String2Long(facts.memReserve);
							sumSwapInuse += ConvertSafely.String2Long(facts.swapInuse);
							sumSwapFree += ConvertSafely.String2Long(facts.swapFree);
							sumCPU += facts.cpu;
							sumMachines += 1;
							sumAliens += facts.aliens.size();
						}
						catch(Exception e) {
							duccLogger.trace(methodName, jobid, e);
						}
					}
				}
			}
			//
			Map<String, Long> allocatedMap = Distiller.getMap();
			long sumMemAllocated = 0;
			for(Long bytes : allocatedMap.values()) {
				sumMemAllocated += bytes;
			}
			SizeBytes sbAllocated = new SizeBytes(Type.Bytes, sumMemAllocated);
			sumMemFree = sumMemTotal - sbAllocated.getGBytes();
			//
			row = new StringBuffer();
			row.append("<tr>");
			// Status
			row.append("<td>");
			row.append(""+"Total");
			row.append("</td>");
			// IP
			row.append("<td>");
			row.append("");
			row.append("</td>");
			// Name
			row.append("<td>");
			row.append("");
			row.append("</td>");
			// Nodepool
			row.append("<td>");
			row.append("");
			row.append("</td>");
			// Memory: usable
			hover = "title=\"total="+sumMemTotal+"\"";
			row.append("<td align=\"right\" "+hover+">");
			row.append(""+sumMemReserve);
			row.append("</td>");
			// Memory: free
			row.append("<td align=\"right\">");
			row.append(""+sumMemFree);
			row.append("</td>");
			// CPU: load average
			row.append("<td align=\"right\">");
			String cpuTotal = formatter1.format(sumCPU/sumMachines);
			row.append(""+cpuTotal);
			row.append("</td>");
			// Swap: inuse
			row.append("<td align=\"right\">");
			row.append(""+sumSwapInuse);
			row.append("</td>");
			// Swap: free
			row.append("<td align=\"right\">");
			row.append(""+sumSwapFree);
			row.append("</td>");
			// C-Groups
			row.append("<td align=\"right\">");
			row.append("");
			row.append("</td>");
			// Alien PIDs
			row.append("<td align=\"right\">");
			row.append(""+sumAliens);
			row.append("</td>");
			// Heartbeat: last
			row.append("<td align=\"right\">");
			row.append("");
			row.append("</td>");
			row.append("</tr>");
			data.append(row);
			// Individual Machines
			listIterator = factsList.listIterator();
			while(listIterator.hasNext()) {
				MachineFacts facts = listIterator.next();
				row = new StringBuffer();
				row.append((trGet(counter)));
				// Status
				StringBuffer sb = new StringBuffer();
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
				row.append("<td>");
				row.append(sb);
				row.append("</td>");
				// IP
				row.append("<td>");
				row.append(facts.ip);
				row.append("</td>");
				// Name
				row.append("<td>");
				row.append(facts.name);
				row.append("</td>");
				// Nodepool
				row.append("<td>");
				String nodepool = DuccSchedulerClasses.getInstance().getNodepool(facts.name);
				row.append(nodepool);
				row.append("</td>");
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
					row.append("<td align=\"right\" "+hover+">");
					row.append(facts.memReserve);
					row.append("</td>");
				}
				else {
					row.append("<td align=\"right\">");
					row.append("</td>");
				}
				// Memory: free
				if(!status.equals("defined")) {
					long memFree = ConvertSafely.String2Long(facts.memTotal);
					if(allocatedMap.containsKey(facts.name)) {
						long bytes = allocatedMap.get(facts.name);
						SizeBytes allocated = new SizeBytes(Type.Bytes, bytes);
						memFree = memFree - allocated.getGBytes();
					}
					row.append("<td align=\"right\">");
					row.append(memFree);
					row.append("</td>");
				}
				else {
					row.append("<td align=\"right\">");
					row.append("</td>");
				}
				// CPU: load average
				row.append("<td align=\"right\">");
				if(facts.status != null) {
					if(facts.status.equals("up")) {
						String cpu = formatter1.format(facts.cpu);
						row.append(cpu);
					}
				}
				row.append("</td>");
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
				row.append("<td align=\"right\">");
				if(!status.equals("defined")) {
					row.append(sb);
				}
				row.append("</td>");
				// Swap: free
				row.append("<td align=\"right\">");
				if(!status.equals("defined")) {
					row.append(facts.swapFree);
				}
				row.append("</td>");
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
				String cgroups = sb.toString();
				row.append("<td align=\"right\">");
				row.append(""+cgroups);
				row.append("</td>");
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
				row.append("<td align=\"right\">");
				if(!status.equals("defined")) {
					row.append(sb);
				}
				row.append("</td>");
				// Heartbeat: last
				row.append("<td align=\"right\">");
				if(!status.equals("defined")) {
					row.append(facts.heartbeat);
				}
				row.append("</td>");
				row.append("</tr>");
				data.append(row);
				counter++;
			}
		}
		else {
			row = new StringBuffer();
			row.append((trGet(counter)));
			// Release
			row.append("<td>");
			row.append("");
			row.append("</td>");
			// Status
			row.append("<td>");
			row.append("");
			row.append("</td>");
			// IP
			row.append("<td>");
			row.append("");
			row.append("</td>");
			// Name
			row.append("<td>");
			row.append("");
			row.append("</td>");
			// Reserve
			row.append("<td>");
			row.append("");
			row.append("</td>");
			// Memory: total
			row.append("<td>");
			row.append("");
			row.append("</td>");
			// Swap: inuse
			row.append("<td>");
			row.append("");
			row.append("</td>");
			// Alien PIDs
			row.append("<td>");
			row.append("");
			row.append("</td>");
			// Shares: total
			row.append("<td>");
			row.append("");
			row.append("</td>");
			// Shares:inuse
			row.append("<td>");
			row.append("");
			row.append("</td>");
			// Heartbeat: last
			row.append("<td>");
			row.append("");
			row.append("</td>");
			row.append("</tr>");
			data.append(row);
		}
		
		duccLogger.debug(methodName, jobid, data);
		response.getWriter().println(data);
		duccLogger.trace(methodName, jobid, messages.fetch("exit"));
	}
	
	private static DecimalFormat formatter1 = new DecimalFormat("##0.0");
	private static DecimalFormat formatter3 = new DecimalFormat("##0.000");
	
	private void handleServletClassicSystemBroker(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleServletClassicBroker";
		duccLogger.trace(methodName, jobid, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();

		BrokerHelper brokerHelper = BrokerHelper.getInstance();
		
		StringBuffer topics = new StringBuffer();
		StringBuffer queues = new StringBuffer();
		
		Map<String, Map<String, String>> topicAttributes = brokerHelper.getEntityAttributes();
		
		if(topicAttributes.size() > 0) {
			for(Entry<String, Map<String, String>> entry : topicAttributes.entrySet()) {
				String topic = entry.getKey();
				String attrValue;
				Map<String, String> map = entry.getValue();
				StringBuffer row = new StringBuffer();
				row.append(messages.fetch("<tr>"));
				// name
				row.append(messages.fetch("<td style=\"font-family: monospace;\" align=\"left\">"));
				row.append(messages.fetch(topic));
				row.append(messages.fetch("</td>"));
				// type
				String type = map.get(JmxAttribute.destinationType.name());
				row.append(messages.fetch("<td style=\"font-family: monospace;\" align=\"left\">"));
				row.append(messages.fetch(type));
				row.append(messages.fetch("</td>"));
				// ConsumerCount
				attrValue = map.get(FrameworkAttribute.ConsumerCount.name());
				row.append(messages.fetch("<td style=\"font-family: monospace;\" align=\"right\">"));
				row.append(messages.fetch(attrValue));
				row.append(messages.fetch("</td>"));
				// QueueSize
				attrValue = map.get(FrameworkAttribute.QueueSize.name());
				row.append(messages.fetch("<td style=\"font-family: monospace;\" align=\"right\">"));
				row.append(messages.fetch(attrValue));
				row.append(messages.fetch("</td>"));
				// MaxEnqueueTime
				attrValue = map.get(FrameworkAttribute.MaxEnqueueTime.name());
				row.append(messages.fetch("<td style=\"font-family: monospace;\" align=\"right\">"));
				row.append(messages.fetch(attrValue));
				row.append(messages.fetch("</td>"));
				// AverageEnqueueTime
				attrValue = map.get(FrameworkAttribute.AverageEnqueueTime.name());
				try {
					Double d = Double.valueOf(attrValue);
					attrValue = formatter3.format(d);
				}
				catch(Exception e) {
					
				}
				row.append(messages.fetch("<td style=\"font-family: monospace;\" align=\"right\">"));
				row.append(messages.fetch(attrValue));
				row.append(messages.fetch("</td>"));
				// MemoryPercentUsage
				attrValue = map.get(FrameworkAttribute.MemoryPercentUsage.name());
				row.append(messages.fetch("<td style=\"font-family: monospace;\" align=\"right\">"));
				row.append(messages.fetch(attrValue));
				row.append(messages.fetch("</td>"));
				//
				row.append(messages.fetch("</tr>"));
				if(type.equals(JmxAttribute.destinationType.name())) {
					topics.append(row);
				}
				else {
					queues.append(row);
				}
			}
			sb.append(topics);
			sb.append(queues);
		}
		else {
			StringBuffer row = new StringBuffer();
			row.append(messages.fetch("<tr>"));
			// name
			row.append(messages.fetch("<td>"));
			row.append(messages.fetch("no data"));
			row.append(messages.fetch("</td>"));
			// ConsumerCount
			row.append(messages.fetch("<td>"));
			row.append(messages.fetch(""));
			row.append(messages.fetch("</td>"));
			// MaxEnqueueTime
			row.append(messages.fetch("<td>"));
			row.append(messages.fetch(""));
			row.append(messages.fetch("</td>"));
			// AverageEnqueueTime
			row.append(messages.fetch("<td>"));
			row.append(messages.fetch(""));
			row.append(messages.fetch("</td>"));
			// MemoryPercentUsage
			row.append(messages.fetch("<td>"));
			row.append(messages.fetch(""));
			row.append(messages.fetch("</td>"));
			//
			row.append(messages.fetch("</tr>"));
			sb.append(row);
		}
		
		duccLogger.debug(methodName, jobid, sb);
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
	throws Exception
	{
		String methodName = "handleDuccRequest";
		duccLogger.trace(methodName, jobid, messages.fetch("enter"));
		duccLogger.debug(methodName, jobid,request.toString());
		duccLogger.debug(methodName, jobid,"getRequestURI():"+request.getRequestURI());
		String reqURI = request.getRequestURI()+"";
		if(reqURI.startsWith(classicJobs)) {
			handleServletClassicJobs(target, baseRequest, request, response);
		}
		else if(reqURI.startsWith(classicReservations)) {
			handleServletClassicReservations(target, baseRequest, request, response);
		}
		else if(reqURI.startsWith(classicServices)) {
			handleServletClassicServices(target, baseRequest, request, response);
		}
		else if(reqURI.startsWith(classicSystemClasses)) {
			handleServletClassicSystemClasses(target, baseRequest, request, response);
		}
		else if(reqURI.startsWith(classicSystemDaemons)) {
			handleServletClassicSystemDaemons(target, baseRequest, request, response);
		}
		else if(reqURI.startsWith(classicSystemMachines)) {
			handleServletClassicSystemMachines(target, baseRequest, request, response);
		}
		else if(reqURI.startsWith(classicSystemBroker)) {
			handleServletClassicSystemBroker(target, baseRequest, request, response);
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
			if(reqURI.startsWith(duccContextClassic)) {
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
