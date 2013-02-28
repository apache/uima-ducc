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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;
import org.apache.uima.ducc.common.boot.DuccDaemonRuntimeProperties;
import org.apache.uima.ducc.common.boot.DuccDaemonRuntimeProperties.DaemonName;
import org.apache.uima.ducc.common.internationalization.Messages;
import org.apache.uima.ducc.common.jd.JdConstants;
import org.apache.uima.ducc.common.persistence.services.IStateServices;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.DuccProperties;
import org.apache.uima.ducc.common.utils.TimeStamp;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.ProcessInfo;
import org.apache.uima.ducc.transport.event.common.DuccWorkJob;
import org.apache.uima.ducc.transport.event.common.DuccWorkReservation;
import org.apache.uima.ducc.transport.event.common.IDuccCompletionType.JobCompletionType;
import org.apache.uima.ducc.transport.event.common.IDuccReservation;
import org.apache.uima.ducc.transport.event.common.IDuccReservationMap;
import org.apache.uima.ducc.transport.event.common.IDuccUnits.MemoryUnits;
import org.apache.uima.ducc.transport.event.common.IDuccWork;
import org.apache.uima.ducc.transport.event.common.IDuccWorkJob;
import org.apache.uima.ducc.transport.event.common.IDuccWorkService.ServiceDeploymentType;
import org.apache.uima.ducc.transport.event.common.IRationale;
import org.apache.uima.ducc.ws.DuccDaemonsData;
import org.apache.uima.ducc.ws.DuccData;
import org.apache.uima.ducc.ws.DuccDataHelper;
import org.apache.uima.ducc.ws.DuccMachinesData;
import org.apache.uima.ducc.ws.JobInfo;
import org.apache.uima.ducc.ws.MachineInfo;
import org.apache.uima.ducc.ws.ReservationInfo;
import org.apache.uima.ducc.ws.registry.IServicesRegistry;
import org.apache.uima.ducc.ws.registry.ServicesRegistry;
import org.apache.uima.ducc.ws.registry.ServicesRegistryMap;
import org.apache.uima.ducc.ws.registry.ServicesRegistryMapPayload;
import org.apache.uima.ducc.ws.types.NodeId;
import org.apache.uima.ducc.ws.types.UserId;
import org.eclipse.jetty.server.Request;

public class DuccHandlerLegacy extends DuccAbstractHandler {
	
	private static DuccLogger duccLogger = DuccLoggerComponents.getWsLogger(DuccHandlerLegacy.class.getName());
	private static Messages messages = Messages.getInstance();
	private static DuccId jobid = null;

	public final String legacyJobs 					= duccContextLegacy+"-jobs-data";
	public final String legacyReservations 			= duccContextLegacy+"-reservations-data";
	public final String legacyServices			 	= duccContextLegacy+"-services-data";
	public final String legacySystemClasses	 		= duccContextLegacy+"-system-classes-data";
	public final String legacySystemDaemons	 		= duccContextLegacy+"-system-daemons-data";
	public final String legacySystemMachines	 	= duccContextLegacy+"-system-machines-data";
	
	private DuccWebServer duccWebServer = null;
	
	public DuccHandlerLegacy(DuccWebServer duccWebServer) {
		this.duccWebServer = duccWebServer;
	}
	
	public DuccWebServer getDuccWebServer() {
		return duccWebServer;
	}
	
	public String getFileName() {
		return dir_home+File.separator+dir_resources+File.separator+getDuccWebServer().getClassDefinitionFile();
	}
	
	private void buildJobsListEntry(HttpServletRequest request, StringBuffer sb, DuccId duccId, IDuccWorkJob job, DuccData duccData, ServicesRegistry servicesRegistry) {
		String type="Job";
		String id = normalize(duccId);
		// Terminate
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
		sb.append(getTimeStamp(request,job.getDuccId(), job.getStandardInfo().getDateOfSubmission()));
		sb.append("</td>");
		// End
		sb.append("<td valign=\"bottom\">");
		sb.append(getCompletionOrProjection(request,job));
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
			boolean multi = false;
			sb.append("<td valign=\"bottom\">");
			ArrayList<String> swappingMachines = getSwappingMachines(job);
			if(!swappingMachines.isEmpty()) {
				StringBuffer mb = new StringBuffer();
				for(String machine : swappingMachines) {
					mb.append(machine);
					mb.append(" ");
				}
				String ml = mb.toString().trim();
				if(multi) {
					sb.append(" ");
				}
				multi = true;
				sb.append("<span class=\"health_red\" title=\""+ml+"\">");
				sb.append("Swapping");
				sb.append("</span>");
			}
			DuccWebMonitor duccWebMonitor = DuccWebMonitor.getInstance();
			Long expiry = duccWebMonitor.getExpiry(duccId);
			if(expiry != null) {
				if(multi) {
					sb.append(" ");
				}
				multi = true;
				String t2 = " left until auto-cancel, unless renewed";
				String t1;
				if(expiry == 0) {
					t1 = "less than 1 minute";
				}
				else {
					t1 = expiry+"+ minutes";
				}
				String text = t1+t2;
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
						
					}
				}
				if(expiry > expiryWarnTime) {
					sb.append("<span class=\"health_green\" title=\""+text+"\">");
					sb.append("MonitorActive");
				}
				else {
					sb.append("<span class=\"health_red\" title=\""+text+"\">");
					sb.append("MonitorWarning");
				}
				sb.append("</span>");
			}
			else if(duccWebMonitor.isCancelPending(duccId)) {
				sb.append("<span class=\"health_red\" >");
				sb.append("CancelPending...");
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
		String description = stringNormalize(job.getStandardInfo().getDescription(),messages.fetch("none"));
		switch(getDescriptionStyle(request)) {
		case Long:
		default:
			sb.append("<span>");
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

	private void handleServletLegacyJobs(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleServletLegacyJobs";
		duccLogger.trace(methodName, jobid, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		
		ServicesRegistry servicesRegistry = new ServicesRegistry();
		
		int maxRecords = getJobsMax(request);
		ArrayList<String> users = getJobsUsers(request);
		DuccData duccData = DuccData.getInstance();
		ConcurrentSkipListMap<JobInfo,JobInfo> sortedJobs = duccData.getSortedJobs();
		FilterUsersStyle filterUsersStyle = getFilterUsersStyle(request);
		if(sortedJobs.size()> 0) {
			Iterator<Entry<JobInfo, JobInfo>> iterator = sortedJobs.entrySet().iterator();
			int counter = 0;
			while(iterator.hasNext()) {
				JobInfo jobInfo = iterator.next().getValue();
				DuccWorkJob job = jobInfo.getJob();
				boolean list = false;
				if(!users.isEmpty()) {
					String jobUser = job.getStandardInfo().getUser().trim();
					switch(filterUsersStyle) {
					case IncludePlusActive:
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
					case ExcludePlusActive:
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
					buildJobsListEntry(request, sb, job.getDuccId(), job, duccData, servicesRegistry);
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
	
	private void buildReservationsListEntry(HttpServletRequest request, StringBuffer sb, DuccId duccId, IDuccWork duccwork, DuccData duccData) {
		String type="Reservation";
		String id = normalize(duccId);
		sb.append("<td class=\"ducc-col-terminate\">");
		if(terminateEnabled) {
			if(!duccwork.isCompleted()) {
				String disabled = getDisabled(request,duccwork);
				String user = duccwork.getStandardInfo().getUser();
				if(user != null) {
					if(user.equals(JdConstants.reserveUser)) {
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
		sb.append("<td>");
		sb.append(id);
		sb.append("</td>");
		// Start
		sb.append("<td>");
		sb.append(getTimeStamp(request,duccwork.getDuccId(),duccwork.getStandardInfo().getDateOfSubmission()));
		sb.append("</td>");
		// End
		sb.append("<td>");
		if(duccwork instanceof DuccWorkReservation) {
			DuccWorkReservation reservation = (DuccWorkReservation) duccwork;
			switch(reservation.getReservationState()) {
			case Completed:
				sb.append(getTimeStamp(request,duccwork.getDuccId(),duccwork.getStandardInfo().getDateOfCompletion()));
				break;
			default:
				break;
			}
		}
		else if(duccwork instanceof DuccWorkJob) {
			DuccWorkJob job = (DuccWorkJob) duccwork;
			switch(job.getJobState()) {
			case Completed:
				sb.append(getTimeStamp(request,duccwork.getDuccId(),duccwork.getStandardInfo().getDateOfCompletion()));
				break;
			default:
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
		String reservationType = "Unmanaged";
		if(duccwork instanceof DuccWorkJob) {
			reservationType = "Managed";
		}
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
		sb.append("<td>");
		if(duccwork instanceof DuccWorkReservation) {
			DuccWorkReservation reservation = (DuccWorkReservation) duccwork;
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
							sb.append("<span title=\""+rationale+"\">");
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
						sb.append("<span title=\""+rationale+"\">");
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
					sb.append("<span title=\""+rationale+"\">");
					sb.append(duccwork.getCompletionTypeObject().toString());
					sb.append("</span>");
				}
				else {
					sb.append(duccwork.getCompletionTypeObject().toString());
				}
				break;
			}
		}
		else if(duccwork instanceof DuccWorkJob) {
			DuccWorkJob job = (DuccWorkJob) duccwork;
			switch(job.getCompletionType()) {
			case Undefined:
				break;
			case CanceledByUser:
			case CanceledByAdministrator:
				try {
					String cancelUser = duccwork.getStandardInfo().getCancelUser();
					if(cancelUser != null) {
						sb.append("<span title=\"canceled by "+cancelUser+"\">");
						sb.append(duccwork.getCompletionTypeObject().toString());
						sb.append("</span>");
					}
					else {							
						IRationale rationale = job.getCompletionRationale();
						if(rationale != null) {
							sb.append("<span title=\""+rationale+"\">");
							sb.append(duccwork.getCompletionTypeObject().toString());
							sb.append("</span>");
						}
						else {
							sb.append(duccwork.getCompletionTypeObject().toString());
						}
						
					}
				} 
				catch(Exception e) {
					IRationale rationale = job.getCompletionRationale();
					if(rationale != null) {
						sb.append("<span title=\""+rationale+"\">");
						sb.append(duccwork.getCompletionTypeObject().toString());
						sb.append("</span>");
					}
					else {
						sb.append(duccwork.getCompletionTypeObject().toString());
					}
				}
				break;
			default:
				IRationale rationale = job.getCompletionRationale();
				if(rationale != null) {
					sb.append("<span title=\""+rationale+"\">");
					sb.append(duccwork.getCompletionTypeObject().toString());
					sb.append("</span>");
				}
				else {
					sb.append(duccwork.getCompletionTypeObject().toString());
				}
				break;
			}
		}
		sb.append("</td>");
		// Allocation
		sb.append("<td align=\"right\">");
		sb.append(duccwork.getSchedulingInfo().getInstancesCount());
		sb.append("</td>");
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
		}
		sb.append("</td>");
		// Size
		sb.append("<td align=\"right\">");
		String size = duccwork.getSchedulingInfo().getShareMemorySize();
		MemoryUnits units = duccwork.getSchedulingInfo().getShareMemoryUnits();
		sb.append(getProcessMemorySize(duccId,type,size,units));
		sb.append("</td>");
		// List
		sb.append("<td>");
		if(duccwork instanceof DuccWorkReservation) {
			if(!nodeMap.isEmpty()) {
				sb.append("<select>");
				for (String node: nodeMap.keySet()) {
					Integer count = nodeMap.get(node);
					String option = node+" "+"["+count+"]";
					sb.append("<option>"+option+"</option>");
				}
				sb.append("</select>");
			}
		}
		else {
			//TODO
		}
		sb.append("</td>");
		// Description
		sb.append("<td>");
		String description = stringNormalize(duccwork.getStandardInfo().getDescription(),messages.fetch("none"));
		switch(getDescriptionStyle(request)) {
		case Long:
		default:
			sb.append("<span>");
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
	
	private boolean isListEligible(ArrayList<String> users, FilterUsersStyle filterUsersStyle, String user, boolean completed) {
		boolean list = false;
		if(!users.isEmpty()) {
			switch(filterUsersStyle) {
			case IncludePlusActive:
				if(!completed) {
					list = true;
				}
				else if(users.contains(user)) {
						list = true;
				}
				break;
			case ExcludePlusActive:
				if(!completed) {
					list = true;
				}
				else if(!users.contains(user)) {
						list = true;
				}
				break;
			case Include:
				if(users.contains(user)) {
						list = true;
				}
				break;
			case Exclude:
				if(!users.contains(user)) {
						list = true;
				}
				break;
			}	
		}
		else {
			if(!completed) {
				list = true;
			}
			else 
				list = true;
		}
		return list;
	}
	
	private void handleServletLegacyReservations(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleServletLegacyReservations";
		duccLogger.trace(methodName, jobid, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		
		int maxRecords = getReservationsMax(request);
		
		DuccData duccData = DuccData.getInstance();
		
		ConcurrentSkipListMap<ReservationInfo,ReservationInfo> sortedReservations = duccData.getSortedReservations();
		ConcurrentSkipListMap<JobInfo, JobInfo> sortedServices = duccData.getSortedServices();
		ConcurrentSkipListMap<Long, Object> sortedCombined = new ConcurrentSkipListMap<Long, Object>();
		
		ArrayList<String> users = getReservationsUsers(request);
		FilterUsersStyle filterUsersStyle = getFilterUsersStyle(request);
		
		if((sortedReservations.size() > 0) || (sortedServices.size() > 0)) {
			Iterator<Entry<ReservationInfo, ReservationInfo>> iR = sortedReservations.entrySet().iterator();
			while(iR.hasNext()) {
				ReservationInfo reservationInfo = iR.next().getValue();
				DuccWorkReservation reservation = reservationInfo.getReservation();
				String user = reservation.getStandardInfo().getUser().trim();
				boolean completed = reservation.isCompleted();
				if(isListEligible(users, filterUsersStyle, user, completed)) {
					Long key = new Long(reservation.getDuccId().getFriendly());
					sortedCombined.put(key, reservation);
				}
			}
			Iterator<Entry<JobInfo, JobInfo>> iS = sortedServices.entrySet().iterator();
			while(iS.hasNext()) {
				JobInfo jobInfo = iS.next().getValue();
				DuccWorkJob job = jobInfo.getJob();
				ServiceDeploymentType sdt = job.getServiceDeploymentType();
				if(sdt != null) {
					switch(sdt) {
					case uima:
					case custom:
					default:
						break;
					case other:
						String user = job.getStandardInfo().getUser().trim();
						boolean completed = job.isCompleted();
						if(isListEligible(users, filterUsersStyle, user, completed)) {
							Long key = new Long(job.getDuccId().getFriendly());
							sortedCombined.put(key, job);
						}
						break;
					}
				}
			}
			if(sortedCombined.size() > 0) {
				int counter = 0;
				Iterator<Long> keys = sortedCombined.descendingKeySet().iterator();
				while(keys.hasNext()) {
					Long key = keys.next();
					Object object = sortedCombined.get(key);
					if(object instanceof DuccWorkReservation) {
						DuccWorkReservation reservation = (DuccWorkReservation) object;
						sb.append(trGet(counter));
						buildReservationsListEntry(request, sb, reservation.getDuccId(), reservation, duccData);
					}
					else if(object instanceof DuccWorkJob) {
						DuccWorkJob job = (DuccWorkJob) object;
						sb.append(trGet(counter));
						buildReservationsListEntry(request, sb, job.getDuccId(), job, duccData);
					}
					else {
						// huh?
					}
					if(counter++ > maxRecords) {
						break;
					}
				}
			}
			else {
				sb.append("<tr>");
				sb.append("<td>");
				sb.append(messages.fetch("no reservations meet criteria"));
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
	
	private void handleServletLegacyServices(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleServletLegacyServices";
		duccLogger.trace(methodName, jobid, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		
		DuccDataHelper duccDataHelper = DuccDataHelper.getInstance();
		TreeMap<String, ArrayList<DuccId>> serviceToJobsMap = duccDataHelper.getServiceToJobsUsageMap();
		TreeMap<String, ArrayList<String>> serviceToServicesMap = duccDataHelper.getServiceToServicesUsageMap();
		TreeMap<String, ArrayList<DuccId>> serviceToReservationsMap = duccDataHelper.getServiceToReservationsUsageMap();
		
		ServicesRegistry servicesRegistry = new ServicesRegistry();
		ServicesRegistryMap map = servicesRegistry.getMap();
		if(!map.isEmpty()) {
			int counter = 0;
			for(Integer key : map.getDescendingKeySet()) {
				ServicesRegistryMapPayload entry = map.get(key);
				Properties propertiesSvc = entry.get(IServicesRegistry.svc);
				Properties propertiesMeta = entry.get(IServicesRegistry.meta);
				String name = getValue(propertiesMeta,IServicesRegistry.endpoint,"");
				String user = getValue(propertiesMeta,IServicesRegistry.user,"");
				String sid = getValue(propertiesMeta,IServicesRegistry.numeric_id,"");
				String instances = getValue(propertiesMeta,IStateServices.instances,"");
				String deployments = getDeployments(servicesRegistry,propertiesMeta);
				sb.append(trGet(++counter));
				
				String typeRegistered = "Registered";
				
				String type = "";
				if(propertiesMeta != null) {
					if(propertiesMeta.containsKey(IServicesRegistry.service_class)) {
						String value = propertiesMeta.getProperty(IServicesRegistry.service_class);
						if(value != null) {
							type = value.trim();
						}
					}
				}
				
				// Start
				sb.append("<td valign=\"bottom\" class=\"ducc-col-start\">");
				if(type.equals(typeRegistered)) {
					if(buttonsEnabled) {
						if(!deployments.equals(instances)) {
							sb.append("<input type=\"button\" onclick=\"ducc_confirm_service_start("+sid+")\" value=\"Start\" "+getDisabled(request,user)+"/>");
						}
					}
				}
				sb.append("</td>");
				// Stop
				sb.append("<td valign=\"bottom\" class=\"ducc-col-stop\">");
				if(type.equals(typeRegistered)) {
					if(buttonsEnabled) {
						if(!deployments.equals("0")) {
							sb.append("<input type=\"button\" onclick=\"ducc_confirm_service_stop("+sid+")\" value=\"Stop\" "+getDisabled(request,user)+"/>");
						}
					}
				}
				sb.append("</td>");
				// Service Id
				sb.append("<td align=\"right\">");
				String id = "<a href=\"service.details.html?name="+name+"\">"+key+"</a>";
				sb.append(id);
				sb.append("</td>");
				// Endpoint
				sb.append("<td>");
				sb.append(name);
				sb.append("</td>");
				// Type
				sb.append("<td>");
				sb.append(type);
				sb.append("</td>");
				// State
				sb.append("<td>");
				String state = DuccHandlerUtils.getUninterpreted(propertiesMeta, IServicesRegistry.service_state);
				sb.append(state);
				sb.append("</td>");
				// Pinging
				sb.append("<td>");
				String pinging = DuccHandlerUtils.getInterpretedYesNo(state, propertiesMeta, IServicesRegistry.ping_active);
				String decoratedPinging = DuccHandlerUtils.getDecorated(pinging,null);
				sb.append(decoratedPinging);
				sb.append("</td>");
				// Health
				sb.append("<td>");
				String health = DuccHandlerUtils.getInterpretedGoodPoor(state, propertiesMeta, IServicesRegistry.service_healthy);
				String statistics = null;
				if(state.equalsIgnoreCase(IServicesRegistry.constant_Available)) {
					statistics = propertiesMeta.getProperty(IServicesRegistry.service_statistics);
					if(statistics != null) {
						statistics = statistics.trim();
					}
				}
				String decoratedHealth = DuccHandlerUtils.getDecorated(health,statistics);
				sb.append(decoratedHealth);
				sb.append("</td>");
				// No. of Instances
				sb.append("<td align=\"right\">");
				sb.append(instances);
				sb.append("</td>");
				// No. of Deployments
				sb.append("<td align=\"right\">");
				sb.append(deployments);
				sb.append("</td>");
				// Owning User
				sb.append("<td>");
				sb.append(getValue(propertiesMeta,IServicesRegistry.user,""));
				sb.append("</td>");
				// Scheduling Class
				sb.append("<td>");
				sb.append(getValue(propertiesSvc,IServicesRegistry.scheduling_class,""));
				sb.append("</td>");
				// Process Memory Size
				sb.append("<td align=\"right\">");
				sb.append(getValue(propertiesSvc,IServicesRegistry.process_memory_size,""));
				sb.append("</td>");
				// Jobs			
				sb.append("<td align=\"right\">");
				String jobs = "0";
				if(serviceToJobsMap.containsKey(name)) {
					ArrayList<DuccId> duccIds = serviceToJobsMap.get(name);
					int size = duccIds.size();
					if(size > 0) {
						StringBuffer idList = new StringBuffer();
						for(DuccId duccId : duccIds) {
							if(idList.length() > 0) {
								idList.append(",");
							}
							idList.append(duccId);
						}
						String title = "active Job Id list: "+idList;
						jobs = "<span title=\""+title+"\">"+size+"</span>";
					}
				}
				sb.append(jobs);
				sb.append("</td>");
				// Services
				sb.append("<td align=\"right\">");
				String services = "0";
				if(serviceToServicesMap.containsKey(name)) {
					ArrayList<String> duccIds = serviceToServicesMap.get(name);
					int size = duccIds.size();
					if(size > 0) {
						StringBuffer idList = new StringBuffer();
						for(String duccId : duccIds) {
							if(idList.length() > 0) {
								idList.append(",");
							}
							idList.append(duccId);
						}
						String title = "active Service Id list: "+idList;
						services = "<span title=\""+title+"\">"+size+"</span>";
					}
				}
				sb.append(services);
				sb.append("</td>");
				// Reservations
				sb.append("<td align=\"right\">");
				String reservations = "0";
				if(serviceToReservationsMap.containsKey(name)) {
					ArrayList<DuccId> duccIds = serviceToReservationsMap.get(name);
					int size = duccIds.size();
					if(size > 0) {
						StringBuffer idList = new StringBuffer();
						for(DuccId duccId : duccIds) {
							if(idList.length() > 0) {
								idList.append(",");
							}
							idList.append(duccId);
						}
						String title = "active Reservation Id list: "+idList;
						reservations = "<span title=\""+title+"\">"+size+"</span>";
					}
				}
				sb.append(reservations);
				sb.append("</td>");
				// Description
				sb.append("<td>");
				String description = getValue(propertiesSvc,IServicesRegistry.description,"");
				switch(getDescriptionStyle(request)) {
				case Long:
				default:
					sb.append("<span>");
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
	
	private void handleServletLegacySystemClasses(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleServletLegacySystemClasses";
		duccLogger.trace(methodName, jobid, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		
		DuccWebSchedulerClasses schedulerClasses = new DuccWebSchedulerClasses(getFileName());
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
		
		duccLogger.debug(methodName, jobid, sb);
		response.getWriter().println(sb);
		duccLogger.trace(methodName, jobid, messages.fetch("exit"));
	}		
	
	private void handleServletLegacySystemDaemons(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleServletLegacySystemDaemons";
		duccLogger.trace(methodName, jobid, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		
		DuccDaemonsData duccDaemonsData = DuccDaemonsData.getInstance();
		int counter = 0;
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
			// Status
			sb.append(trGet(counter));
			sb.append("<td>");
			sb.append(status);
			sb.append("</td>");	
			// Daemon Name
			sb.append("<td>");
			sb.append(getPropertiesValue(properties,DuccDaemonRuntimeProperties.keyDaemonName,daemonName.toString()));
			sb.append("</td>");
			// Boot Time
			sb.append("<td>");
			sb.append(getTimeStamp(getDateStyle(request),getPropertiesValue(properties,DuccDaemonRuntimeProperties.keyBootTime,"")));
			sb.append("</td>");
			// Host IP
			sb.append("<td>");
			sb.append(getPropertiesValue(properties,DuccDaemonRuntimeProperties.keyNodeIpAddress,""));
			sb.append("</td>");	
			// Host Name
			sb.append("<td>");
			sb.append(getPropertiesValue(properties,DuccDaemonRuntimeProperties.keyNodeName,""));
			sb.append("</td>");
			// PID
			sb.append("<td>");
			sb.append(getPropertiesValue(properties,DuccDaemonRuntimeProperties.keyPid,""));
			sb.append("</td>");
			// Publication Size (last)
			sb.append("<td align=\"right\">");
			Long pubSize = duccDaemonsData.getEventSize(daemonName);
			sb.append(""+pubSize);
			sb.append("</td>");	
			// Publication Size (max)
			sb.append("<td align=\"right\">");
			Long pubSizeMax = duccDaemonsData.getEventSizeMax(daemonName);
			sb.append(""+pubSizeMax);
			sb.append("</td>");	
			// Heartbeat (last)
			sb.append("<td align=\"right\">");
			sb.append(heartbeat);
			sb.append("</td>");	
			// Heartbeat (max)
			sb.append("<td align=\"right\">");
			sb.append(heartmax);
			sb.append("</td>");
			// Heartbeat (max) TOD
			sb.append("<td>");
			String heartmaxTOD = TimeStamp.simpleFormat(DuccDaemonsData.getInstance().getMaxHeartbeatTOD(daemonName));
			try {
				heartmaxTOD = getTimeStamp(getDateStyle(request),heartmaxTOD);
			}
			catch(Exception e) {
			}
			sb.append(heartmaxTOD);
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
		// <Agents>
		String cookie = DuccWebUtil.getCookie(request,DuccWebUtil.cookieAgents);
		if(cookie.equals(DuccWebUtil.valueAgentsShow)) {
			duccLogger.trace(methodName, jobid, "== show: "+cookie);
			
			ConcurrentSkipListMap<String,MachineInfo> machines = DuccMachinesData.getInstance().getMachines();
			Iterator<String> iterator = machines.keySet().iterator();
			while(iterator.hasNext()) {
				String key = iterator.next();
				MachineInfo machineInfo = machines.get(key);
				Properties properties = DuccDaemonRuntimeProperties.getInstance().getAgent(machineInfo.getName());
				sb.append(trGet(counter));
				// Status
				StringBuffer status = new StringBuffer();
				String machineStatus = machineInfo.getStatus();
				if(machineStatus.equals("down")) {
					//status.append("<span class=\"health_red\""+">");
					status.append(machineStatus);
					//status.append("</span>");
				}
				else if(machineStatus.equals("up")) {
					//status.append("<span class=\"health_green\""+">");
					status.append(machineStatus);
					//status.append("</span>");
				}
				else {
					status.append("unknown");
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
				String bootTime = getTimeStamp(getDateStyle(request),getPropertiesValue(properties,DuccDaemonRuntimeProperties.keyBootTime,""));
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
						fmtHeartbeatMaxTOD = getTimeStamp(getDateStyle(request),fmtHeartbeatMaxTOD);
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
	
	private void handleServletLegacySystemMachines(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleServletLegacySystemMachines";
		duccLogger.trace(methodName, jobid, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		
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
		
		// Total
		sb.append("<tr>");
		// Status
		sb.append("<td>");
		sb.append(""+"Total");
		sb.append("</td>");
		// IP
		sb.append("<td>");
		sb.append("");
		sb.append("</td>");
		// Name
		sb.append("<td>");
		sb.append("");
		sb.append("</td>");
		// Mem(GB):total
		sb.append("<td align=\"right\">");
		sb.append(""+memTotal);
		sb.append("</td>");
		// Swap(GB):inuse
		sb.append("<td align=\"right\">");
		sb.append(""+memSwap);
		sb.append("</td>");
		// Alien PIDs
		sb.append("<td align=\"right\">");
		sb.append(""+alienPids);
		sb.append("</td>");
		// Shares:total
		sb.append("<td align=\"right\">");
		sb.append(""+sharesTotal);
		sb.append("</td>");
		// Shares:inuse
		sb.append("<td align=\"right\">");
		sb.append(""+sharesInuse);
		sb.append("</td>");
		// Heartbeat (last)
		sb.append("<td align=\"right\">");
		sb.append("");
		sb.append("</td>");
	
		// pass 2
		int counter = 0;
		iterator = sortedMachines.keySet().iterator();
		while(iterator.hasNext()) {
			MachineInfo machineInfo = iterator.next();
			sb.append(trGet(counter));
			// Status
			sb.append("<td>");
			String status = machineInfo.getStatus();
			if(status.equals("down")) {
				sb.append("<span class=\"health_red\""+">");
				sb.append(status);
				sb.append("</span>");
			}
			else if(status.equals("up")) {
				sb.append("<span class=\"health_green\""+">");
				sb.append(status);
				sb.append("</span>");
			}
			else {
				sb.append("<span title=\""+"File:"+machineInfo.getFileDef()+"\""+">");
				sb.append(status);
			}
			sb.append("</td>");
			// IP
			sb.append("<td>");
			sb.append(""+machineInfo.getIp());
			sb.append("</td>");
			// Name
			sb.append("<td>");
			sb.append(""+machineInfo.getName());
			sb.append("</td>");
			// Mem(GB):total
			sb.append("<td align=\"right\">");
			sb.append(""+machineInfo.getMemTotal());
			sb.append("</td>");
			// Swap(GB):inuse
			sb.append("<td align=\"right\">");
			String swapping = machineInfo.getMemSwap();
			if(swapping.equals("0")) {
				sb.append(swapping);
			}
			else {
				sb.append("<span class=\"health_red\">");
				sb.append(swapping);
				sb.append("</span>");
			}
			sb.append("</td>");
			// Alien PIDs
			sb.append("<td align=\"right\">");
			long aliens = machineInfo.getAlienPidsCount();
			if(aliens == 0) {
				sb.append(aliens);
			}
			else {
				sb.append("<span class=\"health_red\">");
				sb.append(aliens);
				sb.append("</span>");
			}
			sb.append("</td>");
			// Shares:total
			sb.append("<td align=\"right\">");
			sb.append(""+machineInfo.getSharesTotal());
			sb.append("</td>");
			// Shares:inuse
			sb.append("<td align=\"right\">");
			sb.append(""+machineInfo.getSharesInuse());
			sb.append("</td>");
			// Heartbeat (last)
			sb.append("<td align=\"right\">");
			sb.append(""+machineInfo.getElapsed());
			sb.append("</td>");
			counter++;
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
	throws IOException, ServletException
	{
		String methodName = "handleDuccRequest";
		duccLogger.trace(methodName, jobid, messages.fetch("enter"));
		duccLogger.debug(methodName, jobid,request.toString());
		duccLogger.debug(methodName, jobid,"getRequestURI():"+request.getRequestURI());
		String reqURI = request.getRequestURI()+"";
		if(reqURI.startsWith(legacyJobs)) {
			handleServletLegacyJobs(target, baseRequest, request, response);
		}
		else if(reqURI.startsWith(legacyReservations)) {
			handleServletLegacyReservations(target, baseRequest, request, response);
		}
		else if(reqURI.startsWith(legacyServices)) {
			handleServletLegacyServices(target, baseRequest, request, response);
		}
		else if(reqURI.startsWith(legacySystemClasses)) {
			handleServletLegacySystemClasses(target, baseRequest, request, response);
		}
		else if(reqURI.startsWith(legacySystemDaemons)) {
			handleServletLegacySystemDaemons(target, baseRequest, request, response);
		}
		else if(reqURI.startsWith(legacySystemMachines)) {
			handleServletLegacySystemMachines(target, baseRequest, request, response);
		}
		else {
			handleServletUnknown(target, baseRequest, request, response);
		}
		duccLogger.trace(methodName, jobid, messages.fetch("exit"));
	}
	
	@Override
	public void handle(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException {
		String methodName = "handle";
		try{ 
			duccLogger.debug(methodName, jobid,request.toString());
			duccLogger.debug(methodName, jobid,"getRequestURI():"+request.getRequestURI());
			String reqURI = request.getRequestURI()+"";
			if(reqURI.startsWith(duccContextLegacy)) {
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
