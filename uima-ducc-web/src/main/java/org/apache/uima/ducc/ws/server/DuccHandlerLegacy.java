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
import org.apache.uima.ducc.common.persistence.services.StateServices;
import org.apache.uima.ducc.common.persistence.services.StateServicesDirectory;
import org.apache.uima.ducc.common.persistence.services.StateServicesSet;
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
import org.apache.uima.ducc.transport.event.common.IDuccWorkJob;
import org.apache.uima.ducc.transport.event.common.IDuccWorkReservation;
import org.apache.uima.ducc.transport.event.common.IRationale;
import org.apache.uima.ducc.ws.DuccDaemonsData;
import org.apache.uima.ducc.ws.DuccData;
import org.apache.uima.ducc.ws.DuccMachinesData;
import org.apache.uima.ducc.ws.JobInfo;
import org.apache.uima.ducc.ws.MachineInfo;
import org.apache.uima.ducc.ws.ReservationInfo;
import org.apache.uima.ducc.ws.types.NodeId;
import org.apache.uima.ducc.ws.types.UserId;
import org.eclipse.jetty.server.Request;

public class DuccHandlerLegacy extends DuccAbstractHandler {
	
	private static DuccLogger duccLogger = DuccLoggerComponents.getWsLogger(DuccHandlerLegacy.class.getName());
	private static Messages messages = Messages.getInstance();
	private static DuccId jobid = null;

	public final String legacyJobs 					= duccContextLegacy+"-jobs-data";
	public final String legacyReservations 			= duccContextLegacy+"-reservations-data";
	public final String legacyServicesDefinitions 	= duccContextLegacy+"-services-definitions-data";
	public final String legacyServicesDeployments 	= duccContextLegacy+"-services-deployments-data";
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
	
	private void buildJobsListEntry(HttpServletRequest request, StringBuffer sb, DuccId duccId, IDuccWorkJob job, DuccData duccData) {
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
			title = "title=\"job submitter PID@host: "+submitter+"\" ";
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
				}
				else {
					sb.append("<span class=\"health_red\" title=\""+text+"\">");
				}
				sb.append("WaitTimeout");
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

	private void handleServletLegacyJobs(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleServletLegacyJobs";
		duccLogger.trace(methodName, jobid, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		
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
		
		duccLogger.debug(methodName, jobid, sb);
		response.getWriter().println(sb);
		duccLogger.trace(methodName, jobid, messages.fetch("exit"));
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
		sb.append(getTimeStamp(request,reservation.getDuccId(),reservation.getStandardInfo().getDateOfSubmission()));
		sb.append("</td>");
		// End
		sb.append("<td>");
		switch(reservation.getReservationState()) {
		case Completed:
			sb.append(getTimeStamp(request,reservation.getDuccId(),reservation.getStandardInfo().getDateOfCompletion()));
			break;
		default:
			break;
		}
		sb.append("</td>");
		// User
		String title = "";
		String submitter = reservation.getStandardInfo().getSubmitter();
		if(submitter != null) {
			title = " title=\"reservation submitter PID@host: "+submitter+"\"";
		}
		sb.append("<td"+title+">");
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
	
	private void handleServletLegacyReservations(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleServletLegacyReservations";
		duccLogger.trace(methodName, jobid, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		
		int maxRecords = getReservationsMax(request);
		ArrayList<String> users = getReservationsUsers(request);
		DuccData duccData = DuccData.getInstance();
		ConcurrentSkipListMap<ReservationInfo,ReservationInfo> sortedReservations = duccData.getSortedReservations();
		FilterUsersStyle filterUsersStyle = getFilterUsersStyle(request);
		if(sortedReservations.size()> 0) {
			Iterator<Entry<ReservationInfo, ReservationInfo>> iterator = sortedReservations.entrySet().iterator();
			int counter = 0;
			while(iterator.hasNext()) {
				ReservationInfo reservationInfo = iterator.next().getValue();
				DuccWorkReservation reservation = reservationInfo.getReservation();
				boolean list = false;
				if(!users.isEmpty()) {
					String reservationUser = reservation.getStandardInfo().getUser().trim();
					switch(filterUsersStyle) {
					case IncludePlusActive:
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
					case ExcludePlusActive:
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
		
		duccLogger.debug(methodName, jobid, sb);
		response.getWriter().println(sb);
		duccLogger.trace(methodName, jobid, messages.fetch("exit"));
	}	
	
	private void handleServletLegacyServicesDefinitions(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleServletLegacyServicesDefinitions";
		duccLogger.trace(methodName, jobid, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		
		IStateServices iss = StateServices.getInstance();
		StateServicesDirectory ssd = iss.getStateServicesDirectory();
		if(!ssd.getDescendingKeySet().isEmpty()) {
			for(Integer key : ssd.getDescendingKeySet()) {
				StateServicesSet entry = ssd.get(key);
				Properties propertiesSvc = entry.get(IStateServices.svc);
				Properties propertiesMeta = entry.get(IStateServices.meta);
				sb.append("<tr>");
				// Service Id
				sb.append("<td>");
				sb.append(""+key);
				sb.append("</td>");
				// Endpoint
				sb.append("<td>");
				sb.append(getValue(propertiesMeta,IStateServices.endpoint,""));
				sb.append("</td>");
				// No. of Instances
				sb.append("<td>");
				sb.append(getValue(propertiesMeta,IStateServices.instances,""));
				sb.append("</td>");
				// Owning User
				sb.append("<td>");
				sb.append(getValue(propertiesMeta,IStateServices.user,""));
				sb.append("</td>");
				// Scheduling Class
				sb.append("<td>");
				sb.append(getValue(propertiesSvc,IStateServices.scheduling_class,""));
				sb.append("</td>");
				// Process Memory Size
				sb.append("<td>");
				sb.append(getValue(propertiesSvc,IStateServices.process_memory_size,""));
				sb.append("</td>");
				// Description
				sb.append("<td>");
				sb.append(getValue(propertiesSvc,IStateServices.description,""));
				sb.append("</td>");
				sb.append("</tr>");
			}
		}
		else {
			sb.append("<tr>");
			sb.append("<td>");
			if(DuccData.getInstance().isPublished()) {
				sb.append(messages.fetch("no services definitions"));
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
	
	private void buildServicesListEntry(HttpServletRequest request, StringBuffer sb, DuccId duccId, IDuccWorkJob job, DuccData duccData) {
		String type = "Service";
		String id = normalize(duccId);
		// Terminate
		sb.append("<td class=\"ducc-col-terminate\">");
		if(terminateEnabled) {
			if(!job.isFinished()) {
				sb.append("<input type=\"button\" onclick=\"ducc_confirm_terminate_service("+id+")\" value=\"Terminate\" "+getDisabled(request,job)+"/>");
			}
		}
		sb.append("</td>");
		// Id
		sb.append("<td>");
		sb.append("<a href=\"service.details.html?id="+id+"\">"+id+"</a>");
		sb.append("</td>");
		// Start
		sb.append("<td>");
		sb.append(getTimeStamp(request,job.getDuccId(), job.getStandardInfo().getDateOfSubmission()));
		sb.append("</td>");
		// End
		sb.append("<td>");
		sb.append(getCompletionOrProjection(request,job));
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

	private void handleServletLegacyServicesDeployments(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleServletLegacyServicesDeployments";
		duccLogger.trace(methodName, jobid, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		
		int maxRecords = getServicesMax(request);
		ArrayList<String> users = getServicesUsers(request);
		DuccData duccData = DuccData.getInstance();
		ConcurrentSkipListMap<JobInfo,JobInfo> sortedServices = duccData.getSortedServices();
		FilterUsersStyle filterUsersStyle = getFilterUsersStyle(request);
		if(sortedServices.size()> 0) {
			Iterator<Entry<JobInfo, JobInfo>> iterator = sortedServices.entrySet().iterator();
			int counter = 0;
			while(iterator.hasNext()) {
				JobInfo jobInfo = iterator.next().getValue();
				DuccWorkJob service = jobInfo.getJob();
				boolean list = false;
				if(!users.isEmpty()) {
					String serviceUser = service.getStandardInfo().getUser().trim();
					switch(filterUsersStyle) {
					case IncludePlusActive:
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
					case ExcludePlusActive:
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
		else if(reqURI.startsWith(legacyServicesDefinitions)) {
			handleServletLegacyServicesDefinitions(target, baseRequest, request, response);
		}
		else if(reqURI.startsWith(legacyServicesDeployments)) {
			handleServletLegacyServicesDeployments(target, baseRequest, request, response);
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
