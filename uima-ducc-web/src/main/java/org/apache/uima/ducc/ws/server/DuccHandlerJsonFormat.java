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
import java.util.ListIterator;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;
import org.apache.uima.ducc.cli.ws.json.MachineFacts;
import org.apache.uima.ducc.cli.ws.json.MachineFactsList;
import org.apache.uima.ducc.cli.ws.json.NodePidList;
import org.apache.uima.ducc.cli.ws.json.ReservationFacts;
import org.apache.uima.ducc.cli.ws.json.ReservationFactsList;
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
import org.apache.uima.ducc.transport.event.common.DuccWorkJob;
import org.apache.uima.ducc.transport.event.common.DuccWorkReservation;
import org.apache.uima.ducc.transport.event.common.IDuccCompletionType.JobCompletionType;
import org.apache.uima.ducc.transport.event.common.IDuccReservation;
import org.apache.uima.ducc.transport.event.common.IDuccReservationMap;
import org.apache.uima.ducc.transport.event.common.IDuccState.ReservationState;
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
import org.apache.uima.ducc.ws.registry.IServicesRegistry;
import org.apache.uima.ducc.ws.registry.ServicesRegistry;
import org.apache.uima.ducc.ws.types.NodeId;
import org.apache.uima.ducc.ws.types.UserId;
import org.eclipse.jetty.server.Request;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

public class DuccHandlerJsonFormat extends DuccAbstractHandler {
	
	private static DuccLogger duccLogger = DuccLoggerComponents.getWsLogger(DuccHandlerJsonFormat.class.getName());
	private static Messages messages = Messages.getInstance();
	private static DuccId jobid = null;

	private final String jsonFormatJobsAaData					= duccContextJsonFormat+"-aaData-jobs";
	private final String jsonFormatReservationsAaData			= duccContextJsonFormat+"-aaData-reservations";
	private final String jsonFormatServicesAaData				= duccContextJsonFormat+"-aaData-services";
	private final String jsonFormatMachinesAaData				= duccContextJsonFormat+"-aaData-machines";
	private final String jsonFormatClassesAaData				= duccContextJsonFormat+"-aaData-classes";
	private final String jsonFormatDaemonsAaData				= duccContextJsonFormat+"-aaData-daemons";
	
	private final String jsonFormatMachines 		= duccContextJsonFormat+"-machines";
	private final String jsonFormatReservations 	= duccContextJsonFormat+"-reservations";
	private DuccWebServer duccWebServer = null;
	
	public DuccHandlerJsonFormat(DuccWebServer duccWebServer) {
		this.duccWebServer = duccWebServer;
	}
	
	public DuccWebServer getDuccWebServer() {
		return duccWebServer;
	}
	
	public String getFileName() {
		return dir_home+File.separator+dir_resources+File.separator+getDuccWebServer().getClassDefinitionFile();
	}
	
	private JsonArray buildJobRow(HttpServletRequest request, IDuccWorkJob job, DuccData duccData) {
		String type="Job";
		JsonArray row = new JsonArray();
		StringBuffer sb;
		DuccId duccId = job.getDuccId();
		// Terminate
		sb = new StringBuffer();
		String id = normalize(duccId);
		sb.append("<span class=\"ducc-col-terminate\">");
		if(terminateEnabled) {
			if(!job.isFinished()) {
				sb.append("<input type=\"button\" onclick=\"ducc_confirm_terminate_job("+id+")\" value=\"Terminate\" "+getDisabled(request,job)+"/>");
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
		row.add(new JsonPrimitive(getTimeStamp(request,job.getDuccId(), job.getStandardInfo().getDateOfSubmission())));
		// End
		row.add(new JsonPrimitive(getCompletionOrProjection(request,job)));
		// User
		sb = new StringBuffer();
		String title = "";
		String submitter = job.getStandardInfo().getSubmitter();
		if(submitter != null) {
			title = "title=\"job submitter PID@host: "+submitter+"\" ";
		}
		sb.append("<span "+title+">");
		sb.append(job.getStandardInfo().getUser());
		sb.append("</span>");
		row.add(new JsonPrimitive(sb.toString()));
		// Class
		row.add(new JsonPrimitive(stringNormalize(job.getSchedulingInfo().getSchedulingClass(),messages.fetch("default"))));
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
		sb = new StringBuffer();
		if(job.isOperational()) {
			boolean multi = false;
			sb.append("<span>");
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
			sb.append("</span>");
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
				sb.append("<span>");
				break;
			case Undefined:
				sb.append("<span>");
				break;
			default:
				IRationale rationale = job.getCompletionRationale();
				if(rationale != null) {
					sb.append("<span title=\""+rationale+"\">");
				}
				else {
					sb.append("<span>");
				}
				break;
			}
			sb.append(jobCompletionType);
			sb.append("</span>");
		}
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
		sb.append("<span>");
		sb.append(buildInitializeFailuresLink(job));
		if(job.getSchedulingInfo().getLongSharesMax() < 0) {
			sb.append("<sup>");
			sb.append("<span title=\"capped at current number of running processes due to excessive initialization failures\">");
			sb.append("^");
			sb.append("</span>");
			sb.append("</sup>");
		}
		sb.append("</span>");
		row.add(new JsonPrimitive(sb.toString()));
		// Run Fails
		sb = new StringBuffer();
		sb.append("<span>");
		sb.append(buildRuntimeFailuresLink(job));
		sb.append("</span>");
		row.add(new JsonPrimitive(sb.toString()));
		// Size
		sb = new StringBuffer();
		sb.append("<span>");
		String size = job.getSchedulingInfo().getShareMemorySize();
		MemoryUnits units = job.getSchedulingInfo().getShareMemoryUnits();
		sb.append(getProcessMemorySize(duccId,type,size,units));
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
		sb.append(job.getSchedulingInfo().getWorkItemsCompleted());
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
		sb.append("<span>");
		if(duccData.isLive(duccId)) {
			sb.append(job.getSchedulingInfo().getWorkItemsDispatched());
		}
		else {
			sb.append("0");
		}
		sb.append("</span>");
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
		sb.append("<span>");
		sb.append(stringNormalize(job.getStandardInfo().getDescription(),messages.fetch("none")));
		sb.append("</span>");
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
					JsonArray row = buildJobRow(request, job, duccData);
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
			// End
			row.add(new JsonPrimitive(""));
			// User
			row.add(new JsonPrimitive(""));
			// Class
			row.add(new JsonPrimitive(""));
			// State
			row.add(new JsonPrimitive(""));
			// Reason
			row.add(new JsonPrimitive(""));
			// Processes
			row.add(new JsonPrimitive(""));
			// Init Fails
			row.add(new JsonPrimitive(""));
			// Run Fails
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
	
	
	private JsonArray buildReservationRow(HttpServletRequest request, IDuccWorkReservation reservation, DuccData duccData) {
		String type="Reservation";
		JsonArray row = new JsonArray();
		StringBuffer sb;
		DuccId duccId = reservation.getDuccId();
		// Terminate
		sb = new StringBuffer();
		String id = normalize(duccId);
		sb.append("<span class=\"ducc-col-terminate\">");
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
		sb.append("</span>");
		row.add(new JsonPrimitive(sb.toString()));
		// Id
		sb = new StringBuffer();
		sb.append("<span>");
		sb.append(id);
		sb.append("</span>");
		row.add(new JsonPrimitive(sb.toString()));
		// Start
		row.add(new JsonPrimitive(getTimeStamp(request,reservation.getDuccId(), reservation.getStandardInfo().getDateOfSubmission())));
		// End
		sb = new StringBuffer();
		sb.append("<span>");
		switch(reservation.getReservationState()) {
		case Completed:
			sb.append(getTimeStamp(request,reservation.getDuccId(),reservation.getStandardInfo().getDateOfCompletion()));
			break;
		default:
			break;
		}
		sb.append("</span>");
		row.add(new JsonPrimitive(sb.toString()));
		// User
		sb = new StringBuffer();
		String title = "";
		String submitter = reservation.getStandardInfo().getSubmitter();
		if(submitter != null) {
			title = " title=\"reservation submitter PID@host: "+submitter+"\"";
		}
		sb.append("<span"+title+">");
		UserId userId = new UserId(reservation.getStandardInfo().getUser());
		sb.append(userId.toString());
		sb.append("</span>");
		row.add(new JsonPrimitive(sb.toString()));
		// Class
		row.add(new JsonPrimitive(stringNormalize(reservation.getSchedulingInfo().getSchedulingClass(),messages.fetch("default"))));
		// State
		sb = new StringBuffer();
		String state = reservation.getStateObject().toString();
		sb.append("<span>");
		if(duccData.isLive(duccId)) {
			if(reservation.isOperational()) {
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
		row.add(new JsonPrimitive(sb.toString()));
		// Allocation
		sb = new StringBuffer();
		sb.append("<span>");
		sb.append(reservation.getSchedulingInfo().getInstancesCount());
		sb.append("</sppn>");
		row.add(new JsonPrimitive(sb.toString()));
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
		sb = new StringBuffer();
		boolean qualify = false;
		if(!nodeMap.isEmpty()) {
			if(nodeMap.keySet().size() > 1) {
				qualify = true;
			}
		}
		sb.append("<span>");
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
		sb.append("</span>");
		row.add(new JsonPrimitive(sb.toString()));
		// Size
		sb = new StringBuffer();
		sb.append("<span>");
		String size = reservation.getSchedulingInfo().getShareMemorySize();
		MemoryUnits units = reservation.getSchedulingInfo().getShareMemoryUnits();
		sb.append(getProcessMemorySize(duccId,type,size,units));
		sb.append("</span>");
		row.add(new JsonPrimitive(sb.toString()));
		// List
		sb = new StringBuffer();
		sb.append("<span>");
		if(!nodeMap.isEmpty()) {
			sb.append("<select>");
			for (String node: nodeMap.keySet()) {
				Integer count = nodeMap.get(node);
				String option = node+" "+"["+count+"]";
				sb.append("<option>"+option+"</option>");
			}
			sb.append("</select>");
		}
		sb.append("</span>");
		row.add(new JsonPrimitive(sb.toString()));
		// Description
		sb = new StringBuffer();
		sb.append("<span>");
		sb.append(stringNormalize(reservation.getStandardInfo().getDescription(),messages.fetch("none")));
		sb.append("</span>");
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
					String reservationUser =reservation.getStandardInfo().getUser().trim();
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
					JsonArray row = buildReservationRow(request, reservation, duccData);
					data.add(row);
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
	
	private void handleServletJsonFormatServicesAaData(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleServletJsonFormatServicesAaData";
		duccLogger.trace(methodName, jobid, messages.fetch("enter"));
		
		JsonObject jsonResponse = new JsonObject();
		JsonArray data = new JsonArray();

		ServicesRegistry servicesRegistry = new ServicesRegistry();
		
		IStateServices iss = StateServices.getInstance();
		StateServicesDirectory ssd = iss.getStateServicesDirectory();
		if(ssd.getDescendingKeySet().size() > 0) {
			for(Integer key : ssd.getDescendingKeySet()) {
				StateServicesSet entry = ssd.get(key);
				Properties propertiesSvc = entry.get(IStateServices.svc);
				Properties propertiesMeta = entry.get(IStateServices.meta);
				String name = getValue(propertiesMeta,IServicesRegistry.endpoint,"");
				String user = getValue(propertiesMeta,IServicesRegistry.user,"");
				String sid = getValue(propertiesMeta,IServicesRegistry.numeric_id,"");
				String instances = getValue(propertiesMeta,IStateServices.instances,"");
				String deployments = getDeployments(servicesRegistry,propertiesMeta);
				JsonArray row = new JsonArray();
				
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
				
				StringBuffer col;
				// Start
				col = new StringBuffer();
				if(type.equals(typeRegistered)) {
					col.append("<span class=\"ducc-col-start\">");
					if(buttonsEnabled) {
						if(!deployments.equals(instances)) {
							col.append("<input type=\"button\" onclick=\"ducc_confirm_service_start("+sid+")\" value=\"Start\" "+getDisabled(request,user)+"/>");
						}
					}
					col.append("</span>");
				}
				row.add(new JsonPrimitive(col.toString()));
				// Stop
				col = new StringBuffer();
				if(type.equals(typeRegistered)) {
					col.append("<span class=\"ducc-col-stop\">");
					if(buttonsEnabled) {
						if(!deployments.equals("0")) {
							col.append("<input type=\"button\" onclick=\"ducc_confirm_service_stop("+sid+")\" value=\"Stop\" "+getDisabled(request,user)+"/>");
						}
					}
					col.append("</span>");
				}
				row.add(new JsonPrimitive(col.toString()));
				// Id
				String id = "<a href=\"service.details.html?name="+name+"\">"+key+"</a>";
				row.add(new JsonPrimitive(id));
				// Endpoint
				row.add(new JsonPrimitive(name));
				// Type
				row.add(new JsonPrimitive(type));
				// State
				String state = "";
				if(propertiesMeta != null) {
					if(propertiesMeta.containsKey(IServicesRegistry.service_state)) {
						String value = propertiesMeta.getProperty(IServicesRegistry.service_state);
						if(value != null) {
							state = value;
						}
					}
				}
				row.add(new JsonPrimitive(state));
				// Health
				String health = "";
				if(propertiesMeta != null) {
					if(propertiesMeta.containsKey(IServicesRegistry.ping_active)) {
						String value = propertiesMeta.getProperty(IServicesRegistry.ping_active);
						String text = "";
						if(value != null) {
							value = value.trim();
							if(value.equals("true")) {
								StringBuffer sb = new StringBuffer();
								value = "up";
								text = "pinging";
								sb.append("<span class=\"health_green\" title=\""+text+"\">");
								sb.append(value);
								sb.append("</span>");
								health = sb.toString();
							}
							else {
								StringBuffer sb = new StringBuffer();
								value = "down";
								text = "not pinging";
								sb.append("<span class=\"health_red\" title=\""+text+"\">");
								sb.append(value);
								sb.append("</span>");
								health = sb.toString();
							}
						}
					}
				}
				row.add(new JsonPrimitive(health));
				// Instances
				row.add(new JsonPrimitive(instances));
				// Deployments
				row.add(new JsonPrimitive(deployments));
				// Owning User
				row.add(new JsonPrimitive(getValue(propertiesMeta,IStateServices.user,"")));
				// Scheduling Class
				row.add(new JsonPrimitive(getValue(propertiesSvc,IStateServices.scheduling_class,"")));
				// Size
				row.add(new JsonPrimitive(getValue(propertiesSvc,IStateServices.process_memory_size,"")));
				// Description
				row.add(new JsonPrimitive(getValue(propertiesSvc,IStateServices.description,"")));
				data.add(row);
			}
		}
		else {
			JsonArray row = new JsonArray();
			// Id
			row.add(new JsonPrimitive(""));
			// Endpoint
			row.add(new JsonPrimitive(""));
			// Instances
			row.add(new JsonPrimitive(""));
			// Owning User
			row.add(new JsonPrimitive(""));
			// Scheduling Class
			row.add(new JsonPrimitive(""));
			// Size
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

	private MachineFactsList getMachineFactsList() {
		MachineFactsList factsList = new MachineFactsList();
		DuccMachinesData instance = DuccMachinesData.getInstance();
		ConcurrentSkipListMap<MachineInfo,String> sortedMachines = instance.getSortedMachines();
		Iterator<MachineInfo> iterator;
		iterator = sortedMachines.keySet().iterator();
		while(iterator.hasNext()) {
			MachineInfo machineInfo = iterator.next();
			String status = machineInfo.getStatus();
			String ip = machineInfo.getIp();
			String name = machineInfo.getName();
			String memory = machineInfo.getMemTotal();
			String swap = machineInfo.getMemSwap();
			List<String> aliens = machineInfo.getAliensPidsOnly();
			String sharesTotal = machineInfo.getSharesTotal();
			String sharesInuse = machineInfo.getSharesInuse();
			String heartbeat = ""+machineInfo.getElapsed();
			MachineFacts facts = new MachineFacts(status,ip,name,memory,swap,aliens,sharesTotal,sharesInuse,heartbeat);
			factsList.add(facts);
		}
		return factsList;
	}
	
	private void handleServletJsonFormatMachinesAaData(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleServletJsonFormatMachinesAaData";
		duccLogger.trace(methodName, jobid, messages.fetch("enter"));
		
		JsonObject jsonResponse = new JsonObject();
		JsonArray data = new JsonArray();

		int sumMem = 0;
		int sumSwap = 0;
		int sumAliens = 0;
		int sumSharesTotal = 0;
		int sumSharesInuse = 0;
		
		ListIterator<MachineFacts> listIterator;
		JsonArray row;
		StringBuffer sb;
		
		MachineFactsList factsList = getMachineFactsList();
		if(factsList.size() > 0) {
			// Total
			listIterator = factsList.listIterator();
			while(listIterator.hasNext()) {
				MachineFacts facts = listIterator.next();
				try {
					sumMem += Integer.parseInt(facts.memory);
					sumSwap += Integer.parseInt(facts.swap);
					sumAliens += facts.aliens.size();
					sumSharesTotal += Integer.parseInt(facts.sharesTotal);
					sumSharesInuse += Integer.parseInt(facts.sharesInuse);
				}
				catch(Exception e) {
					duccLogger.error(methodName, jobid, e);
				}
			}
			row = new JsonArray();
			// Status
			row.add(new JsonPrimitive("Total"));
			// IP
			row.add(new JsonPrimitive(""));
			// Name
			row.add(new JsonPrimitive(""));
			// Mem: total
			row.add(new JsonPrimitive(sumMem));
			// Swap: inuse
			row.add(new JsonPrimitive(sumSwap));
			// Alien PIDs
			row.add(new JsonPrimitive(sumAliens));
			// Shares: total
			row.add(new JsonPrimitive(sumSharesTotal));
			// Shares:inuse
			row.add(new JsonPrimitive(sumSharesInuse));
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
					sb.append(status);
				}
				row.add(new JsonPrimitive(sb.toString()));
				// IP
				row.add(new JsonPrimitive(facts.ip));
				// Name
				row.add(new JsonPrimitive(facts.name));
				// Mem: total
				row.add(new JsonPrimitive(facts.memory));
				// Swap: inuse
				sb = new StringBuffer();
				String swapping = facts.swap;
				if(swapping.equals("0")) {
					sb.append(swapping);
				}
				else {
					sb.append("<span class=\"health_red\">");
					sb.append(swapping);
					sb.append("</span>");
				}
				row.add(new JsonPrimitive(sb.toString()));
				// Alien PIDs
				sb = new StringBuffer();
				long aliens = facts.aliens.size();
				if(aliens == 0) {
					sb.append(aliens);
				}
				else {
					sb.append("<span class=\"health_red\">");
					sb.append(aliens);
					sb.append("</span>");
				}
				row.add(new JsonPrimitive(sb.toString()));
				// Shares: total
				row.add(new JsonPrimitive(facts.sharesTotal));
				// Shares:inuse
				row.add(new JsonPrimitive(facts.sharesInuse));
				// Heartbeat: last
				row.add(new JsonPrimitive(facts.heartbeat));
				data.add(row);
			}
		}
		else {
			row = new JsonArray();
			// Status
			row.add(new JsonPrimitive(""));
			// IP
			row.add(new JsonPrimitive(""));
			// Name
			row.add(new JsonPrimitive(""));
			// Mem: total
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
	
	private void handleServletJsonFormatClassesAaData(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleServletJsonFormatClassesAaData";
		duccLogger.trace(methodName, jobid, messages.fetch("enter"));
		
		JsonObject jsonResponse = new JsonObject();
		JsonArray data = new JsonArray();
		JsonArray row;
		
		DuccWebSchedulerClasses schedulerClasses = new DuccWebSchedulerClasses(getFileName());
		DuccProperties properties = schedulerClasses.getClasses();
		String class_set = properties.getProperty("scheduling.class_set");
		class_set.trim();
		if(class_set != null) {
			String[] class_array = StringUtils.split(class_set);
			for(int i=0; i<class_array.length; i++) {
				row = new JsonArray();
				String class_name = class_array[i].trim();
				// Name
				row.add(new JsonPrimitive(class_name));
				// Policy
                String policy = properties.getStringProperty("scheduling.class."+class_name+".policy");
                row.add(new JsonPrimitive(policy));
                // Weight
                String weight = properties.getStringProperty("scheduling.class."+class_name+".share_weight", "100");
                row.add(new JsonPrimitive(weight));
                // Priority
                String priority = properties.getStringProperty("scheduling.class."+class_name+".priority");
                row.add(new JsonPrimitive(priority));
                
                // cap is either absolute or proportional.  if proprotional, it ends with '%'.  It's always
                // either-or so at least one of these columns will have N/A
                
                // Relative & Absolute Caps
				String val = properties.getStringProperty("scheduling.class."+class_name+".cap", "0");
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

				// Initialization Cap
				String defaultInitializationCap = "2";
				val = properties.getStringProperty("scheduling.class."+class_name+".initialization.cap", 
                                                   System.getProperty("ducc.rm.initialization.cap",defaultInitializationCap));
				row.add(new JsonPrimitive(val));

				// Expand-by-Doubling
				boolean bval = properties.getBooleanProperty("scheduling.class."+class_name+".expand.by.doubling", true);
				row.add(new JsonPrimitive(bval));

				// Use Prediction
				String defaultUsePrediction = "true";
				val = properties.getStringProperty("scheduling.class."+class_name+".prediction", 
                                                   System.getProperty("ducc.rm.prediction",defaultUsePrediction));
				row.add(new JsonPrimitive(val));
				
				// Prediction Fudge
				String defaultPredictionFudge = "10000";
				val = properties.getStringProperty("scheduling.class."+class_name+".prediction.fudge",
                                                   System.getProperty("ducc.rm.prediction.fudge",defaultPredictionFudge));
				row.add(new JsonPrimitive(val));

                // max for reserve in in machines.  For fixed is in processes.  No max on fair-share. So slightly
                // ugly code here.
				
				// Max Allocation 
				
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
				row.add(new JsonPrimitive(val));

				// Nodepool
				val = properties.getStringProperty("scheduling.class."+class_name+".nodepool", "--global--");
				row.add(new JsonPrimitive(val));
				
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
	
	private void handleServletJsonFormatDaemonsAaData(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleServletJsonFormatDaemonsAaData";
		duccLogger.trace(methodName, jobid, messages.fetch("enter"));
		
		JsonObject jsonResponse = new JsonObject();
		JsonArray data = new JsonArray();
		JsonArray row;
		
		DuccDaemonsData duccDaemonsData = DuccDaemonsData.getInstance();

		for(DaemonName daemonName : DuccDaemonRuntimeProperties.daemonNames) {
			row = new JsonArray();
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
			row.add(new JsonPrimitive(status));
			// Daemon Name
			String name = getPropertiesValue(properties,DuccDaemonRuntimeProperties.keyDaemonName,daemonName.toString());
			row.add(new JsonPrimitive(name));
			// Boot Time
			String boot = getTimeStamp(getDateStyle(request),getPropertiesValue(properties,DuccDaemonRuntimeProperties.keyBootTime,""));
			row.add(new JsonPrimitive(boot));
			// Host IP
			String ip = getPropertiesValue(properties,DuccDaemonRuntimeProperties.keyNodeIpAddress,"");
			row.add(new JsonPrimitive(ip));
			// Host Name
			String node = getPropertiesValue(properties,DuccDaemonRuntimeProperties.keyNodeName,"");
			row.add(new JsonPrimitive(node));
			// PID
			String pid = getPropertiesValue(properties,DuccDaemonRuntimeProperties.keyPid,"");
			row.add(new JsonPrimitive(pid));
			// Publication Size (last)
			Long pubSize = duccDaemonsData.getEventSize(daemonName);
			row.add(new JsonPrimitive(""+pubSize));
			// Publication Size (max)
			Long pubSizeMax = duccDaemonsData.getEventSizeMax(daemonName);
			row.add(new JsonPrimitive(""+pubSizeMax));
			// Heartbeat (last)
			row.add(new JsonPrimitive(""+heartbeat));
			// Heartbeat (max)
			row.add(new JsonPrimitive(""+heartmax));
			// Heartbeat (max) TOD
			String heartmaxTOD = TimeStamp.simpleFormat(DuccDaemonsData.getInstance().getMaxHeartbeatTOD(daemonName));
			try {
				heartmaxTOD = getTimeStamp(getDateStyle(request),heartmaxTOD);
			}
			catch(Exception e) {
			}
			row.add(new JsonPrimitive(""+heartmaxTOD));
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

		// <Agents>
		String cookie = DuccWebUtil.getCookie(request,DuccWebUtil.cookieAgents);
		if(cookie.equals(DuccWebUtil.valueAgentsShow)) {
			duccLogger.trace(methodName, jobid, "== show: "+cookie);
			
			ConcurrentSkipListMap<String,MachineInfo> machines = DuccMachinesData.getInstance().getMachines();
			Iterator<String> iterator = machines.keySet().iterator();
			while(iterator.hasNext()) {
				row = new JsonArray();
				String key = iterator.next();
				MachineInfo machineInfo = machines.get(key);
				Properties properties = DuccDaemonRuntimeProperties.getInstance().getAgent(machineInfo.getName());
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
				row.add(new JsonPrimitive(status.toString()));
				// Daemon Name
				String daemonName = "Agent";
				row.add(new JsonPrimitive(daemonName));
				// Boot Time
				String bootTime = getTimeStamp(getDateStyle(request),getPropertiesValue(properties,DuccDaemonRuntimeProperties.keyBootTime,""));
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
						fmtHeartbeatMaxTOD = getTimeStamp(getDateStyle(request),fmtHeartbeatMaxTOD);
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
		else {
			duccLogger.trace(methodName, jobid, "!= show: "+cookie);
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
		
		MachineFactsList factsList = getMachineFactsList();
		
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
							String size = getProcessMemorySize(reservation.getDuccId(),"Reservation",reservation.getSchedulingInfo().getShareMemorySize(),reservation.getSchedulingInfo().getShareMemoryUnits());
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
	throws IOException, ServletException
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
		else if(reqURI.startsWith(jsonFormatClassesAaData)) {
			handleServletJsonFormatClassesAaData(target, baseRequest, request, response);
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
	
	@Override
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
