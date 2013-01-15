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

import org.apache.uima.ducc.cli.ws.json.MachineFacts;
import org.apache.uima.ducc.cli.ws.json.MachineFactsList;
import org.apache.uima.ducc.cli.ws.json.NodePidList;
import org.apache.uima.ducc.cli.ws.json.ReservationFacts;
import org.apache.uima.ducc.cli.ws.json.ReservationFactsList;
import org.apache.uima.ducc.common.internationalization.Messages;
import org.apache.uima.ducc.common.jd.JdConstants;
import org.apache.uima.ducc.common.persistence.services.IStateServices;
import org.apache.uima.ducc.common.persistence.services.StateServices;
import org.apache.uima.ducc.common.persistence.services.StateServicesDirectory;
import org.apache.uima.ducc.common.persistence.services.StateServicesSet;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
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
import org.apache.uima.ducc.ws.DuccData;
import org.apache.uima.ducc.ws.DuccMachinesData;
import org.apache.uima.ducc.ws.JobInfo;
import org.apache.uima.ducc.ws.MachineInfo;
import org.apache.uima.ducc.ws.ReservationInfo;
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
	private final String jsonFormatServicesDefinitionsAaData	= duccContextJsonFormat+"-aaData-services-definitions";
	private final String jsonFormatServicesDeploymentsAaData	= duccContextJsonFormat+"-aaData-services-deployments";
	
	private final String jsonFormatMachines 		= duccContextJsonFormat+"-machines";
	private final String jsonFormatReservations 	= duccContextJsonFormat+"-reservations";
	
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
			sb.append("<span>");
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
			sb.append("</span>");
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
	
	private void handleServletJsonFormatServicesDefinitionsAaData(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleServletJsonFormatServicesDefinitionsAaData";
		duccLogger.trace(methodName, jobid, messages.fetch("enter"));
		
		JsonObject jsonResponse = new JsonObject();
		JsonArray data = new JsonArray();

		IStateServices iss = StateServices.getInstance();
		StateServicesDirectory ssd = iss.getStateServicesDirectory();
		if(ssd.getDescendingKeySet().size() > 0) {
			for(Integer key : ssd.getDescendingKeySet()) {
				StateServicesSet entry = ssd.get(key);
				Properties propertiesSvc = entry.get(IStateServices.svc);
				Properties propertiesMeta = entry.get(IStateServices.meta);
				JsonArray row = new JsonArray();
				// Id
				row.add(new JsonPrimitive(key));
				// Endpoint
				row.add(new JsonPrimitive(getValue(propertiesMeta,IStateServices.endpoint,"")));
				// Instances
				row.add(new JsonPrimitive(getValue(propertiesMeta,IStateServices.instances,"")));
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
	
	private JsonArray buildServiceRow(HttpServletRequest request, IDuccWorkJob service, DuccData duccData) {
		String type="Service";
		JsonArray row = new JsonArray();
		StringBuffer sb;
		DuccId duccId = service.getDuccId();
		// Terminate
		sb = new StringBuffer();
		String id = normalize(duccId);
		sb.append("<span class=\"ducc-col-terminate\">");
		if(terminateEnabled) {
			if(!service.isFinished()) {
				sb.append("<input type=\"button\" onclick=\"ducc_confirm_terminate_service("+id+")\" value=\"Terminate\" "+getDisabled(request,service)+"/>");
			}
		}
		sb.append("</span>");
		row.add(new JsonPrimitive(sb.toString()));
		// Id
		sb = new StringBuffer();
		sb.append("<a href=\"service.details.html?id="+duccId+"\">"+duccId+"</a>");
		row.add(new JsonPrimitive(sb.toString()));
		// Start
		sb = new StringBuffer();
		sb.append(getTimeStamp(request,duccId, service.getStandardInfo().getDateOfSubmission()));
		row.add(new JsonPrimitive(sb.toString()));
		// End
		sb = new StringBuffer();
		sb.append(getCompletionOrProjection(request,service));
		row.add(new JsonPrimitive(sb.toString()));
		// User
		sb = new StringBuffer();
		sb.append(service.getStandardInfo().getUser());
		row.add(new JsonPrimitive(sb.toString()));
		// Class
		sb = new StringBuffer();
		sb.append(stringNormalize(service.getSchedulingInfo().getSchedulingClass(),messages.fetch("default")));
		row.add(new JsonPrimitive(sb.toString()));
		/*
		sb.append("<td align=\"right\">");
		sb.append(stringNormalize(duccWorkJob.getSchedulingInfo().getSchedulingPriority(),messages.fetch("default")));
		sb.append("</td>");
		*/
		// State
		sb = new StringBuffer();
		if(duccData.isLive(duccId)) {
			if(service.isOperational()) {
				sb.append("<span class=\"active_state\">");
			}
			else {
				sb.append("<span class=\"completed_state\">");
			}
		}
		else {
			sb.append("<span class=\"historic_state\">");
		}
		sb.append(service.getStateObject().toString());
		if(duccData.isLive(duccId)) {
			sb.append("</span>");
		}
		row.add(new JsonPrimitive(sb.toString()));
		// Reason
		sb = new StringBuffer();
		if(service.isOperational()) {
			sb.append("<span>");
			ArrayList<String> swappingMachines = getSwappingMachines(service);
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
			sb.append("</span>");
		}
		else if(service.isCompleted()) {
			JobCompletionType jobCompletionType = service.getCompletionType();
			switch(jobCompletionType) {
			case EndOfJob:
			case Undefined:
				sb.append("<span>");
				break;
			default:
				IRationale rationale = service.getCompletionRationale();
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
		sb = new StringBuffer();;
		if(duccData.isLive(duccId)) {
			sb.append(service.getProcessMap().getAliveProcessCount());
		}
		else {
			sb.append("0");
		}
		row.add(new JsonPrimitive(sb.toString()));
		// Initialize Failures
		sb = new StringBuffer();;
		sb.append(buildInitializeFailuresLink(service));
		row.add(new JsonPrimitive(sb.toString()));
		// Runtime Failures
		sb = new StringBuffer();;
		sb.append(buildRuntimeFailuresLink(service));
		row.add(new JsonPrimitive(sb.toString()));
		// Size
		sb = new StringBuffer();;
		String size = service.getSchedulingInfo().getShareMemorySize();
		MemoryUnits units = service.getSchedulingInfo().getShareMemoryUnits();
		sb.append(getProcessMemorySize(duccId,type,size,units));
		row.add(new JsonPrimitive(sb.toString()));
		// Description
		sb = new StringBuffer();;
		sb.append(stringNormalize(service.getStandardInfo().getDescription(),messages.fetch("none")));
		row.add(new JsonPrimitive(sb.toString()));
		
		return row;
	}
	
	private void handleServletJsonFormatServicesDeploymentsAaData(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleServletJsonFormatServicesDeploymentsAaData";
		duccLogger.trace(methodName, jobid, messages.fetch("enter"));
		
		JsonObject jsonResponse = new JsonObject();
		JsonArray data = new JsonArray();

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
					JsonArray row = buildServiceRow(request, service, duccData);
					data.add(row);
				}
			}
		}
		else {
			JsonArray row = new JsonArray();
			// Terminate
			row.add(new JsonPrimitive("no services"));
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
	
	private void handleServletJsonFormatMachines(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
	throws IOException, ServletException
	{
		String methodName = "handleServletJsonFormatMachines";
		duccLogger.trace(methodName, jobid, messages.fetch("enter"));
		StringBuffer sb = new StringBuffer();
		
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
		else if(reqURI.startsWith(jsonFormatServicesDefinitionsAaData)) {
			handleServletJsonFormatServicesDefinitionsAaData(target, baseRequest, request, response);
		}
		else if(reqURI.startsWith(jsonFormatServicesDeploymentsAaData)) {
			handleServletJsonFormatServicesDeploymentsAaData(target, baseRequest, request, response);
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
			duccLogger.info(methodName, jobid, "", t.getMessage(), t);
			duccLogger.error(methodName, jobid, t);
		}
	}

}
