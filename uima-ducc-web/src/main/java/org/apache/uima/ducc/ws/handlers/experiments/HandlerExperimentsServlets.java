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
package org.apache.uima.ducc.ws.handlers.experiments;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.TreeMap;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.common.IDuccWork;
import org.apache.uima.ducc.transport.event.common.IDuccWorkJob;
import org.apache.uima.ducc.ws.DuccData;
import org.apache.uima.ducc.ws.authentication.DuccAsUser;
import org.apache.uima.ducc.ws.handlers.utilities.HandlersUtilities;
import org.apache.uima.ducc.ws.handlers.utilities.ResponseHelper;
import org.apache.uima.ducc.ws.log.WsLog;
import org.apache.uima.ducc.ws.server.DuccCookies;
import org.apache.uima.ducc.ws.server.DuccCookies.DateStyle;
import org.apache.uima.ducc.ws.server.DuccLocalConstants;
import org.apache.uima.ducc.ws.server.DuccWebServer;
import org.apache.uima.ducc.ws.server.DuccWebUtil;
import org.apache.uima.ducc.ws.utils.FormatHelper;
import org.apache.uima.ducc.ws.utils.FormatHelper.Precision;
import org.apache.uima.ducc.ws.utils.HandlersHelper;
import org.apache.uima.ducc.ws.xd.ExperimentsRegistryManager;
import org.apache.uima.ducc.ws.xd.IExperiment;
import org.apache.uima.ducc.ws.xd.Jed;
import org.apache.uima.ducc.ws.xd.Jed.Status;
import org.apache.uima.ducc.ws.xd.Task;
import org.eclipse.jetty.server.Request;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

public class HandlerExperimentsServlets extends HandlerExperimentsAbstract {
	
  //NOTE - this variable used to hold the class name before WsLog was simplified
  private static DuccLogger cName = DuccLogger.getLogger(HandlerExperimentsServlets.class);

	public final int defaultRecordsExperiments = 16;
	
	public final String duccContextExperimentCancelRequest =  DuccLocalConstants.duccContextExperimentCancelRequest;
	
	public final String duccContextExperiments =  DuccLocalConstants.duccContextExperiments;
	public final String duccContextExperimentDetails =  DuccLocalConstants.duccContextExperimentDetails;
	public final String duccContextExperimentDetailsDirectory =  DuccLocalConstants.duccContextExperimentDetailsDirectory;
	
	public final String duccContextJsonExperiments =  DuccLocalConstants.duccContextJsonExperiments;
	public final String duccContextJsonExperimentDetails =  DuccLocalConstants.duccContextJsonExperimentDetails;

	private static ExperimentsRegistryManager experimentsRegistryManager = ExperimentsRegistryManager.getInstance();
	
	private static DuccData duccData = DuccData.getInstance();
	
	protected boolean terminateEnabled = true;
	
	public HandlerExperimentsServlets(DuccWebServer duccWebServer) {
		super.init(duccWebServer);
	}
	
	private String getStartDate(HttpServletRequest request, IExperiment experiment) {
		String startDate ="";
		if(experiment.getStartDate() != null) {
			startDate = experiment.getStartDate();
			DateStyle dateStyle = DuccCookies.getDateStyle(request);
			startDate = HandlersUtilities.reFormatDate(dateStyle, startDate);
		}
		return startDate;
	}
	
	private long getRunTime(HttpServletRequest request, IExperiment experiment) {
		long runTime = 0;
		if(experiment.getTasks() != null) {
			ArrayList<Task> tasks = experiment.getTasks();
			for(Task task : tasks) {
				if(task.parentId == 0) {
					if(task.runTime > runTime) {
						runTime = task.runTime;
					}
				}
			}
		}
		return runTime;
	}
	
	private String getDuration(HttpServletRequest request, IExperiment experiment) {
		Status experimentStatus = experiment.getStatus();
		String durationUntitled = "";
		long runTime = getRunTime(request, experiment);
		if(runTime > 0) {
			durationUntitled = FormatHelper.duration(runTime,Precision.Whole);
		}
		else {
			switch(experimentStatus) {
			case Running:
				try {
					String startDate = "";
					if(experiment.getStartDate() != null) {
						startDate = experiment.getStartDate();
					}
					long millisStart = HandlersUtilities.getMillis(startDate);
					long millisEnd = System.currentTimeMillis();
					if(millisStart > 0) {
						runTime = millisEnd - millisStart;
						durationUntitled = FormatHelper.duration(runTime,Precision.Whole);
					}
				}
				catch(Exception e) {
					// no worries
				}
				break;
      default:
        break;
			}
		}
		String health = " class=\"health_black\" ";
		switch(experimentStatus) {
		case Running:
			health = " class=\"health_green\" ";
			break;
    default:
      break;
		}
		StringBuffer db = new StringBuffer();
		db.append("<span "+health+"title=\"Time (ddd:hh:mm:ss) elapsed for task, including all child tasks\">");
		db.append(durationUntitled);
		db.append("</span>");
		String duration = db.toString();
		return duration;
	}
	
	private String getUser(HttpServletRequest request, IExperiment experiment) {
		String user = "";
		if(experiment.getUser() != null) {
			user = experiment.getUser();
		}
		return user;
	}
	
	private String getTasks(HttpServletRequest request, IExperiment experiment) {
		String tasks = "0";
		if(experiment.getTasks() != null) {
			tasks = ""+experiment.getTasks().size();
		}
		return tasks;
	}
	
	private String getState(HttpServletRequest request, IExperiment experiment) {
		String health;
		Status experimentStatus = experiment.getStatus();
		switch (experimentStatus) {
		    case Failed:
		    case Canceled:
		    case Unknown:
		        health = "red";
		        break;
		    case Running:
		        health = "green";
		        break;
		    default:
		        health = "black";
		}
		String state = "<span class=\"health_" + health + "\">" +experimentStatus.name() + "</span>";
		return state;
	}
	
	private String getDirectoryLink(HttpServletRequest request, IExperiment experiment) {
		String directory = experiment.getDirectory();
		String id = experiment.getId();
		String href = "href=\"experiment.details.html?id="+id+"\"";
		String directoryLink = "<a"+" "+href+" "+">"+directory+"</a>";
		return directoryLink;
	}
	
	private boolean handleServletJsonExperiments(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
			throws Exception {
		String mName = "handleServletJsonExperiments";
		WsLog.enter(cName, mName);
		
		boolean handled = false;
		
		JsonObject jsonResponse = new JsonObject();
		JsonArray data = new JsonArray();

		TreeMap<IExperiment, String> map = experimentsRegistryManager.getMapByStatus();
		
		int maxRecords = HandlersUtilities.getExperimentsMax(request);
		ArrayList<String> users = HandlersUtilities.getExperimentsUsers(request);
		
		int counter = 0;
		
		for(Entry<IExperiment, String> entry: map.entrySet()) {
			
			IExperiment experiment = entry.getKey();

			if(HandlersUtilities.isListable(request, users, maxRecords, counter, experiment)) {
				counter++;
				JsonArray row = new JsonArray();
				// 00 Terminate
				String terminate = "";
				if(terminateEnabled) {
					Status experimentStatus = experiment.getStatus();
					switch(experimentStatus) {
					case Running:
						String id = experiment.getId();
						String directory = experiment.getDirectory();
						String disabled = "";
						String resourceOwnerUserid = experiment.getUser();
						if(!HandlersHelper.isUserAuthorized(request, resourceOwnerUserid)) {
							disabled = "disabled title=\"Hint: use Login to enable\"";
						}
						String p0 = "'"+id+"'";
						String p1 = "'"+directory+"'";
						terminate = "<input type=\"button\" onclick=\"ducc_confirm_terminate_experiment("+p0+","+p1+")\" value=\"Terminate\" "+disabled+"/>";
          default:
            break;
					}
				}
				row.add(new JsonPrimitive(terminate));
				// 01 Start
				String startDate = getStartDate(request, experiment);
				row.add(new JsonPrimitive(startDate));
				// 02 Duration
				String duration = getDuration(request, experiment);
				row.add(new JsonPrimitive(duration));
				// 03 User
				String user = getUser(request, experiment);
				row.add(new JsonPrimitive(user));
				// 04 Tasks
				String tasks = getTasks(request, experiment);
				row.add(new JsonPrimitive(tasks));
				// 05 State
				String state = getState(request, experiment);
				row.add(new JsonPrimitive(state));
				// 06 Directory (link)
				String directoryLink = getDirectoryLink(request, experiment);
				row.add(new JsonPrimitive(directoryLink));
				// Row
				data.add(row);
			}
		}
		
		if(counter == 0) {
			JsonArray row = new JsonArray();
			row.add(new JsonPrimitive("no data"));
			row.add(new JsonPrimitive(""));
			row.add(new JsonPrimitive(""));
			row.add(new JsonPrimitive(""));
			row.add(new JsonPrimitive(""));
			row.add(new JsonPrimitive(""));
			data.add(row);
		}
		
		jsonResponse.add("aaData", data);
		
		String json = jsonResponse.toString();
		WsLog.debug(cName, mName, json);
		response.getWriter().println(json);
		response.setContentType("application/json");
		
		handled = true;
		
		WsLog.exit(cName, mName);
		return handled;
	}
	
	private boolean handleServletExperiments(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
			throws Exception {
		String mName = "handleServletExperiments";
		WsLog.enter(cName, mName);
		
		boolean handled = false;
		
		StringBuffer sb = new StringBuffer();
		
		TreeMap<IExperiment, String> map = experimentsRegistryManager.getMapByStatus();
		
		int maxRecords = HandlersUtilities.getExperimentsMax(request);
		ArrayList<String> users = HandlersUtilities.getExperimentsUsers(request);
		
		int counter = -1;
		int nListed = 0;
		
		for(Entry<IExperiment, String> entry: map.entrySet()) {
			
			counter++;
			IExperiment experiment = entry.getKey();

			if(HandlersUtilities.isListable(request, users, maxRecords, counter, experiment)) {
				
			  ++nListed;
				int COLS = 7;
				StringBuffer[] cbList = new StringBuffer[COLS];
				for(int i=0; i < COLS; i++) {
					cbList[i] = new StringBuffer();
				}
				
				int index = -1;
				
				// 00 Terminate
				String terminate = "";
				index++;
				if(terminateEnabled) {
					Status experimentStatus = experiment.getStatus();
					switch(experimentStatus) {
					case Running:
						String id = experiment.getId();
						String directory = experiment.getDirectory();
						String disabled = "";
						String resourceOwnerUserid = experiment.getUser();
						if(!HandlersHelper.isUserAuthorized(request, resourceOwnerUserid)) {
							disabled = "disabled title=\"Hint: use Login to enable\"";
						}
						String p0 = "'"+id+"'";
						String p1 = "'"+directory+"'";
						terminate = "<input type=\"button\" onclick=\"ducc_confirm_terminate_experiment("+p0+","+p1+")\" value=\"Terminate\" "+disabled+"/>";
          default:
            break;
					}
				}
				cbList[index].append("<td valign=\"bottom\" align=\"right\">"); 
				cbList[index].append(terminate);
				cbList[index].append("</td>");
				
				// 01 Start
				String startDate = getStartDate(request, experiment);
				index++;
				cbList[index].append("<td valign=\"bottom\">");
				cbList[index].append(startDate);
				cbList[index].append("</td>");
				
				// 02 Duration
				String duration = getDuration(request, experiment);
				long runTime = getRunTime(request, experiment);
				index++;
				String sort = "sorttable_customkey=\""+runTime+"\"";
				cbList[index].append("<td valign=\"bottom\" "+sort+" align=\"right\">");
				cbList[index].append(duration);
				cbList[index].append("</td>");
				
				// 03 User
				String user = getUser(request, experiment);
				index++;
				cbList[index].append("<td valign=\"bottom\">"); 
				cbList[index].append(user);
				cbList[index].append("</td>");
				
				// 04 Tasks
				String tasks = getTasks(request, experiment);
				index++;
				cbList[index].append("<td valign=\"bottom\" align=\"right\">"); 
				cbList[index].append(tasks);
				cbList[index].append("</td>");
				
				// 05 State
				String state = getState(request, experiment);
				index++;
				cbList[index].append("<td valign=\"bottom\">"); 
				cbList[index].append(state);
				cbList[index].append("</td>");
				
				// 06 Directory (link)
				String directoryLink = getDirectoryLink(request, experiment);
				index++;
				cbList[index].append("<td valign=\"bottom\">");
				cbList[index].append(directoryLink);
				cbList[index].append("</td>");
				
				/////
				
				StringBuffer row = new StringBuffer();
				
				row.append(ResponseHelper.trStart(counter));
				for(int i=0; i < COLS; i++) {
					row.append(cbList[i]);
				}
				row.append(ResponseHelper.trEnd(counter));
				sb.append(row);
			}
		}
		WsLog.trace(cName, mName, "!! listing "+nListed+" of "+map.size()+" experiments");
		
		/////
		
		if(sb.length() == 0) {
			sb.append("<tr>");
			sb.append("<td>");
			sb.append("not found");
			sb.append("</td>");
			sb.append("</tr>");
		}
		
		response.getWriter().println(sb);
		
		handled = true;
		
		WsLog.exit(cName, mName);
		return handled;
	}
	
	private boolean handleServletJsonExperimentDetails(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
			throws Exception {
		String mName = "handleServletJsonExperimentDetails";
		WsLog.enter(cName, mName);
		
		boolean handled = false;
		
		WsLog.exit(cName, mName);
		return handled;
	}
	
	private enum IdType { Job, Reservation, None };
	
	private class IdBundle {
		
		private IdType idType = IdType.None;
		private int[] idList = new int[0];
		
		public IdBundle() {
		}
		
		public IdBundle(IdType type, int[] list) {
			idType = type;
			idList = list;
		}
		
		public IdType getType() {
			return idType;
		}
		
		public int[] getList() {
			return idList;
		}
	}
	
	private class IdListIterator {
		
		private ArrayList<Long> reservations = new ArrayList<Long>();
		private ArrayList<Long> jobs = new ArrayList<Long>();
		
		public IdListIterator(Task task) {
			if(task != null) {
				long[] duccIdList = task.duccId;
				for(long duccId : duccIdList) {
					if(duccId < 0) {
						Long r = new Long(duccId);
						reservations.add(r);
					}
					else {
						Long j = new Long(duccId);
						jobs.add(j);
					}
				}
			}
		}
		
		public IdBundle getNext() {
			IdBundle retVal = new IdBundle();
			if(reservations.size() > 0) {
				int[] list = new int[1];
				Long r = reservations.remove(0);
				list[0] = r.intValue();
				retVal = new IdBundle(IdType.Reservation, list);
			}
			else if(jobs.size() > 0) {
				int[] list = new int[1];
				Long j = jobs.remove(0);
				list[0] = j.intValue();
				retVal = new IdBundle(IdType.Job, list);
			}
			return retVal;
		}
		
		public boolean isEmpty() {
			boolean retVal = true;
			if(reservations.size() > 0) {
				retVal = false;
			}
			if(jobs.size() > 0) {
				retVal = false;
			}
			return retVal;
		}
	}
	
	public enum ExperimentDetailsColumns {
		Id(0), Name(1) , Parent(2), State(3), Type(4), StepStart(5), StepDuration(6), DuccId(7), DuccDuration(8), Total(9), Done(10), Error(11), Dispatch(12), Retry(13), Preempt(14);
		private int index;
		ExperimentDetailsColumns(int index){
			this.index = index;
		}
		public int getIndex(){
			return index;
		}
		public static int getCols() {
			return Preempt.getIndex()+1;
		}
	}

	private void addPlaceholder(StringBuffer[] cbList, int index) {
		cbList[index].append("<td>");
		cbList[index].append("</td>");
	}
	
	private void edId(StringBuffer[] cbList, Task task) {
		long taskId = task.taskId;
		int index = ExperimentDetailsColumns.Id.getIndex();
		cbList[index].append("<td align=\"right\">");
		cbList[index].append(""+taskId);
		cbList[index].append("</td>");
	}
	
	private void edName(StringBuffer[] cbList, Task task) {
		String name = "";
		if(task.name != null) {
			name = task.name;
		}
		int index = ExperimentDetailsColumns.Name.getIndex();
		cbList[index].append("<td>");
		cbList[index].append(name);
		cbList[index].append("</td>");
	}
	
	private void edParent(StringBuffer[] cbList, Task task) {
		long taskParentId = task.parentId;
		int index = ExperimentDetailsColumns.Parent.getIndex();
		cbList[index].append("<td align=\"right\">");
		cbList[index].append(""+taskParentId);
		cbList[index].append("</td>");
	}
	
	private void edState(StringBuffer[] cbList, IExperiment experiment, Task task) {
		String mName = "edState";
		String state = "";
		if(task.status != null) {
			state = task.status;
			String type = task.type;
			boolean leaf = Jed.Type.isLeaf(type);
			if(leaf) {
				Jed.Status status = Jed.Status.getEnum(state);
				switch(status) {
				case Running:
					if(experiment.isStale()) {
						state = "<span class=\"health_red\">Unknown";
						WsLog.info(cName, mName, experiment.getDirectory()+" "+"stale:"+task.taskId);
					}
					else {
						state = "<span class=\"health_green\">Running";
					}
					break;
				case Failed:
				case DependencyFailed:
				case Canceled:
					state = "<span class=\"health_red\""+">"+state+"</span>";
					break;
				default:
					break;
				}
			}
		}
		int index = ExperimentDetailsColumns.State.getIndex();
		cbList[index].append("<td>");
		cbList[index].append(state);
		cbList[index].append("</td>");
	}
	
	private void edType(StringBuffer[] cbList, Task task) {
		String type = "";
		if(task.type != null) {
			type = task.type;
		}
		int index = ExperimentDetailsColumns.Type.getIndex();
		cbList[index].append("<td>");
		cbList[index].append(type);
		cbList[index].append("</td>");
	}
	
	private void edStepStart(StringBuffer[] cbList, Task task, HttpServletRequest request) {
		String startTime = "";
		if(task.startTime != null) {
			startTime = task.startTime;
			//
			DateStyle dateStyle = DuccCookies.getDateStyle(request);
			startTime = HandlersUtilities.reFormatDate(dateStyle, startTime);
			//
		}
		int index = ExperimentDetailsColumns.StepStart.getIndex();
		cbList[index].append("<td>");
		cbList[index].append(startTime);
		cbList[index].append("</td>");
	}
	
	private void edStepDuration(StringBuffer[] cbList, Task task, HttpServletRequest request) {
		String displayRunTime = "";
		long runTime = task.runTime;
		if(runTime > 0) {
			displayRunTime = FormatHelper.duration(runTime,Precision.Whole);
		}
		String sort = "sorttable_customkey=\""+runTime+"\"";
		int index = ExperimentDetailsColumns.StepDuration.getIndex();
		cbList[index].append("<td "+sort+" align=\"right\">");
		cbList[index].append("<span title=\"Time (ddd:hh:mm:ss) elapsed for task, including all child tasks\">");
		cbList[index].append(displayRunTime);
		cbList[index].append("</span>");
		cbList[index].append("</td>");
	}
	
	private void edDuccId(StringBuffer[] cbList, Task task, int[] duccIdList) {
	  //WsLog.info(cName, "edDuccId", "!! #ids = "+duccIdList.length);
		StringBuffer db = new StringBuffer();
		String type = "";
		if(task.type != null) {
			type = task.type;
		}
		Jed.Type jedType = Jed.Type.getEnum(type);
		switch(jedType) {
		case DuccJob:
		case Java:
		case Trainer:
			for(int duccId : duccIdList) {
				if(duccId < 0) {
					String parm = ""+(0-duccId);
					String displayId = "r"+parm;
					String link = "<a href=\"reservation.details.html?id="+parm+"\">"+displayId+"</a>";
					db.append(link+" ");
				}
				else {
					String parm = ""+(0+duccId);
					String displayId =  "j"+parm;
					String link = "<a href=\"job.details.html?id="+parm+"\">"+displayId+"</a>";
					db.append(link+" ");
				}
			}
			break;
		case Other:
		default:
			break;
		}
		String duccLinks = db.toString().trim();
		int index = ExperimentDetailsColumns.DuccId.getIndex();
		cbList[index].append("<td>");
		cbList[index].append(duccLinks);
		cbList[index].append("</td>");
	}
	
	private void edDuccDuration(StringBuffer[] cbList, Task task, HttpServletRequest request, IDuccWorkJob job, long now) {
		int index = ExperimentDetailsColumns.DuccDuration.getIndex();
		cbList[index].append("<td valign=\"bottom\" align=\"right\">");
		try {
			if(job.isCompleted()) {
				String duration = getDuration(request,job);
				String decoratedDuration = decorateDuration(request,job, duration);
				cbList[index].append("<span>");
				cbList[index].append(decoratedDuration);
				cbList[index].append("</span>");
			}
			else {
				String duration = getDuration(request,job,now);
				String decoratedDuration = decorateDuration(request,job, duration);
				cbList[index].append("<span class=\"health_green\""+">");
				cbList[index].append(decoratedDuration);
				cbList[index].append("</span>");
				String projection = getProjection(request,job);
				cbList[index].append(projection);
			}
		}
		catch(Exception e) {
		}
		cbList[index].append("</td>");
	}
	
	private void edDuccDuration(StringBuffer[] cbList, Task task, HttpServletRequest request, IDuccWork dw, long now) {
		int index = ExperimentDetailsColumns.DuccDuration.getIndex();
		cbList[index].append("<td valign=\"bottom\" align=\"right\">");
		try {
			if(dw.isCompleted()) {
				String duration = getDuration(request,dw);
				cbList[index].append("<span>");
				cbList[index].append(duration);
				cbList[index].append("</span>");
			}
			else {
				String duration = getDuration(request,dw,now);
				cbList[index].append("<span class=\"health_green\""+">");
				cbList[index].append(duration);
				cbList[index].append("</span>");
			}
		}
		catch(Exception e) {
		}
		cbList[index].append("</td>");
	}
	private void edTotal(StringBuffer[] cbList, Task task, HttpServletRequest request, IDuccWorkJob job) {
		int index = ExperimentDetailsColumns.Total.getIndex();
		cbList[index].append("<td valign=\"bottom\" align=\"right\">");
		try {
			cbList[index].append(job.getSchedulingInfo().getWorkItemsTotal());
		}
		catch(Exception e) {
		}
		cbList[index].append("</td>");
	}
	
	private void edDone(StringBuffer[] cbList, Task task, HttpServletRequest request, IDuccWorkJob job) {
		int index = ExperimentDetailsColumns.Done.getIndex();
		cbList[index].append("<td valign=\"bottom\" align=\"right\">");
		try {
			cbList[index].append(job.getSchedulingInfo().getWorkItemsCompleted());
		}
		catch(Exception e) {
		}
		cbList[index].append("</td>");
	}
	
	private void edError(StringBuffer[] cbList, Task task, HttpServletRequest request, IDuccWorkJob job) {
		int index = ExperimentDetailsColumns.Error.getIndex();
		cbList[index].append("<td valign=\"bottom\" align=\"right\">");
		try {
			cbList[index].append(buildErrorLink(job));
		}
		catch(Exception e) {
		}
		cbList[index].append("</td>");
	}
	
	private void edDispatch(StringBuffer[] cbList, Task task, HttpServletRequest request, IDuccWorkJob job) {
		int index = ExperimentDetailsColumns.Dispatch.getIndex();
		cbList[index].append("<td valign=\"bottom\" align=\"right\">");
		try {
			if(duccData.isLive(job.getDuccId())) {
				int dispatch = 0;
				int unassigned = job.getSchedulingInfo().getCasQueuedMap().size();
				try {
					dispatch = Integer.parseInt(job.getSchedulingInfo().getWorkItemsDispatched())-unassigned;
				}
				catch(Exception e) {
				}
				cbList[index].append(dispatch);
			}
			else {
				cbList[index].append("0");
			}
		}
		catch(Exception e) {
		}
		cbList[index].append("</td>");
	}
	
	private void edRetry(StringBuffer[] cbList, Task task, HttpServletRequest request, IDuccWorkJob job) {
		int index = ExperimentDetailsColumns.Retry.getIndex();
		cbList[index].append("<td valign=\"bottom\" align=\"right\">");
		try {
			cbList[index].append(job.getSchedulingInfo().getWorkItemsRetry());
		}
		catch(Exception e) {
		}
		cbList[index].append("</td>");
	}
	
	private void edPreempt(StringBuffer[] cbList, Task task, HttpServletRequest request, IDuccWorkJob job) {
		int index = ExperimentDetailsColumns.Preempt.getIndex();
		cbList[index].append("<td valign=\"bottom\" align=\"right\">");
		try {
			cbList[index].append(job.getSchedulingInfo().getWorkItemsPreempt());
		}
		catch(Exception e) {
		}
		cbList[index].append("</td>");
	}
	
	private void edTaskNonDucc(StringBuffer[] cbList, IExperiment experiment, Task task, HttpServletRequest request) {
		edId(cbList, task);
		edName(cbList, task);
		edParent(cbList, task);
		edState(cbList, experiment, task);
		edType(cbList, task);
		edStepStart(cbList, task, request);
		edStepDuration(cbList, task, request);
		addPlaceholder(cbList, ExperimentDetailsColumns.DuccId.getIndex());
		addPlaceholder(cbList, ExperimentDetailsColumns.DuccDuration.getIndex());
		addPlaceholder(cbList, ExperimentDetailsColumns.Total.getIndex());
		addPlaceholder(cbList, ExperimentDetailsColumns.Done.getIndex());
		addPlaceholder(cbList, ExperimentDetailsColumns.Error.getIndex());
		addPlaceholder(cbList, ExperimentDetailsColumns.Dispatch.getIndex());
		addPlaceholder(cbList, ExperimentDetailsColumns.Retry.getIndex());
		addPlaceholder(cbList, ExperimentDetailsColumns.Preempt.getIndex());
	}
	
	private void edTaskDuccFirst(StringBuffer[] cbList, IExperiment experiment, Task task, HttpServletRequest request, IdBundle idBundle, long now) {
		int[] duccIdList = idBundle.getList();
		//WsLog.info(cName, "edTaskDuccFirst", "!! #ids = "+duccIdList.length);
		edId(cbList, task);
		edName(cbList, task);
		edParent(cbList, task);
		edState(cbList, experiment, task);
		edType(cbList, task);
		edStepStart(cbList, task, request);
		edStepDuration(cbList, task, request);
		edDuccId(cbList, task, duccIdList);
		switch(idBundle.getType()) {
		case Job:
			DuccId jDuccId = new DuccId(duccIdList[0]);
			IDuccWorkJob dwj = duccData.getJob(jDuccId);
			edDuccDuration(cbList, task, request, dwj, now);
			edTotal(cbList, task, request, dwj);
			edDone(cbList, task, request, dwj);
			edError(cbList, task, request, dwj);
			edDispatch(cbList, task, request, dwj);
			edRetry(cbList, task, request, dwj);
			edPreempt(cbList, task, request, dwj);
			break;
		case Reservation:
			int id = 0-duccIdList[0];
			DuccId rDuccId = new DuccId(id);
			IDuccWork dw = duccData.getReservation(rDuccId);
			edDuccDuration(cbList, task, request, dw, now);
			addPlaceholder(cbList, ExperimentDetailsColumns.Total.getIndex());
			addPlaceholder(cbList, ExperimentDetailsColumns.Done.getIndex());
			addPlaceholder(cbList, ExperimentDetailsColumns.Error.getIndex());
			addPlaceholder(cbList, ExperimentDetailsColumns.Dispatch.getIndex());
			addPlaceholder(cbList, ExperimentDetailsColumns.Retry.getIndex());
			addPlaceholder(cbList, ExperimentDetailsColumns.Preempt.getIndex());
			break;
		default:
			addPlaceholder(cbList, ExperimentDetailsColumns.DuccDuration.getIndex());
			addPlaceholder(cbList, ExperimentDetailsColumns.Total.getIndex());
			addPlaceholder(cbList, ExperimentDetailsColumns.Done.getIndex());
			addPlaceholder(cbList, ExperimentDetailsColumns.Error.getIndex());
			addPlaceholder(cbList, ExperimentDetailsColumns.Dispatch.getIndex());
			addPlaceholder(cbList, ExperimentDetailsColumns.Retry.getIndex());
			addPlaceholder(cbList, ExperimentDetailsColumns.Preempt.getIndex());
			break;
		}
	}
	
	private void edTaskDuccNext(StringBuffer[] cbList, Task task, HttpServletRequest request, IdBundle idBundle, long now) {
		int[] duccIdList = idBundle.getList();
		//WsLog.info(cName, "edTaskDuccNext", "!! #ids = "+duccIdList.length);
		addPlaceholder(cbList, ExperimentDetailsColumns.Id.getIndex());
		addPlaceholder(cbList, ExperimentDetailsColumns.Name.getIndex());
		addPlaceholder(cbList, ExperimentDetailsColumns.Parent.getIndex());
		addPlaceholder(cbList, ExperimentDetailsColumns.State.getIndex());
		addPlaceholder(cbList, ExperimentDetailsColumns.Type.getIndex());
		addPlaceholder(cbList, ExperimentDetailsColumns.StepStart.getIndex());
		addPlaceholder(cbList, ExperimentDetailsColumns.StepDuration.getIndex());
		edDuccId(cbList, task, duccIdList);
		switch(idBundle.getType()) {
		case Job:
			DuccId duccId = new DuccId(duccIdList[0]);
			IDuccWorkJob job = duccData.getJob(duccId);
			edDuccDuration(cbList, task, request, job, now);
			edTotal(cbList, task, request, job);
			edDone(cbList, task, request, job);
			edError(cbList, task, request, job);
			edDispatch(cbList, task, request, job);
			edRetry(cbList, task, request, job);
			edPreempt(cbList, task, request, job);
			break;
		case Reservation:
			int id = 0-duccIdList[0];
			DuccId rDuccId = new DuccId(id);
			IDuccWork dw = duccData.getReservation(rDuccId);
			edDuccDuration(cbList, task, request, dw, now);
			addPlaceholder(cbList, ExperimentDetailsColumns.Total.getIndex());
			addPlaceholder(cbList, ExperimentDetailsColumns.Done.getIndex());
			addPlaceholder(cbList, ExperimentDetailsColumns.Error.getIndex());
			addPlaceholder(cbList, ExperimentDetailsColumns.Dispatch.getIndex());
			addPlaceholder(cbList, ExperimentDetailsColumns.Retry.getIndex());
			addPlaceholder(cbList, ExperimentDetailsColumns.Preempt.getIndex());
			break;
		default:
			addPlaceholder(cbList, ExperimentDetailsColumns.DuccDuration.getIndex());
			addPlaceholder(cbList, ExperimentDetailsColumns.Total.getIndex());
			addPlaceholder(cbList, ExperimentDetailsColumns.Done.getIndex());
			addPlaceholder(cbList, ExperimentDetailsColumns.Error.getIndex());
			addPlaceholder(cbList, ExperimentDetailsColumns.Dispatch.getIndex());
			addPlaceholder(cbList, ExperimentDetailsColumns.Retry.getIndex());
			addPlaceholder(cbList, ExperimentDetailsColumns.Preempt.getIndex());
			break;
		}
	}
	
	private StringBuffer[] getCbList() {
		int COLS = ExperimentDetailsColumns.getCols();
		StringBuffer[] cbList = new StringBuffer[COLS];
		for(int i=0; i < COLS; i++) {
			cbList[i] = new StringBuffer();
		}
		return cbList;
	}
	
	private boolean handleServletExperimentDetails(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
			throws Exception {
		String mName = "handleServletExperimentDetails";
		WsLog.enter(cName, mName);
		
		boolean handled = false;
		
		StringBuffer sb = new StringBuffer();
		
		String id = request.getParameter("id");
		
		IExperiment experiment = experimentsRegistryManager.getById(id);
		
		long now = System.currentTimeMillis();
		
		if(experiment != null) {
			ArrayList<Task> tasks = experiment.getTasks();
			if(tasks != null) {
			  WsLog.trace(cName, mName, "!! "+id+": "+tasks.size()+" tasks");
				int counter = -1;
				StringBuffer[] cbList;
				StringBuffer row;
				for(Task task : tasks) {
					String type = "";
					if(task.type != null) {
						type = task.type;
					}
					Jed.Type jedType = Jed.Type.getEnum(type);
					switch(jedType) {
					case DuccJob:
					case Java:
					case Trainer:
						counter++;
						cbList = getCbList();
						IdListIterator idListIterator = new IdListIterator(task);
						edTaskDuccFirst(cbList, experiment, task, request, idListIterator.getNext(), now);
						row = new StringBuffer();
						row.append(ResponseHelper.trStart(counter));
						for(int i=0; i < cbList.length; i++) {
							row.append(cbList[i]);
						}
						row.append(ResponseHelper.trEnd(counter));
						sb.append(row);
						while(!idListIterator.isEmpty()) {
							counter++;
							cbList = getCbList();
							edTaskDuccNext(cbList, task, request, idListIterator.getNext(), now);
							row = new StringBuffer();
							row.append(ResponseHelper.trStart(counter));
							for(int i=0; i < cbList.length; i++) {
								row.append(cbList[i]);
							}
							row.append(ResponseHelper.trEnd(counter));
							sb.append(row);
						}
						break;
					default:
						counter++;
						cbList = getCbList();
						edTaskNonDucc(cbList, experiment, task, request);
						row = new StringBuffer();
						row.append(ResponseHelper.trStart(counter));
						for(int i=0; i < cbList.length; i++) {
							row.append(cbList[i]);
						}
						row.append(ResponseHelper.trEnd(counter));
						sb.append(row);
						break;
					}
				}
			}
		}
	
		/////
		
		if(sb.length() == 0) {
			sb.append("<tr>");
			sb.append("<td>");
			sb.append("not found");
			sb.append("</td>");
			sb.append("</tr>");
			WsLog.trace(cName, mName, "!! "+id+": not found");
		}
		
		response.getWriter().println(sb);
		
		handled = true;
		
		WsLog.exit(cName, mName);
		return handled;
	}
	
	private boolean handleServletExperimentDetailsDirectory(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
			throws Exception {
		String mName = "handleServletExperimentDetailsDirectory";
		WsLog.enter(cName, mName);
		
		boolean handled = false;
		
		StringBuffer sb = new StringBuffer();
		
		String id = request.getParameter("id");
		
		IExperiment experiment = experimentsRegistryManager.getById(id);
		
		if(experiment != null) {
			String directory = experiment.getDirectory();
			if(directory != null) {
				sb.append("<b>");
				sb.append("Directory:");
				sb.append(" ");
				sb.append(directory);
				sb.append("</b>");
			}
		}
		WsLog.debug(cName, mName, "!! "+id+": -> "+experiment==null?"?":experiment.getDirectory());
		response.getWriter().println(sb);
		
		handled = true;
		
		WsLog.exit(cName, mName);
		return handled;
	}
	
	private boolean handleServletExperimentCancelRequest(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
			throws Exception {
		String mName = "handleServletExperimentCancelRequest";
		WsLog.enter(cName, mName);
		
		boolean handled = false;
		
		StringBuffer sb = new StringBuffer();

		String id = request.getParameter("id");

		IExperiment experiment = experimentsRegistryManager.getById(id);
		
		String resourceOwnerUserId = experiment.getUser();
		
		String directory = experiment.getDirectory();
		String file = "DRIVER.running";
		String path = directory+File.separator+file;
		
		WsLog.info(cName, mName, path);
		
		String command = "/bin/rm";
		
		String result;
		
		if(HandlersHelper.isUserAuthorized(request,resourceOwnerUserId)) {
			String userId = resourceOwnerUserId;
			String[] arglist = { "-u", userId, "--", command, path };
			result = DuccAsUser.duckling(userId, arglist);
			response.getWriter().println(result);
		}
		else {
			result = "user not authorized";
		}
		
		sb.append(result);
		
		response.getWriter().println(sb);
		
		handled = true;
		
		WsLog.exit(cName, mName);
		return handled;
	}
	
	private boolean handleDuccRequest(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
			throws Exception {
		String mName = "handleDuccRequest";
		WsLog.enter(cName, mName);
		String reqURI = request.getRequestURI()+"";
		boolean handled = false;
		if(handled) {
		}
		else if(reqURI.startsWith(duccContextExperimentDetailsDirectory)) {
			handled = handleServletExperimentDetailsDirectory(target, baseRequest, request, response);
		}
		else if(reqURI.startsWith(duccContextExperimentDetails)) {
			handled = handleServletExperimentDetails(target, baseRequest, request, response);
		}
		else if(reqURI.startsWith(duccContextExperiments)) {
			handled = handleServletExperiments(target, baseRequest, request, response);
		}
		else if(reqURI.startsWith(duccContextJsonExperimentDetails)) {
			handled = handleServletJsonExperimentDetails(target, baseRequest, request, response);
		}
		else if(reqURI.startsWith(duccContextJsonExperiments)) {
			handled = handleServletJsonExperiments(target, baseRequest, request, response);
		}
		else if(reqURI.startsWith(duccContextExperimentCancelRequest)) {
			handled = handleServletExperimentCancelRequest(target, baseRequest, request, response);
		}
		
		WsLog.exit(cName, mName);
		return handled;
	}
	
	public void handle(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) 
			throws IOException, ServletException {
		String mName = "handle";
		WsLog.enter(cName, mName);
		try { 
			WsLog.debug(cName, mName, request.toString());
			//WsLog.debug(cName, mName, "getRequestURI():"+request.getRequestURI());
			boolean handled = handleDuccRequest(target, baseRequest, request, response);
			if(handled) {
				response.setContentType("text/html;charset=utf-8");
				response.setStatus(HttpServletResponse.SC_OK);
				baseRequest.setHandled(true);
				DuccWebUtil.noCache(response);
			}
		}
		catch(Throwable t) {
			if(isIgnorable(t)) {
				WsLog.trace(cName, mName, t);
			}
			else {
				WsLog.info(cName, mName, t.getMessage());
				WsLog.error(cName, mName, t);
			}
		}
		WsLog.exit(cName, mName);
	}

}
