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
import java.util.Arrays;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.uima.ducc.common.head.DuccHead;
import org.apache.uima.ducc.common.head.IDuccHead;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.common.IDuccWork;
import org.apache.uima.ducc.transport.event.common.IDuccWorkJob;
import org.apache.uima.ducc.ws.DuccData;
import org.apache.uima.ducc.ws.authentication.DuccAsUser;
import org.apache.uima.ducc.ws.handlers.utilities.HandlersUtilities;
import org.apache.uima.ducc.ws.log.WsLog;
import org.apache.uima.ducc.ws.server.DuccCookies;
import org.apache.uima.ducc.ws.server.DuccCookies.DateStyle;
import org.apache.uima.ducc.ws.server.DuccLocalConstants;
import org.apache.uima.ducc.ws.server.DuccWebServer;
import org.apache.uima.ducc.ws.server.DuccWebUtil;
import org.apache.uima.ducc.ws.utils.FormatHelper;
import org.apache.uima.ducc.ws.utils.FormatHelper.Precision;
import org.apache.uima.ducc.ws.utils.FormatServlet;
import org.apache.uima.ducc.ws.utils.FormatServletClassic;
import org.apache.uima.ducc.ws.utils.FormatServletScroll;
import org.apache.uima.ducc.ws.utils.HandlersHelper;
import org.apache.uima.ducc.ws.utils.HandlersHelper.AuthorizationStatus;
import org.apache.uima.ducc.ws.xd.Experiment;
import org.apache.uima.ducc.ws.xd.ExperimentsRegistryManager;
import org.apache.uima.ducc.ws.xd.ExperimentsRegistryUtilities;
import org.apache.uima.ducc.ws.xd.Jed;
import org.apache.uima.ducc.ws.xd.Jed.Status;
import org.apache.uima.ducc.ws.xd.Task;
import org.eclipse.jetty.server.Request;

public class HandlerExperimentsServlets extends HandlerExperimentsAbstract {

  // NOTE - this variable used to hold the class name before WsLog was simplified
  private static DuccLogger cName = DuccLogger.getLogger(HandlerExperimentsServlets.class);
  
  private static IDuccHead dh = DuccHead.getInstance();

  public final int defaultRecordsExperiments = 16;

  public final String duccContextExperimentCancelRequest = DuccLocalConstants.duccContextExperimentCancelRequest;

  public final String duccContextExperiments = DuccLocalConstants.duccContextExperiments;

  public final String duccContextExperimentDetails = DuccLocalConstants.duccContextExperimentDetails;

  public final String duccContextExperimentDetailsDirectory = DuccLocalConstants.duccContextExperimentDetailsDirectory;

  public final String duccContextJsonExperiments = DuccLocalConstants.duccContextJsonExperiments;

  public final String duccContextJsonExperimentDetails = DuccLocalConstants.duccContextJsonExperimentDetails;

  private static ExperimentsRegistryManager experimentsRegistryManager = ExperimentsRegistryManager
          .getInstance();

  public HandlerExperimentsServlets(DuccWebServer duccWebServer) {
    super.init(duccWebServer);
  }

  private String getStartDate(HttpServletRequest request, Experiment experiment) {
    String startDate = "";
    if (experiment.getStartDate() != null) {
      startDate = experiment.getStartDate();
      DateStyle dateStyle = DuccCookies.getDateStyle(request);
      startDate = HandlersUtilities.reFormatDate(dateStyle, startDate);
    }
    return startDate;
  }

  private long getRunTime(HttpServletRequest request, Experiment experiment) {
    long runTime = 0;
    ArrayList<Task> tasks = experiment.getTasks();
    for (Task task : tasks) {
      if (task.parentId == 0) {
        if (task.runTime > runTime) {
          runTime = task.runTime;
        }
      }
    }
    
    // If not yet set get the elapsed time since it started
    if (runTime == 0) {
      String startDate = experiment.getStartDate();
      if (startDate != null) {
        long millisStart = HandlersUtilities.getMillis(startDate);
        long millisEnd = System.currentTimeMillis();
        runTime = millisEnd - millisStart;
      }
    }
    return runTime;
  }

  private String fmtDuration(Experiment experiment, long runTime) {
    StringBuffer db = new StringBuffer();
    Status experimentStatus = experiment.getStatus();
    String health = experimentStatus==Status.Running ? "class=\"health_green\"" : "class=\"health_black\"";
    db.append("<span " + health
            + "title=\"Time (ddd:hh:mm:ss) elapsed for task, including all child tasks and restarts\">");
    String duration = FormatHelper.duration(runTime, Precision.Whole);
    db.append(duration);
    db.append("</span>");
    return db.toString();
  }

  private String getUser(HttpServletRequest request, Experiment experiment) {
    String user = "";
    if (experiment.getUser() != null) {
      user = experiment.getUser();
    }
    return user;
  }

  private String getState(HttpServletRequest request, Experiment experiment) {
    String health;
    Status experimentStatus = experiment.getStatus();
    switch (experimentStatus) {
      case Failed:
      case Canceled:
      case Unknown:
        health = "red";
        break;
      case Running:
      case Restarting:
        health = "green";
        break;
      default:
        health = "black";
    }
    String state = "<span class=\"health_" + health + "\">" + experimentStatus.name() + "</span>";
    return state;
  }

  private String getDirectoryLink(HttpServletRequest request, Experiment experiment) {
    String directory = experiment.getDirectory();
    String href = "href='experiment.details.html?dir=" + directory + "'";
    String directoryLink = "<a" + " " + href + " " + ">" + directory + "</a>";
    return directoryLink;
  }

  private boolean handleServletExperiments(String tableStyle, Request baseRequest,
          HttpServletRequest request, HttpServletResponse response) throws Exception {
    String mName = "handleServletExperiments";
    WsLog.enter(cName, mName);

    boolean handled = false;

    FormatServlet fmt = tableStyle.equals("scroll") ? new FormatServletScroll() : new FormatServletClassic();
    
    // Display an empty page if a backup
    if (dh.is_ducc_head_backup()) {
      fmt.startRow();
      fmt.addElemL("");
      fmt.addElemL("no data - not master");
      fmt.pad(5); // DataTables needs all 7 elements for column alignment
      fmt.endRow();

    } else { // Master

      int maxRecords = HandlersUtilities.getExperimentsMax(request);
      ArrayList<String> users = HandlersUtilities.getExperimentsUsers(request);

      // List experiments in "experiment" order: active, newest start-date, directory
      for (Experiment experiment : experimentsRegistryManager.getMapByStatus().keySet()) {

        boolean fullTable = fmt.numRows() >= maxRecords;

        if (!HandlersUtilities.isListable(request, users, fullTable, experiment)) {
          continue;
        }

        // Format each row with:  Terminate-Button Start Duration User Tasks State Directory
        // Display Terminate button if experiment is Running - activated only if owned by the logged-in user.
        // (Column headings defined in expeiments.jsp)
        fmt.startRow();

        String terminate = "";
        Status experimentStatus = experiment.getStatus();
        switch (experimentStatus) {
          case Running:
            String directory = experiment.getDirectory();
            String disabled = "";
            String resourceOwnerUserid = experiment.getUser();
            if (!HandlersHelper.isUserAuthorized(request, resourceOwnerUserid)) {
              // Set the disabled attribute and a hover for the greyed-out button
              disabled = "disabled title=\"Hint: use Login to enable\"";
            }
            terminate = "<input type=\"button\"" + 
                        " onclick=\"ducc_confirm_terminate_experiment('" + directory + "')\"" +
                        " value=\"Terminate\" " + disabled + "/>";
          default:
            break;
        }
        fmt.addElemR(terminate);
        fmt.addElemL(getStartDate(request, experiment));

        // Get experiment runtime in msecs from JED to use as the numeric sort key
        long runTime = getRunTime(request, experiment);
        String duration = fmtDuration(experiment, runTime);
        fmt.addElemR(duration, runTime);

        fmt.addElemL(getUser(request, experiment));
        fmt.addElemR(experiment.getTasks().size());
        fmt.addElemL(getState(request, experiment));
        fmt.addElemL(getDirectoryLink(request, experiment));

        fmt.endRow();
      }
    }

    /////

    if (fmt.numRows() == 0) {
      fmt.startRow();
      fmt.addElemL("");
      fmt.addElemL("not found");
      fmt.pad(5);   // DataTables needs all 7 elements for column alignment
      fmt.endRow();
    }

    fmt.send(response);;

    handled = true;

    WsLog.exit(cName, mName);
    return handled;
  }

  // TODO - Might be nice to mark the top-level tasks as "Restarting"
  private String decorateState(Experiment experiment, Task task, boolean isRestartable, boolean isCanceled) {
    String mName = "decorateState";
    String state = "";
    if (task.status == null) {
      if (task.rerun) {  // If status has been cleared the task may be about to be rerun
        state = "<span class=\"health_green\">Rerun";
      }
    } else {
      state = task.status;
      // If experiment has been canceled change any "Running" tasks to "Canceled"
      Jed.Status status = Jed.Status.getEnum(state);
      if (isCanceled && (status == Jed.Status.Running)) {
        status = Jed.Status.Canceled;
        state = "Canceled";
      }
      String color = "";   // Default button coloring
      switch (status) {
        case Running:
          if (experiment.isStale()) {
            state = "<span class=\"health_red\">Unknown";
            WsLog.info(cName, mName, experiment.getDirectory() + " " + "stale:" + task.taskId);
          } else {
            state = "<span class=\"health_green\">Running";
          }
          break;
        case Canceled:
          state = "Canceled";  // TODO - JED spellings should be cleaned up
        case Failed:
        case DependencyFailed:
          // Use red text for error states
          if (isRestartable) {
            color = "color:red";
          } else {
            state = "<span class=\"health_red\"" + ">" + state + "</span>";
          }
        case Completed:
        case Done:
          // If experiment can be restarted display a button that can toggle to green "Rerun"
          if (isRestartable) {
            if (task.rerun) {
              state = "Rerun";
              color = "background-color:PaleGreen;";
            }
            state = "<input type='button' style='" + color + "'"
                    + " onclick=\"ducc_toggle_task_state('" + task.taskId + "')\""
                    + " title=\"Click to toggle state\"" 
                    + " value=\"" + state + "\" />";
          }
          break;
        default:
          break;
      }
    }
    return state;
  }

  private String decorateStepStart(Task task, HttpServletRequest request) {
    String startTime = "";
    if (task.startTime != null) {
      startTime = task.startTime;
      DateStyle dateStyle = DuccCookies.getDateStyle(request);
      startTime = HandlersUtilities.reFormatDate(dateStyle, startTime);
    }
    return startTime;
  }
  
  private String decorateStepDuration(Task task) {
    String displayRunTime = "";
    long runTime = task.runTime;
    if (runTime > 0) {
      displayRunTime = FormatHelper.duration(runTime, Precision.Whole);
    }
    return "<span title=\"Time (ddd:hh:mm:ss) elapsed for task, including all child tasks\">" + displayRunTime + "</span>";
  }
  
  private String decorateDuccId(Task task, long duccId) {
    String link;
    if (duccId < 0) {
      String parm = Long.toString(0 - duccId);
      link = "<a href=\"reservation.details.html?id=" + parm + "\">" + parm + "</a> ";
    } else {
      String parm = Long.toString(duccId);
      link = "<a href=\"job.details.html?id=" + parm + "\">" + parm + "</a> ";
    }
    return link;
  }
  
  private String decorateDuccDuration(Task task, HttpServletRequest request,
          IDuccWorkJob job, long now) {
    StringBuffer sbuff = new StringBuffer();
    try {
      if (job.isCompleted()) {
        String duration = getDuration(request, job);
        String decoratedDuration = decorateDuration(request, job, duration);
        sbuff.append("<span>");
        sbuff.append(decoratedDuration);
        sbuff.append("</span>");
      } else {
        String duration = getDuration(request, job, now);
        String decoratedDuration = decorateDuration(request, job, duration);
        sbuff.append("<span class=\"health_green\"" + ">");
        sbuff.append(decoratedDuration);
        sbuff.append("</span>");
        String projection = getProjection(request, job);
        sbuff.append(projection);
      }
    } catch (Exception e) {
      WsLog.error(cName, "decorateDuccDuration", e);
    }
    return sbuff.toString();
  }

  private String decorateDuccDuration(Task task, HttpServletRequest request, IDuccWork dw, long now) {
    StringBuffer sbuff = new StringBuffer();
    try {
      if (dw.isCompleted()) {
        String duration = getDuration(request, dw);
        sbuff.append("<span>");
        sbuff.append(duration);
        sbuff.append("</span>");
      } else {
        String duration = getDuration(request, dw, now);
        sbuff.append("<span class=\"health_green\"" + ">");
        sbuff.append(duration);
        sbuff.append("</span>");
      }
    } catch (Exception e) {
    }
    return sbuff.toString();
  }

  private void edTaskDucc(FormatServlet fmt, Experiment experiment, Task task,
          HttpServletRequest request, long duccId, long now, boolean isRestartable, boolean isCanceled) {
    DuccData duccData = DuccData.getInstance();
    
    // Format first 8 columns: 
    //   Path-ID Id Parent Name State Type Step-Start Step-Duration
    fmt.addElemL(task.pathId);
    fmt.addElemR(task.taskId);
    fmt.addElemR(task.parentId);
    fmt.addElemL(task.name);
    fmt.addElemL(decorateState(experiment, task, isRestartable, isCanceled));
    fmt.addElemL(task.type);
    fmt.addElemL(decorateStepStart(task, request));
    fmt.addElemR(decorateStepDuration(task), task.runTime);
  
    // Format next 8 columns for DUCC jobs or next 2 for DUCC managed reservations (with padding)
    //    DuccId Ducc-Duration Total Done Error Dispatch Retry Preempt
    // (Column headings defined in experiment.details.jsp)
    if (duccId > 0) {
      DuccId jDuccId = new DuccId(duccId);
      IDuccWorkJob job = duccData.getJob(jDuccId);
      
      fmt.addElemL(decorateDuccId(task, duccId));                     // DUCC ID
      fmt.addElemR(decorateDuccDuration(task, request, job, now));    // DUCC duration
      fmt.addElemR(job.getSchedulingInfo().getWorkItemsTotal());      // Total
      fmt.addElemR(job.getSchedulingInfo().getWorkItemsCompleted());  // Done
      fmt.addElemR(buildErrorLink(job));                              // Error
      fmt.addElemR(getDispatch(job));                                 // Dispatch
      fmt.addElemR(job.getSchedulingInfo().getWorkItemsRetry());      // Retry
      fmt.addElemR(job.getSchedulingInfo().getWorkItemsPreempt());    // Preempt
    } else {
      if (duccId < 0) {
        DuccId rDuccId = new DuccId(-duccId);
        IDuccWork dw = duccData.getReservation(rDuccId);
        
        fmt.addElemL(decorateDuccId(task, duccId));
        fmt.addElemR(decorateDuccDuration(task, request, dw, now));
      } else {
        fmt.pad(2);  // duccId == 0 => pad last 8
      }
      fmt.pad(6);   // Finish with 6 place-holders
    }
  }

/*  private String reverse(long[] ids, int ix) {
    if (ix == 0) {
      return "";
    }
    StringBuffer sb = new StringBuffer();
    while (--ix >= 0) {
      sb.append(',').append(Long.toString(ids[ix]));
    }
    return sb.substring(1);  // Skip the first , 
  }*/
  
  private boolean handleServletExperimentDetails(String tableStyle, Request baseRequest,
          HttpServletRequest request, HttpServletResponse response) throws Exception {
    String mName = "handleServletExperimentDetails";
    WsLog.enter(cName, mName);

    boolean handled = false;
    boolean restart = false;
    long now = System.currentTimeMillis();
    FormatServlet fmt = tableStyle.equals("scroll") ? new FormatServletScroll() : new FormatServletClassic();

    String dir = request.getParameter("dir");
    Experiment experiment = experimentsRegistryManager.getExperiment(dir);
    
    // May show nothing if experiment is missing or this is a backup node
    // Unlikely to be called if a backup as the Experiments page should have been empty
    String nodataMsg = "not found";
    if (dh.is_ducc_head_backup()) {
      experiment = null;
      nodataMsg = "no data - not master";
    }
    
    if (experiment != null) {
      // If restart requested update the tasks to be rerun & set restarting flag
      restart = request.getParameter("restart") != null;
      if (restart) {
        experiment.updateStatus();
      }
      // Check if the experiment can be restarted, i.e.
      // launched by DUCC as a JED AP, stopped, and owned by the logged-in user
      boolean isRestartable = experiment.getJedId() > 0  
              && !experiment.isActive()
              && HandlersHelper.getAuthorizationStatus(request, experiment.getUser()) == AuthorizationStatus.LoggedInOwner;
      
      boolean isCanceled = experiment.getStatus() == Jed.Status.Canceled;
      
      //if (cName.isTrace()) WsLog.trace(cName, mName, experiment.getDirectory() + " AP: "+experiment.getJedId()
      //        + " canrestart: "+isRestartable + " active: "+experiment.isActive() + " canceled: "+isCanceled
      //        + " loggedin: "+HandlersHelper.getAuthorizationStatus(request, experiment.getUser()));

      ArrayList<Task> tasks = experiment.getTasks();
      if (tasks != null) {
        // Check if given a task whose state is to be toggled between Completed & Rerun
        String toggleTask = request.getParameter("taskid");
        if (toggleTask != null) {
          int toggleId = Integer.parseInt(toggleTask);
          Task task = findTask(tasks, toggleId);
          task.rerun = !task.rerun;
          markSubtasks(tasks, toggleId, task.rerun);
        }
        // Find the latest duccId to display for a task ... omit if not started or is being rerun
        // i.e. display it if has been run and is not marked to be rerun or is marked but not yet restarted
        for (Task task : tasks) {
          long latestDuccId = 0;
          // Omit if: task.status==null || (task.rerun && !isRestartable)
          if (task.status != null && (!task.rerun || isRestartable)) {
            Jed.Type jedType = Jed.Type.getEnum(task.type);
            if (jedType == Jed.Type.Ducc_Job || jedType == Jed.Type.Java) {
              long[] duccIds = task.duccId;
              if (duccIds != null) {
                int nIds = duccIds.length;
                latestDuccId = nIds == 0 ? 0 : duccIds[--nIds];
                // String otherIds = reverse(duccIds, nIds);
              }
            }
          }
          fmt.startRow();
          edTaskDucc(fmt, experiment, task, request, latestDuccId, now, isRestartable, isCanceled);
          fmt.endRow();
        }
      }
    }

    if (fmt.numRows() == 0) {
      fmt.startRow();
      fmt.addElemL(nodataMsg);
      fmt.pad(15);   // DataTables needs all 16 elements for column alignment
      fmt.endRow();
    }

    fmt.send(response);
    
    // Restart the experiment AFTER the page has been generated
    // Don't trigger on the restart state of the experiment as do't want to launch on every page refresh
    // If restart fails clear the restarting state
    if (restart) {
      boolean ok = ExperimentsRegistryUtilities.launchJed(experiment);
      if (!ok) {
        WsLog.warn(cName, mName, "Failed to relaunch JED - reset the is-rerunning state");
        experiment.setRestart(false);
      }
    }
    
    handled = true;

    WsLog.exit(cName, mName);
    return handled;
  }

  private Task findTask(ArrayList<Task> tasks, int taskId) {
    for (Task task : tasks) {
      if (task.taskId == taskId) {
        return task;
      }
    }
    return null;   // Will cause an NPE ??
  }
  
  // Mark each child with the same rerun state as the parent.
  private void markSubtasks(ArrayList<Task> tasks, int parentId, boolean rerun) {
    for (Task task : tasks) {
      if (task.parentId == parentId) {
        task.rerun = rerun;
        markSubtasks(tasks, task.taskId, rerun);
      }
    }
  }
  
  private boolean handleServletExperimentDetailsDirectory(String target, Request baseRequest,
          HttpServletRequest request, HttpServletResponse response) throws Exception {
    String mName = "handleServletExperimentDetailsDirectory";
    WsLog.enter(cName, mName);

    StringBuffer sb = new StringBuffer();

    String directory = request.getParameter("dir");

    boolean restart = false;

    Experiment experiment = experimentsRegistryManager.getExperiment(directory);

    if (experiment != null && !dh.is_ducc_head_backup()) {
      // Display Terminate/Restart button if DUCC-launched && the owner logged in
      String button = null;
      //if (cName.isTrace()) WsLog.trace(cName, mName, experiment.getDirectory() + " AP: " + experiment.getJedId() + 
      //                                 " loggedin: " + HandlersHelper.getAuthorizationStatus(request, experiment.getUser()));
      if (experiment.getJedId() > 0 &&
        HandlersHelper.getAuthorizationStatus(request, experiment.getUser()) == AuthorizationStatus.LoggedInOwner) {
        
        restart = request.getParameter("restart") != null;
        if (restart) {           // Mark as "restarting" ... so getStatus will reflect that
          experiment.setRestart(true);
        }
        Status status = experiment.getStatus();
        // TODO - If still restarting should check if the restartJedId AP is actually running
        // If not the launch must have failed for some reason, and we should reset the restart status.
        // If the restart worked JED should have updated the state file and the Experiment object replaced.
        if (status == Jed.Status.Restarting) {
          button = "<button style='background-color:Beige;font-size:16px' "
                  + "disabled"
                  + " title='experiment is restarting'>Restarting...</button>";
        } else if (status == Jed.Status.Running) {
          button = "<button style='background-color:Khaki;font-size:16px' "
                  + "onclick=\"ducc_confirm_terminate_experiment('" + directory + "')\""
                  + " title='click to terminate experiment'>TERMINATE</button>";
        } else {
          button = "<button style='background-color:PaleGreen;font-size:16px' "
                  + "onclick=\"ducc_restart_experiment()\""
                  + " title='click to restart experiment'>RESTART</button>";
        }
      }

      sb.append("<b>");
      sb.append("Directory:");
      sb.append(" ");
      sb.append(directory);
      if (button != null) {
        sb.append("&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;");
        sb.append(button);
        sb.append("&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;");
        long jedId = experiment.getJedId();
        sb.append(" <a href='reservation.details.html?id=" + jedId + "' title='click to view the JED Managed Reservation'>" + jedId + "</a>");
      }
      sb.append("</b>");
    }
    response.getWriter().println(sb);
    
    WsLog.exit(cName, mName);
    return true;
  }

  private boolean handleServletExperimentCancelRequest(String target, Request baseRequest,
          HttpServletRequest request, HttpServletResponse response) throws Exception {
    String mName = "handleServletExperimentCancelRequest";
    WsLog.enter(cName, mName);

    boolean handled = false;

    StringBuffer sb = new StringBuffer();

    String directory = request.getParameter("dir");
    Experiment experiment = experimentsRegistryManager.getExperiment(directory);

    String resourceOwnerUserId = experiment.getUser();

    String file = "DRIVER.running";
    String path = new File(directory, file).getAbsolutePath();

    WsLog.info(cName, mName, path);

    String command = "/bin/rm";

    String result;

    if (HandlersHelper.isUserAuthorized(request, resourceOwnerUserId)) {
      String userId = resourceOwnerUserId;
      String[] arglist = { command, path };
      WsLog.info(cName, mName, "cmd: " + Arrays.toString(arglist));
      result = DuccAsUser.execute(userId, null, arglist);
      response.getWriter().println(result);
    } else {
      result = "user not authorized";
    }

    sb.append(result);

    response.getWriter().println(sb);

    handled = true;

    WsLog.exit(cName, mName);
    return handled;
  }

  private boolean handleDuccRequest(String target, Request baseRequest, HttpServletRequest request,
          HttpServletResponse response) throws Exception {
    //String mName = "handleDuccRequest";
    //WsLog.enter(cName, mName);
    String reqURI = request.getRequestURI();
    boolean handled = false;
    //if (reqURI.contains("experiment")) WsLog.info(cName, mName, "!! start "+reqURI);
    if (handled) {
    } else if (reqURI.startsWith(duccContextExperimentDetailsDirectory)) {
      handled = handleServletExperimentDetailsDirectory(target, baseRequest, request, response);
      
    } else if (reqURI.startsWith(duccContextExperiments)) {
      handled = handleServletExperiments("classic", baseRequest, request, response);
      
    } else if (reqURI.startsWith(duccContextJsonExperiments)) {
      handled = handleServletExperiments("scroll", baseRequest, request, response);
      
    } else if (reqURI.startsWith(duccContextExperimentDetails)) {
      handled = handleServletExperimentDetails("classic", baseRequest, request, response);
      
    } else if (reqURI.startsWith(duccContextJsonExperimentDetails)) {
      handled = handleServletExperimentDetails("scroll", baseRequest, request, response);

    } else if (reqURI.startsWith(duccContextExperimentCancelRequest)) {
      handled = handleServletExperimentCancelRequest(target, baseRequest, request, response);
    }
    //if (reqURI.contains("experiment")) WsLog.info(cName, mName, "!! end   "+reqURI);
    
    //WsLog.exit(cName, mName);
    return handled;
  }

  public void handle(String target, Request baseRequest, HttpServletRequest request,
          HttpServletResponse response) throws IOException, ServletException {
    String mName = "handle";
    //WsLog.enter(cName, mName);
    try {
      boolean handled = handleDuccRequest(target, baseRequest, request, response);
      if (handled) {
        response.setContentType("text/html;charset=utf-8");
        response.setStatus(HttpServletResponse.SC_OK);
        baseRequest.setHandled(true);
        DuccWebUtil.noCache(response);
        WsLog.debug(cName, mName, "handled "+request.toString());
      }
    } catch (Throwable t) {
      if (isIgnorable(t)) {
        WsLog.trace(cName, mName, t);
      } else {
        WsLog.info(cName, mName, t.getMessage());
        WsLog.error(cName, mName, t);
      }
    }
    //WsLog.exit(cName, mName);
  }

}
