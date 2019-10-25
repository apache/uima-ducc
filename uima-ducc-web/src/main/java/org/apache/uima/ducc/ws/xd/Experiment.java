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
package org.apache.uima.ducc.ws.xd;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.common.IDuccWork;
import org.apache.uima.ducc.ws.authentication.DuccAsUser;
import org.apache.uima.ducc.ws.log.WsLog;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class Experiment implements IExperiment {
  
  private static DuccLogger logger = DuccLogger.getLogger(Experiment.class);

  private String user = null;

  private String directory = null;

  private ArrayList<Task> tasks = null;

  private long fileDate = 0;

  private DuccId jedDuccId;

  
  public Experiment(String user, String directory, long fileDate, ArrayList<Task> tasks, IDuccWork work) {
    this.user = user;
    this.directory = directory;
    this.fileDate = fileDate;
    this.tasks = tasks;
    tasks.sort(null);        // Sort tasks in taskId order for display
    if (work != null) {
      this.jedDuccId = work.getDuccId();
    }
  }

  @Override
  public String getUser() {
    return user;
  }

  @Override
  public String getDirectory() {
    return directory;
  }

  @Override
  public long getFileDate() {
    return fileDate;
  }

  @Override
  public ArrayList<Task> getTasks() {
    return tasks;
  }
  
  @Override
  public DuccId getJedDuccId() {
    return jedDuccId;
  }
  
  // Update the duccId for the JED AP if missing or is older
  @Override
  public void updateJedId(DuccId duccId) {
    if (jedDuccId == null || jedDuccId.getFriendly() < duccId.getFriendly()) {
      jedDuccId = duccId;
    }
  }
  
  @Override
  public boolean isActive() {
    boolean retVal = false;
    switch (getStatus()) {
      case Running:
      case Restarting:
        retVal = true;
        break;
      default:
        break;
    }
    return retVal;
  }

  // TODO - the experiment status could be determined in the constructor
  @Override
  public Jed.Status getStatus() {
    Jed.Status retVal = Jed.Status.Unknown;
    if (tasks != null) {
      boolean canceled = false;
      boolean failed = false;
      boolean running = false;
      boolean done = false;
      boolean restarting = false;
      for (Task task : tasks) {
        if (task.parentId == 0 && task.status != null) {
          Jed.Status status = Jed.Status.getEnum(task.status);
          switch (status) {
            case Canceled:
              canceled = true;
              break;
            case Running:
              running = true;
              break;
            case Restarting:
              restarting = true;
              break;
            case Failed:
            case DependencyFailed:
              failed = true;
              break;
            case Done:
            case Completed:
              done = true;
              break;
            default:
              break;
          }
        }
      }
      // If more than 1 parent task use the most "important" state
      // Running > Failed/DependencyFailed > Canceled > Done/Completed
      // But if JED appears to have been killed while running change state to Unknown
      if (running) {
        retVal = isStale() ? Jed.Status.Unknown : Jed.Status.Running;
      } else if (restarting) {
        retVal = Jed.Status.Restarting;
      } else if (failed) {
        retVal = Jed.Status.Failed;
      } else if (canceled) {
        retVal = Jed.Status.Canceled;
      } else if (done) {
        retVal = Jed.Status.Done;
      } else {
        retVal = Jed.Status.Unknown;
      }
    }
    return retVal;
  }

  /*
   * Set status of the top-level task(s) to "Restarting",
   * clear status of all rerun tasks selected to be rerun,
   + then rewrite the Experiment.state file
   */
  @Override
  public boolean updateStateFile(String umask) {
    if (tasks == null) {
      return true;
    }
    for (Task task : tasks) {
      if (task.parentId == 0) {
        task.status = "Restarting";
      } else if (task.rerun) {
        // Indicate that task has not yet started
        task.status = null;
        task.startTime = null;
        task.runTime = 0;
      }
    }
    return writeStateFile(umask);
  }
  
  /*
   * Write the state as a temporary file, 
   * as the user copy it to the output directory,
   * delete the temp file.
   */
  private boolean writeStateFile(String umask) {
    File tempFile = null;
    Gson gson = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().setPrettyPrinting().create();
    try {
      tempFile = File.createTempFile("experiment", jedDuccId.toString());
      FileWriter out = new FileWriter(tempFile);
      String text = gson.toJson(tasks);
      out.write(text);
      out.close();
    } catch (IOException e) {
      WsLog.error(logger, "writeExperiment", "Failed to write experiment state as " + tempFile + " - " + e);
      return false;
    }

    File stateFile = new File(directory, "Experiment.state");
    HashMap<String, String> environment = new HashMap<String, String>();
    environment.put("DUCC_UMASK", umask);
    String sysout = DuccAsUser.execute(user, environment, "/bin/cp", tempFile.getAbsolutePath(), stateFile.getAbsolutePath());
    if (sysout.length() == 0) {
      tempFile.delete();
      return true;
    }
    WsLog.error(logger, "writeExperiment", "Failed to copy experiment state file\n" + sysout);
    return false;
  }
  
  private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd-HH:mm:ss");

  private static long getMillis(String dateString) {
    long retVal = 0;
    try {
      if (dateString != null) {
        Date date = sdf.parse(dateString);
        retVal = date.getTime();
      }
    } catch (Exception e) {
    }
    return retVal;
  }

  @Override
  public String getStartDate() {
    String retVal = "";
    long experimentStartMillis = Long.MAX_VALUE;
    if (tasks != null) {
      for (Task task : tasks) {
        String dateString = task.startTime;
        if (dateString != null) {
          long taskStartMillis = getMillis(dateString);
          if (taskStartMillis < experimentStartMillis) {
            experimentStartMillis = taskStartMillis;
            retVal = dateString;
          }
        }
      }
    }
    return retVal;
  }

  private long staleTime = 1000 * 60 * 6;

  @Override
  public boolean isStale() {
    // If the log lock file has been removed then the driver has stopped.
    // If the lock file is still present the driver may have been killed, so check the age of the state file.
    // If the lock-file exists  its timestamp will be > 0
    File lockFile = new File(directory, "DRIVER.log.lck");
    boolean lockExists = ExperimentsRegistryUtilities.getFileTime(user, lockFile) > 0;
    if (lockExists) {
      long now = System.currentTimeMillis();
      long fileDate = getFileDate();
      if (fileDate > 0) {
        if (fileDate < now) {
          long elapsed = now - fileDate;
          if (elapsed < staleTime) {
            return false;
          }
        }
      }
    }
    return true;
  }

  // The main experiments map uses the directory name as a key, and since it is unique its hashcode can be used when experiment is a key
  @Override
  public int hashCode() {
    return directory.hashCode();
  }

  /*
   * Sort active experiments first, then by start date (newest first) then by directory
   */
  @Override
  public int compareTo(IExperiment that) {
    // Booleans sort false before true so must reverse and compare that with this
    int retVal = new Boolean(that.isActive()).compareTo(new Boolean(this.isActive()));  // active first
    if (retVal == 0) {
      retVal = that.getStartDate().compareTo(this.getStartDate()); // reverse of natural ordering i.e. newest first
      if (retVal == 0) {
        retVal = this.getDirectory().compareTo(that.getDirectory()); // natural alphabetic
      }
      // Since the map is keyed by directory we should never see identical directories
    }
    return retVal;
  }

  /*
   * Satisfy the Comparable contract by making equals returns true only when compareTo returns 0
   * (which should never happen in practice)
   */
  @Override
  public boolean equals(Object obj) {
    if (obj == null || ! (obj instanceof Experiment)) {
      return false;
    }
    Experiment that = (Experiment) obj;
    return (this.isActive() == that.isActive()) &&
            this.getStartDate().equals(that.getStartDate()) &&
            this.getDirectory().equals(that.getDirectory());
  }
}
