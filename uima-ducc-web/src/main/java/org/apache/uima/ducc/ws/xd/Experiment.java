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
import org.apache.uima.ducc.transport.event.common.IDuccWork;
import org.apache.uima.ducc.ws.authentication.DuccAsUser;
import org.apache.uima.ducc.ws.log.WsLog;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class Experiment implements Comparable<Experiment> {
  
  private static DuccLogger logger = DuccLogger.getLogger(Experiment.class);

  private String user = null;

  private String directory = null;

  private ArrayList<Task> tasks = null;

  private long fileDate = 0;

  private long jedId = 0;   // ID of JED AP that launched this experiment (0 => not DUCC launched)

  private IDuccWork jedWork = null;  // DUCC managed reservation running JED 
  
  private boolean restarting = false;

    
  public Experiment(String user, String directory, long fileDate, ArrayList<Task> tasks, IDuccWork jedWork) {
    this.user = user;
    this.directory = directory;
    this.fileDate = fileDate;
    this.tasks = tasks;
    tasks.sort(null);        // Sort tasks in taskId order for display
    if (jedWork != null) {
      this.jedWork = jedWork;
      this.jedId = jedWork.getDuccId().getFriendly();
    }
  }

  public String getUser() {
    return user;
  }

  public String getDirectory() {
    return directory;
  }

  public long getFileDate() {
    return fileDate;
  }

  public ArrayList<Task> getTasks() {
    return tasks;
  }
  
  public long getJedId() {
    return jedId;
  }
  
  public void setJedId(long duccId) {
    jedId = duccId;
  }
  
  public IDuccWork getJedWork() {
    return jedWork;
  }
  
  // Update the AP & duccId for the JED AP if given a newer one
  // Called when replacing an existing Experiment with an updated one or when only the JED ID needs updating
  // Also may indicate that a restarted JED has checked in
  public void updateJedWork(IDuccWork work) {
    if (work == null) return;
    long duccId = work.getDuccId().getFriendly();
    if (duccId > jedId) {
      jedId = duccId;
      jedWork = work;
    }
  }
  
  // Unused
  //  public boolean isRestarting() {
  //    return restarting;
  //  }
  
  public void setRestart(boolean restart) {
    restarting = restart;
  }
  
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

  // TODO - the experiment status could be determined in the constructor but the restarting state can change
  // The restarting state is NOT saved in the Experiment.state file
  public Jed.Status getStatus() {
    Jed.Status retVal = Jed.Status.Unknown;
    if (restarting) {
      return Jed.Status.Restarting;
    }
    if (tasks != null) {
      boolean canceled = false;
      boolean failed = false;
      boolean running = false;
      boolean done = false;
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
   * Set the status of the tasks to be rerun
   */
  public void updateStatus() {
    for (Task task : tasks) {
      if (task.rerun) {
        task.status = null;
        if (Jed.Type.isLeaf(task.type)) { // Times are not accumulated for primitive tasks
          task.startTime = null;
          task.runTime = 0;
        }
      }
    }
    restarting = true;      // Will be cleared when the restarted AP updates the state file and replaces this object
  }
  
  /*
   * Update state file before re-launching JED
   * Experiment has stopped but state file will be updated when JED starts
   */
  public boolean writeStateFile(String umask) {
    String mName = "writeStateFile";
    int version = 1;
    File tempFile = null;
    Gson gson = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().setPrettyPrinting().create();
    try {
      tempFile = File.createTempFile("experiment", String.valueOf(jedId));
      FileWriter out = new FileWriter(tempFile);
      out.write(String.valueOf(version));
      out.write('\n');
      String text = gson.toJson(tasks);
      out.write(text);
      out.close();
    } catch (IOException e) {
      WsLog.error(logger, mName, "Failed to write experiment state as " + tempFile + " - " + e);
      return false;
    }

    File stateFile = new File(directory, "Experiment.state");
    HashMap<String, String> environment = new HashMap<String, String>();
    environment.put("DUCC_UMASK", umask);
    // Synchronize with the check for a newer state file in ExperimentsRegistryManager to ensure that
    // the file time in memory matches the actual file time
    synchronized (this) {
      String sysout = DuccAsUser.execute(user, environment, "/bin/cp", tempFile.getAbsolutePath(), stateFile.getAbsolutePath());
      if (sysout.length() > 0) {
        WsLog.error(logger, mName, "Failed to copy experiment state file\n" + sysout);
        return false;
      }
      fileDate = ExperimentsRegistryUtilities.getFileTime(user, stateFile);
      tempFile.delete();
      return true;
    }
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
    // Check if DUCC thinks JED is still running
    // (the JED process may have ended but the updated state file may not yet be visible to DUCC)
    if (jedWork != null) {
      return jedWork.isCompleted();
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
  public int compareTo(Experiment that) {
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
