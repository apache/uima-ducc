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
import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.common.IDuccStandardInfo;
import org.apache.uima.ducc.transport.event.common.IDuccWork;
import org.apache.uima.ducc.transport.event.common.IDuccWorkJob;
import org.apache.uima.ducc.transport.event.common.IDuccWorkMap;
import org.apache.uima.ducc.transport.event.common.IDuccWorkService;
import org.apache.uima.ducc.ws.log.WsLog;

import com.google.gson.Gson;
import com.google.gson.JsonParseException;
import com.google.gson.reflect.TypeToken;

public class ExperimentsRegistryManager {

  // NOTE - this variable used to hold the class name before WsLog was simplified
  private static DuccLogger cName = DuccLogger.getLogger(ExperimentsRegistryManager.class);

  private static Gson gson = new Gson();

  private static ExperimentsRegistryManager instance = new ExperimentsRegistryManager();
  
  private static String stateFileName = "Experiment.state";

  private int MAX_CACHE_SIZE = 4096;

  private Set<String> seenDirs = new HashSet<String>();   // Set of directories seen when booting or during an OR publication

  private Map<String, Experiment> experimentsByDir = new ConcurrentHashMap<String,Experiment>();

  public static ExperimentsRegistryManager getInstance() {
    return instance;
  }

  public Experiment getExperiment(String directory) {
    return experimentsByDir.get(directory);
  }
  
  /*
   *  Called by DuccPlugins with work restored from the DB when web-server boots
   */
  public void initialize(IDuccWorkJob job) {
    update(job);
  }

  public void initialize(IDuccWorkService service) {
    update(service);
  }
  
  /*
   * Called by DuccPlugins for each OR publication
   */
  public void update(IDuccWorkMap dwm) {
    String mName = "update";
    if (dwm == null) {
      WsLog.warn(cName, mName, "missing map");
      return;
    }
    seenDirs.clear();   // Clear set of visited dirs before every update
    try {
      // Inspect all jobs
      for (DuccId duccId : dwm.getJobKeySet()) {
        IDuccWorkJob job = (IDuccWorkJob) dwm.findDuccWork(duccId);
        if (job != null) {
          update(job);
        }
      }

      // Inspect managed reservations ... internally are called services
      for (DuccId duccId : dwm.getManagedReservationKeySet()) {
        IDuccWorkService service = (IDuccWorkService) dwm.findDuccWork(duccId);
        if (service != null) {
          update(service);
        }
      }
      prune();  // if list too large
    } catch (Exception e) {
      WsLog.error(cName, mName, e);
    }
  }
  
  private void update(IDuccWorkJob job) {
    IDuccStandardInfo stdInfo = job.getStandardInfo();
    if (stdInfo != null) {
      String user = stdInfo.getUser();
      String logDirectory = stdInfo.getLogDirectory();
      String parent = new File(logDirectory).getParent();
      if (seenDirs.add(parent)) {  // Process the first job with this parent dir
        update(user, parent, null);
      }
    }
  }
  
  // Check if the managed reservation is a DUCC-launched AP
  private void update(IDuccWorkService managedReservation) {
    IDuccStandardInfo stdInfo = managedReservation.getStandardInfo();
    if (stdInfo != null) {
      String user = stdInfo.getUser();
      String logDirectory = stdInfo.getLogDirectory();
      File logDir = new File(logDirectory, managedReservation.getId());
      File link = new File(logDir, "outputDirectoryLink");
      String experimentDirectory = ExperimentsRegistryUtilities.readLink(user, link);
      if (experimentDirectory != null) {
        // Even if have seen this dir earlier we must check that we have the duccId of the latest AP
        // JED tasks usually have a log dir 1 level below the exp dir
        seenDirs.add(experimentDirectory); // This prevents processing of any of its child jobs
        update(user, experimentDirectory, managedReservation);
      } else {
        String parent = new File(logDirectory).getParent();
        if (seenDirs.add(parent)) { // Process the first AP with this parent dir
          update(user, parent, null); // (unless already seen as a JED exp dir)
        }
      }
    }
  }
  
  // Load or refresh the experiment
  // May need to run as the user if DUCC cannot read the user's files
  private void update(String user, String directory, IDuccWork jedProcess) {
    String mName = "update";
    
    // "normalize" directory name
    try {
      directory = new File(directory).getCanonicalPath();
    } catch (IOException e) {
      WsLog.error(cName, mName, "Failed to create canonical name for " + directory + "\n" + e);
      return;
    }

    File stateFile = new File(directory, stateFileName);
    long fileTime = ExperimentsRegistryUtilities.getFileTime(user, stateFile);
    if (fileTime == 0) {
	  //WsLog.trace(cName, mName, "Failed to find "+stateFile.getAbsolutePath());
      return;           // File not found
    }
    
    // Check if state file is newer than any existing one
    // If newer refresh it, otherwise just update the JED duccId in case it is a newer AP
    // But if the experiment is running refresh the details as getFileDate may not reflect the latest changes
    Experiment existingExperiment = experimentsByDir.get(directory);
    if (existingExperiment != null && !existingExperiment.isActive()) {
      // Synchronize the check for a newer state file with the rewrite of the file in Experiment.writeStateFile
      // to ensure that the rewritten file does does not look newer that the in-memory Experiment
      long existingFileTime;
      synchronized (existingExperiment) {
        existingFileTime = existingExperiment.getFileDate();
      }
      if (fileTime <= existingFileTime) {    // No need to reload, but update the jED ID if newer
        if (jedProcess != null) {
          existingExperiment.updateJedWork(jedProcess);
        }
        return;
      }
    } 

    // Load or reload changed state file (fileTime could be older than what is read here)
    String contents = ExperimentsRegistryUtilities.readFile(user, stateFile);
    if (contents == null) {
      WsLog.warn(cName, mName, "State file not found " + stateFile.getAbsolutePath());
      return;
    }
    // Ignore version prefix - old non-prefixed files had a larger "stale" threshold
    int offset = contents.indexOf('[');
    if (offset > 0) {
      contents = contents.substring(offset);
    }
    // Load from a Json array of Tasks
    StringReader sr = new StringReader(contents);
    Type typeOfTaskList = new TypeToken<ArrayList<Task>>(){}.getType();
    try {
      ArrayList<Task> taskArray = gson.fromJson(sr, typeOfTaskList);
      Experiment experiment = new Experiment(user, directory, fileTime, taskArray, jedProcess);
      Experiment oldExperiment = experimentsByDir.put(directory, experiment);
      if (oldExperiment != null) {
        experiment.updateJedWork(oldExperiment.getJedWork());    // Ensure the new instance has the latest DuccId
      }
    } catch (JsonParseException e) {
      WsLog.warn(cName, mName, "Ignoring " + stateFile + " as has Json syntax error " + e.getMessage());
    }
  }
  
  // Create map ordered by experiment: active, newest start date, directory
  public TreeMap<Experiment, String> getMapByStatus() {
    TreeMap<Experiment, String> mapInverse = new TreeMap<Experiment, String>();
    for (Entry<String, Experiment> entry : experimentsByDir.entrySet()) {
      mapInverse.put(entry.getValue(), entry.getKey());
    }
    return mapInverse;
  }

  // Create map from file last update time to directory ordered oldest first
  private TreeMap<Long, String> getMapByDate() {
    TreeMap<Long, String> mapByDate = new TreeMap<Long, String>();
    for (Entry<String, Experiment> entry : experimentsByDir.entrySet()) {
      Experiment experiment = entry.getValue();
      mapByDate.put(experiment.getFileDate(), entry.getKey());
    }
    return mapByDate;
  }

  // Keep all active experiments plus the newer inactive ones.
  // i.e. prune inactive entries with the oldest start-time
  private void prune() {
    String mName = "prune";
    int excess = experimentsByDir.size() - MAX_CACHE_SIZE;
    if (excess <= 0) {
      return;
    }
    // Create map sorted by filetime, oldest first and discard the first "excess" inactive entries
    WsLog.info(cName, mName, "Pruning " + excess + " old experiments");
    TreeMap<Long, String> mapByDate = getMapByDate();
    for (String directory : mapByDate.values()) {
      Experiment experiment = experimentsByDir.get(directory);
      if (!experiment.isActive()) {
        WsLog.info(cName, mName, "Pruned " + directory + " filetime " + experiment.getFileDate());
        experimentsByDir.remove(directory);
        if (--excess <= 0) {
          break;
        }
      }
    }
    WsLog.info(cName, mName, "Pruned map size: " + experimentsByDir.size());
  }

}
