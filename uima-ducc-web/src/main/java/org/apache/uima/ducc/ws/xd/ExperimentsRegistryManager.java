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
import org.apache.uima.ducc.transport.event.common.IDuccWorkMap;
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

  private Map<String, IExperiment> experimentsByDir = new ConcurrentHashMap<String,IExperiment>();

  public static ExperimentsRegistryManager getInstance() {
    return instance;
  }

  public IExperiment getExperiment(String directory) {
    return experimentsByDir.get(directory);
  }
  
  // Called by DuccPlugins with work restored from the DB when web-server boots
  public void initialize(IDuccWork dw) {
    //String mName = "initialize";
    IDuccStandardInfo stdInfo = dw.getStandardInfo();
    if (stdInfo != null) {
      String experimentDirectory = stdInfo.getExperimentDirectory();
      if (experimentDirectory != null) {
        // Is a JED AP so check that we have the duccId of the latest AP even if not the first seen
        seenDirs.add(experimentDirectory);
        update(stdInfo.getUser(), experimentDirectory, dw);
      } else {
        // May be one of the tasks from a local or ducc-launched JED so process only the first
        String logDirectory = stdInfo.getLogDirectory();
        String parent = new File(logDirectory).getParent();
        if (seenDirs.add(parent)) {
          update(stdInfo.getUser(), parent, null);
        }
      }
    }
  }

  // Called by DuccPlugins for each OR publication
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
        IDuccWork job = dwm.findDuccWork(duccId);
        if (job != null) {
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
      }

      // Inspect managed reservations ... check if a JED AP
      for (DuccId duccId : dwm.getManagedReservationKeySet()) {
        IDuccWork work = dwm.findDuccWork(duccId);
        if (work != null) {
          IDuccStandardInfo stdInfo = work.getStandardInfo();
          if (stdInfo != null) {
            String user = stdInfo.getUser();
            String experimentDirectory = stdInfo.getExperimentDirectory();
            if (experimentDirectory != null) {
              // Even if have seen this dir earlier we must check that we have the duccId of the latest AP
              // JED tasks usually have a log dir 1 level below the exp dir
              seenDirs.add(experimentDirectory);  // This prevents processing of any of its child jobs
              update(user, experimentDirectory, work);
            } else {
              String logDirectory = stdInfo.getLogDirectory();
              String parent = new File(logDirectory).getParent();
              if (seenDirs.add(parent)) {    // Process the first AP with this parent dir
                update(user, parent, null);  // (unless already seen as a JED exp dir)
              }
            }
          }
        }
      }
      prune();  // if list too large
    } catch (Exception e) {
      WsLog.error(cName, mName, e);
    }
  }
  
  // Load or refresh the experiment
  private void update(String user, String directory, IDuccWork work) {
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
      return;           // File not found
    }
    
    // Check if state file is newer than any existing one
    // If newer refresh it, otherwise just update the JED duccId in case it is a newer AP
    IExperiment existingExperiment = experimentsByDir.get(directory);
    if (existingExperiment != null) {
      // Synchronize the check for a newer state file with the rewrite of the file in Experiment.writeStateFile
      // to ensure that the rewritten file does does not look newer that the in-memory Experiment
      long existingFileTime;
      synchronized (existingExperiment) {
        existingFileTime = existingExperiment.getFileDate();
      }
      if (fileTime <= existingFileTime) {
        if (work != null) {
          existingExperiment.updateJedId(work.getDuccId());
        }
        return;
      }
    }

    // Load or reload changed state file
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
      IExperiment experiment = new Experiment(user, directory, fileTime, taskArray, work);
      IExperiment oldExperiment = experimentsByDir.put(directory, experiment);
      if (oldExperiment != null) {
        experiment.updateJedId(oldExperiment.getJedDuccId());    // Ensure the new instance has the latest DuccId
      }
    } catch (JsonParseException e) {
      WsLog.warn(cName, mName, "Ignoring " + stateFile + " as has Json syntax error " + e.getMessage());
    }
  }
  
  // Create map ordered by experiment: active, newest start date, directory
  public TreeMap<IExperiment, String> getMapByStatus() {
    TreeMap<IExperiment, String> mapInverse = new TreeMap<IExperiment, String>();
    for (Entry<String, IExperiment> entry : experimentsByDir.entrySet()) {
      mapInverse.put(entry.getValue(), entry.getKey());
    }
    return mapInverse;
  }

  // Create map from file last update time to directory ordered oldest first
  private TreeMap<Long, String> getMapByDate() {
    TreeMap<Long, String> mapByDate = new TreeMap<Long, String>();
    for (Entry<String, IExperiment> entry : experimentsByDir.entrySet()) {
      IExperiment experiment = entry.getValue();
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
      IExperiment experiment = experimentsByDir.get(directory);
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
