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
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.common.IDuccStandardInfo;
import org.apache.uima.ducc.transport.event.common.IDuccWork;
import org.apache.uima.ducc.transport.event.common.IDuccWorkMap;
import org.apache.uima.ducc.ws.log.WsLog;
import org.apache.uima.ducc.ws.server.DuccWebProperties;

import com.google.gson.Gson;
import com.google.gson.JsonParseException;
import com.google.gson.reflect.TypeToken;

public class ExperimentsRegistryManager {

  // NOTE - this variable used to hold the class name before WsLog was simplified
  private static DuccLogger cName = DuccLogger.getLogger(ExperimentsRegistryManager.class);

  private static Gson gson = new Gson();

  private static ExperimentsRegistryManager instance = new ExperimentsRegistryManager();

  private int MAX_CACHE_SIZE = 4096;

  private TreeMap<String, IExperiment> map = new TreeMap<String, IExperiment>();

  private AtomicLong updateCounter = new AtomicLong(0);

  private boolean enabled;   // true if Experiments button is enabled/displayed

  public static ExperimentsRegistryManager getInstance() {
    return instance;
  }

  private ExperimentsRegistryManager() {
    String ducc_experiments = DuccWebProperties.get().getProperty("ducc.experiments", "false");
    enabled = ducc_experiments.equalsIgnoreCase("true");
    WsLog.info(cName, "<constructor>", "Experiments enabled: " + enabled);
  }
  
  public TreeMap<IExperiment, String> getMapByStatus() {
    TreeMap<IExperiment, String> mapInverse = new TreeMap<IExperiment, String>();
    synchronized (map) {
      for (Entry<String, IExperiment> entry : map.entrySet()) {
        mapInverse.put(entry.getValue(), entry.getKey());
      }
    }
    return mapInverse;
  }

  private TreeMap<Long, String> getMapByDate() {
    TreeMap<Long, String> mapByDate = new TreeMap<Long, String>();
    synchronized (map) {
      for (Entry<String, IExperiment> entry : map.entrySet()) {
        IExperiment experiment = entry.getValue();
        Long key = new Long(experiment.getStartTime());
        String value = experiment.getId();
        mapByDate.put(key, value);
      }
    }
    return mapByDate;
  }

  public IExperiment getById(String id) {
    IExperiment retVal = null;
    if (id != null) {
      synchronized (map) {
        for (Entry<String, IExperiment> entry : map.entrySet()) {
          IExperiment experiment = entry.getValue();
          if (id.equals(experiment.getId())) {
            retVal = experiment;
            break;
          }
        }
      }
    }
    return retVal;
  }

  private void replace(String directory, Experiment experiment) {
    String mName = "replace";
    
    // Keep the same id
    String id = map.get(directory).getId();
    experiment.setId(id);
    
    // Keep latest JED id ... and umask in case this is not the JED AP
    DuccId oldDuccId = map.get(directory).getJedDuccId();
    long oldNum = oldDuccId==null? 0 : oldDuccId.getFriendly();
    DuccId newDuccId = experiment.getJedDuccId();
    long newNum = newDuccId==null? 0 : newDuccId.getFriendly();
    if (oldNum > newNum) {
      experiment.setJedDuccId(oldDuccId);
      experiment.umask = ((Experiment)map.get(directory)).umask;   // Ugh - should update rather than replace ??
    }
    
    map.put(directory, experiment);
    WsLog.debug(cName, mName, directory);
  }

  private void add(String directory, Experiment experiment) {
    String mName = "add";
    map.put(directory, experiment);
    WsLog.debug(cName, mName, directory);
  }

  private void put(String directory, Experiment experiment) {
    synchronized (map) {
      if (map.containsKey(directory)) {
        replace(directory, experiment);
      } else {
        add(directory, experiment);
      }
    }
  }

  private void remove(String directory) {
    String mName = "remove";
    synchronized (map) {
      map.remove(directory);

    }
    WsLog.debug(cName, mName, directory);
  }

  private boolean containsKey(String directory) {
    synchronized (map) {
      return map.containsKey(directory);
    }
  }

  // Called by DuccPlugins when web-server boots
  public void initialize(IDuccWork dw) {
    IDuccStandardInfo stdInfo = dw.getStandardInfo();
    if (stdInfo != null) {
      String user = stdInfo.getUser();
      String directory = stdInfo.getLogDirectory();
      String experimentDirectory = stdInfo.getExperimentDirectory();
      if (experimentDirectory != null) {
        update(user, experimentDirectory, false, dw);
      } else {
        directory = ExperimentsRegistryUtilities.upOne(directory);
        update(user, directory, false, null);
      }
    }      
  }
  
  // DuccWork provided only for the AP that launches JED
  private void update(String user, String directory, IDuccWork work) {
    String mName = "update";
    
    // "normalize" directory name
    try {
      directory = new File(directory).getCanonicalPath();
    } catch (IOException e) {
      WsLog.error(cName, mName, "Failed to create canonical name for " + directory + "\n" + e);
      return;
    }

    try {
      String fileName = ExperimentsRegistryUtilities.getStateFilePath(directory);
      long date = ExperimentsRegistryUtilities.getFileDate(user, fileName);
      WsLog.debug(cName, mName, "Reading " + fileName + " date: " + date);
      String contents = ExperimentsRegistryUtilities.getFileContents(user, fileName);
      if (contents != null) {
        // Version may precede the initial '[' and may be on a separate line
        int version = 0;
        int offset = contents.indexOf('[');
        if (offset > 0) {
          String s = contents.substring(0, offset).trim();
          try {
            version = Integer.parseInt(s);
          } catch (NumberFormatException e) {
            WsLog.warn(cName, mName, "Invalid version '" + s + "' in state file : " + fileName);
            return;
          }
          contents = contents.substring(offset);
        }
        if (offset < 0) {
          WsLog.warn(cName, mName, "Invalid syntax (missing '[') in state file : " + fileName);
          return;
        }
        StringReader sr = new StringReader(contents);
        Type tasksType = new TypeToken<ArrayList<Task>>() {
        }.getType();
        try {
          ArrayList<Task> taskArray = gson.fromJson(sr, tasksType);
          Experiment experiment = new Experiment(user, directory, date, version, taskArray, work);
          put(directory, experiment);
        } catch (JsonParseException e) {
          WsLog.warn(cName, mName,
                  "Ignoring " + fileName + " as has Json syntax error " + e.getMessage());
        }
      } else {
        if (work != null) {
          WsLog.warn(cName, mName, "State file missing or inaccessible in " + directory + " for JED AP " + work.getDuccId());
        }
        WsLog.trace(cName, mName, "state file missing or inaccessible in " + directory);
        remove(directory);
      }
    } catch (Exception e) {
      WsLog.error(cName, mName, e);
    }
  }

  private void update(String user, String directory, boolean overwrite) {
    update(user, directory, overwrite, null);
  }
  
  private void update(String user, String directory, boolean overwrite, IDuccWork work) {
    String mName = "update";
    try {
      if (overwrite) {
        update(user, directory, work);
      } else {
        if (!containsKey(directory)) {
          update(user, directory, work);
        } else {
          WsLog.trace(cName, mName, "duplicate directory: " + directory);
        }
      }

    } catch (Exception e) {
      WsLog.error(cName, mName, e);
    }
  }

  private void check() {
    String mName = "check";
    try {
      ArrayList<IExperiment> list = new ArrayList<IExperiment>();
      synchronized (map) {
        for (Entry<String, IExperiment> entry : map.entrySet()) {
          IExperiment experiment = entry.getValue();
          if (experiment.isActive()) {
            list.add(experiment);
          }
        }
      }
      for (IExperiment experiment : list) {
        String user = experiment.getUser();
        String directory = experiment.getDirectory();
        WsLog.debug(cName, mName, "user: " + user + " " + "dirextory: " + directory);
        update(experiment.getUser(), experiment.getDirectory(), true);
      }
    } catch (Exception e) {
      WsLog.error(cName, mName, e);
    }
  }

  // Prune if map is large or after every 3rd update
  private boolean timeToPrune(int size) {
    boolean retVal = false;
    if (size > MAX_CACHE_SIZE) {
      retVal = true;
    }
    if ((updateCounter.get() % 3) == 0) {
      retVal = true;
    }
    return retVal;
  }

  // Keep an experiment if it is active or if cache has room
  // i.e. prune entries with the oldest start-time if not active and cache is full
  // ?? could prune a long-running active one ??
  private void prune() {
    String mName = "prune";
    WsLog.enter(cName, mName);
    try {
      TreeMap<Long, String> mapByDate = getMapByDate();
      if (timeToPrune(mapByDate.size())) {
        int cacheCount = 0;
        for (Entry<Long, String> entry : mapByDate.entrySet()) {
          String key = entry.getValue();
          IExperiment experiment = getById(key);
          if (experiment != null) {
            if (experiment.isActive()) {
              cacheCount++;
            } else {
              if (cacheCount < MAX_CACHE_SIZE) {
                cacheCount++;
              } else {
                String directory = experiment.getDirectory();
                remove(directory);
              }
            }
          }
        }
      }
    } catch (Exception e) {
      WsLog.error(cName, mName, e);
    }
    // WsLog.exit(cName, mName);
  }

  // Called by DuccPlugins for each OR publication
  public void update(IDuccWorkMap dwm) {
    String mName = "update";
    if (!enabled) 
      return;
    WsLog.enter(cName, mName);
    try {
      if (dwm == null) {
        WsLog.warn(cName, mName, "missing map");
      } else {
        updateCounter.incrementAndGet();
        Iterator<DuccId> iterator = dwm.getJobKeySet().iterator();
        while (iterator.hasNext()) {
          DuccId duccId = iterator.next();
          IDuccWork job = dwm.findDuccWork(duccId);
          if (job != null) {
            IDuccStandardInfo stdInfo = job.getStandardInfo();
            if (stdInfo != null) {
              String user = stdInfo.getUser();
              String directory = stdInfo.getLogDirectory();
              String parent = ExperimentsRegistryUtilities.upOne(directory);
              update(user, parent, true);
            }
          }
        }

        // Also process managed reservations in case the experiment has only these.
        iterator = dwm.getManagedReservationKeySet().iterator();
        while (iterator.hasNext()) {
          DuccId duccId = iterator.next();
          IDuccWork job = dwm.findDuccWork(duccId);
          if (job != null) {
            IDuccStandardInfo stdInfo = job.getStandardInfo();
            if (stdInfo != null) {
              String user = stdInfo.getUser();
              String directory = stdInfo.getLogDirectory();
              String experimentDirectory = stdInfo.getExperimentDirectory();
              if (experimentDirectory != null) {
                update(user, experimentDirectory, true, job);
              } else {
                directory = ExperimentsRegistryUtilities.upOne(directory);
                update(user, directory, true, null);
              }
            }
          }
        }
      }
      check();
      prune();
    } catch (Exception e) {
      WsLog.error(cName, mName, e);
    }
    // WsLog.exit(cName, mName);
  }
}
