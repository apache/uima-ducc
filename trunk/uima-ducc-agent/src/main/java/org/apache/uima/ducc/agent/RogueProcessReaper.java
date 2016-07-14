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
package org.apache.uima.ducc.agent;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.uima.ducc.common.node.metrics.NodeUsersInfo;
import org.apache.uima.ducc.common.utils.DuccLogger;

/**
 * Manages rogue processes on a node.
 * 
 * 
 */
public class RogueProcessReaper {

  private Map<String, RogueProcessEntry> userRogueProcessMap = new TreeMap<String, RogueProcessEntry>();

  private Map<String, Process> processMap = new HashMap<String, Process>();

  private int counterValue = 1;

  private int cleanupCounterValue = 5;

  int maxSecondsBeforeEntryExpires = 120; // number of seconds a process entry is kept in

  // the rogue process map before it is removed.
  // Default: 2 minutes

  private DuccLogger logger;

  boolean doKillRogueProcess = false;

  private String reaperScript;

  public RogueProcessReaper(DuccLogger logger, int counterValue, int cleanupCounterValue) {
    // this.counterValue = counterValue;
    if (cleanupCounterValue > 0) {
      this.cleanupCounterValue = cleanupCounterValue;
    } else {
      this.cleanupCounterValue = counterValue + 5;
    }
    reaperScript = System.getProperty("ducc.agent.rogue.process.reaper.script");

    // check if purge delay is defined in ducc.properties.
    if (System.getProperty("ducc.agent.rogue.process.purge.delay") != null) {
      try {
        maxSecondsBeforeEntryExpires = Integer.valueOf(System
                .getProperty("ducc.agent.rogue.process.purge.delay"));
      } catch (Exception e) {
        if (logger == null) {
          e.printStackTrace();
        } else {
          logger.error("RogueProcessReaper.ctor", null, e);
        }
        maxSecondsBeforeEntryExpires = 120; // defaulting to 2 minutes
      }
    }
    this.logger = logger;
    // final String kill = System.getProperty("ducc.agent.rogue.process.kill");

    if (Boolean.getBoolean("ducc.agent.rogue.process.kill") == true) {
      doKillRogueProcess = true;
    }
    if (logger == null) {
      System.out.println("ducc.agent.rogue.process.kill=" + doKillRogueProcess);

    } else {
      logger.info("RogueProcessReaper.ctor", null, "ducc.agent.rogue.process.kill="
              + doKillRogueProcess);

    }

  }

  public void submitRogueProcessForKill(String user, String pid, String ppid, boolean isJava) {
    final String methodName = "RogueProcessReaper.submitRogueProcessForKill";
    RogueProcessEntry entry = null;
    if (userRogueProcessMap.containsKey(pid)) {
      entry = userRogueProcessMap.get(pid);
    } else {
      if (cleanupCounterValue <= counterValue) {
        cleanupCounterValue += counterValue;
      }
      entry = new RogueProcessEntry(counterValue, cleanupCounterValue, user,
              maxSecondsBeforeEntryExpires, isJava, ppid);
      userRogueProcessMap.put(pid, entry);
    }
    entry.markAsRogue(3);
    if (!entry.isRogue()) {
      if (logger == null) {
        System.out
                .println("PID:" + pid + " Not Rogue Yet - It takes 3 iterations to make it Rogue");

      } else {
        logger.info("submitRogueProcessForKill", null, "PID:" + pid
                + " Not Rogue Yet - It takes 3 iterations to make it Rogue");

      }
      return;
    }
    if (reaperScript != null) {
      try {
        // Dont kill the process immediately. Kill if this method is called "counterValue"
        // number of times.
        long counter=0;
        if (logger != null) {
          logger.info(methodName, null,
                  "Decrementing Counter - Current Value:" + entry.counter.getCount());
        }
        if ( entry.counter.getCount() > 0) {
        	counter = entry.countDown();
        }
        // check if the rogue process needs to be killed
        if (counter <= 0 && !entry.isKilled()) {
          if (logger == null) {
            System.out.println("Process Scheduled for Kill PID:" + pid + " Owner:" + user + " ");

          } else {
            logger.info(methodName, null, "Process Scheduled for Kill PID:" + pid + " Owner:"
                    + user + " ");

          }
          entry.resetCounter(counterValue);
          kill(user, pid);
          entry.killed();
        } else {
          if (logger == null) {
            System.out
                    .println("Process ***NOT*** Scheduled for Kill PID:" + pid + " Owner:" + user);
 
          } else {
            logger.info(methodName, null, "Process ***NOT*** Scheduled for Kill PID:" + pid
                    + " Owner:" + user);

          }

        }

        if (entry.isKilled() && entry.countDownCleanupCounter() == 0) {
          if (logger == null) {
            System.out.println("Removing Entry From RougeProcessMap for PID:" + pid + " Owner:"
                    + user);

          } else {
            logger.info(methodName, null, "Removing Entry From RougeProcessMap for PID:" + pid
                    + " Owner:" + user);

          }
          userRogueProcessMap.remove(pid);
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    } else {
      if (logger == null) {
        System.out
                .println("Ducc Not Configured to Kill Rogue Proces (PID:)"
                        + pid
                        + " Owner:"
                        + user
                        + ". Change (or define) ducc.agent.rogue.process.reaper.script property in ducc.properties if you want rogue processes to be cleaned up.");

      } else {
        logger.info(
                methodName,
                null,
                "Ducc Not Configured to Kill Rogue Proces (PID:)"
                        + pid
                        + " Owner:"
                        + user
                        + ". Change (or define) ducc.agent.rogue.process.reaper.script property in ducc.properties if you want rogue processes to be cleaned up.");

      }
    }
    if (logger == null) {
      System.out.println("UserRougeProcessMap size:" + userRogueProcessMap.size());

    } else {
      logger.info(methodName, null, "UserRougeProcessMap size:" + userRogueProcessMap.size());

    }
  }

  public List<String> getUserRogueProcesses(String user) {
    List<String> rogues = new ArrayList<String>();
    for (Map.Entry<String, RogueProcessEntry> entry : userRogueProcessMap.entrySet()) {
      if (entry.getValue().getUser().equals(user) && entry.getValue().isRogue()) {
        rogues.add(entry.getKey());
      }
    }
    return rogues;
  }

  public boolean removeRogueProcess(String pid) {
    if (userRogueProcessMap.containsKey(pid)) {
      userRogueProcessMap.remove(pid);
      return true;
    }
    return false;
  }

  public void removeDeadRogueProcesses(List<String> currentPids) {
    List<String> deadPIDs = new ArrayList<String>();

    for (Map.Entry<String, RogueProcessEntry> entry : userRogueProcessMap.entrySet()) {
      if (!currentPids.contains(entry.getKey())) {
        deadPIDs.add(entry.getKey());
      }
    }
    for (String deadPID : deadPIDs) {
      userRogueProcessMap.remove(deadPID);
    }
  }

  public void copyAllUserRogueProcesses(TreeMap<String, NodeUsersInfo> map) {
    // List containing old entries which should be deleted from userRogueProcessMap
    List<String> entryCleanupList = new ArrayList<String>();

    for (Map.Entry<String, RogueProcessEntry> entry : userRogueProcessMap.entrySet()) {
      if (!entry.getValue().isRogue()) {
        continue;
      }
      NodeUsersInfo nui;
      if (map.containsKey(entry.getValue().getUser())) {
        nui = map.get(entry.getValue().getUser());
      } else {
        nui = new NodeUsersInfo(entry.getValue().getUser());
        map.put(entry.getValue().getUser(), nui);
      }
      nui.addRogueProcess(entry.getKey(), entry.getValue().getPpid(), entry.getValue().isJava());
    }
    for (String entryToRemove : entryCleanupList) {
      if (logger == null) {
        System.out.println("Removing Expired Entry From RogueProcessMap for PID:" + entryToRemove);

      } else {
        logger.info("copyAllUserRogueProcesses", null,
                "Removing Expired Entry From RogueProcessMap for PID:" + entryToRemove);

      }
      userRogueProcessMap.remove(entryToRemove);
    }
  }

  /**
   * This method checks if ducc is configured to kill rogue processes and if so, proceeds to kill
   * via -9.
   * 
   * @param user
   *          - process owner
   * @param pid
   *          - process id
   * @throws Exception
   */
  public void kill(final String user, final String pid) throws Exception {
    final String methodName = "RogueProcessReaper.kill.run()";

    try {
      // if the previously started shell script is still alive, kill it before starting a
      // new one.
      synchronized (this) {
        if (processMap.containsKey(pid)) {
          Process p = processMap.get(pid);
          if (p != null) {
            p.destroy();
          }
        }
      }
    } catch (Exception e) {
      logger.error(methodName, null, e);
    }
    new Thread(new Runnable() {
      public void run() {
    	  InputStream is = null;
    	  BufferedReader reader = null;
        try {
          String[] repearScriptCommand = new String[] { reaperScript, pid };
          ProcessBuilder pb = new ProcessBuilder(repearScriptCommand);
          pb.redirectErrorStream(true);
          Process shellProcess = pb.start();
          synchronized (this) {
            processMap.put(pid, shellProcess);
          }
          StringBuffer sb = new StringBuffer();
          for (String part : repearScriptCommand) {
            sb.append(part).append(" ");
          }
          if (logger == null) {
            System.out.println("--------- Started Rogue Process Reaper Script For Pid:" + pid
                    + " Owned by:" + user + " Command:" + sb.toString());

          } else {
            logger.info(methodName, null, "--------- Started Rogue Process Reaper Script For Pid:"
                    + pid + " Owned by:" + user + " Command:" + sb.toString());

          }
          is = shellProcess.getInputStream();
          reader = new BufferedReader(new InputStreamReader(is));

          // read the next line from stdout and stderr
          while (reader.readLine() != null) {
            // dont care about the output, just drain the buffers
          }
         
          sb.setLength(0);

          if (logger == null) {
            System.out.println("--------- Rogue Process Reaper (for PID:" + pid + ") Terminated");

          } else {
            logger.info(methodName, null, "--------- Rogue Process Reaper (for PID:" + pid
                    + ") Terminated");

          }

        } catch (Exception e) {
          logger.error(methodName, null, e);
        } finally {
        	
          synchronized (this) {
            processMap.remove(pid);
          }
          if ( reader != null ) {
        	  try {
            	  reader.close();
        	  } catch( Exception exx){}
          }
        }
      }
    }).start();

  }

  private static class RogueProcessEntry {
    CountDownLatch counter;

    CountDownLatch cleanupCounter;

    String user;

    boolean killed;

    boolean java;

    String ppid;

    AtomicInteger pendingCounter = new AtomicInteger(1);

    boolean rogue;

    public RogueProcessEntry(int counterValue, int cleanupCounterValue, String user,
            int maxSecondsBeforeEntryExpires, boolean isJava, String ppid) {
      counter = new CountDownLatch(counterValue);
      cleanupCounter = new CountDownLatch(cleanupCounterValue);
      this.user = user;
      this.java = isJava;
      this.ppid = ppid;
    }

    public String getPpid() {
      return ppid;
    }

    public boolean isRogue() {
      return rogue;
    }

    public void killed() {
      killed = true;
    }

    public boolean isKilled() {
      return killed;
    }

    public String getUser() {
      return user;
    }

    public long countDown() {
      counter.countDown();
      return counter.getCount();
    }

    public void resetCounter(int counterValue) {
      counter = new CountDownLatch(counterValue);
    }

    public long countDownCleanupCounter() {
      cleanupCounter.countDown();
      return cleanupCounter.getCount();
    }

    public void markAsRogue(int ceiling) {
      if (pendingCounter.get() < ceiling) {
        pendingCounter.addAndGet(1);
      } else {
        rogue = true;
      }
    }

    public boolean isJava() {
      return java;
    }

  }

  public static void main(String[] args) {
  }
}
