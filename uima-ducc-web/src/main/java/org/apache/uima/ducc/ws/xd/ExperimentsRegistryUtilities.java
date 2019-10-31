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
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Map.Entry;

import org.apache.uima.ducc.common.IDuccUser;
import org.apache.uima.ducc.common.utils.AlienFile;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.transport.cmdline.ACommandLine;
import org.apache.uima.ducc.transport.cmdline.ICommandLine;
import org.apache.uima.ducc.transport.event.common.IDuccWorkJob;
import org.apache.uima.ducc.transport.event.common.IDuccWorkService;
import org.apache.uima.ducc.transport.event.common.history.HistoryFactory;
import org.apache.uima.ducc.transport.event.common.history.IHistoryPersistenceManager;
import org.apache.uima.ducc.ws.authentication.DuccAsUser;
import org.apache.uima.ducc.ws.log.WsLog;

public class ExperimentsRegistryUtilities {

  // NOTE - this variable used to hold the class name before WsLog was simplified
  private static DuccLogger cName = DuccLogger.getLogger(ExperimentsRegistryUtilities.class);

  /*
   * Get the file timestamp ... return 0 if file doesn't exist
   */
  public static long getFileTime(String user, File stateFile) {
    if (stateFile.canRead()) {
      return stateFile.lastModified();
    }
    // May be unreadable by DUCC or may not exist!
    // Use duccling to get time in "date" style seconds
    String[] cmd = { "/bin/ls", "-l", "--time-style=+%s", stateFile.getAbsolutePath() };
    String result = DuccAsUser.execute(user, null, cmd);
    String[] toks = result.split("\\s+");
    // If file doesn't exist 6-th token will be part of an error msg
    if (toks.length >= 6) {
      try {
        return Long.valueOf(toks[5]) * 1000;
      } catch (NumberFormatException e) {
      }
    }
    return 0;

  }

  /*
   * Read the Experiment.state file ... as the user if necessary
   */
  public static String readFile(String user, File file) {
    String mName = "readFile";
    try {
      if (file.canRead()) {
        byte[] bytes = Files.readAllBytes(file.toPath());
        return new String(bytes);
      } else {
        AlienFile alienFile = new AlienFile(user, file.getAbsolutePath());
        return alienFile.getString();
      }
    } catch (Exception e) {
      WsLog.error(cName, mName, "Failed to read file " + file.getAbsoluteFile());
      WsLog.error(cName, mName, e);
      return null;
    }
  }

  public static boolean launchJed(Experiment experiment) {
    String mName = "launchJed";
    
    IHistoryPersistenceManager hpm = HistoryFactory.getInstance(ExperimentsRegistryUtilities.class.getName());
    IDuccWorkService service = null;
    try {
      service = hpm.restoreArbitraryProcess(experiment.getJedId());
      if (service == null) {
        // If relaunch is too quick DB may not have been updated so wait 10 secs and try again
        Thread.sleep(10000);
        service = hpm.restoreArbitraryProcess(experiment.getJedId());
        if (service == null) {
          WsLog.error(cName, mName, "No entry found in DB for JED AP "+experiment.getJedId());
          return false;
        }
      }
    } catch (Exception e) {
      WsLog.error(cName, mName, "Failed to access DB for JED AP "+experiment.getJedId());
      WsLog.error(cName, mName, e);
      return false;
    }
       
    if (! (service instanceof IDuccWorkJob)) {
      WsLog.error(cName, mName, "Wrong class for JED AP? "+service.getClass().getName());
      return false;
    }
    IDuccWorkJob dwj = (IDuccWorkJob) service;
    ICommandLine cmd = dwj.getCommandLine();
    if (cmd == null) {
      WsLog.info(cName, mName, "No cmdline for JED AP " + experiment.getJedId());
      return false;
    }

    // Build the ducc_process_submit command to launch JED.
    // Create blank-delimited lists of arguments and environment variable assignments

    StringBuilder args = new StringBuilder();
    if (cmd.getOptions() != null) {
      for (String opt : cmd.getOptions()) {
        args.append(opt).append(' ');
      }
    }
    for (String arg : cmd.getArguments()) {
      args.append(arg).append(' ');
    }
    StringBuilder envs = new StringBuilder();
    if (cmd instanceof ACommandLine) {
      for (Entry<String, String> ent : ((ACommandLine) cmd).getEnvironment().entrySet()) {
        envs.append(ent.getKey() + "=" + ent.getValue() + " ");
      }
    }

    // Create the java command with the appropriate DUCC options
    String duccHome = System.getProperty(IDuccUser.EnvironmentVariable.DUCC_HOME.value());
    String[] submitCmd = { 
        cmd.getExecutable(), 
        "-cp", duccHome + "/lib/uima-ducc-cli.jar", "org.apache.uima.ducc.cli.DuccManagedReservationSubmit",
        "--process_executable",      cmd.getExecutable(), 
        "--process_executable_args", args.toString(),
        "--environment",             envs.toString(),
        "--log_directory",           dwj.getStandardInfo().getLogDirectory(),
        "--working_directory",       dwj.getStandardInfo().getWorkingDirectory(),
        "--description",             dwj.getStandardInfo().getDescription(),
        "--scheduling_class",        dwj.getSchedulingInfo().getSchedulingClass(),
        "--process_memory_size",     dwj.getSchedulingInfo().getMemorySizeRequested(),
    };
    
    // Write the state file with the user's umask AFTER successfully restoring the JED AP from the DB
    if (!experiment.writeStateFile(dwj.getStandardInfo().getUmask())) {
      return false;
    }
    
    WsLog.info(cName, mName, "Submitting: " + Arrays.toString(submitCmd));
    
    // Submit the AP as the user
    String sysout = DuccAsUser.execute(experiment.getUser(), null, submitCmd);
    WsLog.info(cName, mName, sysout);
    
    // Should report: "Managed Reservation ### submitted."
    // If successful save the ID of the JED AP
    // If it fails return false and the restarting status will be reset by the caller
    boolean launched = sysout.startsWith("Managed Reservation");
    if (launched) {
      String[] toks = sysout.split("\\s+");
      if (toks.length >= 3) {
        long duccId = Long.parseLong(toks[2]);
        experiment.setJedId(duccId);
      }
    }
    
    return launched;
  }

}
