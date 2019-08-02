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
package org.apache.uima.ducc.agent.launcher;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.uima.ducc.agent.NodeAgent;
import org.apache.uima.ducc.agent.launcher.ManagedProcess.StopPriority;
import org.apache.uima.ducc.common.IDuccUser;
import org.apache.uima.ducc.common.container.FlagsHelper;
import org.apache.uima.ducc.common.exception.DuccRuntimeException;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.TimeStamp;
import org.apache.uima.ducc.common.utils.Utils;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.agent.ProcessStateUpdate;
import org.apache.uima.ducc.transport.cmdline.ACommandLine;
import org.apache.uima.ducc.transport.cmdline.ICommandLine;
import org.apache.uima.ducc.transport.cmdline.JavaCommandLine;
import org.apache.uima.ducc.transport.cmdline.NonJavaCommandLine;
import org.apache.uima.ducc.transport.event.common.IDuccProcess;
import org.apache.uima.ducc.transport.event.common.IDuccProcess.ReasonForStoppingProcess;
import org.apache.uima.ducc.transport.event.common.IDuccProcessType.ProcessType;
import org.apache.uima.ducc.transport.event.common.IProcessState.ProcessState;
import org.apache.uima.ducc.transport.event.common.ITimeWindow;
import org.apache.uima.ducc.transport.event.common.TimeWindow;
import org.apache.uima.ducc.user.common.PrivateClassLoader;

public class DuccCommandExecutor extends CommandExecutor {
  DuccLogger logger = DuccLogger.getLogger(this.getClass(), NodeAgent.COMPONENT_NAME);

  @SuppressWarnings("unused")
  private static AtomicInteger nextPort = new AtomicInteger(30000);

  public DuccCommandExecutor(NodeAgent agent, ICommandLine cmdLine, String host, String ip,
          Process managedProcess) throws Exception {
    super(agent, cmdLine, host, ip, managedProcess);
  }

  public DuccCommandExecutor(ICommandLine cmdLine, String host, String ip, Process managedProcess)
          throws Exception {
    super(null, cmdLine, host, ip, managedProcess);
  }

  private boolean useDuccSpawn() {
    if (super.managedProcess.isAgentProcess() || Utils.isWindows()) {
      return false;
    }
    // On non-windows check if we should spawn the process via ducc_ling
    String useSpawn = System.getProperty("ducc.agent.launcher.use.ducc_spawn");
    if (useSpawn != null && useSpawn.toLowerCase().equals("true")) {
      return true;
    }
    // default
    return false;
  }

  private boolean createCGroupContainer(IDuccProcess duccProcess, String containerId, String owner)
          throws Exception {
    // create cgroups container and assign limits
    if (agent.cgroupsManager.createContainer(containerId, System.getProperty("user.name"),
            agent.cgroupsManager.getUserGroupName(System.getProperty("user.name")),
            useDuccSpawn())) {
      logger.info("createCGroupContainer", null,
              "Calculating CPU shares \nProcess Max Memory="
                      + duccProcess.getCGroup().getMaxMemoryLimit() + "\nNode Memory Total="
                      + agent.getNodeInfo().getNodeMetrics().getNodeMemory().getMemTotal());
      long cpuShares = duccProcess.getCGroup().getMaxMemoryLimit()
              / agent.getNodeInfo().getNodeMetrics().getNodeMemory().getMemTotal();
      logger.info("createCGroupContainer", null, "\nCalculated Shares=" + cpuShares);
      agent.cgroupsManager.setContainerCpuShares(containerId, owner, useDuccSpawn(), cpuShares);
      long swappiness = 10; // default
      if (agent.configurationFactory.nodeSwappiness != null) {
        swappiness = Long.valueOf(agent.configurationFactory.nodeSwappiness);
      }
      agent.cgroupsManager.setContainerSwappiness(containerId, owner, useDuccSpawn(), swappiness);

      return agent.cgroupsManager.setContainerMaxMemoryLimit(containerId, owner, useDuccSpawn(),
              duccProcess.getCGroup().getMaxMemoryLimit());
    }
    return false;
  }

  private String getContainerId() {
    String containerId;
    if (((ManagedProcess) super.managedProcess).getDuccProcess().getProcessType()
            .equals(ProcessType.Service)) {
      containerId = String
              .valueOf(((ManagedProcess) managedProcess).getDuccProcess().getCGroup().getId());
    } else {
      containerId = ((ManagedProcess) managedProcess).getWorkDuccId().getFriendly() + "."
              + ((ManagedProcess) managedProcess).getDuccProcess().getCGroup().getId();
    }
    return containerId;
  }

  public Process exec(ICommandLine cmdLine, Map<String, String> processEnv) throws Exception {
    String methodName = "exec";
    try {
      String[] cmd = getDeployableCommandLine(cmdLine, processEnv);
      if (isKillCommand(cmdLine)) {
        logger.debug(methodName, null, "Killing process");
        stopProcess(cmdLine, cmd);
      } else {
        IDuccProcess duccProcess = ((ManagedProcess) managedProcess).getDuccProcess();
        // If running a real agent on a node, collect swap info and
        // assign max swap usage threshold
        // for each process. In virtual mode, where there are multiple
        // agents per node, we dont
        // set nor enforce swap limits.
        if (!agent.isVirtual()) {
          // Calculate how much swap space the process is allowed to
          // use. The calculation is based on
          // the percentage of real memory the process is assigned.
          // The process is entitled the
          // same percentage of the swap.
          // Normalize node's total memory as it is expressed in KB.
          // The calculation below is based on bytes.
          double percentOfTotal = ((double) duccProcess.getCGroup().getMaxMemoryLimit())
                  / (agent.getNodeInfo().getNodeMetrics().getNodeMemory().getMemTotal() * 1024); // need
          // bytes

          // substract 1Gig from total swap on this node to
          // accommodate OS needs for swapping. The
          // getSwapTotal() returns swap space in KBs so normalize
          // 1Gig
          long adjustedTotalSwapAvailable = agent.getNodeInfo().getNodeMetrics().getNodeMemory()
                  .getSwapTotal() - 1048576;
          // calculate the portion (in bytes) of swap this process is
          // entitled to
          long maxProcessSwapUsage = (long) (adjustedTotalSwapAvailable * percentOfTotal) * 1024;
          // assigned how much swap this process is entitled to. If it
          // exceeds this number the Agent
          // will kill the process.
          ((ManagedProcess) managedProcess).setMaxSwapThreshold(maxProcessSwapUsage);
          logger.info(methodName, null, "---Process DuccId:" + duccProcess.getDuccId()
                  + " CGroup.getMaxMemoryLimit():"
                  + ((duccProcess.getCGroup().getMaxMemoryLimit() / 1024) / 1024) + " MBs"
                  + " Node Memory Total:"
                  + (agent.getNodeInfo().getNodeMetrics().getNodeMemory().getMemTotal() / 1024)
                  + " MBs" + " Percentage Of Real Memory:" + percentOfTotal
                  + " Adjusted Total Swap Available On Node:" + adjustedTotalSwapAvailable / 1024
                  + " MBs" + " Process Entitled To Max:" + (maxProcessSwapUsage / 1024) / 1024
                  + " MBs of Swap");

          // logger.info(methodName, null,
          // "The Process With ID:"+duccProcess.getDuccId()+" is Entitled to the Max "+(
          // (maxProcessSwapUsage/1024)/1024)+" Megs of Swap Space");
          // if configured to use cgroups and the process is the
          // cgroup owner, create a cgroup
          // using Process DuccId as a name. Additional processes may
          // be injected into the
          // cgroup by declaring cgroup owner id.
          if (agent.useCgroups) {
            // JDs are of type Pop (Plain Old Process). JDs run in a
            // reservation. The cgroup container
            // is created for the reservation and we co-locate as
            // many JDs as we can fit in it.
            // String containerId = ((ManagedProcess)
            // managedProcess).getWorkDuccId()+"."+duccProcess.getCGroup().getId().getFriendly();
            String containerId = getContainerId();
            logger.info(methodName, null, "Checking for CGroup Existance with ID:" + containerId);
            if (!agent.cgroupsManager.cgroupExists(
                    agent.cgroupsManager.getDuccCGroupBaseDir() + "/" + containerId)) {
              logger.info(methodName, null, "No CGroup with ID:" + containerId + " Found");
              boolean failed = false;
              // create cgroup container for JDs
              try {
                if (createCGroupContainer(duccProcess, containerId,
                        ((ManagedProcess) super.managedProcess).getOwner())) {
                  logger.info(
                          methodName, null, "Created CGroup with ID:" + containerId
                                  + " With Memory Limit=" + ((ManagedProcess) super.managedProcess)
                                          .getDuccProcess().getCGroup().getMaxMemoryLimit()
                                  + " Bytes");
                } else {
                  logger.info(methodName, null, "Failed To Create CGroup with ID:" + containerId);
                  duccProcess.setProcessState(ProcessState.Failed);
                  duccProcess.setReasonForStoppingProcess("CGroupCreationFailed");
                  failed = true;
                  // agent.stop();
                }
              } catch (Exception e) {
                logger.error(methodName, null, e);
                failed = true;
                duccProcess.setProcessState(ProcessState.Failed);
                duccProcess.setReasonForStoppingProcess("CGroupCreationFailed");
                // agent.stop();
              }
              if (failed) {
                logger.error(methodName, null,
                        new RuntimeException(
                                "The Agent is Unable To Create A CGroup with Container ID: "
                                        + containerId + ". Rejecting Deployment of Process with ID:"
                                        + duccProcess.getDuccId()));
                return managedProcess;
              }
            } else {
              logger.info(methodName, null, "CGroup Exists with ID:" + containerId);

            }

            String[] cgroupCmd = new String[cmd.length + 3];
            cgroupCmd[0] = agent.cgroupsManager.getCGroupsUtilsDir() + "/cgexec";
            cgroupCmd[1] = "-g";
            cgroupCmd[2] = agent.cgroupsManager.getSubsystems() + containerId; // UIMA-5405
                                                                               // subsystems
                                                                               // includes the
                                                                               // "ducc" id
            int inx = 3;
            for (String cmdPart : cmd) {
              cgroupCmd[inx++] = cmdPart;
            }
            startProcess(cmdLine, cgroupCmd, processEnv);
          } else {
            // Not configured to use CGroups
            startProcess(cmdLine, cmd, processEnv);
          }
        } else {
          // dont use CGroups on virtual agents
          startProcess(cmdLine, cmd, processEnv);
        }

      }
      return managedProcess;
    } catch (Throwable e) {
      if (((ManagedProcess) super.managedProcess).getDuccProcess() != null) {
        DuccId duccId = ((ManagedProcess) super.managedProcess).getDuccId();
        logger.error(methodName, duccId,
                ((ManagedProcess) super.managedProcess).getDuccProcess().getDuccId(), e,
                new Object[] {});
      }
      logger.error(methodName, null, e);
      throw e;
    }
  }

  private boolean processInRunningOrInitializingState() {
    return (((ManagedProcess) managedProcess).getDuccProcess().getProcessState()
            .equals(ProcessState.Running)
            || ((ManagedProcess) managedProcess).getDuccProcess().getProcessState()
                    .equals(ProcessState.Initializing)
            || ((ManagedProcess) managedProcess).getDuccProcess().getProcessState()
                    .equals(ProcessState.Starting)
            || ((ManagedProcess) managedProcess).getDuccProcess().getProcessState()
                    .equals(ProcessState.Stopping)
            || ((ManagedProcess) managedProcess).getDuccProcess().getProcessState()
                    .equals(ProcessState.Started));
  }

  private void stopProcess(ICommandLine cmdLine, String[] cmd) throws Exception {
    String methodName = "stopProcess";

    if (processInRunningOrInitializingState()) {
      Future<?> future = ((ManagedProcess) managedProcess).getFuture();
      if (future == null) {
        throw new Exception("Future Object not Found. Unable to Stop Process with PID:"
                + ((ManagedProcess) managedProcess).getPid());
      }
      // for stop to work, PID must be provided
      if (((ManagedProcess) managedProcess).getDuccProcess().getPID() == null
              || ((ManagedProcess) managedProcess).getDuccProcess().getPID().trim().length() == 0) {
        throw new Exception("Process Stop Command Failed. PID not provided.");
      }
      try {
        // NEW Code
        logger.debug(methodName, ((ManagedProcess) super.managedProcess).getDuccId(),
                ">>>>>>>>>>>>>>> Stopping Process:" + ((ManagedProcess) managedProcess).getPid());
        ICommandLine cmdL;
        if (Utils.isWindows()) {
          cmdL = new NonJavaCommandLine("taskkill");
          cmdL.addArgument("/PID");
        } else {
          cmdL = new NonJavaCommandLine("/bin/kill");
          cmdL.addArgument("-15");
        }
        cmdL.addArgument(((ManagedProcess) managedProcess).getDuccProcess().getPID());

        String[] sigTermCmdLine = getDeployableCommandLine(cmdL, new HashMap<String, String>());
        doExec(new ProcessBuilder(sigTermCmdLine), sigTermCmdLine, true);
        // if agent receives admin STOP request, all managed processes should
        // be stopped without each waiting for 60 secs. The agent
        // blasts SIGTERM in parallel to all running child processes
        if (!StopPriority.DONT_WAIT.equals(((ManagedProcess) managedProcess).getStopPriority())) {
          long maxTimeToWaitForProcessToStop = 60000; // default 1 minute
          if (super.agent.configurationFactory.processStopTimeout != null) {
            maxTimeToWaitForProcessToStop = Long
                    .valueOf(super.agent.configurationFactory.processStopTimeout);
          }

          try {
            logger.info(methodName, ((ManagedProcess) super.managedProcess).getDuccId(),
                    "------------ Agent Starting Killer Timer Task For Process with PID:"
                            + ((ManagedProcess) managedProcess).getDuccProcess().getPID()
                            + " Process State: "
                            + ((ManagedProcess) managedProcess).getDuccProcess().getProcessState());
            future.get(maxTimeToWaitForProcessToStop, TimeUnit.MILLISECONDS);

          } catch (TimeoutException te) {
            if (!((ManagedProcess) managedProcess).getDuccProcess().getProcessState()
                    .equals(ProcessState.Stopped)) {
              logger.info(methodName, ((ManagedProcess) super.managedProcess).getDuccId(),
                      "------------ Agent Timed-out Waiting for Process with PID:"
                              + ((ManagedProcess) managedProcess).getDuccProcess().getPID()
                              + " to Stop. Process State:"
                              + ((ManagedProcess) managedProcess).getDuccProcess().getProcessState()
                              + " .Process did not stop in allotted time of "
                              + maxTimeToWaitForProcessToStop + " millis");
              logger.info(methodName, ((ManagedProcess) super.managedProcess).getDuccId(),
                      ">>>>>>>>>>>>>>> Killing Process:"
                              + ((ManagedProcess) managedProcess).getDuccProcess().getPID()
                              + " .Process State:" + ((ManagedProcess) managedProcess)
                                      .getDuccProcess().getProcessState());
              doExec(new ProcessBuilder(cmd), cmd, true);

            }

          }

        }
      } catch (Exception e) { // InterruptedException, ExecutionException
        logger.error(methodName, ((ManagedProcess) super.managedProcess).getDuccId(), e,
                new Object[] {});
      }

    }
  }

  private void startProcess(ICommandLine cmdLine, String[] cmd, Map<String, String> processEnv)
          throws Exception {
    String methodName = "startProcess";

    ProcessBuilder pb = new ProcessBuilder(cmd);

    Map<String, String> env = pb.environment();
    // Dont enherit agent's environment
    env.clear();
    // enrich Process environment
    env.putAll(processEnv);
    if (cmdLine instanceof ACommandLine) {
      // enrich Process environment with one from a given command line
      env.putAll(((ACommandLine) cmdLine).getEnvironment());
    }
    if (logger.isTrace()) {
      for (Entry<String, String> entry : env.entrySet()) {
        String message = "key:" + entry.getKey() + " " + "value:" + entry.getValue();
        logger.trace(methodName, ((ManagedProcess) super.managedProcess).getDuccId(), message);

      }
    }
    try {
      doExec(pb, cmd, isKillCommand(cmdLine));
    } catch (Exception e) {
      throw e;
    } finally {
      // millis = TimeStamp.getCurrentMillis();
      // twr.setEnd(millis);
    }
  }

  /**
   * Checks if a given process is AP. The code checks if process type is POP and it is *not* JD
   *
   * @param process
   *          - process instance
   * @return - true if AP, false otherwise
   */
  private boolean isAP(ManagedProcess process) {
    if (!process.isJd() && process.getDuccProcess().getProcessType().equals(ProcessType.Pop)) {
      return true;
    } else {
      return false;
    }
  }

  public void doExec(ProcessBuilder pb, String[] cmd, boolean isKillCmd) throws Exception {
    String methodName = "doExec";
    int exitCode = 0;
    boolean failed = false;
    try {
      StringBuilder sb = new StringBuilder(
              (isKillCommand(cmdLine) ? "--->Killing Process " : "---> Launching Process:")
                      + " Using command line:");
      int inx = 0;
      for (String cmdPart : cmd) {
        sb.append("\n\t[").append(inx++).append("]")
                .append(Utils.resolvePlaceholderIfExists(cmdPart, System.getProperties()));
        // Not sure why place-holders are replaced just for this msg?
      }
      logger.info(methodName, ((ManagedProcess) super.managedProcess).getDuccId(), sb.toString());

      java.lang.Process process = pb.start();
      // Drain process streams
      postExecStep(process, logger, isKillCmd);
      // block waiting for the process to terminate.
      exitCode = process.waitFor();
      if (!isKillCommand(cmdLine)) {
        logger.info(methodName, ((ManagedProcess) super.managedProcess).getDuccId(),
                ">>>>>>>>>>>>> Process with PID:"
                        + ((ManagedProcess) super.managedProcess).getDuccProcess().getPID()
                        + " Terminated. Exit Code:" + exitCode);
        // Process is dead, determine if the cgroup container should be
        // destroyed as well.
        if (agent.useCgroups) {
          String containerId = getContainerId();
          String userId = ((ManagedProcess) super.managedProcess).getOwner();
          // before destroying the container the code checks if there
          // are processes still running in it. This could be true if
          // user code launched child processes. If there are child
          // processes still running, the code kills each one at a
          // time and at the end the container is removed.
          agent.cgroupsManager.destroyContainer(containerId, userId, NodeAgent.SIGKILL);
          logger.info(methodName, null, "Removed CGroup Container with ID:" + containerId);
        }
      }

    } catch (NullPointerException ex) {
      ((ManagedProcess) super.managedProcess).getDuccProcess().setProcessState(ProcessState.Failed);
      StringBuffer sb = new StringBuffer();
      sb.setLength(0);
      sb.append(
              "\n\tJava ProcessBuilder Failed to Launch Process due to NullPointerException. An Entry in the Command Array Must be Null. Look at Command Array Below:\n");
      for (String cmdPart : cmd) {
        if (cmdPart != null) {
          sb.append("\n\t").append(cmdPart);
        }
      }
      logger.info(methodName, ((ManagedProcess) super.managedProcess).getDuccId(), sb.toString());
      ((ManagedProcess) super.managedProcess).getDuccProcess().setProcessState(ProcessState.Failed);
      throw ex;
    } catch (Exception ex) {
      ((ManagedProcess) super.managedProcess).getDuccProcess()
              .setProcessState(ProcessState.LaunchFailed);

      ((ManagedProcess) super.managedProcess).getDuccProcess().setProcessExitCode(-1); // overwrite
                                                                                       // process
                                                                                       // exit code
                                                                                       // if stderr
                                                                                       // has a msg

      StringWriter stackTraceBuffer = new StringWriter();
      ex.printStackTrace(new PrintWriter(stackTraceBuffer));

      ((ManagedProcess) managedProcess).getDuccProcess()
              .setReasonForStoppingProcess(stackTraceBuffer.toString());
      failed = true;
      logger.info(methodName, ((ManagedProcess) super.managedProcess).getDuccId(),
              "Failed to launch Process - Reason:" + stackTraceBuffer.toString());

      throw ex;
    } finally {
      if (isKillCmd) {
        // the kill command process has been launched. Nothing else to do.
        // We now wait for the process to die.
        return;
      }
      if (!failed) {
        // associate exit code
        ((ManagedProcess) managedProcess).getDuccProcess().setProcessExitCode(exitCode);

      }

      // Per team discussion on Aug 31 2011, the process is stopped by an
      // agent when initialization
      // times out or initialization failed. Both Initialization_Timeout
      // and FailedIntialization imply
      // that the process is stopped. If the process is AP and it exited
      // it should be marked
      // as Stopped. If the exit was due to Ducc kill mark reason as
      // KilledByDucc otherwise we have
      // no way of knowing why the process exited and in such case reason
      // is Other.
      if ((isAP((ManagedProcess) super.managedProcess))) {

        // if failed to execute the command line, the process state is already set to Failed
        if (!((ManagedProcess) super.managedProcess).getDuccProcess().getProcessState()
                .equals(ProcessState.LaunchFailed)) {

          ((ManagedProcess) managedProcess).getDuccProcess().setProcessState(ProcessState.Stopped);
        }

        if (((ManagedProcess) super.managedProcess).doKill()) { // killed
          // by
          // agent/ducc
          ((ManagedProcess) managedProcess).getDuccProcess()
                  .setReasonForStoppingProcess(ReasonForStoppingProcess.KilledByDucc.toString());
        } else if (!failed) {

          ((ManagedProcess) managedProcess).getDuccProcess()
                  .setReasonForStoppingProcess(ReasonForStoppingProcess.Other.toString());
        }

      } else if (!isKillCommand(cmdLine)
              && !((ManagedProcess) managedProcess).getDuccProcess().getProcessState()
                      .equals(ProcessState.InitializationTimeout)
              && !((ManagedProcess) managedProcess).getDuccProcess().getProcessState()
                      .equals(ProcessState.FailedInitialization)
              && !((ManagedProcess) managedProcess).getDuccProcess().getProcessState()
                      .equals(ProcessState.Failed)
              && !((ManagedProcess) managedProcess).getDuccProcess().getProcessState()
                      .equals(ProcessState.LaunchFailed)
              && !((ManagedProcess) managedProcess).getDuccProcess().getProcessState()
                      .equals(ProcessState.Killed)) {
        ((ManagedProcess) managedProcess).getDuccProcess().setProcessState(ProcessState.Stopped);
      }
    }

  }

  private String[] getDeployableCommandLine(ICommandLine cmdLine, Map<String, String> processEnv)
          throws Exception {
    // String methodName = "getDeployableCommandLine";
    String[] cmd = new String[0];
    boolean uimaBasedAP = false;
    try {
      // lock using Agent single permit semaphore. The
      // Utils.concatAllArrays()
      // uses native call (for efficiency) which appears not thread safe.
      NodeAgent.lock();
      // Use ducc_ling (c code) as a launcher for the actual process. The
      // ducc_ling
      // allows the process to run as a specified user in order to write
      // out logs in
      // user's space as oppose to ducc space.
      String c_launcher_path = Utils.resolvePlaceholderIfExists(
              System.getProperty("ducc.agent.launcher.ducc_spawn_path"), System.getProperties());

      // if the command line is kill, don't provide any logging info to
      // the ducc_ling. Otherwise,
      // ducc_ling creates and empty log for each time we are killing a
      // process
      if (isKillCommand(cmdLine)) {
        // Duccling, with no logging, always run by ducc, no need for
        // workingdir
        String[] duccling_nolog = new String[] { c_launcher_path, "-u",
            ((ManagedProcess) super.managedProcess).getOwner(), "--" };
        if (useDuccSpawn()) {
          cmd = Utils.concatAllArrays(duccling_nolog, new String[] { cmdLine.getExecutable() },
                  cmdLine.getCommandLine());
        } else {
          cmd = Utils.concatAllArrays(new String[] { cmdLine.getExecutable() },
                  cmdLine.getCommandLine());
        }

      } else {
        String processType = "-UIMA-";
        // If Java then may run many JPs from the same cmdLine so make a local copy that we can
        // modify
        if (cmdLine instanceof JavaCommandLine) {
          cmdLine = ((JavaCommandLine) cmdLine).copy();
        }

        logger.info("getDeployableCommandLine", ((ManagedProcess) super.managedProcess).getDuccId(),
                "Deploying Process Type:" + ((ManagedProcess) super.managedProcess).getDuccProcess()
                        .getProcessType().name());

        switch (((ManagedProcess) super.managedProcess).getDuccProcess().getProcessType()) {
          case Pop:
            // Both JD and POP arbitrary process are POPs. Assume this
            // is an arbitrary process
            processType = "-POP-";
            if (cmdLine instanceof JavaCommandLine) {
              for (String option : cmdLine.getOptions()) {

                logger.info("getDeployableCommandLine",
                        ((ManagedProcess) super.managedProcess).getDuccId(),
                        "POP CmdLine Option:" + option);

                // Both services and JD have processType=POP.
                // However, only the JD
                // will have -Dducc.deploy.components option set.
                if (option.startsWith("-Dducc.deploy.components=")) {
                  processType = "-JD-";
                  ((ManagedProcess) super.managedProcess).setIsJD(); // mark this process as JD
                  // break;
                } else if (option.startsWith("-Dducc.deploy.JpType=uima")) {
                  // this is an AP with JP-based "nature". Meaning its using
                  // a JP process configuration and its pull based model.
                  uimaBasedAP = true;
                }

              }
            }
            break;
          case Service:
            // processType = "-AP-";
            break;
          case Job_Uima_AS_Process:
            processType = "-UIMA-";
            boolean isDucc20JpProcess = false;
            boolean isDucc20ServiceProcess = false;
            // determine if we are launching Ducc2.0 or Ducc1.+ JP
            List<String> options = cmdLine.getOptions();
            for (String option : options) {
              if (option.indexOf(FlagsHelper.Name.JpType.pname()) > -1) {
                isDucc20JpProcess = true;
              }
              if (option.indexOf("ducc.deploy.components=service") > -1) {
                isDucc20ServiceProcess = true;
              }
              // break;
              if (option.trim().equals("-Dducc.deploy.JpType=uima")) {
                // this is an AP with JP-based "nature". Meaning its using
                // a JP process configuration and its pull based model.
                uimaBasedAP = true;
              } else if (option.trim().equals("-Dducc.deploy.JpType=uima-as")) {
                ((ManagedProcess) super.managedProcess).setUimaAs();
              }
            }

            // Add main class and component type to the command line
            if (isDucc20JpProcess) {
              if (!isDucc20ServiceProcess) {
                cmdLine.addOption("-Dducc.deploy.components=job-process");
              }
              // add port where an agent listens for process state update events
              processEnv.put(IDuccUser.EnvironmentVariable.DUCC_UPDATE_PORT.value(),
                      System.getProperty("AGENT_AP_STATE_UPDATE_PORT"));
              ((JavaCommandLine) cmdLine)
                      .setClassName("org.apache.uima.ducc.user.common.main.DuccJobService");
              // ((JavaCommandLine)cmdLine).setClassName("org.apache.uima.ducc.ps.service.main.ServiceWrapper");
            } else {
              ((ManagedProcess) super.managedProcess).setUimaAs();
              cmdLine.addOption("-Dducc.deploy.components=uima-as");
              processEnv.put(IDuccUser.EnvironmentVariable.DUCC_UPDATE_PORT.value(),
                      System.getProperty(ProcessStateUpdate.ProcessStateUpdatePort));
              ((JavaCommandLine) cmdLine)
                      .setClassName("org.apache.uima.ducc.common.main.DuccService");
            }
            break;
        }
        String processLogDir = ((ManagedProcess) super.managedProcess).getProcessInfo()
                .getLogDirectory()
                + (((ManagedProcess) super.managedProcess).getProcessInfo().getLogDirectory()
                        .endsWith(File.separator) ? "" : File.separator)
                + ((ManagedProcess) super.managedProcess).getWorkDuccId() + File.separator;
        String processLogFile = ((ManagedProcess) super.managedProcess).getWorkDuccId()
                + processType + host;
        String workingDir = ((ManagedProcess) super.managedProcess).getProcessInfo()
                .getWorkingDirectory();
        if (workingDir == null) {
          workingDir = "NONE";
        }

        // Duccling, with logging
        String[] duccling = new String[] { c_launcher_path, "-f", processLogDir + processLogFile,
            "-w", workingDir, "-u", ((ManagedProcess) super.managedProcess).getOwner(), "--" };

        String executable = cmdLine.getExecutable();
        // Check if user specified which java to use to launch the
        // process. If not provided,
        // use the same java that the agent is running with
        if (executable == null || executable.trim().length() == 0) {
          executable = System.getProperty("java.home") + File.separator + "bin" + File.separator
                  + "java";
        }
        boolean jd = ((ManagedProcess) super.managedProcess).isJd();

        if (cmdLine instanceof JavaCommandLine) {
          String classpath = "";
          if (jd) {
            for (String option : ((JavaCommandLine) cmdLine).getOptions()) {
              if (option.startsWith("-Dducc.deploy.UserClasspath")) {
                classpath = option.split("=")[1];
                break;
              }
            }
          } else {
            classpath = ((JavaCommandLine) cmdLine).getClasspath();
          }

          String duccHomePath = Utils.findDuccHome();
          /*
           * String[] jars = classpath.split(":"); URLClassLoader clsLoader = newClassLoader(jars);
           * Class<?> cls = clsLoader.loadClass("org.apache.uima.impl.UimaVersion"); Method
           * majorVersionMethod = cls.getMethod("getMajorVersion"); short majorVersion =
           * (short)majorVersionMethod.invoke(null); if ( !duccHomePath.trim().endsWith("/") ) {
           * duccHomePath = duccHomePath.concat("/"); } String workItemJarDir =
           * duccHomePath+"lib/uima-ducc/workitem/uima-ducc-workitem-"; if ( majorVersion < 3 ) {
           * classpath = workItemJarDir+"v2.jar:"+classpath; } else if ( majorVersion >= 3 ) {
           * classpath = workItemJarDir+"v3.jar:"+classpath; } else { throw new
           * DuccRuntimeException("Unknown version of UIMA - majorVersion="+majorVersion); }
           * 
           * if ( jd ) { // JD uses classloader separation to run user specified jars.
           * ((JavaCommandLine) cmdLine).replaceOption("-Dducc.deploy.UserClasspath", classpath); }
           * else { ((JavaCommandLine) cmdLine).setClasspath(classpath); }
           */
          cmdLine.addOption("-DDUCC_HOME=" + duccHomePath);
          cmdLine.addOption(
                  "-Dducc.deploy.configuration=" + System.getProperty("ducc.deploy.configuration"));
          // UIMA-4935 Following moved from CommandExecutor to avoid duplications in the shared
          // cmdLine
          cmdLine.addOption("-Dducc.deploy.JpUniqueId="
                  + ((ManagedProcess) managedProcess).getDuccId().getUnique());

          // NOTE - These are redundant since the information is also
          // in the environment for both Java and non-Java processes
          cmdLine.addOption("-Dducc.process.log.dir=" + processLogDir);
          cmdLine.addOption("-Dducc.process.log.basename=" + processLogFile);
          cmdLine.addOption(
                  "-Dducc.job.id=" + ((ManagedProcess) super.managedProcess).getWorkDuccId());

        }

        if (useDuccSpawn()) {
          cmd = Utils.concatAllArrays(duccling, new String[] { executable },
                  cmdLine.getCommandLine());
        } else {
          cmd = Utils.concatAllArrays(new String[] { executable }, cmdLine.getCommandLine());
        }
        // add JobId and the log prefix to the env so additional
        // similarly-named log files can be created
        // Also put the state update port in the environment for all processes ... instead of just
        // some as a system property
        processEnv.put(IDuccUser.EnvironmentVariable.DUCC_ID_JOB.value(), String
                .valueOf(((ManagedProcess) super.managedProcess).getWorkDuccId().getFriendly()));
        processEnv.put(IDuccUser.EnvironmentVariable.DUCC_LOG_PREFIX.value(),
                processLogDir + processLogFile);

        for (String part : cmd) {
          if (part.startsWith("-Dducc.deploy.JpType=uima")) {
            // this is an AP with JP-based "nature". Meaning its using
            // a JP process configuration and its pull based model.
            uimaBasedAP = true;
            break;
          }
        }

        // Currently agent has two ports where it listens for process state updates. One is
        // for JPs and the other is for APs and JDs. The latter use a simplified state update
        // protocol
        // which is String based. The JPs actually serialize a more complex state Object.
        // There is a way to deploy an AP with a JP "nature". Meaning an AP whose internal
        // components looks just like a JP. Such AP must report its state to the same
        // agent port as a JP. Only non-uima based APs will report to the other port.
        if (jd || (isAP((ManagedProcess) super.managedProcess) && !uimaBasedAP)) {
          logger.info("getDeployableCommandLine",
                  ((ManagedProcess) super.managedProcess).getDuccId(),
                  "Deploying Process Type:"
                          + ((ManagedProcess) super.managedProcess).getDuccProcess()
                                  .getProcessType().name()
                          + " This process will report state update to AP specific socket listener running on port:"
                          + System.getProperty("AGENT_AP_STATE_UPDATE_PORT"));

          ITimeWindow twi = new TimeWindow();
          ((ManagedProcess) managedProcess).getDuccProcess().setTimeWindowInit(twi);
          String millis = TimeStamp.getCurrentMillis();
          twi.setStart(millis);
          twi.setEnd(millis);

          ITimeWindow twr = new TimeWindow();
          ((ManagedProcess) managedProcess).getDuccProcess().setTimeWindowRun(twr);
          twr.setStart(millis);
          processEnv.put(IDuccUser.EnvironmentVariable.DUCC_UPDATE_PORT.value(),
                  System.getProperty("AGENT_AP_STATE_UPDATE_PORT"));
        } else {
          logger.info("getDeployableCommandLine",
                  ((ManagedProcess) super.managedProcess).getDuccId(),
                  "Deploying Process Type:"
                          + ((ManagedProcess) super.managedProcess).getDuccProcess()
                                  .getProcessType().name()
                          + " This process will report state update to Camel-Mina listener running on port:"
                          + System.getProperty(ProcessStateUpdate.ProcessStateUpdatePort));
          // processEnv.put(IDuccUser.EnvironmentVariable.DUCC_UPDATE_PORT.value(),
          // System.getProperty(ProcessStateUpdate.ProcessStateUpdatePort));
        }
      }

      // Replace the reserved DUCC variable with the architecture of this node (ppc64 or amd64 or
      // ...)
      // (could have been done in getCommandLine if that did return the full cmd line!)
      String osArch = System.getProperty("os.arch");
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < cmd.length; ++i) {
        if (cmd[i].contains("${DUCC_OS_ARCH}")) {
          cmd[i] = cmd[i].replace("${DUCC_OS_ARCH}", osArch);
        }
        sb.append(cmd[i]).append(" ");
      }
      logger.info("getDeployableCommandLine --------------- ", null, sb.toString());

      return cmd;

    } catch (Exception ex) {
      ((ManagedProcess) super.managedProcess).getDuccProcess().setProcessState(ProcessState.Failed);
      throw ex;
    } finally {
      NodeAgent.unlock();
    }

  }

  public URLClassLoader newClassLoader(String[] classPathElements) throws IOException {
    ArrayList<URL> urlList = new ArrayList<URL>(classPathElements.length);
    for (String element : classPathElements) {
      if (element.endsWith("*")) {
        File dir = new File(element.substring(0, element.length() - 1));
        File[] files = dir.listFiles(); // Will be null if missing or not a dir
        if (files != null) {
          for (File f : files) {
            if (f.getName().endsWith(".jar")) {
              urlList.add(f.getCanonicalFile().toURI().toURL());
            }
          }
        }
      } else {
        File f = new File(element);
        if (f.exists()) {
          urlList.add(f.getCanonicalFile().toURI().toURL());
        }
      }
    }
    URL[] urls = new URL[urlList.size()];
    return new URLClassLoader(urlList.toArray(urls),
            ClassLoader.getSystemClassLoader().getParent());
  }

  public void stop() {
  }
}
