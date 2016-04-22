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
import java.io.PrintWriter;
import java.io.StringWriter;
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
import org.apache.uima.ducc.common.IDuccUser;
import org.apache.uima.ducc.common.container.FlagsHelper;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.TimeStamp;
import org.apache.uima.ducc.common.utils.Utils;
import org.apache.uima.ducc.common.utils.id.DuccId;
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

public class DuccCommandExecutor extends CommandExecutor {
	DuccLogger logger = DuccLogger.getLogger(this.getClass(),
			NodeAgent.COMPONENT_NAME);
	@SuppressWarnings("unused")
	private static AtomicInteger nextPort = new AtomicInteger(30000);

	public DuccCommandExecutor(NodeAgent agent, ICommandLine cmdLine,
			String host, String ip, Process managedProcess) throws Exception {
		super(agent, cmdLine, host, ip, managedProcess);
	}

	public DuccCommandExecutor(ICommandLine cmdLine, String host, String ip,
			Process managedProcess) throws Exception {
		super(null, cmdLine, host, ip, managedProcess);
	}

	private boolean useDuccSpawn() {
		if (super.managedProcess.isAgentProcess() || Utils.isWindows()) {
			return false;
		}
		// On non-windows check if we should spawn the process via ducc_ling
		String useSpawn = System
				.getProperty("ducc.agent.launcher.use.ducc_spawn");
		if (useSpawn != null && useSpawn.toLowerCase().equals("true")) {
			return true;
		}
		// default
		return false;
	}

	private boolean createCGroupContainer(IDuccProcess duccProcess,
			String containerId, String owner) throws Exception {
		// create cgroups container and assign limits
		if (agent.cgroupsManager.createContainer(containerId, owner,
				useDuccSpawn())) {
			logger.info("createCGroupContainer", null,
					"Calculating CPU shares \nProcess Max Memory="
							+ duccProcess.getCGroup().getMaxMemoryLimit()
							+ "\nNode Memory Total="
							+ agent.getNodeInfo().getNodeMetrics()
									.getNodeMemory().getMemTotal());
			long cpuShares = duccProcess.getCGroup().getMaxMemoryLimit()
					/ agent.getNodeInfo().getNodeMetrics().getNodeMemory()
							.getMemTotal();
			logger.info("createCGroupContainer", null, "\nCalculated Shares="
					+ cpuShares);
			agent.cgroupsManager.setContainerCpuShares(containerId, owner,
					useDuccSpawn(), cpuShares);
			long swappiness = 10; //default
			if ( agent.configurationFactory.nodeSwappiness != null ) {
				swappiness = Long.valueOf(agent.configurationFactory.nodeSwappiness);
			}
			agent.cgroupsManager.setContainerSwappiness(containerId, owner,
					useDuccSpawn(), swappiness);

			return agent.cgroupsManager.setContainerMaxMemoryLimit(containerId,
					owner, useDuccSpawn(), duccProcess.getCGroup()
							.getMaxMemoryLimit());
		}
		return false;
	}

	private String getContainerId() {
		String containerId;
		if (((ManagedProcess) super.managedProcess).getDuccProcess()
				.getProcessType().equals(ProcessType.Service)) {
			containerId = String.valueOf(((ManagedProcess) managedProcess)
					.getDuccProcess().getCGroup().getId());
		} else {
			containerId = ((ManagedProcess) managedProcess).getWorkDuccId()
					.getFriendly()
					+ "."
					+ ((ManagedProcess) managedProcess).getDuccProcess()
							.getCGroup().getId();
		}
		return containerId;
	}

	public Process exec(ICommandLine cmdLine, Map<String, String> processEnv)
			throws Exception {
		String methodName = "exec";
		try {
			String[] cmd = getDeployableCommandLine(cmdLine, processEnv);
			if (isKillCommand(cmdLine)) {
				logger.info(methodName, null, "Killing process");
				stopProcess(cmdLine, cmd);
			} else {
				IDuccProcess duccProcess = ((ManagedProcess) managedProcess)
						.getDuccProcess();
				// If running a real agent on a node, collect swap info and
				// assign max swap usage threshold
				// for each process. In virtual mode, where there are multiple
				// agents per node, we dont
				// set nor enforce swap limits.
				if (!agent.virtualAgent) {
					// Calculate how much swap space the process is allowed to
					// use. The calculation is based on
					// the percentage of real memory the process is assigned.
					// The process is entitled the
					// same percentage of the swap.
					// Normalize node's total memory as it is expressed in KB.
					// The calculation below is based on bytes.
					double percentOfTotal = ((double) duccProcess.getCGroup()
							.getMaxMemoryLimit())
							/ (agent.getNodeInfo().getNodeMetrics()
									.getNodeMemory().getMemTotal() * 1024); // need
																			// bytes

					// substract 1Gig from total swap on this node to
					// accommodate OS needs for swapping. The
					// getSwapTotal() returns swap space in KBs so normalize
					// 1Gig
					long adjustedTotalSwapAvailable = agent.getNodeInfo()
							.getNodeMetrics().getNodeMemory().getSwapTotal() - 1048576;
					// calculate the portion (in bytes) of swap this process is
					// entitled to
					long maxProcessSwapUsage = (long) (adjustedTotalSwapAvailable * percentOfTotal) * 1024;
					// assigned how much swap this process is entitled to. If it
					// exceeds this number the Agent
					// will kill the process.
					((ManagedProcess) managedProcess)
							.setMaxSwapThreshold(maxProcessSwapUsage);
					logger.info(
							methodName,
							null,
							"---Process DuccId:"
									+ duccProcess.getDuccId()
									+ " CGroup.getMaxMemoryLimit():"
									+ ((duccProcess.getCGroup()
											.getMaxMemoryLimit() / 1024) / 1024)
									+ " MBs"
									+ " Node Memory Total:"
									+ (agent.getNodeInfo().getNodeMetrics()
											.getNodeMemory().getMemTotal() / 1024)
									+ " MBs" + " Percentage Of Real Memory:"
									+ percentOfTotal
									+ " Adjusted Total Swap Available On Node:"
									+ adjustedTotalSwapAvailable / 1024
									+ " MBs" + " Process Entitled To Max:"
									+ (maxProcessSwapUsage / 1024) / 1024
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
						logger.info(methodName, null,
								"Checking for CGroup Existance with ID:"
										+ containerId);
						if (!agent.cgroupsManager
								.cgroupExists(agent.cgroupsManager
										.getDuccCGroupBaseDir()
										+ "/"
										+ containerId)) {
							logger.info(methodName, null, "No CGroup with ID:"
									+ containerId + " Found");
							boolean failed = false;
							// create cgroup container for JDs
							try {
								if (createCGroupContainer(duccProcess,
										containerId,
										((ManagedProcess) super.managedProcess)
												.getOwner())) {
									logger.info(
											methodName,
											null,
											"Created CGroup with ID:"
													+ containerId
													+ " With Memory Limit="
													+ ((ManagedProcess) super.managedProcess)
															.getDuccProcess()
															.getCGroup()
															.getMaxMemoryLimit()
													+ " Bytes");
								} else {
									logger.info(methodName, null,
											"Failed To Create CGroup with ID:"
													+ containerId);
									duccProcess
											.setProcessState(ProcessState.Failed);
									duccProcess
											.setReasonForStoppingProcess("CGroupCreationFailed");
									failed = true;
									//agent.stop();
								}
							} catch (Exception e) {
								logger.error(methodName, null, e);
								failed = true;
								duccProcess
								    .setProcessState(ProcessState.Failed);
						        duccProcess
								    .setReasonForStoppingProcess("CGroupCreationFailed");
								//agent.stop();
							}
							if (failed) {
								logger.error(methodName, null, new RuntimeException(
										"The Agent is Unable To Create A CGroup with Container ID: "
												+ containerId
												+ ". Rejecting Deployment of Process with ID:"
												+ duccProcess.getDuccId()));
								return managedProcess;
							}
						} else {
							logger.info(methodName, null,
									"CGroup Exists with ID:" + containerId);

						}

						String[] cgroupCmd = new String[cmd.length + 3];
						cgroupCmd[0] = agent.cgroupsManager
								.getCGroupsUtilsDir() + "/cgexec";
						cgroupCmd[1] = "-g";
						cgroupCmd[2] = agent.cgroupsManager.getSubsystems()
								+ ":ducc/" + containerId;
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
		} catch (Exception e) {
			if (((ManagedProcess) super.managedProcess).getDuccProcess() != null) {
				DuccId duccId = ((ManagedProcess) super.managedProcess)
						.getDuccId();
				logger.error(methodName, duccId,
						((ManagedProcess) super.managedProcess)
								.getDuccProcess().getDuccId(), e,
						new Object[] {});
			}
			throw e;
		}
	}

	private void stopProcess(ICommandLine cmdLine, String[] cmd)
			throws Exception {
		String methodName = "stopProcess";


		Future<?> future = ((ManagedProcess) managedProcess).getFuture();
		if (future == null) {
		    throw new Exception(
					"Future Object not Found. Unable to Stop Process with PID:"
					+ ((ManagedProcess) managedProcess).getPid());
		}
		// for stop to work, PID must be provided
		if (((ManagedProcess) managedProcess).getDuccProcess().getPID() == null
		    || ((ManagedProcess) managedProcess).getDuccProcess().getPID()
		    .trim().length() == 0) {
		    throw new Exception(
					"Process Stop Command Failed. PID not provided.");
		}
		long maxTimeToWaitForProcessToStop = 60000; // default 1 minute
		if (super.agent.configurationFactory.processStopTimeout != null) {
		    maxTimeToWaitForProcessToStop = Long
			.valueOf(super.agent.configurationFactory.processStopTimeout);
		}
		try {
		    // NEW Code
		    logger.info(methodName,
				((ManagedProcess) super.managedProcess).getDuccId(),
				">>>>>>>>>>>>>>> Stopping Process:"
				+ ((ManagedProcess) managedProcess).getPid());
		    ICommandLine cmdL;
		    if (Utils.isWindows()) {
			cmdL = new NonJavaCommandLine("taskkill");
			cmdL.addArgument("/PID");
		    } else {
			cmdL = new NonJavaCommandLine("/bin/kill");
			cmdL.addArgument("-15");
		    }
		    cmdL.addArgument(((ManagedProcess) managedProcess)
				     .getDuccProcess().getPID());

		    String[] sigTermCmdLine = getDeployableCommandLine(cmdL,
								       new HashMap<String, String>());
		    doExec(new ProcessBuilder(sigTermCmdLine), sigTermCmdLine,
			   true);

		    try {
			logger.info(methodName,
				    ((ManagedProcess) super.managedProcess)
				    .getDuccId(),
				    "------------ Agent Starting Killer Timer Task For Process with PID:"
				    + ((ManagedProcess) managedProcess)
				    .getDuccProcess().getPID()
				    + " Process State: "
				    + ((ManagedProcess) managedProcess)
				    .getDuccProcess()
				    .getProcessState());
			future.get(maxTimeToWaitForProcessToStop,
				   TimeUnit.MILLISECONDS);

		    } catch(TimeoutException te) {
			    
			    logger.info(
					methodName,
					((ManagedProcess) super.managedProcess)
					.getDuccId(),
					"------------ Agent Timed-out Waiting for Process with PID:"
					+ ((ManagedProcess) managedProcess)
					.getDuccProcess().getPID()
					+ " to Stop. Process State:"
					+ ((ManagedProcess) managedProcess)
					.getDuccProcess()
					.getProcessState()
					+ " .Process did not stop in allotted time of "
					+ maxTimeToWaitForProcessToStop
					+ " millis");
			    logger.info(methodName,
					((ManagedProcess) super.managedProcess)
					.getDuccId(),
					">>>>>>>>>>>>>>> Killing Process:"
					+ ((ManagedProcess) managedProcess)
					.getDuccProcess().getPID()
					+ " .Process State:"
					+ ((ManagedProcess) managedProcess)
					.getDuccProcess()
					.getProcessState());
			    doExec(new ProcessBuilder(cmd), cmd, true);

			}

		} catch (Exception e) { // InterruptedException, ExecutionException
		    logger.error(methodName,
				 ((ManagedProcess) super.managedProcess).getDuccId(), e,
				 new Object[] {});
		}

	}

	private void startProcess(ICommandLine cmdLine, String[] cmd,
			Map<String, String> processEnv) throws Exception {
		String methodName = "startProcess";

		String millis;
		millis = TimeStamp.getCurrentMillis();

		ProcessBuilder pb = new ProcessBuilder(cmd);

		if (((ManagedProcess) super.managedProcess).getDuccProcess()
				.getProcessType().equals(ProcessType.Pop)
				|| ((ManagedProcess) super.managedProcess).getDuccProcess()
						.getProcessType().equals(ProcessType.Service)) {
			ITimeWindow twi = new TimeWindow();
			((ManagedProcess) managedProcess).getDuccProcess()
					.setTimeWindowInit(twi);
			twi.setStart(millis);
			twi.setEnd(millis);

			ITimeWindow twr = new TimeWindow();
			((ManagedProcess) managedProcess).getDuccProcess()
					.setTimeWindowRun(twr);
			twr.setStart(millis);

		}

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
				String message = "key:" + entry.getKey() + " " + "value:"
						+ entry.getValue();
				logger.trace(methodName,
						((ManagedProcess) super.managedProcess).getDuccId(),
						message);

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
	 * Checks if a given process is AP. The code checks if process type is POP
	 * and it is *not* JD
	 * 
	 * @param process
	 *            - process instance
	 * @return - true if AP, false otherwise
	 */
	private boolean isAP(ManagedProcess process) {
		if (!process.isJd()
				&& process.getDuccProcess().getProcessType()
						.equals(ProcessType.Pop)) {
			return true;
		} else {
			return false;
		}
	}

	private void doExec(ProcessBuilder pb, String[] cmd, boolean isKillCmd)
			throws Exception {
		String methodName = "doExec";
		int exitCode = 0;
		boolean failed = false;
		try {

			StringBuilder sb = new StringBuilder(
					(isKillCommand(cmdLine) ? "--->Killing Process "
							: "---> Launching Process:")
							+ " Using command line:");
			int inx = 0;
			for (String cmdPart : cmd) {
				sb.append("\n\t[")
						.append(inx++)
						.append("]")
						.append(Utils.resolvePlaceholderIfExists(cmdPart,
								System.getProperties()));
			}
			logger.info(methodName,
					((ManagedProcess) super.managedProcess).getDuccId(),
					sb.toString());

			
			java.lang.Process process = pb.start();
			// Drain process streams
			postExecStep(process, logger, isKillCmd);
			// block waiting for the process to terminate.
			exitCode = process.waitFor();
			if (!isKillCommand(cmdLine)) {
				logger.info(methodName, ((ManagedProcess) super.managedProcess)
						.getDuccId(), ">>>>>>>>>>>>> Process with PID:"
						+ ((ManagedProcess) super.managedProcess)
								.getDuccProcess().getPID()
						+ " Terminated. Exit Code:" + exitCode);
				// Process is dead, determine if the cgroup container should be
				// destroyed as well.
				if (agent.useCgroups) {
					String containerId = getContainerId();
					String userId = ((ManagedProcess) super.managedProcess)
					.getOwner();
					// before destroying the container the code checks if there
					// are processes still running in it. This could be true if
					// user code launched child processes. If there are child
					// processes still running, the code kills each one at a 
					// time and at the end the container is removed.
					agent.cgroupsManager.destroyContainer(containerId, userId, NodeAgent.SIGKILL);
					logger.info(methodName, null,
							"Removed CGroup Container with ID:" + containerId);
				}
			}

		} catch (NullPointerException ex) {
			((ManagedProcess) super.managedProcess).getDuccProcess()
					.setProcessState(ProcessState.Failed);
			StringBuffer sb = new StringBuffer();
			sb.setLength(0);
			sb.append("\n\tJava ProcessBuilder Failed to Launch Process due to NullPointerException. An Entry in the Command Array Must be Null. Look at Command Array Below:\n");
			for (String cmdPart : cmd) {
				if (cmdPart != null) {
					sb.append("\n\t").append(cmdPart);
				}
			}
			logger.info(methodName,
					((ManagedProcess) super.managedProcess).getDuccId(),
					sb.toString());
			((ManagedProcess) super.managedProcess).getDuccProcess()
					.setProcessState(ProcessState.Failed);
			throw ex;
		} catch (Exception ex) {
			((ManagedProcess) super.managedProcess).getDuccProcess()
					.setProcessState(ProcessState.LaunchFailed);

			((ManagedProcess) super.managedProcess).getDuccProcess().setProcessExitCode(-1);  // overwrite process exit code if stderr has a msg 

		        StringWriter stackTraceBuffer = new StringWriter();
			ex.printStackTrace(new PrintWriter(stackTraceBuffer));

			((ManagedProcess) managedProcess).getDuccProcess()
					    .setReasonForStoppingProcess(stackTraceBuffer.toString());
                        failed = true;
			logger.info(methodName, 
                                    ((ManagedProcess) super.managedProcess).getDuccId(),
                                    "Failed to launch Process - Reason:"+stackTraceBuffer.toString());
			
			throw ex;
		} finally {
		    if ( !failed ) {
			// associate exit code
			((ManagedProcess) managedProcess).getDuccProcess()
					.setProcessExitCode(exitCode);

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
			    if ( !((ManagedProcess) super.managedProcess).getDuccProcess()
				 .getProcessState().equals(ProcessState.LaunchFailed)) {

				((ManagedProcess) managedProcess).getDuccProcess()
						.setProcessState(ProcessState.Stopped);
			    } 


				if (((ManagedProcess) super.managedProcess).doKill()) { // killed
																		// by
																		// agent/ducc
					((ManagedProcess) managedProcess).getDuccProcess()
							.setReasonForStoppingProcess(
									ReasonForStoppingProcess.KilledByDucc
											.toString());
				} else if ( !failed ) {

				    ((ManagedProcess) managedProcess).getDuccProcess()
							.setReasonForStoppingProcess(
									ReasonForStoppingProcess.Other.toString());
				}

			} else if ( !isKillCommand(cmdLine) &&
                                        !((ManagedProcess) managedProcess).getDuccProcess()
					.getProcessState()
					.equals(ProcessState.InitializationTimeout)
					&& !((ManagedProcess) managedProcess).getDuccProcess()
							.getProcessState()
							.equals(ProcessState.FailedInitialization)
					&& !((ManagedProcess) managedProcess).getDuccProcess()
							.getProcessState().equals(ProcessState.Failed)
					&& !((ManagedProcess) managedProcess).getDuccProcess()
							.getProcessState().equals(ProcessState.LaunchFailed)
					&& !((ManagedProcess) managedProcess).getDuccProcess()
							.getProcessState().equals(ProcessState.Killed)) {
				((ManagedProcess) managedProcess).getDuccProcess()
						.setProcessState(ProcessState.Stopped);
			}
		}

	}

	private String[] getDeployableCommandLine(ICommandLine cmdLine,
			Map<String, String> processEnv) throws Exception {
		// String methodName = "getDeployableCommandLine";
		String[] cmd = new String[0];

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
					System.getProperty("ducc.agent.launcher.ducc_spawn_path"),
					System.getProperties());

			// if the command line is kill, don't provide any logging info to
			// the ducc_ling. Otherwise,
			// ducc_ling creates and empty log for each time we are killing a
			// process
			if (isKillCommand(cmdLine)) {
				// Duccling, with no logging, always run by ducc, no need for
				// workingdir
				String[] duccling_nolog = new String[] { c_launcher_path, "-u",
						((ManagedProcess) super.managedProcess).getOwner(),
						"--" };
				if (useDuccSpawn()) {
					cmd = Utils.concatAllArrays(duccling_nolog,
							new String[] { cmdLine.getExecutable() },
							cmdLine.getCommandLine());
				} else {
					cmd = Utils.concatAllArrays(
							new String[] { cmdLine.getExecutable() },
							cmdLine.getCommandLine());
				}
			} else {
				String processType = "-UIMA-";
				switch (((ManagedProcess) super.managedProcess)
						.getDuccProcess().getProcessType()) {
				case Pop:
					// Both JD and POP arbitrary process are POPs. Assume this
					// is an arbitrary process
					processType = "-POP-";
					if (cmdLine instanceof JavaCommandLine) {
						List<String> options = ((JavaCommandLine) cmdLine)
								.getOptions();
						for (String option : options) {
							// Both services and JD have processType=POP.
							// However, only the JD
							// will have -Dducc.deploy.components option set.
							if (option.startsWith("-Dducc.deploy.components=")) {
								processType = "-JD-";
								((ManagedProcess) super.managedProcess)
										.setIsJD(); // mark this process as JD
								break;
							}
						}
					}
					break;
				case Service:
					// processType = "-AP-";
					break;
				case Job_Uima_AS_Process:
					processType = "-UIMA-";
					List<String> options = ((JavaCommandLine) cmdLine)
							.getOptions();
					boolean isDucc20JpProcess = false;
					boolean isDucc20ServiceProcess = false;
					// determine if we are launching Ducc2.0 or Ducc1.+ JP
					for (String option : options) {
						if (option.indexOf(FlagsHelper.Name.JpType.pname()) > -1) {
							isDucc20JpProcess = true;
						}
						if (option.indexOf("ducc.deploy.components=service") > -1) {
							isDucc20ServiceProcess = true;
						}
					}
					// Add main class and component type to the command line
					if (isDucc20JpProcess) {
						if (!isDucc20ServiceProcess) {
							((JavaCommandLine) cmdLine)
									.addOption("-Dducc.deploy.components=job-process");
						}

						((JavaCommandLine) cmdLine)
								.setClassName("org.apache.uima.ducc.user.common.main.DuccJobService");
					} else {
						((JavaCommandLine) cmdLine)
								.addOption("-Dducc.deploy.components=uima-as");
						((JavaCommandLine) cmdLine)
								.setClassName("org.apache.uima.ducc.common.main.DuccService");
					}
					break;
				}
				// if (
				// ((ManagedProcess)super.managedProcess).getDuccProcess().getProcessType().equals(ProcessType.Pop))
				// {
				// processType = "-JD-";
				// }
				String processLogDir = ((ManagedProcess) super.managedProcess)
						.getProcessInfo().getLogDirectory()
						+ (((ManagedProcess) super.managedProcess)
								.getProcessInfo().getLogDirectory()
								.endsWith(File.separator) ? "" : File.separator)
						+ ((ManagedProcess) super.managedProcess)
								.getWorkDuccId() + File.separator;
				String processLogFile = ((ManagedProcess) super.managedProcess)
						.getWorkDuccId() + processType + host;
				String workingDir = ((ManagedProcess) super.managedProcess)
						.getProcessInfo().getWorkingDirectory();
				if (workingDir == null) {
					workingDir = "NONE";
				}

				// Duccling, with logging
				String[] duccling = new String[] { c_launcher_path, "-f",
						processLogDir + processLogFile, "-w", workingDir, "-u",
						((ManagedProcess) super.managedProcess).getOwner(),
						"--" };

				String executable = cmdLine.getExecutable();
				// Check if user specified which java to use to launch the
				// process. If not provided,
				// use the same java that the agent is running with
				if (executable == null || executable.trim().length() == 0) {
					executable = System.getProperty("java.home")
							+ File.separator + "bin" + File.separator + "java";
				}
				List<String> operationalProperties = new ArrayList<String>();

				if (cmdLine instanceof JavaCommandLine) {
					String duccHomePath = Utils.findDuccHome();
					operationalProperties.add("-DDUCC_HOME=" + duccHomePath);
					operationalProperties.add("-Dducc.deploy.configuration="
							+ System.getProperty("ducc.deploy.configuration"));
					if (System
							.getProperties()
							.containsKey(
									"ducc.agent.managed.process.state.update.endpoint.type")) {
						String type = System
								.getProperty("ducc.agent.managed.process.state.update.endpoint.type");
						if (type != null && type.equalsIgnoreCase("socket")) {
							operationalProperties
									.add("-D"
											+ NodeAgent.ProcessStateUpdatePort
											+ "="
											+ System.getProperty(NodeAgent.ProcessStateUpdatePort));
						}
					}
					// NOTE - These are redundant since the information is also
					// in the environment for both Java and non-Java processes
					operationalProperties.add("-Dducc.process.log.dir="
							+ processLogDir);
					operationalProperties.add("-Dducc.process.log.basename="
							+ processLogFile); // ((ManagedProcess)super.managedProcess).getWorkDuccId()+
												// processType+host);
					operationalProperties.add("-Dducc.job.id="
							+ ((ManagedProcess) super.managedProcess)
									.getWorkDuccId());

				}
				String[] operationalPropertiesArray = new String[operationalProperties
						.size()];

				if (useDuccSpawn()) {
					cmd = Utils.concatAllArrays(duccling,
							new String[] { executable }, operationalProperties
									.toArray(operationalPropertiesArray),
							cmdLine.getCommandLine());
				} else {
					cmd = Utils.concatAllArrays(new String[] { executable },
							operationalProperties
									.toArray(operationalPropertiesArray),
							cmdLine.getCommandLine());
				}
				// add JobId and the log prefix to the env so additional
				// similarly-named log files can be created
				processEnv.put(IDuccUser.EnvironmentVariable.DUCC_ID_JOB.value(), String
						.valueOf(((ManagedProcess) super.managedProcess)
								.getWorkDuccId().getFriendly()));
				processEnv.put(IDuccUser.EnvironmentVariable.DUCC_LOG_PREFIX.value(), processLogDir
						+ processLogFile);
			}
			return cmd;
		} catch (Exception ex) {
			((ManagedProcess) super.managedProcess).getDuccProcess()
					.setProcessState(ProcessState.Failed);
			throw ex;
		} finally {
			NodeAgent.unlock();
		}

	}

	public void stop() {
	}
}
