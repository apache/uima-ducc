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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.camel.builder.RouteBuilder;
import org.apache.uima.ducc.agent.NodeAgent;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.Utils;
import org.apache.uima.ducc.transport.cmdline.ICommandLine;
import org.apache.uima.ducc.transport.event.common.IDuccProcessType.ProcessType;
import org.apache.uima.ducc.transport.event.common.IDuccStandardInfo;
import org.apache.uima.ducc.transport.event.common.IProcessState.ProcessState;

public abstract class CommandExecutor implements Callable<Process> {
	protected Process managedProcess = null;
	protected String host;
	protected String ip;
	protected ICommandLine cmdLine;
	protected NodeAgent agent;

	public abstract void stop() throws Exception;

	public abstract Process exec(ICommandLine commandLine,
			Map<String, String> processEnv) throws Exception;

	public CommandExecutor(NodeAgent agent, ICommandLine cmdLine, String host,
			String ip, Process managedProcess) throws Exception {
		this.agent = agent;
		this.host = host;
		this.ip = ip;
		this.managedProcess = managedProcess;
		this.cmdLine = cmdLine;
	}

	/**
	 * Called after process is launched. Assign PID, state and finally drain
	 * process streams.
	 * 
	 * @param process
	 *            - launched process
	 */
	protected void postExecStep(java.lang.Process process, DuccLogger logger,
			boolean isKillCmd) {
		if (!isKillCmd) {
			int pid = Utils.getPID(process);
			if (pid != -1) {
				((ManagedProcess) managedProcess).setPid(String.valueOf(pid));
				if (!((ManagedProcess) managedProcess).getDuccProcess()
						.getProcessType()
						.equals(ProcessType.Job_Uima_AS_Process)) {
					// For non uima as processes assume the process is Running
					// after launch
					if (!((ManagedProcess) managedProcess).getDuccProcess()
							.getProcessState().equals(ProcessState.Stopped)) {
						((ManagedProcess) managedProcess).getDuccProcess()
								.setProcessState(ProcessState.Running);
					}
					
          try {
        	  synchronized(this) {
        		  // wait for 5 seconds before starting the camel route
        		  // responsible for collecting process related stats. Allow
        		  // enough time for the process to start.
        		  wait(5000); 
        		  
        	  }
            RouteBuilder rb = agent.new ProcessMemoryUsageRoute(agent, 
                    ((ManagedProcess) managedProcess).getDuccProcess(),
                    (ManagedProcess) managedProcess);
            agent.getContext().addRoutes(rb);
          
          } catch( Exception e) {
            logger.error("postExecStep", null, e);
          }
				}
			}
		}

		// Drain process streams in dedicated threads.
		((ManagedProcess) managedProcess).drainProcessStreams(process, logger,
				System.out, isKillCmd);
	}

	/**
	 * Called by Executor to exec a process.
	 */
	public Process call() throws Exception {
		Process deployedProcess = null;
		try {
			// ICommandLine commandLine = ((ManagedProcess)
			// managedProcess).getCommandLine();
			Map<String, String> env = new HashMap<String, String>();
			if (!isKillCommand(cmdLine)) {
				// Enrich environment for the new process. Via these settings
				// the UIMA AS
				// service wrapper can notify the agent of its state.
				env.put("IP", ip);
				env.put("NodeName", host);
				// Add process unique ducc id to correlate process state updates
				env.put("ProcessDuccId", ((ManagedProcess) managedProcess)
						.getDuccId().getUnique());
				if (((ManagedProcess) managedProcess).getDuccProcess()
						.getProcessType()
						.equals(ProcessType.Job_Uima_AS_Process)) {
					IDuccStandardInfo processInfo = ((ManagedProcess) managedProcess)
							.getProcessInfo();
					long maxInitTime = 0;

					if (processInfo != null) {
						maxInitTime = processInfo
								.getProcessInitializationTimeMax();
					}
					agent.logger.info("CommandExecutor.call",
							((ManagedProcess) managedProcess).getWorkDuccId(),
							"Starting Process Initialization Monitor with Max Process Initialization Time:" + maxInitTime);
					((ManagedProcess) managedProcess)
							.startInitializationTimer(maxInitTime); 
				}
			}
			deployedProcess = exec(cmdLine, env);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (((ManagedProcess) managedProcess).getDuccProcess()
					.getProcessType().equals(ProcessType.Job_Uima_AS_Process)) {
				((ManagedProcess) managedProcess).stopInitializationTimer();
			}
		}
		return deployedProcess;
	}

	protected boolean isKillCommand(ICommandLine cmdLine) {
		return (cmdLine.getExecutable() != null && (cmdLine.getExecutable()
				.startsWith("/bin/kill") || cmdLine.getExecutable().startsWith(
				"taskkill")));
	}
}
