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

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.transport.cmdline.ICommandLine;
import org.apache.uima.ducc.transport.cmdline.NonJavaCommandLine;

public class SigKillCommand implements ICommand {
  private static final String SIGKILL = "-9";

  private String[] sigKillCmdLine;

  private final DuccLogger logger;

  private final ManagedProcess deployProcess;

  public SigKillCommand(ManagedProcess managedProcess, DuccLogger logger) {
    ICommandLine cmdL = new NonJavaCommandLine("/bin/kill");
    this.logger = logger;
    this.deployProcess = managedProcess;
    cmdL.addArgument(SIGKILL);
    cmdL.addArgument(managedProcess.getPid());
    sigKillCmdLine = CommandBuilder.deployableStopCommand(cmdL, managedProcess);
  }

  @Override
  public Integer call() {
    String methodName = "SigKillCommand.call";
    ProcessBuilder processBuilder = new ProcessBuilder(sigKillCmdLine);
    try {
      StringBuilder sb = new StringBuilder("--->Killing Process ");
      sb.append(" Using command line:");
      int inx = 0;
      for (String cmdPart : sigKillCmdLine) {
        sb.append("\n\t[").append(inx++).append("]").append(cmdPart);
        // Not sure why place-holders are replaced just for this msg?
      }
      logger.info(methodName, deployProcess.getDuccId(), sb.toString());

      java.lang.Process process = processBuilder.start();
      // Drain process streams in dedicated threads.

      deployProcess.drainProcessStreams(process, logger, System.out, true);
      // wait for the kill command to complete
      return process.waitFor();

    } catch (Exception e) {
      // Log this
      logger.warn(methodName, null, e);
    }
    return -1;
  }

}
