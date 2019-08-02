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

import java.util.Iterator;
import java.util.TimerTask;

import org.apache.uima.ducc.agent.launcher.ManagedProcess;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.transport.event.common.IDuccProcess.ReasonForStoppingProcess;

public class ProcessReaperTask extends TimerTask {

  private NodeAgent agent;

  private DuccLogger logger;

  public ProcessReaperTask(NodeAgent agent, DuccLogger logger) {
    this.agent = agent;
    this.logger = logger;
  }

  public void run() {
    if (agent.deployedProcesses.size() > 0) {
      logger.warn("ProcessReaperTask.run()", null,
              "Agent timed out waiting for a Ping Message. Assuming network connectivity problem and killing all running JPs");
      Iterator<ManagedProcess> it = agent.deployedProcesses.iterator();
      while (it.hasNext()) {
        ManagedProcess mp = it.next();
        mp.kill();
        mp.getDuccProcess().setReasonForStoppingProcess(
                ReasonForStoppingProcess.AgentTimedOutWaitingForORState.toString());
        String pid = mp.getDuccProcess().getPID();
        agent.stopProcess(mp.getDuccProcess());
        logger.info("ProcessReaperTask.run()", null, "Agent calling stopProcess:" + pid);
      }
    }
  }

}
