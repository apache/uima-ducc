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
package org.apache.uima.ducc.agent.event;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.camel.Body;
import org.apache.uima.ducc.agent.Agent;
import org.apache.uima.ducc.agent.NodeAgent;
import org.apache.uima.ducc.agent.ProcessLifecycleController;
import org.apache.uima.ducc.agent.deploy.DuccWorkHelper;
import org.apache.uima.ducc.common.head.IDuccHead.DuccHeadState;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.Utils;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.agent.ProcessStateUpdate;
import org.apache.uima.ducc.transport.cmdline.ICommandLine;
import org.apache.uima.ducc.transport.dispatcher.DuccEventDispatcher;
import org.apache.uima.ducc.transport.event.DuccJobsStateEvent;
import org.apache.uima.ducc.transport.event.ProcessPurgeDuccEvent;
import org.apache.uima.ducc.transport.event.ProcessStartDuccEvent;
import org.apache.uima.ducc.transport.event.ProcessStateUpdateDuccEvent;
import org.apache.uima.ducc.transport.event.ProcessStopDuccEvent;
import org.apache.uima.ducc.transport.event.common.DuccUserReservation;
import org.apache.uima.ducc.transport.event.common.DuccWorkPopDriver;
import org.apache.uima.ducc.transport.event.common.IDuccJobDeployment;
import org.apache.uima.ducc.transport.event.common.IDuccProcess;
import org.apache.uima.ducc.transport.event.common.IDuccProcessType.ProcessType;
import org.apache.uima.ducc.transport.event.common.IDuccTypes.DuccType;
import org.apache.uima.ducc.transport.event.common.IDuccWork;
import org.apache.uima.ducc.transport.event.common.IDuccWorkExecutable;
import org.apache.uima.ducc.transport.event.common.IDuccWorkJob;
import org.apache.uima.ducc.transport.event.common.IProcessState.ProcessState;
import org.apache.uima.ducc.transport.event.delegate.DuccEventDelegateListener;
import org.springframework.beans.factory.annotation.Qualifier;

@Qualifier(value = "ael")

public class AgentEventListener implements DuccEventDelegateListener {
  DuccLogger logger = DuccLogger.getLogger(this.getClass(), Agent.COMPONENT_NAME);

  DuccId jobid = null;

  ProcessLifecycleController lifecycleController = null;

  // On startup of the Agent we may need to do cleanup of cgroups.
  // This cleanup will happen once right after processing of the first OR publication.
  // private boolean cleanupPhase = true;
  private AtomicLong lastSequence = new AtomicLong();

  private volatile boolean forceInventoryUpdateDueToSequence = false;

  private NodeAgent agent;

  private static DuccWorkHelper dwHelper = null;

  public AgentEventListener(NodeAgent agent, ProcessLifecycleController lifecycleController) {
    this.agent = agent;
    this.lifecycleController = lifecycleController;
    dwHelper = new DuccWorkHelper();
  }

  public AgentEventListener(NodeAgent agent) {
    this(agent, null);
    // this.agent = agent;
  }

  public void setDuccEventDispatcher(DuccEventDispatcher eventDispatcher) {
    // this.eventDispatcher = eventDispatcher;
  }

  private void reportIncomingStateForThisNode(DuccJobsStateEvent duccEvent) throws Exception {
    StringBuffer sb = new StringBuffer();
    for (IDuccJobDeployment jobDeployment : duccEvent.getJobList()) {
      if (isTargetNodeForProcess(jobDeployment.getJdProcess())) {
        IDuccProcess process = jobDeployment.getJdProcess();
        sb.append("\nJD--> JobId:" + jobDeployment.getJobId() + " ProcessId:" + process.getDuccId()
                + " PID:" + process.getPID() + " Status:" + process.getProcessState()
                + " Resource State:" + process.getResourceState() + " isDeallocated:"
                + process.isDeallocated());
      }
      for (IDuccProcess process : jobDeployment.getJpProcessList()) {
        if (isTargetNodeForProcess(process)) {
          sb.append("\n\tJob ID:" + jobDeployment.getJobId() + " ProcessId:" + process.getDuccId()
                  + " PID:" + process.getPID() + " Status:" + process.getProcessState()
                  + " Resource State:" + process.getResourceState() + " isDeallocated:"
                  + process.isDeallocated());
        }
      }
    }
    if (sb.length() > 0) {
      logger.info("reportIncomingStateForThisNode", null, sb.toString());
    }

  }

  public boolean forceInvotoryUpdate() {
    return forceInventoryUpdateDueToSequence;
  }

  public void resetForceInventoryUpdateFlag() {
    forceInventoryUpdateDueToSequence = false;
  }

  private void handleJobOrPOPState(IDuccJobDeployment jobDeployment, IDuccWork dw,
          IDuccProcess process) throws Exception {
    logger.info("onDuccJobsStateEvent", jobDeployment.getJobId(),
            "JP>>>>>>>>>>>> -PID:" + process.getPID());

    IDuccWorkExecutable dwe = (IDuccWorkExecutable) dw;
    boolean preemptable = true;

    try {
      // this may fail if scheduling class is wrong
      preemptable = dw.getPreemptableStatus();
    } catch (Exception e) {
      // tag the process as failed so the reconciler does not
      // try to launch the process. Instead agent will publish
      // this process as Failed
      process.setProcessState(ProcessState.Failed);
      logger.info("onDuccJobsStateEvent", jobDeployment.getJobId(), ">>>>>>>>>>>> "
              + " Unable to Determine Scheduling Class for Ducc Process: " + process.getDuccId());
    }

    // agent will check the state of JP
    // process and either start, stop,
    // or take no action
    agent.reconcileProcessStateAndTakeAction(lifecycleController, process, dwe.getCommandLine(),
            jobDeployment.getStandardInfo(), jobDeployment.getProcessMemoryAssignment(),
            jobDeployment.getJobId(), preemptable);

  }

  private void handleServiceState(IDuccJobDeployment jobDeployment, IDuccWork dw,
          IDuccProcess process) throws Exception {
    logger.info("onDuccJobsStateEvent", jobDeployment.getJobId(), "SERVICE>>>>>>>>>>>> -PID:"
            + process.getPID() + " Preemptable:" + dw.getPreemptableStatus());

    IDuccWorkJob service = (IDuccWorkJob) dw;
    ICommandLine processCmdLine = service.getCommandLine();
    processCmdLine.addOption("-Dducc.deploy.components=service");
    agent.reconcileProcessStateAndTakeAction(lifecycleController, process, processCmdLine,
            jobDeployment.getStandardInfo(), jobDeployment.getProcessMemoryAssignment(),
            jobDeployment.getJobId(), dw.getPreemptableStatus());

  }

  private void handleJobDriverState(IDuccJobDeployment jobDeployment,
          Map<DuccId, IDuccProcess> processes) throws Exception {
    IDuccWork dw = null;
    boolean callReconcile = false;
    boolean preemptable = false;
    ICommandLine cmdLine = null;
    // if driver not running, launch it
    if (!processes.containsKey(jobDeployment.getJdProcess().getDuccId())) {
      dw = dwHelper.fetch(jobDeployment.getJobId());
      if (dw == null) {
        logger.info("onDuccJobsStateEvent", jobDeployment.getJobId(),
                "!!!!! The OR did not provide commndline spec and other details required to launch processes for the Job. Received value of NULL from the OR");
      } else {
        IDuccWorkJob job = (IDuccWorkJob) dw;
        DuccWorkPopDriver driver = job.getDriver();
        if (driver != null) {
          callReconcile = true;
          cmdLine = driver.getCommandLine();
        }
        preemptable = dw.getPreemptableStatus();
      }
    } else {
      // driver running already. Still call reconcileProcessStateAndTakeAction()
      // in case we need to stop it if deallocate=true
      callReconcile = true;
    }
    if (callReconcile) {
      agent.reconcileProcessStateAndTakeAction(lifecycleController, jobDeployment.getJdProcess(),
              cmdLine, jobDeployment.getStandardInfo(), jobDeployment.getProcessMemoryAssignment(),
              jobDeployment.getJobId(), preemptable);

    }
  }

  private void handleAPJobServiceProcesses(IDuccJobDeployment jobDeployment,
          Map<DuccId, IDuccProcess> processes) throws Exception {
    // Service, AP, JP processes will be checked below
    if (jobDeployment.getJpProcessList() != null) {
      IDuccWork dw = null;
      for (IDuccProcess process : jobDeployment.getJpProcessList()) {
        // check if the process is targeted for this node
        if (isTargetNodeForProcess(process)) {
          // check if this is a new process we need to launch
          // on this node. The 'processes' is a Map of child
          // processes already running on this node.
          if (!processes.containsKey(process.getDuccId())) {
            // call the OR to fetch task details
            if ((dw = dwHelper.fetch(jobDeployment.getJobId())) == null) {
              logger.info("onDuccJobsStateEvent", jobDeployment.getJobId(),
                      "!!!!! The OR did not provide commndline spec and other details required to launch processes for the Job. Received value of NULL from the OR");
            } else {
              logger.debug("onDuccJobsStateEvent", jobDeployment.getJobId(),
                      "........... Job Type:" + jobDeployment.getType().name());

              if (DuccType.Service.equals(jobDeployment.getType())) {
                handleServiceState(jobDeployment, dw, process);
              } else if (dw instanceof IDuccWorkExecutable) {
                handleJobOrPOPState(jobDeployment, dw, process);
              } else {
                logger.error("onDuccJobsStateEvent", jobDeployment.getJobId(),
                        "Unexpected process type -PID:" + process.getPID());
              }
            }
          } else {
            // process already running, check state and stop it if deallocate=true
            agent.reconcileProcessStateAndTakeAction(lifecycleController, process, null,
                    jobDeployment.getStandardInfo(), jobDeployment.getProcessMemoryAssignment(),
                    jobDeployment.getJobId(), false);
          }
        }
      } // for
    }
  }

  /**
   * This method is called by Camel when PM sends DUCC state to agent's queue. It takes
   * responsibility of reconciling processes on this node.
   * 
   * @param duccEvent
   *          - contains list of current DUCC jobs.
   * @throws Exception
   */
  public void onDuccJobsStateEvent(@Body DuccJobsStateEvent duccEvent) throws Exception {
    String location = "onDuccJobsStateEvent";
    long sequence = duccEvent.getSequence();

    try {

      synchronized (this) {

        String host = duccEvent.getProducerHost();
        DuccHeadState dhs = duccEvent.getDuccHeadState();
        int jobs = 0;
        List<IDuccJobDeployment> listJobs = duccEvent.getJobList();
        if (listJobs != null) {
          jobs = listJobs.size();
        }
        int reservations = 0;
        List<DuccUserReservation> listReservations = duccEvent.getUserReservations();
        if (listReservations != null) {
          reservations = listReservations.size();
        }
        long tid = Thread.currentThread().getId();
        String message = "sequence=" + sequence + " " + "type=" + dhs + " " + "producer=" + host
                + " " + "jobs=" + jobs + " " + "reservations=" + reservations + " " + "tid=" + tid;
        switch (dhs) {
          case master:
          case unspecified:
            // Issue info and process master type publication
            logger.info(location, null, "accept=Yes" + " " + message);
            break;
          default:
            // Issue warning and ignore non-master type publication
            logger.warn(location, null, "accept=No" + " " + message);
            return;
        }

        // check for out of band messages. Expecting a message with a
        // sequence number larger than the previous message.
        if (sequence > lastSequence.get()) {
          lastSequence.set(sequence);
          logger.debug("reportIncomingStateForThisNode", null, "Received OR Sequence:" + sequence
                  + " Thread ID:" + Thread.currentThread().getId());
        } else {
          // Out of band message. Expected message with sequence
          // larger than a previous message
          logger.warn("reportIncomingStateForThisNode", null,
                  "Received Out of Band Message. Expected Sequence Greater Than " + lastSequence
                          + " Received " + sequence + " Instead");
          forceInventoryUpdateDueToSequence = true;
          return;
        }

        // typically lifecycleController is null and the agent assumes
        // the role. For jUnit testing though,
        // a different lifecycleController is injected to facilitate
        // black box testing
        if (lifecycleController == null) {
          lifecycleController = agent;
        }
        // print JP report targeted for this node
        reportIncomingStateForThisNode(duccEvent);

        agent.setReservations(duccEvent.getUserReservations());
        // Stop any process that is in this Agent's inventory but not
        // associated with any
        // of the jobs we just received
        agent.takeDownProcessWithNoJobV2(agent, duccEvent.getJobList());
        Map<DuccId, IDuccProcess> processes = agent.getInventoryCopy();
        // iterate over all jobs and reconcile those processes that are
        // assigned to this agent. First,
        // look at the job's JD process and than JPs.
        for (IDuccJobDeployment jobDeployment : duccEvent.getJobList()) {
          // check if this node is a target for this job's JD
          if (jobDeployment.getJdProcess() != null
                  && isTargetNodeForProcess(jobDeployment.getJdProcess())) {
            handleJobDriverState(jobDeployment, processes);
          }

          handleAPJobServiceProcesses(jobDeployment, processes);
        }
      }
      // received at least one Ducc State
      if (!agent.receivedDuccState) {
        agent.receivedDuccState = true;
      }
    } catch (Exception e) {
      logger.error("onDuccJobsStateEvent", null, e);
    }
  }

  /**
   * Wrapper method for Utils.isTargetNodeForMessage()
   * 
   * @param process
   * @return
   * @throws Exception
   */
  private boolean isTargetNodeForProcess(IDuccProcess process) throws Exception {
    boolean retVal = false;
    if (process != null) {
      retVal = Utils.isTargetNodeForMessage(process.getNodeIdentity().getIp(),
              agent.getIdentity().getIp());
    }
    return retVal;
  }

  public void onProcessStartEvent(@Body ProcessStartDuccEvent duccEvent) throws Exception {
    // iterate given ProcessMap and start a Process if this node is a target
    for (Entry<DuccId, IDuccProcess> processEntry : duccEvent.getProcessMap().entrySet()) {
      // check if this Process should be launched on this node. A process map
      // may contain processes not meant to run on this node. Each Process instance
      // in a Map has a node assignment. Only if this assignment matches this node
      // the agent will start a process.
      if (Utils.isTargetNodeForMessage(processEntry.getValue().getNodeIdentity().getIp(),
              agent.getIdentity().getIp())) {
        logger.info(">>> onProcessStartEvent", null,
                "... Agent [" + agent.getIdentity().getIp() + "] Matches Target Node Assignment:"
                        + processEntry.getValue().getNodeIdentity().getIp() + " For Share Id:"
                        + processEntry.getValue().getDuccId());
        agent.doStartProcess(processEntry.getValue(), duccEvent.getCommandLine(),
                duccEvent.getStandardInfo(), duccEvent.getDuccWorkId(), true);
        if (processEntry.getValue().getProcessType().equals(ProcessType.Pop)) {
          break; // there should only be one JD process to launch
        } else {
          continue;
        }
      }
    }
  }

  public void onProcessStopEvent(@Body ProcessStopDuccEvent duccEvent) throws Exception {
    for (Entry<DuccId, IDuccProcess> processEntry : duccEvent.getProcessMap().entrySet()) {
      if (Utils.isTargetNodeForMessage(processEntry.getValue().getNodeIdentity().getIp(),
              agent.getIdentity().getIp())) {
        logger.info(">>> onProcessStopEvent", null,
                "... Agent Received StopProces Event - Process Ducc Id:"
                        + processEntry.getValue().getDuccId() + " PID:"
                        + processEntry.getValue().getPID());
        agent.doStopProcess(processEntry.getValue());
      }
    }
  }

  public void onProcessStateUpdate(@Body ProcessStateUpdateDuccEvent duccEvent) throws Exception {
    logger.info(">>> onProcessStateUpdate", null,
            "... Agent Received ProcessStateUpdateDuccEvent - Process State:" + duccEvent.getState()
                    + " Process ID:" + duccEvent.getDuccProcessId());
    // agent.updateProcessStatus(duccEvent.getDuccProcessId(), duccEvent.getPid(),
    // duccEvent.getState());
    agent.updateProcessStatus(duccEvent);
  }

  public void onProcessStateUpdate(@Body String serviceUpdate) throws Exception {
    logger.info(">>> onProcessStateUpdate", null, "Recv'd Process Update >>> ");
    String[] stateUpdateProperties = serviceUpdate.split(",");

    String duccProcessId = null;
    String duccProcessState = null;
    String jmxUrl = null;
    ProcessState state = null;
    for (String prop : stateUpdateProperties) {
      String[] nv = prop.split("=");
      if (nv[0].equals("DUCC_PROCESS_UNIQUEID")) {
        duccProcessId = nv[1];
      } else if (nv[0].equals("DUCC_PROCESS_STATE")) {
        duccProcessState = nv[1];
        try {
          // validates value. Fails with IllegalArgumentException if
          // a value does not match a defined enum
          state = ProcessState.valueOf(nv[1]);
        } catch (IllegalArgumentException e) {
          // invalid state
        }
      } else if (nv[0].equals("SERVICE_JMX_PORT")) {
        jmxUrl = nv[1];
      }
    }
    if (state == null) {
      logger.info(">>> onProcessStateUpdate", null,
              "... Agent Received Invalid State Update event - Unsupported Process State:"
                      + duccProcessState + " Process ID:" + duccProcessId);
    } else if (duccProcessId == null) {
      logger.info(">>> onProcessStateUpdate", null,
              "... Agent Received Invalid State Update event - Process State:" + duccProcessState
                      + " Missing Process ID");
    } else {
      ProcessStateUpdate update = new ProcessStateUpdate(state, null, duccProcessId, jmxUrl);
      ProcessStateUpdateDuccEvent duccEvent = new ProcessStateUpdateDuccEvent(update);
      String jmxInfo = "";
      if (Objects.nonNull(jmxUrl)) {
        jmxInfo = " Service JMX connect url:" + jmxUrl;
      }
      logger.info(">>> onProcessStateUpdate", null,
              "... Agent Received ProcessStateUpdateDuccEvent - Process State:"
                      + duccEvent.getState() + " Process ID:" + duccEvent.getDuccProcessId()
                      + jmxInfo);
      agent.updateProcessStatus(duccEvent);
    }
  }

  public void onProcessPurgeEvent(@Body ProcessPurgeDuccEvent duccEvent) throws Exception {
    logger.info(">>> onProcessPurgeEvent", null, "... Agent Received ProcessPurgeDuccEvent -"
            + " Process ID:" + duccEvent.getProcess().getPID());
    agent.purgeProcess(duccEvent.getProcess());
  }

  public long getLastSequence() {
    return lastSequence.get();
  }
}
