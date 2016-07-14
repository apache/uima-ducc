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
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.camel.Body;
import org.apache.uima.ducc.agent.Agent;
import org.apache.uima.ducc.agent.NodeAgent;
import org.apache.uima.ducc.agent.ProcessLifecycleController;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.Utils;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.cmdline.ICommandLine;
import org.apache.uima.ducc.transport.dispatcher.DuccEventDispatcher;
import org.apache.uima.ducc.transport.event.DuccJobsStateEvent;
import org.apache.uima.ducc.transport.event.ProcessPurgeDuccEvent;
import org.apache.uima.ducc.transport.event.ProcessStartDuccEvent;
import org.apache.uima.ducc.transport.event.ProcessStateUpdateDuccEvent;
import org.apache.uima.ducc.transport.event.ProcessStopDuccEvent;
import org.apache.uima.ducc.transport.event.common.DuccUserReservation;
import org.apache.uima.ducc.transport.event.common.IDuccJobDeployment;
import org.apache.uima.ducc.transport.event.common.IDuccProcess;
import org.apache.uima.ducc.transport.event.common.IDuccProcessType.ProcessType;
import org.apache.uima.ducc.transport.event.delegate.DuccEventDelegateListener;
import org.springframework.beans.factory.annotation.Qualifier;


@Qualifier(value = "ael")

public class AgentEventListener implements DuccEventDelegateListener {
	DuccLogger logger = DuccLogger.getLogger(this.getClass(), Agent.COMPONENT_NAME);
	ProcessLifecycleController lifecycleController = null;
	// On startup of the Agent we may need to do cleanup of cgroups.
	// This cleanup will happen once right after processing of the first OR publication.
	private boolean cleanupPhase = true;  
	private AtomicLong lastSequence = new AtomicLong();
	
	private NodeAgent agent;
	public AgentEventListener(NodeAgent agent, ProcessLifecycleController lifecycleController) {
		this.agent = agent;
		this.lifecycleController = lifecycleController;
	}
	public AgentEventListener(NodeAgent agent) {
		this.agent = agent;
	}
	public void setDuccEventDispatcher( DuccEventDispatcher eventDispatcher ) {
		//this.eventDispatcher = eventDispatcher;
	}
	private void reportIncomingStateForThisNode(DuccJobsStateEvent duccEvent) throws Exception {
    StringBuffer sb = new StringBuffer();
    for( IDuccJobDeployment jobDeployment : duccEvent.getJobList()) {
  	      if ( isTargetNodeForProcess(jobDeployment.getJdProcess()) ) {
  	        IDuccProcess process = jobDeployment.getJdProcess();
  	        sb.append("\nJD--> JobId:"+jobDeployment.getJobId()+" ProcessId:"+process.getDuccId()+" PID:"+process.getPID()+" Status:"+process.getProcessState() + " Resource State:"+process.getResourceState()+" isDeallocated:"+process.isDeallocated());
  	      }
  	      for( IDuccProcess process : jobDeployment.getJpProcessList() ) {
  	        if ( isTargetNodeForProcess(process) ) {
  	          sb.append("\n\tJob ID:"+jobDeployment.getJobId()+" ProcessId:"+process.getDuccId()+" PID:"+process.getPID()+" Status:"+process.getProcessState() + " Resource State:"+process.getResourceState()+" isDeallocated:"+process.isDeallocated());
  	        }
  	      }
  	    }
  	    logger.info("reportIncomingStateForThisNode",null,sb.toString());
	}
	/**
	 * This method is called by Camel when PM sends DUCC state to agent's queue. It 
	 * takes responsibility of reconciling processes on this node. 
	 * 
	 * @param duccEvent - contains list of current DUCC jobs. 
	 * @throws Exception
	 */
	public void onDuccJobsStateEvent(@Body DuccJobsStateEvent duccEvent) throws Exception {
	    long sequence = duccEvent.getSequence();
	  // Recv'd ducc state update, restart process reaper task which detects missing
	  // OR state due to a network problem. 
//	  try {
//	    agent.restartProcessReaperTimer();
//	  } catch( Exception e) {
//	    logger.error("onDuccJobsStateEvent", null, "", e);
//	  }
	  
		try {

		  synchronized( this ) {
			  // check for out of band messages. Expecting a message with a sequence number
			  // larger than the previous message.
			    if ( sequence > lastSequence.get() ) {
			    	lastSequence.set(sequence);
			    	logger.info("reportIncomingStateForThisNode", null, "Received OR Sequence:"+sequence+" Thread ID:"+Thread.currentThread().getId());
			    } else {
			    	// Out of band message. Expected message with sequence larger than a previous message
			    	logger.warn("reportIncomingStateForThisNode",null,"Received Out of Band Message. Expected Sequence Greater Than "+lastSequence+" Received "+sequence+" Instead");
			  	    return;
			    } 

			  //  typically lifecycleController is null and the agent assumes the role. For jUnit testing though, 
			  //  a different lifecycleController is injected to facilitate black box testing
			  if ( lifecycleController == null ) {
					lifecycleController = agent;
			  }
			  //  print JP report targeted for this node
			  reportIncomingStateForThisNode(duccEvent);

			  List<DuccUserReservation> reservations = 
			           duccEvent.getUserReservations();
			  if ( cleanupPhase ) {   // true on Agent startup
				  // cleanup reservation cgroups
			  }
			   agent.setReservations(reservations);
				//	Stop any process that is in this Agent's inventory but not associated with any
				//  of the jobs we just received
				agent.takeDownProcessWithNoJob(agent,duccEvent.getJobList());
				//	iterate over all jobs and reconcile those processes that are assigned to this agent. First,
				//  look at the job's JD process and than JPs.
				for( IDuccJobDeployment jobDeployment : duccEvent.getJobList()) {
					//	check if this node is a target for this job's JD 
					if ( isTargetNodeForProcess(jobDeployment.getJdProcess()) ) {
						// agent will check the state of JD process and either start, stop, or take no action
						ICommandLine jdCommandLine = jobDeployment.getJdCmdLine();
						if(jdCommandLine != null) {
							agent.reconcileProcessStateAndTakeAction(lifecycleController, jobDeployment.getJdProcess(), jobDeployment.getJdCmdLine(), 
								jobDeployment.getStandardInfo(), jobDeployment.getProcessMemoryAssignment(), jobDeployment.getJobId());
						}
						else {
							logger.error("onDuccJobsStateEvent", null, "job is service");
						}
					} 
					// check JPs
					for( IDuccProcess process : jobDeployment.getJpProcessList() ) {
						if ( isTargetNodeForProcess(process) ) {
		          // agent will check the state of JP process and either start, stop, or take no action 
							agent.reconcileProcessStateAndTakeAction(lifecycleController, process, jobDeployment.getJpCmdLine(), 
									jobDeployment.getStandardInfo(), jobDeployment.getProcessMemoryAssignment(), jobDeployment.getJobId());
						}
					}
				}
			  
		  }
		  // 	received at least one Ducc State
		  if ( !agent.receivedDuccState ) {
			  agent.receivedDuccState = true;
		  }
		} catch( Exception e ) {
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
	private boolean isTargetNodeForProcess( IDuccProcess process ) throws Exception {
		boolean retVal = false;
		if(process != null) {
			retVal = Utils.isTargetNodeForMessage(process.getNodeIdentity().getIp(),agent.getIdentity().getIp());
		}
		return retVal;
	}
	public void onProcessStartEvent(@Body ProcessStartDuccEvent duccEvent) throws Exception {
		//	iterate given ProcessMap and start a Process if this node is a target
		for( Entry<DuccId, IDuccProcess> processEntry : duccEvent.getProcessMap().entrySet()) {
			//	check if this Process should be launched on this node. A process map
			//	may contain processes not meant to run on this node. Each Process instance
			//  in a Map has a node assignment. Only if this assignment matches this node
			//  the agent will start a process.
			if ( Utils.isTargetNodeForMessage(processEntry.getValue().getNodeIdentity().getIp(),agent.getIdentity().getIp()) ) { 
				logger.info(">>> onProcessStartEvent", null,"... Agent ["+agent.getIdentity().getIp()+"] Matches Target Node Assignment:"+processEntry.getValue().getNodeIdentity().getIp()+" For Share Id:"+  processEntry.getValue().getDuccId());
				agent.doStartProcess(processEntry.getValue(),duccEvent.getCommandLine(), duccEvent.getStandardInfo(), duccEvent.getDuccWorkId());
                if ( processEntry.getValue().getProcessType().equals(ProcessType.Pop)) {
                	break; // there should only be one JD process to launch
                } else {
    				continue;
                }
			} 
		}
	}
	public void onProcessStopEvent(@Body ProcessStopDuccEvent duccEvent) throws Exception {
		for( Entry<DuccId, IDuccProcess> processEntry : duccEvent.getProcessMap().entrySet()) {
			if ( Utils.isTargetNodeForMessage(processEntry.getValue().getNodeIdentity().getIp(), agent.getIdentity().getIp()) ) {
				logger.info(">>> onProcessStopEvent", null,"... Agent Received StopProces Event - Process Ducc Id:"+processEntry.getValue().getDuccId()+" PID:"+processEntry.getValue().getPID());
				agent.doStopProcess(processEntry.getValue());
			}
		}
	}
	public void onProcessStateUpdate(@Body ProcessStateUpdateDuccEvent duccEvent) throws Exception {
		logger.info(">>> onProcessStateUpdate", null,"... Agent Received ProcessStateUpdateDuccEvent - Process State:"+duccEvent.getState()+" Process ID:"+duccEvent.getDuccProcessId());
//		agent.updateProcessStatus(duccEvent.getDuccProcessId(), duccEvent.getPid(), duccEvent.getState());
		agent.updateProcessStatus(duccEvent);
	}
	public void onProcessPurgeEvent(@Body ProcessPurgeDuccEvent duccEvent) throws Exception {
		logger.info(">>> onProcessPurgeEvent", null,"... Agent Received ProcessPurgeDuccEvent -"+" Process ID:"+duccEvent.getProcess().getPID());
		agent.purgeProcess(duccEvent.getProcess());
	}
}
