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
package org.apache.uima.ducc.jd.client;

import org.apache.uima.aae.client.UimaASProcessStatus;
import org.apache.uima.aae.client.UimaAsBaseCallbackListener;
import org.apache.uima.cas.CAS;
import org.apache.uima.collection.EntityProcessStatus;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.jd.IJobDriver;


public class WorkItemListener extends UimaAsBaseCallbackListener {
	
	private static DuccLogger duccOut = DuccLoggerComponents.getJdOut(WorkItemListener.class.getName());
	//private static Messages duccMsg = JobDriverContext.getInstance().getSystemMessages();
	
	private IJobDriver jobDriver;
	
	// <for testing only!!!>
	boolean injectLost = false;
	// </for testing only!!!>
	
	public WorkItemListener(IJobDriver jobDriver) {
		super();
		this.jobDriver = jobDriver;
	}
	
	@Override
	public void onBeforeMessageSend(UimaASProcessStatus status) {
		String methodName = "onBeforeMessageSend";
		try {
			onBeforeMessageSendHandler(status);
		}
		catch(Exception e) {
			duccOut.error(methodName, null, e);
		}
	}
	
	private void onBeforeMessageSendHandler(UimaASProcessStatus status) {
		String methodName = "onBeforeMessageSendHandler";
		String casId = null;
		ThreadLocation threadLocation = null;
		try {
			casId = ""+status.getCAS().hashCode();
			// <for testing only!!!>
			if(injectLost) {
				WorkItem wi = jobDriver.getWorkItem(casId);
				wi.getCallbackState().statePendingAssigned();
				duccOut.warn(methodName, null, "seqNo:"+wi.getSeqNo()+" "+wi.getCallbackState().getState());
				int seqNo = wi.getSeqNo();
				if(seqNo <= 1) {
					duccOut.warn(methodName, null, "callback #1 discarded seqNo:"+seqNo+" "+"casId:"+casId);
					return;
				}
			}
			// </for testing only!!!>
			jobDriver.queued(jobDriver.getWorkItem(casId));
			threadLocation = jobDriver.getCasDispatchMap().get(casId);
			DuccId jobid = jobDriver.getJob().getDuccId();
			duccOut.debug(methodName, jobid, "action:send "+threadLocation.getInfo());
			jobDriver.getDriverStatusReportLive().workItemQueued(casId,jobid);
			jobDriver.getWorkItemStateManager().queued(threadLocation.getSeqNo());
			duccOut.debug(methodName, null, "seqNo:"+threadLocation.getSeqNo()+" "+"casId:"+casId);
		}
		catch(Exception e) {
			duccOut.error(methodName, null, "seqNo:"+threadLocation.getSeqNo()+" "+"casId:"+casId, e);
		}
	}
	
	@Override
	public void onBeforeProcessCAS(UimaASProcessStatus status, String nodeIP, String pid) {
		String methodName = "onBeforeProcessCAS";
		try {
			onBeforeProcessCASHandler(status, nodeIP, pid);
		}
		catch(Exception e) {
			duccOut.error(methodName, null, e);
		}
	}
	
	private void onBeforeProcessCASHandler(UimaASProcessStatus status, String nodeIP, String pid) {
		String methodName = "onBeforeProcessCASHandler";
		String casId = null;
		ThreadLocation threadLocation = null;
		try {
			casId = ""+status.getCAS().hashCode();
			WorkItem wi = jobDriver.getWorkItem(casId);
			// <for testing only!!!>
			if(injectLost) {
				wi.getCallbackState().statePendingAssigned();
				duccOut.warn(methodName, null, "seqNo:"+wi.getSeqNo()+" "+wi.getCallbackState().getState());
				int seqNo = wi.getSeqNo();
				if(seqNo <= 3) {
					duccOut.warn(methodName, null, "callback #2 discarded seqNo:"+seqNo+" "+"casId:"+casId);
					return;
				}
			}
			// </for testing only!!!>
			wi.getCallbackState().stateNotPending();
			duccOut.debug(methodName, null, "seqNo:"+wi.getSeqNo()+" "+wi.getCallbackState().getState());
			String PID = pid.split(":")[0];
			jobDriver.dequeued(jobDriver.getWorkItem(casId), nodeIP, PID);
			threadLocation = jobDriver.getCasDispatchMap().get(casId);
			threadLocation.setNodeId(nodeIP);
			threadLocation.setProcessId(pid);
			duccOut.debug(methodName, jobDriver.getJob().getDuccId(), "action:process "+threadLocation.getInfo());
			jobDriver.assignLocation(jobDriver, casId, nodeIP, PID);
			jobDriver.getDriverStatusReportLive().workItemOperatingStart(casId, nodeIP, PID);
			duccOut.debug(methodName, null, "seqNo:"+threadLocation.getSeqNo()+" "+"casId:"+casId+" "+"node:"+nodeIP+" "+"PID:"+pid);
			jobDriver.getCasDispatchMap().update(casId, nodeIP, pid);
			jobDriver.getDriverStatusReportLive().workItemPendingProcessAssignmentRemove(casId);
			jobDriver.getWorkItemStateManager().operating(threadLocation.getSeqNo());
			jobDriver.getWorkItemStateManager().location(threadLocation.getSeqNo(),nodeIP, PID);
		}
		catch(Exception e) {
			String seqNo = null;
			if(threadLocation != null) {
				seqNo = threadLocation.getSeqNo();
			}
			duccOut.error(methodName, null, "seqNo:"+seqNo+" "+"casId:"+casId, e);
		}
	}
	
	@Override
	public void initializationComplete(EntityProcessStatus aStatus) {
		String methodName = "initializationComplete";
		duccOut.debug(methodName, null, "status!");
	}

	@Override
	public void entityProcessComplete(CAS aCas, EntityProcessStatus aStatus) {
		String methodName = "entityProcessComplete";
		duccOut.debug(methodName, null, "status!");
	}

	@Override
	public void collectionProcessComplete(EntityProcessStatus aStatus) {
		String methodName = "collectionProcessComplete";
		duccOut.debug(methodName, null, "status!");
	}
}
