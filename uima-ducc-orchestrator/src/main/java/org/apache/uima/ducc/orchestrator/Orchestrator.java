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
package org.apache.uima.ducc.orchestrator;

import org.apache.uima.ducc.transport.event.CancelJobDuccEvent;
import org.apache.uima.ducc.transport.event.CancelReservationDuccEvent;
import org.apache.uima.ducc.transport.event.CancelServiceDuccEvent;
import org.apache.uima.ducc.transport.event.DuccWorkRequestEvent;
import org.apache.uima.ducc.transport.event.JdRequestEvent;
import org.apache.uima.ducc.transport.event.NodeInventoryUpdateDuccEvent;
import org.apache.uima.ducc.transport.event.OrchestratorStateDuccEvent;
import org.apache.uima.ducc.transport.event.RmStateDuccEvent;
import org.apache.uima.ducc.transport.event.SmStateDuccEvent;
import org.apache.uima.ducc.transport.event.SubmitJobDuccEvent;
import org.apache.uima.ducc.transport.event.SubmitReservationDuccEvent;
import org.apache.uima.ducc.transport.event.SubmitServiceDuccEvent;

public interface Orchestrator {
	public void reconcileDwState(DuccWorkRequestEvent duccEvent);
	public void reconcileRmState(RmStateDuccEvent duccEvent);
	public void reconcileSmState(SmStateDuccEvent duccEvent);
	public void reconcileJdState(JdRequestEvent duccEvent);
	public void reconcileNodeInventory(NodeInventoryUpdateDuccEvent duccEvent);
	public OrchestratorStateDuccEvent getState();
	public void startJob(SubmitJobDuccEvent duccEvent);
	public void stopJob(CancelJobDuccEvent duccEvent);
	public void stopJobProcess(CancelJobDuccEvent duccEvent);
	public void startReservation(SubmitReservationDuccEvent duccEvent);
	public void stopReservation(CancelReservationDuccEvent duccEvent);
	public void startService(SubmitServiceDuccEvent duccEvent);
	public void stopService(CancelServiceDuccEvent duccEvent);
}
