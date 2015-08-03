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
package org.apache.uima.ducc.agent.processors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.camel.Exchange;
import org.apache.uima.ducc.agent.NodeAgent;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.agent.IUimaPipelineAEComponent;
import org.apache.uima.ducc.transport.event.NodeInventoryUpdateDuccEvent;
import org.apache.uima.ducc.transport.event.common.IDuccProcess;
import org.apache.uima.ducc.transport.event.common.IProcessState.ProcessState;

/**
 * 
 * 
 */
public class DefaultNodeInventoryProcessor implements NodeInventoryProcessor {
	DuccLogger logger = new DuccLogger(this.getClass(), "AGENT");
	boolean inventoryChanged = true;
	private NodeAgent agent;
	private HashMap<DuccId, IDuccProcess> previousInventory;
	private int forceInventoryUpdateMaxThreshold = 0;
	private long counter = 0;

	public DefaultNodeInventoryProcessor(NodeAgent agent,
			String inventoryPublishRateSkipCount) {
		this.agent = agent;
		try {
			forceInventoryUpdateMaxThreshold = Integer
					.parseInt(inventoryPublishRateSkipCount);
		} catch (Exception e) {
		}
		// Dont allow 0
		if (forceInventoryUpdateMaxThreshold == 0) {
			forceInventoryUpdateMaxThreshold = 1;
		}
	}

	/**
	 * Get a copy of agent {@code Process} inventory
	 */
	public HashMap<DuccId, IDuccProcess> getInventory() {
		return agent.getInventoryCopy();
	}

	/**
	 * 
	 */
	public void process(Exchange outgoingMessage) throws Exception {
		String methodName = "process";
		// Get a deep copy of agent's inventory
		HashMap<DuccId, IDuccProcess> inventory = getInventory();
		// Determine if the inventory changed since the last publishing was done
		// First check if the inventory expanded or shrunk. If the same in size,
		// compare process states and PID. If either of the two changed for any
		// of the processes trigger immediate publish. If no changes found,
		// publish
		// according to skip counter
		// (ducc.agent.node.inventory.publish.rate.skip)
		// configured in ducc.properties.
		if (previousInventory != null) {
			if (inventory.size() != previousInventory.size()) {
				inventoryChanged = true;
			} else {
				// Inventory maps are equal in size, check if all processes in
				// the current
				// inventory exist in the previous inventory snapshot. If not,
				// it means that
				// that perhaps a new process was added and one was removed. In
				// this case,
				// force the publish, since there was a change.
				for (Map.Entry<DuccId, IDuccProcess> currentProcess : inventory
						.entrySet()) {
					// Check if a process in the current inventory exists in a
					// previous
					// inventory snapshot
					if (previousInventory.containsKey(currentProcess.getKey())) {
						IDuccProcess previousProcess = previousInventory
								.get(currentProcess.getKey());
						// check if either PID or process state has changed
						if (currentProcess.getValue().getPID() != null
								&& previousProcess.getPID() == null) {
							inventoryChanged = true;
							break;
						} else if (!currentProcess.getValue().getProcessState()
								.equals(previousProcess.getProcessState())) {
							inventoryChanged = true;
							break;
						} else {
							List<IUimaPipelineAEComponent> breakdown = currentProcess
									.getValue().getUimaPipelineComponents();
							if (breakdown != null && breakdown.size() > 0) {
								List<IUimaPipelineAEComponent> previousBreakdown = previousProcess
										.getUimaPipelineComponents();
								if (previousBreakdown == null
										|| previousBreakdown.size() == 0
										|| breakdown.size() != previousBreakdown
												.size()) {
									inventoryChanged = true;
								} else {
									for (IUimaPipelineAEComponent uimaAeState : breakdown) {
										boolean found = false;
										for (IUimaPipelineAEComponent previousUimaAeState : previousBreakdown) {
											if (uimaAeState.getAeName().equals(
													previousUimaAeState
															.getAeName())) {
												found = true;
												if (!uimaAeState
														.getAeState()
														.equals(previousUimaAeState
																.getAeState())
														|| uimaAeState
																.getInitializationTime() != previousUimaAeState
																.getInitializationTime()) {
													inventoryChanged = true;
													break;
												}
											}
										}
										if (!found) {
											inventoryChanged = true;
										}

										if (inventoryChanged) {
											break;
										}

									}
								}

							}
						}
					} else {
						// New inventory contains a process not in the previous
						// snapshot
						inventoryChanged = true;
						break;
					}
				}
			}
		}

		// Get this inventory snapshot
		previousInventory = inventory;
		// Broadcast inventory if there is a change or configured number of
		// epochs
		// passed since the last broadcast. This is configured in
		// ducc.properties with
		// property ducc.agent.node.inventory.publish.rate.skip
		try {
			if (inventory.size() > 0 && (inventoryChanged || // if there is
																// inventory
																// change,
																// publish
					forceInventoryUpdateMaxThreshold == 0 || // skip rate in
																// ducc.properties
																// is zero,
																// publish
					(counter > 0 && (counter % forceInventoryUpdateMaxThreshold) == 0))) { // if
																							// reached
																							// skip
																							// rate,
																							// publish

				StringBuffer sb = new StringBuffer("Node Inventory ("
						+ inventory.size() + ")");
				for (Map.Entry<DuccId, IDuccProcess> p : inventory.entrySet()) {
					/*
					 * long endInitLong = 0; String endInit = ""; ITimeWindow
					 * wInit = p.getValue().getTimeWindowInit(); if(wInit !=
					 * null) { endInit = wInit.getEnd(); endInitLong =
					 * wInit.getEndLong(); } long startRunLong = 0; String
					 * startRun = ""; ITimeWindow wRun =
					 * p.getValue().getTimeWindowRun(); if(wRun != null) {
					 * startRun = wRun.getStart(); startRunLong =
					 * wRun.getStartLong(); } if(endInitLong > startRunLong) {
					 * logger.warn(methodName, null,
					 * "endInit:"+endInitLong+" "+"startRun:"+startRunLong); }
					 */
					if (p.getValue().getUimaPipelineComponents() == null) {
						p.getValue().setUimaPipelineComponents(
								new ArrayList<IUimaPipelineAEComponent>());
					}
					if ( p.getValue().getProcessState().equals(ProcessState.Running)) {
						p.getValue().getUimaPipelineComponents().clear();
					}
					int pipelineInitStats = (p.getValue()
							.getUimaPipelineComponents() == null) ? 0 : p
							.getValue().getUimaPipelineComponents().size();

					sb.append("\n\t[Process Type=")
							.append(p.getValue().getProcessType())
							.append(" DUCC ID=")
							.append(p.getValue().getDuccId())
							.append(" PID=")
							.append(p.getValue().getPID())
							.append(" State=")
							.append(p.getValue().getProcessState())
							.append(" Resident Memory=")
							.append(p.getValue().getResidentMemory())
							.append(" Init Stats List Size:"
									+ pipelineInitStats).
							// append(" end init:"+endInit).
							// append(" start run:"+startRun).
							append("] ");
					if (p.getValue().getProcessState()
							.equals(ProcessState.Stopped)
							|| p.getValue().getProcessState()
									.equals(ProcessState.Failed)
							|| p.getValue().getProcessState()
									.equals(ProcessState.Killed)) {
						sb.append(" Reason:"
								+ p.getValue().getReasonForStoppingProcess());
					}
					sb.append(" Exit Code=" + p.getValue().getProcessExitCode());
				}
				logger.info(methodName, null, "Agent "
						+ agent.getIdentity().getName() + " Posting Inventory:"
						+ sb.toString());
				outgoingMessage.getIn().setBody(new NodeInventoryUpdateDuccEvent(inventory));

			} else {
				// Add null to the body of the message. A filter
				// defined in the Camel route (AgentConfiguration.java)
				// has a predicate to check for null body and throws
				// away such a message.
				outgoingMessage.getIn().setBody(null);
			}
		} catch (Exception e) {
			logger.error(methodName, null, e);
		} finally {
			if (inventoryChanged) {
				counter = 0;
			} else {
				counter++;
			}
			inventoryChanged = false;
		}
	}

}
