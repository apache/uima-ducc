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

import java.util.ArrayList;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.uima.ducc.common.NodeIdentity;
import org.apache.uima.ducc.common.config.CommonConfiguration;
import org.apache.uima.ducc.common.internationalization.Messages;
import org.apache.uima.ducc.common.jd.JdConstants;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.cli.ReservationRequestProperties;
import org.apache.uima.ducc.transport.event.cli.ReservationSpecificationProperties;
import org.apache.uima.ducc.transport.event.common.DuccWorkMap;
import org.apache.uima.ducc.transport.event.common.DuccWorkReservation;
import org.apache.uima.ducc.transport.event.common.IDuccReservation;
import org.apache.uima.ducc.transport.event.common.IDuccReservationMap;
import org.apache.uima.ducc.transport.event.common.IDuccState.ReservationState;


public class JobDriverHostManager {
	
	private static final DuccLogger logger = DuccLoggerComponents.getOrLogger(JobDriverHostManager.class.getName());
	
	private static OrchestratorCommonArea orchestratorCommonArea = OrchestratorCommonArea.getInstance();
	private static Messages messages = orchestratorCommonArea.getSystemMessages();
	private static CommonConfiguration commonConfiguration = orchestratorCommonArea.getCommonConfiguration();
	
	private static JobDriverHostManager hostManager = new JobDriverHostManager();
	
	public static JobDriverHostManager getInstance() {
		return hostManager;
	}
	
	private String jdHostClass = "job-driver";
	private String jdHostDescription = "Job Driver";
	private String jdHostMemorySize = "8GB";
	private String jdHostUser = JdConstants.reserveUser;
	private String jdHostNumberOfMachines = "1";
	
	private ArrayList<String> keyList = new ArrayList<String>();
	private TreeMap<String,NodeIdentity> nodeMap = new TreeMap<String,NodeIdentity>();
	
	private AtomicBoolean assigned = new AtomicBoolean(false);
	
	private DuccWorkReservation duccWorkReservation = null;
	
	public JobDriverHostManager() {
	}
	
	private void updateAssigned() {
		if(keyList.isEmpty()) {
			assigned.set(false);
		}
		else {
			assigned.set(true);
		}
	}
	
	public void addNode(NodeIdentity node) {
		synchronized(nodeMap) {
			if(node != null) {
				String key = node.getName();
				nodeMap.put(key, node);
				keyList.add(key);
			}
			updateAssigned();
		}
	}
	
	public void delNode(NodeIdentity node) {
		synchronized(nodeMap) {
			if(node != null) {
				String key = node.getName();
				nodeMap.remove(key);
				keyList.remove(key);
			}
			updateAssigned();
		}
	}
	
	public int nodes() {
		if(!assigned.get()) {
			tryAssignment();
		}
		return nodeMap.size();
	}
	
	public NodeIdentity getNode() {
		NodeIdentity retVal = null;
		if(!assigned.get()) {
			tryAssignment();
		}
		synchronized(nodeMap) {
			if(!nodeMap.isEmpty()) {
				String key = keyList.remove(0);
				keyList.add(key);
				retVal = nodeMap.get(key);
			}
		}
		return retVal;
	}
	
	private void tryAssignment() {
		String methodName = "tryAssignment";
		synchronized(nodeMap) {
			if(duccWorkReservation != null) {
				if(duccWorkReservation.isDispatchable()) {
					if(!duccWorkReservation.getReservationMap().isEmpty()) {
						IDuccReservationMap map = duccWorkReservation.getReservationMap();
						if(!map.isEmpty()) {
							keyList = new ArrayList<String>();
							nodeMap = new TreeMap<String,NodeIdentity>();
							for (DuccId key : map.keySet()) { 
								IDuccReservation value = duccWorkReservation.getReservationMap().get(key);
								NodeIdentity node = value.getNodeIdentity();
								addNode(node);
								logger.info(methodName, null, messages.fetchLabel("assigned")+node.getName()+" "+node.getIp());
							}
						}
					}
				}
			}
		}
		return;
	}
	
	private void setConfiguration() {
		String methodName="setConfiguration";
		CommonConfiguration common = JobDriverHostManager.commonConfiguration;
		if(common.jdHostClass != null) {
			jdHostClass = common.jdHostClass;
			logger.debug(methodName, null, messages.fetchLabel("jd.host.class")+jdHostClass);
		}
		else {
			logger.debug(methodName, null, messages.fetchLabel("jd.host.class")+jdHostClass+" "+messages.fetch("(default)"));
		}
		if(common.jdHostDescription != null) {
			jdHostDescription = common.jdHostDescription;
			logger.debug(methodName, null, messages.fetchLabel("jd.host.description")+jdHostDescription);
		}
		else {
			logger.debug(methodName, null, messages.fetchLabel("jd.host.description")+jdHostDescription+" "+messages.fetch("(default)"));
		}
		if(common.jdHostMemorySize != null) {
			jdHostMemorySize = common.jdHostMemorySize;
			logger.debug(methodName, null, messages.fetchLabel("jd.host.memory.size")+jdHostMemorySize);
		}
		else {
			logger.debug(methodName, null, messages.fetchLabel("jd.host.memory.size")+jdHostMemorySize+" "+messages.fetch("(default)"));
		}
		if(common.jdHostNumberOfMachines != null) {
			jdHostNumberOfMachines = common.jdHostNumberOfMachines;
			logger.debug(methodName, null, messages.fetchLabel("jd.host.number.of.machines")+jdHostNumberOfMachines);
		}
		else {
			logger.debug(methodName, null, messages.fetchLabel("jd.host.number.of.machines")+jdHostNumberOfMachines+" "+messages.fetch("(default)"));
		}
		if(common.jdHostUser != null) {
			jdHostUser = common.jdHostUser;
			logger.debug(methodName, null, messages.fetchLabel("jd.host.user")+jdHostUser);
		}
		else {
			logger.debug(methodName, null, messages.fetchLabel("jd.host.user")+jdHostUser+" "+messages.fetch("(default)"));
		}
	}
	
	private boolean processJdHostClass() {
		String methodName = "processJdHostClass";
		logger.trace(methodName, null, messages.fetch("enter"));
		boolean retVal = false;
		if(commonConfiguration.jdHostClass != null) {
			setConfiguration();
			ReservationRequestProperties reservationRequestProperties = new ReservationRequestProperties();
			reservationRequestProperties.put(ReservationSpecificationProperties.key_scheduling_class, jdHostClass);
			reservationRequestProperties.put(ReservationSpecificationProperties.key_description, jdHostDescription);
			reservationRequestProperties.put(ReservationSpecificationProperties.key_instance_memory_size, jdHostMemorySize);
			reservationRequestProperties.put(ReservationSpecificationProperties.key_number_of_instances, jdHostNumberOfMachines);
			reservationRequestProperties.put(ReservationSpecificationProperties.key_user, jdHostUser);
			duccWorkReservation = ReservationFactory.getInstance().create(commonConfiguration, reservationRequestProperties);
			DuccWorkMap workMap = orchestratorCommonArea.getWorkMap();
			workMap.addDuccWork(duccWorkReservation);
			// state: Received
			duccWorkReservation.stateChange(ReservationState.Received);
			OrchestratorCheckpoint.getInstance().saveState();
			// state: WaitingForResources
			duccWorkReservation.stateChange(ReservationState.WaitingForResources);
			OrchestratorCheckpoint.getInstance().saveState();
			retVal = true;
		}
		logger.trace(methodName, null, messages.fetch("exit"));
		return retVal;
	}
	
	public void init() {
		String methodName = "init";
		logger.trace(methodName, null, messages.fetch("enter"));
		processJdHostClass();
		logger.trace(methodName, null, messages.fetch("exit"));
		return ;
	}
	
	public void conditional() {
		String methodName = "conditional";
		logger.trace(methodName, null, messages.fetch("enter"));
		DuccWorkMap workMap = orchestratorCommonArea.getWorkMap();
		if(workMap.size() == 0) {
			logger.info(methodName, null, messages.fetch("make allocation for JD"));
			init();
		}
		else {
			logger.info(methodName, null, messages.fetch("bypass allocation for JD"));
		}
		logger.trace(methodName, null, messages.fetch("exit"));
		return ;
	}
}
