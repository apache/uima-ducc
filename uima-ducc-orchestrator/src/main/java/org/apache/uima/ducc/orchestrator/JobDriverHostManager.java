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
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.uima.ducc.common.NodeIdentity;
import org.apache.uima.ducc.common.config.CommonConfiguration;
import org.apache.uima.ducc.common.internationalization.Messages;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.DuccPropertiesResolver;
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
	
	private DuccId jobid = null;
	
	private ArrayList<String> keyList = new ArrayList<String>();
	private TreeMap<String,NodeIdentity> nodeMap = new TreeMap<String,NodeIdentity>();
	
	ConcurrentLinkedQueue<DuccWorkReservation> mapJdPending = new ConcurrentLinkedQueue<DuccWorkReservation>();
	ConcurrentLinkedQueue<DuccWorkReservation> mapJdAssigned = new ConcurrentLinkedQueue<DuccWorkReservation>();
	
	public JobDriverHostManager() {
	}
	
	public void addNode(NodeIdentity node) {
		synchronized(nodeMap) {
			if(node != null) {
				String key = node.getName();
				nodeMap.put(key, node);
				keyList.add(key);
			}
		}
	}
	
	public void delNode(NodeIdentity node) {
		synchronized(nodeMap) {
			if(node != null) {
				String key = node.getName();
				nodeMap.remove(key);
				keyList.remove(key);
			}
		}
	}
	
	public int nodes() {
		if(!mapJdPending.isEmpty()) {
			tryAssignment();
		}
		return nodeMap.size();
	}
	
	public NodeIdentity getNode() {
		NodeIdentity retVal = null;
		if(!mapJdPending.isEmpty()) {
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
			for(DuccWorkReservation duccWorkReservation : mapJdPending) {
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
								mapJdPending.remove(duccWorkReservation);
								mapJdAssigned.add(duccWorkReservation);
								logger.info(methodName, null, messages.fetchLabel("assigned")+node.getName()+" "+node.getIp());
							}
						}
					}
				}
			}
		}
		return;
	}
	
	private void processJdHostClass() {
		String methodName = "processJdHostClass";
		logger.trace(methodName, null, messages.fetch("enter"));
		DuccPropertiesResolver dpr = DuccPropertiesResolver.getInstance();
		String jdHostClass = dpr.getCachedProperty(DuccPropertiesResolver.ducc_jd_host_class);
		String jdHostDescription = dpr.getCachedProperty(DuccPropertiesResolver.ducc_jd_host_description);
		String jdHostMemorySize = dpr.getCachedProperty(DuccPropertiesResolver.ducc_jd_host_memory_size);
		String jdHostumberOfMachines = dpr.getCachedProperty(DuccPropertiesResolver.ducc_jd_host_number_of_machines);
		String jdHostUser = dpr.getCachedProperty(DuccPropertiesResolver.ducc_jd_host_user);
		ReservationRequestProperties reservationRequestProperties = new ReservationRequestProperties();
		reservationRequestProperties.put(ReservationSpecificationProperties.key_scheduling_class, jdHostClass);
		reservationRequestProperties.put(ReservationSpecificationProperties.key_description, jdHostDescription);			
		reservationRequestProperties.put(ReservationSpecificationProperties.key_memory_size, jdHostMemorySize);
		reservationRequestProperties.put(ReservationSpecificationProperties.key_user, jdHostUser);
		int jd_host_number_of_machines = 1;
		try {
			jd_host_number_of_machines = Integer.parseInt(jdHostumberOfMachines.trim());
		}
		catch(Exception e) {
			logger.error(methodName, jobid, e);
		}
		for(int i=0; i < jd_host_number_of_machines; i++) {
			DuccWorkReservation duccWorkReservation = ReservationFactory.getInstance().create(commonConfiguration, reservationRequestProperties);
			DuccWorkMap workMap = orchestratorCommonArea.getWorkMap();
			WorkMapHelper.addDuccWork(workMap, duccWorkReservation, this, methodName);
			// state: Received
			duccWorkReservation.stateChange(ReservationState.Received);
			OrchestratorCheckpoint.getInstance().saveState();
			// state: WaitingForResources
			duccWorkReservation.stateChange(ReservationState.WaitingForResources);
			OrchestratorCheckpoint.getInstance().saveState();
			mapJdPending.add(duccWorkReservation);
		}
		logger.trace(methodName, null, messages.fetch("exit"));
		return;
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
