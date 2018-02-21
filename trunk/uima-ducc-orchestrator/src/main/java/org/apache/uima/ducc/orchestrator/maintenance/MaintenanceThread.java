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
package org.apache.uima.ducc.orchestrator.maintenance;

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccPropertiesResolver;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.orchestrator.OrchestratorCommonArea;
import org.apache.uima.ducc.orchestrator.StateManager;
import org.apache.uima.ducc.transport.event.common.DuccWorkMap;


public class MaintenanceThread extends Thread {
	
	private static final DuccLogger logger = DuccLogger.getLogger(MaintenanceThread.class);
	
	private static MaintenanceThread instance = new MaintenanceThread();
	
	public static MaintenanceThread getInstance() {
		return instance;
	}
	
	private static DuccId jobid = null;
	
	private OrchestratorCommonArea orchestratorCommonArea = OrchestratorCommonArea.getInstance();
	private DuccWorkMap workMap = orchestratorCommonArea.getWorkMap();
	
	private StateManager stateManager = StateManager.getInstance();
	private HealthMonitor healthMonitor = HealthMonitor.getInstance();
	
	private long minMillis = 1000;
	private long wakeUpMillis = 2*60*1000;
	
	private long sleepTime = wakeUpMillis;
	private long lastTime = System.currentTimeMillis();;
	
	private boolean die = false;
	
	private MaintenanceThread() {
		initialize();
	}
	
	private void initialize() {
		String location = "initialize";
		String maintenance_rate = DuccPropertiesResolver.getInstance().getProperty(DuccPropertiesResolver.ducc_orchestrator_maintenance_rate);
		if(maintenance_rate != null) {
			try {
				long rate = Long.parseLong(maintenance_rate);
				if(rate < minMillis) {
					logger.error(location, jobid, DuccPropertiesResolver.ducc_orchestrator_maintenance_rate+" < minimum of "+minMillis);
				}
				else {
					wakeUpMillis = rate;
					sleepTime = wakeUpMillis;
				}
			}
			catch(Throwable t) {
				logger.error(location, jobid, t);
			}
		}
		logger.info(location, jobid, "rate:"+wakeUpMillis);
	}
	
	private boolean isTime() {
		boolean retVal = true;
		long currTime = System.currentTimeMillis();
		long diffTime = currTime - lastTime;
		if(diffTime < wakeUpMillis) {
			retVal = false;
			sleepTime = diffTime;
		}
		else {
			lastTime = currTime;
			sleepTime = wakeUpMillis;
		}
		return retVal;
	}
	
	public void run() {
		String location = "run";
		logger.trace(location, jobid, "enter");
		while(!die) {
			try {
				if(isTime()) {
					stateManager.prune(workMap);
					healthMonitor.ajudicate();
				}
			}
			catch(Throwable t) {
				logger.error(location, jobid, t);
			}
			try {
				Thread.sleep(sleepTime);
			}
			catch(Throwable t) {
				logger.error(location, jobid, t);
			}
		}
		logger.trace(location, jobid, "exit");
	}
}
