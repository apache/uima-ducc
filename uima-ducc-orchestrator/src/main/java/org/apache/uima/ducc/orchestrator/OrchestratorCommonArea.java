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

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.SerializationUtils;
import org.apache.uima.ducc.common.IDuccEnv;
import org.apache.uima.ducc.common.internationalization.Messages;
import org.apache.uima.ducc.common.utils.ComponentHelper;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccPropertiesResolver;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.common.utils.id.IDuccIdFactory;
import org.apache.uima.ducc.orchestrator.ckpt.OrchestratorCheckpoint;
import org.apache.uima.ducc.orchestrator.jd.scheduler.JdScheduler;
import org.apache.uima.ducc.orchestrator.state.DuccWorkIdFactory;
import org.apache.uima.ducc.orchestrator.utilities.Checkpointable;
import org.apache.uima.ducc.orchestrator.utilities.TrackSync;
import org.apache.uima.ducc.transport.event.common.DuccWorkMap;
import org.apache.uima.ducc.transport.event.common.history.HistoryFactory;
import org.apache.uima.ducc.transport.event.common.history.IHistoryPersistenceManager;


public class OrchestratorCommonArea {

	private static OrchestratorCommonArea orchestratorCommonArea = null;
	
	private static final DuccLogger logger = DuccLogger.getLogger(OrchestratorCommonArea.class);
	private static final DuccId jobid = null;
	
	private static DuccPropertiesResolver dpr = DuccPropertiesResolver.getInstance();
	
	public static OrchestratorCommonArea getInstance() {
		synchronized(OrchestratorCommonArea.class) {
			if(orchestratorCommonArea == null) {
				orchestratorCommonArea = new OrchestratorCommonArea();
				orchestratorCommonArea.init();
			}
		}
		return orchestratorCommonArea;
	}
	
	private OrchestratorCommonArea() {
	}
	
	private IHistoryPersistenceManager historyPersistenceManager = null;
	
	public void restart() {
		String location = "restart";
		init();
		logger.debug(location, jobid,"jobs:"+workMap.getJobCount());
		logger.debug(location, jobid,"reservations:"+workMap.getReservationCount());
		logger.debug(location, jobid,"services:"+workMap.getServiceCount());
		Set<DuccId> serviceKeys = workMap.getServiceKeySet();
		if(serviceKeys != null) {
			for(DuccId duccId : serviceKeys) {
				logger.info(location, duccId, "");
			}
		}
	}
	
	private void init() {
		// <Jira 3414>
        String methodName="init";
		Boolean use_lock_file = new Boolean(dpr.getProperty(DuccPropertiesResolver.ducc_orchestrator_use_lock_file));
		if(use_lock_file) {
			ComponentHelper.oneInstance(IDuccEnv.DUCC_STATE_DIR,"orchestrator");
		}
		// </Jira 3414>
		
		setDuccIdFactory(new DuccWorkIdFactory());
		workMap = new DuccWorkMap();
		processAccounting = new ProcessAccounting();
		OrchestratorCheckpoint.getInstance().restoreState();
		OrchestratorCheckpoint.getInstance().saveState();
		jdScheduler = JdScheduler.getInstance();
        try {
            historyPersistenceManager = HistoryFactory.getInstance(this.getClass().getName());
        } catch ( Exception e ) {
            logger.error(methodName, null, "Cannot acquire the history manager", e);
            System.exit(1);       // what should we do here? exit or acquire the NullHistoryManager?
        }
        logger.info(methodName, null, "Got history manager of class", historyPersistenceManager.getClass().getName());
        //
        OrchestratorRecovery orchestratorRecovery = new OrchestratorRecovery(historyPersistenceManager);
        long historicSeqNo = orchestratorRecovery.recoverSeqNo();
        long previousSeqNo = getDuccIdFactory().setIfMax(historicSeqNo);
        if(previousSeqNo != historicSeqNo) {
        	logger.warn(methodName, jobid, "properties:"+previousSeqNo+" "+""+"historic:"+historicSeqNo);
        }
	}
	
	public String getStateDirectory() {
		return IDuccEnv.DUCC_STATE_DIR;
	}
	
	// **********
	
	private IDuccIdFactory duccIdFactory = null;
	
	private void setDuccIdFactory(IDuccIdFactory instance) {
		duccIdFactory = instance;
	}
	
	public IDuccIdFactory getDuccIdFactory() {
		return duccIdFactory;
	}
	
	// **********
	
	public Checkpointable getCheckpointable() {
		String methodName = "getCheckpointable";
		DuccWorkMap ckptWorkMap;
		ConcurrentHashMap<DuccId,DuccId> ckptProcessToJobMap;
		TrackSync ts = TrackSync.await(workMap, this.getClass(), methodName);
		synchronized(this) {
			ts.using();
			ckptWorkMap = (DuccWorkMap)SerializationUtils.clone(workMap);
			ckptProcessToJobMap = ProcessToJobMap.getInstance().getMap();
		}
		ts.ended();
		return new Checkpointable(ckptWorkMap,ckptProcessToJobMap);
	}
	
	public void setCheckpointable(Checkpointable checkpointable) {
		String methodName = "setCheckpointable";
		TrackSync ts = TrackSync.await(workMap, this.getClass(), methodName);
		synchronized(this) {
			ts.using();
			workMap = checkpointable.getWorkMap();
			ProcessToJobMap.getInstance().putMap(checkpointable.getProcessToJobMap());
		}
		ts.ended();
	}
	
	// **********
	
	private ProcessAccounting processAccounting = null;
	
	public ProcessAccounting getProcessAccounting() {
		return processAccounting;
	}
	
	// **********
	
	private DuccWorkMap workMap = null;
	
	public DuccWorkMap getWorkMap() {
		return workMap;
	}
	
	public void setWorkMap(DuccWorkMap workMap) {
		this.workMap = workMap;
	}
	
	// **********
	
	private Messages systemMessages= Messages.getInstance();
	private Messages userMessages= Messages.getInstance();
	
	public void initSystemMessages(String language, String country) {
		systemMessages = Messages.getInstance(language,country);
	}
	
	public void initUserMessages(String language, String country) {
		userMessages = Messages.getInstance(language,country);
	}

	public Messages getSystemMessages() {
		return systemMessages;
	}
	
	public Messages getUserMessages() {
		return userMessages;
	}
	
	// **********
	
	private JdScheduler jdScheduler = null;
	
	public JdScheduler getJdScheduler() {
		return jdScheduler;
	}
	
	// **********
	
	public IHistoryPersistenceManager getHistoryPersistencemanager() {
		return historyPersistenceManager;
	}
	
	// **********
	
	private boolean signatureRequired = true;
	
	public void setSignatureRequired() {
		signatureRequired = true;
	}
	
	public void resetSignatureRequired() {
		signatureRequired = false;
	}
	
	public boolean isSignatureRequired() {
		return signatureRequired;
	}
	
}
