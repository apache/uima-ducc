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

import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.SerializationUtils;
import org.apache.uima.ducc.common.IDuccEnv;
import org.apache.uima.ducc.common.internationalization.Messages;
import org.apache.uima.ducc.common.persistence.IPropertiesFileManager;
import org.apache.uima.ducc.common.persistence.PropertiesFileManager;
import org.apache.uima.ducc.common.utils.ComponentHelper;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.DuccPropertiesResolver;
import org.apache.uima.ducc.common.utils.IDuccLoggerComponents;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.common.utils.id.DuccIdFactory;
import org.apache.uima.ducc.common.utils.id.IDuccIdFactory;
import org.apache.uima.ducc.orchestrator.jd.scheduler.JdScheduler;
import org.apache.uima.ducc.orchestrator.utilities.Checkpointable;
import org.apache.uima.ducc.orchestrator.utilities.TrackSync;
import org.apache.uima.ducc.transport.event.common.DuccWorkMap;
import org.apache.uima.ducc.transport.event.common.history.HistoryFactory;
import org.apache.uima.ducc.transport.event.common.history.IHistoryPersistenceManager;


public class OrchestratorCommonArea {

	private static OrchestratorCommonArea orchestratorCommonArea = null;
	
	private static final DuccLogger logger = DuccLoggerComponents.getOrLogger(OrchestratorCommonArea.class.getName());
	
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
	
	@Deprecated
	private void initSeqNo() {
		String location = "initSeqNo";
		DuccId jobid = null;
		PropertiesFileManager pfm = (PropertiesFileManager) propertiesFileManager;
		if(!pfm.containsKey(constSeqNo)) {
			int biggest = -1;
			try {
				int seqno = Integer.valueOf(pfm.get(constJobSeqNo,"-1"));
				if(seqno > biggest) {
					biggest = seqno;
				}
			}
			catch(Exception e) {
				logger.error(location, jobid, e);
			}
			try {
				int seqno = Integer.valueOf(pfm.get(constServiceSeqNo,"-1"));
				if(seqno > biggest) {
					biggest = seqno;
				}
			}
			catch(Exception e) {
				logger.error(location, jobid, e);
			}
			try {
				int seqno = Integer.valueOf(pfm.get(constReservationSeqNo,"-1"));
				if(seqno > biggest) {
					biggest = seqno;
				}
			}
			catch(Exception e) {
				logger.error(location, jobid, e);
			}
			try {
				pfm.set(constSeqNo,""+biggest);
				pfm.remove(constServiceSeqNo);
				pfm.remove(constReservationSeqNo);
				pfm.remove(constJobSeqNo);
			}
			catch(Exception e) {
				logger.error(location, jobid, e);
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
		setPropertiesFileManager(new PropertiesFileManager(IDuccLoggerComponents.abbrv_orchestrator, IDuccEnv.DUCC_STATE_DIR, constOrchestratorProperties, false, true));
		initSeqNo();
		setDuccIdFactory(new DuccIdFactory(propertiesFileManager,constSeqNo));
		workMap = new DuccWorkMap();
		processAccounting = new ProcessAccounting();
		String ckpt_setting = dpr.getCachedProperty(DuccPropertiesResolver.ducc_orchestrator_checkpoint);
		OrchestratorCheckpoint.getInstance().switchOnOff(ckpt_setting);
		OrchestratorCheckpoint.getInstance().restoreState();
		jdScheduler = JdScheduler.getInstance();
        try {
            historyPersistenceManager = HistoryFactory.getInstance(this.getClass().getName());
        } catch ( Exception e ) {
            logger.error(methodName, null, "Cannot acquire the history manager", e);
            System.exit(1);       // what should we do here? exit or acquire the NullHistoryManager?
        }
        logger.info(methodName, null, "Got history manager of class", historyPersistenceManager.getClass().getName());
	}
	
	public String getStateDirectory() {
		return IDuccEnv.DUCC_STATE_DIR;
	}
	
	private static final String constOrchestratorProperties = "orchestrator.properties";
	@Deprecated
	private static final String constJobSeqNo = "job.seqno";
	@Deprecated
	private static final String constServiceSeqNo = "service.seqno";
	@Deprecated
	private static final String constReservationSeqNo = "reservation.seqno";
	private static final String constSeqNo = "seqno";
	
	// **********
	
	private IPropertiesFileManager propertiesFileManager = null;
	
	private void setPropertiesFileManager(IPropertiesFileManager instance) {
		propertiesFileManager = instance;
	}
	
	public IPropertiesFileManager getPropertiesFileManager() {
		assert(propertiesFileManager != null);
		return propertiesFileManager;
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
