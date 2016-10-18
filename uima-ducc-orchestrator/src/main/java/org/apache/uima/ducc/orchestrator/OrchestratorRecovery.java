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

import java.util.List;
import java.util.Set;

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.common.DuccWorkMap;
import org.apache.uima.ducc.transport.event.common.IDuccWorkJob;
import org.apache.uima.ducc.transport.event.common.IDuccWorkReservation;
import org.apache.uima.ducc.transport.event.common.history.IHistoryPersistenceManager;

/**
 * Purpose: scan job+reservation history+checkpoint data for the largest sequence number
 */
public class OrchestratorRecovery {
	
	private static final DuccLogger logger = DuccLoggerComponents.getOrLogger(OrchestratorRecovery.class.getName());
	private static final DuccId jobid = null;
	
	IHistoryPersistenceManager historyPersistenceManager = null;
	
	public OrchestratorRecovery(IHistoryPersistenceManager historyPersistenceManager) {
		setHistoryPersistenceManager(historyPersistenceManager);
	}
	
	private void setHistoryPersistenceManager(IHistoryPersistenceManager value) {
		historyPersistenceManager = value;
	}
	
	private Long findHistoryLastJob() {
		String location = "findHistoryLastJob";
		Long retVal = new Long(-1);
		try {
			List<IDuccWorkJob> duccWorkJobs = historyPersistenceManager.restoreJobs(1);
			if(duccWorkJobs != null) {
				if(!duccWorkJobs.isEmpty()) {
					for(IDuccWorkJob job : duccWorkJobs) {
						DuccId duccId = job.getDuccId();
						retVal = duccId.getFriendly();
						logger.info(location, jobid, "hist job "+retVal);
						break;
					}
				}
				else {
					logger.debug(location, jobid, "hist job empty");
				}
			}
			else {
				logger.debug(location, jobid, "hist job not found");
			}
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
		}
		return retVal;
	}
	
	private Long findHistoryLastReservation() {
		String location = "findHistoryLastReservation";
		Long retVal = new Long(-1);
		try {
			List<IDuccWorkReservation> duccWorkReservations = historyPersistenceManager.restoreReservations(1);
			if(duccWorkReservations != null) {
				if(!duccWorkReservations.isEmpty()) {
					for(IDuccWorkReservation reservation : duccWorkReservations) {
						DuccId duccId = reservation.getDuccId();
						retVal = duccId.getFriendly();
						logger.info(location, jobid, "hist reservation "+retVal);
						break;
					}
				}
				else {
					logger.debug(location, jobid, "hist reservation empty");
				}
			}
			else {
				logger.debug(location, jobid, "hist reservation not found");
			}
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
		}
		return retVal;
	}
	
	private Long findCheckpointLastJob() {
		String location = "findCheckpointLastJob";
		Long retVal = new Long(-1);
		DuccWorkMap workMap = OrchestratorCommonArea.getInstance().getWorkMap();
		try {
			if(workMap != null) {
				DuccId duccId = getMax(workMap.getJobKeySet());
				if(duccId != null) {
					retVal = duccId.getFriendly();
					logger.info(location, jobid, "ckpt job "+retVal);
				}
				else {
					logger.debug(location, jobid, "ckpt job not found");
				}
			}
			else {
				logger.debug(location, jobid, "workmap not found");
			}
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
		}
		return retVal;
	}
	
	private Long findCheckpointLastReservation() {
		String location = "findCheckpointLastReservation";
		Long retVal = new Long(-1);
		DuccWorkMap workMap = OrchestratorCommonArea.getInstance().getWorkMap();
		try {
			if(workMap != null) {
				DuccId duccId = getMax(workMap.getReservationKeySet());
				if(duccId != null) {
					retVal = duccId.getFriendly();
					logger.info(location, jobid, "ckpt reservation "+retVal);
				}
				else {
					logger.debug(location, jobid, "ckpt reservation not found");
				}
			}
			else {
				logger.debug(location, jobid, "workmap not found");
			}
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
		}
		return retVal;
	}
	
	private DuccId getMax(Set<DuccId> keys) {
		DuccId retVal = null;
		if(keys != null) {
			for(DuccId key : keys) {
				if(retVal == null) {
					retVal = key;
				}
				else if(key.getFriendly() > retVal.getFriendly()) {
					retVal = key;
				}
			}
		}
		return retVal;
	}
	
	private Long maxOf(Long v0, Long v1, String text) {
		String location = "maxOf";
		Long retVal = v0;
		if(v1 > v0) {
			retVal = v1+1;
			logger.info(location, jobid, text);
		}
		return retVal;
	}
	
	public Long recoverSeqNo() {
		String location = "recoverSeqNo";
		Long seqNo = new Long(-1);
		seqNo = maxOf(seqNo, findHistoryLastJob(), "history:job");
		seqNo = maxOf(seqNo, findHistoryLastReservation(), "history:reservation");
		seqNo = maxOf(seqNo, findCheckpointLastJob(), "checkpoint:job");
		seqNo = maxOf(seqNo, findCheckpointLastReservation(), "checkpoint:reservation");
		logger.info(location, jobid, seqNo);
		return seqNo;
	}
}
