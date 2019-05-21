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
package org.apache.uima.ducc.orchestrator.state;

import org.apache.uima.ducc.common.NodeIdentity;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.orchestrator.database.OrDbOrchestratorProperties;

public class OrchestratorStateDb implements IOrchestratorState {

	private static DuccLogger logger = DuccLogger.getLogger(OrchestratorStateDb.class);
	private static DuccId jobid = null;
	
	private OrDbOrchestratorProperties orDbOrchestratorProperties = OrDbOrchestratorProperties.getInstance();
	
	private static OrchestratorStateDb instance = null;
	
	public static IOrchestratorState getInstance() {
		String location = "getInstance";
		synchronized(OrchestratorStateDb.class) {
			logger.debug(location, jobid, ""+instance);
			if(instance == null) {
				instance = new OrchestratorStateDb();
				instance.initialize();
			}
		}
		return instance;
	}
	
	private void initialize() {
		OrchestratorStateDbConversion.convert();
	}
	
	private long getNextSequenceNumberState() {
		return orDbOrchestratorProperties.getNextPublicationSeqNo();
	}
	
	@Override
	public long getNextPublicationSequenceNumber() {
		return getNextSequenceNumberState();
	}
	
	private void setNextSequenceNumberState(long value) {
		orDbOrchestratorProperties.setPublicationSeqNo(value);
	}
	
	@Override
	public void setNextPublicationSequenceNumber(long seqNo) {
		setNextSequenceNumberState(seqNo);
	}
	
	private void setNextSequenceNumberStateIfGreater(NodeIdentity nodeIdentity, long value) {
		orDbOrchestratorProperties.setPublicationSeqNoIfLarger(value);
	}
	
	@Override
	public void setNextPublicationSequenceNumberIfGreater(long value, NodeIdentity nodeIdentity) {
		setNextSequenceNumberStateIfGreater(nodeIdentity, value);
	}

	@Override
	public synchronized long getNextDuccWorkSequenceNumber() {
		long prev = orDbOrchestratorProperties.getDuccWorkSeqNo();
		long next = prev+1;
		orDbOrchestratorProperties.setDuccWorkSeqNo(next);
		return next;
	}

	@Override
	public long getDuccWorkSequenceNumber() {
		long prev = orDbOrchestratorProperties.getDuccWorkSeqNo();
		return prev;
	}
	
	@Override
	public void setDuccWorkSequenceNumber(long seqNo) {
		orDbOrchestratorProperties.setDuccWorkSeqNo(seqNo);
	}

	@Override
	public void setDuccWorkSequenceNumberIfGreater(long seqNo) {
		orDbOrchestratorProperties.setDuccWorkSeqNoIfLarger(seqNo);
	}
	
}
