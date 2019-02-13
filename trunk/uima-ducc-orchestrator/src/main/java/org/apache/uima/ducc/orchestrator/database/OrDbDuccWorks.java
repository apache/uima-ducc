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
package org.apache.uima.ducc.orchestrator.database;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.uima.ducc.common.head.IDuccHead;
import org.apache.uima.ducc.common.persistence.or.ITypedProperties;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.database.DbDuccWorks;
import org.apache.uima.ducc.orchestrator.DuccHead;

/**
 * Class for logging purposes only.
 */
public class OrDbDuccWorks extends DbDuccWorks {

	private static DuccLogger logger = DuccLogger.getLogger(OrDbDuccWorks.class);
	private static DuccId jobid = null;
	
	private IDuccHead dh = DuccHead.getInstance();
	
	public OrDbDuccWorks() throws Exception {
		super(logger);
	}
	
	
	private AtomicBoolean resumeAnnounced = new AtomicBoolean(true);
	
	private void resume() {
		String location = "resume";
		if(!resumeAnnounced.get()) {
			logger.warn(location, jobid, "operations resumed - master mode");
			resumeAnnounced.set(true);
			suspendAnnounced.set(false);
		}
	}
	
	private AtomicBoolean suspendAnnounced = new AtomicBoolean(false);
	
	private void suspend() {
		String location = "suspend";
		if(!suspendAnnounced.get()) {
			logger.warn(location, jobid, "operations suspended - backup mode");
			suspendAnnounced.set(true);
			resumeAnnounced.set(false);
		}
	}
	
	public void dbInit() throws Exception {
		if(dh.is_ducc_head_virtual_master()) {
			resume();
			super.dbInit();
		}
		else {
			suspend();
		}
	}
	
	public void upsertSpecification(String type, long id, ITypedProperties properties) throws Exception {
		if(dh.is_ducc_head_virtual_master()) {
			resume();
			super.upsertSpecification(type, id, properties);
		}
		else {
			suspend();
		}
	}

}
