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
package org.apache.uima.ducc.orchestrator.ckpt;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.uima.ducc.common.head.IDuccHead;
import org.apache.uima.ducc.common.internationalization.Messages;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccPropertiesResolver;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.orchestrator.DuccHead;
import org.apache.uima.ducc.orchestrator.OrchestratorCommonArea;

public abstract class AOrchestratorCheckpoint {
	
	private static DuccLogger logger = DuccLogger.getLogger(AOrchestratorCheckpoint.class);
	private static DuccId jobid = null;
	
	protected static OrchestratorCommonArea orchestratorCommonArea = OrchestratorCommonArea.getInstance();
	protected static Messages messages = orchestratorCommonArea.getSystemMessages();
	
	protected static DuccPropertiesResolver dpr = DuccPropertiesResolver.getInstance();
	
	protected static String on = "on";
	protected static String off = "off";
	
	private AtomicBoolean ckptEnabled = new AtomicBoolean(true);
	
	private IDuccHead dh = DuccHead.getInstance();
	
	private AtomicBoolean ckptMaster = new AtomicBoolean(true);
	
	protected String getStatus() {
		String retVal = off;
		if(ckptEnabled.get()) {
			return on;
		}
		return retVal;
	}
	
	protected void ckptOnOff() {
		String location = "ckptTrueFalse";
		logger.trace(location, jobid, messages.fetch("enter"));
		String position = dpr.getCachedProperty(DuccPropertiesResolver.ducc_orchestrator_checkpoint);
		if(position != null) {
			String desiredPosition = position.toLowerCase();
			if(desiredPosition.equals(off)) {
				ckptEnabled.set(false);
			}
			else if(desiredPosition.equals(on)) {
				ckptEnabled.set(true);
			}
		}
		String status = getStatus();
		logger.debug(location, jobid, status);
		logger.trace(location, jobid, messages.fetch("exit"));
	}
	
	private void head_resume() {
		String location = "head_resume";
		if(ckptMaster.get()) {
			// already master
		}
		else {
			ckptMaster.set(true);
			logger.info(location, jobid, "");
		}
	}
	
	private void head_suspend() {
		String location = "head_suspend";
		if(ckptMaster.get()) {
			ckptMaster.set(false);
			logger.info(location, jobid, "");
		}
		else {
			// already backup
		}
	}
	
	protected boolean isCkptEnabled() {
		boolean retVal = false;
		if(dh.is_ducc_head_virtual_master()) {
			head_resume();
			if(ckptEnabled.get()) {
				retVal = true;
			}
		}
		else {
			head_suspend();
		}
		return retVal;
	}
	
}
