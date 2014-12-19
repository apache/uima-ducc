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
package org.apache.uima.ducc.container.jd.classload;

import org.apache.uima.ducc.container.common.MessageBuffer;
import org.apache.uima.ducc.container.common.Standardize;
import org.apache.uima.ducc.container.common.logger.IComponent;
import org.apache.uima.ducc.container.common.logger.ILogger;
import org.apache.uima.ducc.container.common.logger.Logger;

public class ProxyJobDriverDirective {

	private static Logger logger = Logger.getLogger(ProxyJobDriverDirective.class, IComponent.Id.JD.name());
	
	private boolean killJob = false;
	private boolean killProcess = false;
	private boolean killWorkItem = true;
	
	public ProxyJobDriverDirective() {
	}
	
	public ProxyJobDriverDirective(boolean killJob, boolean killProcess, boolean killWorkItem) {
		initialize(killJob, killProcess, killWorkItem);
	}
	
	private void initialize(boolean killJob, boolean killProcess, boolean killWorkItem) {
		String location = "initialize";
		setKillJob(killJob);
		setKillProcess(killProcess);
		setKillWorkItem(killWorkItem);
		MessageBuffer mb = new MessageBuffer();
		mb.append(Standardize.Label.killJob.get()+killJob);
		mb.append(Standardize.Label.killProcess.get()+killProcess);
		mb.append(Standardize.Label.killWorkItem.get()+killWorkItem);
		logger.debug(location, ILogger.null_id, mb);
	}
	
	private void setKillJob(boolean value) {
		killJob = value;
	}
	
	public boolean isKillJob() {
		return killJob;
	}
	
	private void setKillProcess(boolean value) {
		killProcess = value;
	}
	
	public boolean isKillProcess() {
		return killProcess;
	}
	
	private void setKillWorkItem(boolean value) {
		killWorkItem = value;
	}
	
	public boolean isKillWorkItem() {
		return killWorkItem;
	}
}
