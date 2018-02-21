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
package org.apache.uima.ducc.common.utils;

public interface IDuccLoggerComponents {

	public enum Daemon { 
		Agent("AG"), 
		Broker("BR"), 
		Database("DB"),
		JobDriver("JD"),
		Orchestrator("OR"), 
		ProcessManager("PM"), 
		ResourceManager("RM"), 
		ServicesManager("SM"),
		WebServer("WS");
		private String abbrev = null;
		private Daemon(String abbrev) {
			setAbbrev(abbrev);
		}
		private void setAbbrev(String value) {
			abbrev = value;
		}
		public String getAbbrev() {
			return abbrev;
		}
	};
	
	public final String abbrv_jobDriver = Daemon.JobDriver.getAbbrev();
	public final String abbrv_db = Daemon.Database.getAbbrev();
	public final String abbrv_orchestrator = Daemon.Orchestrator.getAbbrev();
	public final String abbrv_servicesManager = Daemon.ServicesManager.getAbbrev();
	public final String abbrv_resourceManager = Daemon.ResourceManager.getAbbrev();
	public final String abbrv_webServer = Daemon.WebServer.getAbbrev();
}
