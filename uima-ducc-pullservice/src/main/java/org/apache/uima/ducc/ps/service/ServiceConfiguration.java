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

package org.apache.uima.ducc.ps.service;

import org.apache.uima.ducc.ps.service.errors.ServiceInitializationException;

public class ServiceConfiguration {
	private String clientURL;
	private String threadCount;
	private String duccHome;
	private String jobId;
	private String duccProcessId;
	private String duccProcessUniqueId;
	private String monitorPort;
	private String analysisEngineDescriptorPath;
	private String serviceType;
	private String jpType;
	private String assignedJmxPort;
	private String customRegistryClass;
	private String customProcessorClass;
	private String serviceJmxConnectURL;
	
	public String getJpType() {
		return jpType;
	}
	public void setJpType(String type) {
		jpType = type;
	}
	public String getServiceJmxConnectURL() {
		return serviceJmxConnectURL;
	}

	public void setServiceJmxConnectURL(String serviceJmxConnectURL) {
		this.serviceJmxConnectURL = serviceJmxConnectURL;
	}

	public String getClientURL() {
		return clientURL;
	}

	public String getThreadCount() {
		return threadCount;
	}

	public void setThreadCount(String threadCount) {
		this.threadCount = threadCount;
	}

	public String getDuccHome() {
		return duccHome;
	}

	public String getJobId() {
		return jobId;
	}

	public String getDuccProcessId() {
		return duccProcessId;
	}

	public String getDuccProcessUniqueId() {
		return duccProcessUniqueId;
	}

	public String getMonitorPort() {
		return monitorPort;
	}

	public String getAnalysisEngineDescriptorPath() {
		return analysisEngineDescriptorPath;
	}

	public String getServiceType() {
		return serviceType;
	}

	public void setServiceType(String serviceType) {
		this.serviceType = serviceType;
	}
	
	public String getAssignedJmxPort() {
		return assignedJmxPort;
	}

	public String getCustomRegistryClass() {
		return customRegistryClass;
	}

	public String getCustomProcessorClass() {
		return customProcessorClass;
	}

	public void validateProperties() throws ServiceInitializationException {
		if ( analysisEngineDescriptorPath == null ) {
			throw new ServiceInitializationException("AE descriptor is missing - unable to launch the service");
		}
		if (threadCount == null) {
			threadCount = "1";
		}
		if (serviceType == null) {
			serviceType = "";
		}

	}

	public void collectProperties(String[] args) {
		clientURL = System.getProperty("ducc.deploy.JdURL");
		threadCount = System.getProperty("ducc.deploy.JpThreadCount");
		serviceType = System.getProperty("ducc.deploy.service.type");
		jpType = System.getProperty("ducc.deploy.JpType");
		assignedJmxPort = System.getProperty("ducc.jmx.port");
		customRegistryClass = System.getProperty("ducc.deploy.registry.class");
		customProcessorClass = System.getProperty("ducc.deploy.custom.processor.class");

		duccHome = System.getenv("DUCC_HOME");
		jobId = System.getenv("DUCC_JOBID");
		duccProcessId = System.getenv("DUCC_PROCESSID");
		duccProcessUniqueId = System.getenv("DUCC_PROCESS_UNIQUEID");
		monitorPort = System.getenv("DUCC_STATE_UPDATE_PORT");

		analysisEngineDescriptorPath = args[0];
	}
}
