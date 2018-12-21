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
	private String customRegistryClass;
	private String customProcessorClass;
	private String serviceJmxConnectURL;
	private String jobDirectory;
	private String aeDescriptor;
	private String ccDescriptor;
	private String cmDescriptor;
	private String jpFlowController;
	private String ccOverrides;
	private String cmOverrides;
	private String aeOverrides;
	private String maxErrors;
	private String errorWindowSize;
	private int waitTimeWhenNoTaskGiven = 0;
	private ClassLoader sysCL=null;
    private String processType;

	public String getProcessType() {
		return processType;
	}
	public void setProcessType(String processType) {
		this.processType = processType;
	}
	public int getWaitTime() {
		return this.waitTimeWhenNoTaskGiven;
	}
	public String getMaxErrors() {
		return maxErrors;
	}
	public void setMaxErrors(String maxErrors) {
		this.maxErrors = maxErrors;
	}

	public String getErrorWindowSize() {
		return errorWindowSize;
	}
	public void setErrorWindowSize(String errorWindowSize) {
		this.errorWindowSize = errorWindowSize;
	}
	public ClassLoader getSysCL() {
		return sysCL;
	}
	public void setSysCL(ClassLoader sysCL) {
		this.sysCL = sysCL;
	}
	public String getJobDirectory() {
		return jobDirectory;
	}
	public void setJobDirectory(String jobDirectory) {
		this.jobDirectory = jobDirectory;
	}
	public String getAeDescriptor() {
		return aeDescriptor;
	}
	public void setAeDescriptor(String aeDescriptor) {
		this.aeDescriptor = aeDescriptor;
	}
	public String getCcDescriptor() {
		return ccDescriptor;
	}
	public void setCcDescriptor(String ccDescriptor) {
		this.ccDescriptor = ccDescriptor;
	}
	public String getCmDescriptor() {
		return cmDescriptor;
	}
	public void setCmDescriptor(String cmDescriptor) {
		this.cmDescriptor = cmDescriptor;
	}
	public String getJpFlowController() {
		return jpFlowController;
	}
	public void setJpFlowController(String jpFlowController) {
		this.jpFlowController = jpFlowController;
	}
	public String getCcOverrides() {
		return ccOverrides;
	}
	public void setCcOverrides(String ccOverrides) {
		this.ccOverrides = ccOverrides;
	}
	public String getCmOverrides() {
		return cmOverrides;
	}
	public void setCmOverrides(String cmOverrides) {
		this.cmOverrides = cmOverrides;
	}
	public String getAeOverrides() {
		return aeOverrides;
	}
	public void setAeOverrides(String aeOverrides) {
		this.aeOverrides = aeOverrides;
	}
	public void setClientURL(String clientURL) {
		this.clientURL = clientURL;
	}
	public void setDuccHome(String duccHome) {
		this.duccHome = duccHome;
	}
	public void setJobId(String jobId) {
		this.jobId = jobId;
	}
	public void setDuccProcessId(String duccProcessId) {
		this.duccProcessId = duccProcessId;
	}
	public void setDuccProcessUniqueId(String duccProcessUniqueId) {
		this.duccProcessUniqueId = duccProcessUniqueId;
	}
	public void setMonitorPort(String monitorPort) {
		this.monitorPort = monitorPort;
	}
	public void setAnalysisEngineDescriptorPath(String analysisEngineDescriptorPath) {
		this.analysisEngineDescriptorPath = analysisEngineDescriptorPath;
	}
//	public void setAssignedJmxPort(String assignedJmxPort) {
//		this.assignedJmxPort = assignedJmxPort;
//	}
	public void setCustomRegistryClass(String customRegistryClass) {
		this.customRegistryClass = customRegistryClass;
	}
	public void setCustomProcessorClass(String customProcessorClass) {
		this.customProcessorClass = customProcessorClass;
	}
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

//	public String getAssignedJmxPort() {
//		return assignedJmxPort;
//	}

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
	    maxErrors = System.getProperty("ducc.deploy.JpErrorThreshold");
	    errorWindowSize = System.getProperty("ducc.deploy.JpErrorWindowSize");
	    if ( System.getProperty("ducc.process.thread.sleep.time") != null ) {
	    	waitTimeWhenNoTaskGiven =
	    			Integer.valueOf(System.getProperty("ducc.process.thread.sleep.time"));
	    }
		jpType = System.getProperty("ducc.deploy.JpType");
		serviceJmxConnectURL = System.getProperty("ducc.jmx.port");
		customRegistryClass = System.getProperty("ducc.deploy.registry.class");
		customProcessorClass = System.getProperty("ducc.deploy.custom.processor.class");
		processType = System.getProperty("ducc.deploy.components");//=job-process
		duccHome = System.getenv("DUCC_HOME");
		jobId = System.getenv("DUCC_JOBID");
		duccProcessId = System.getenv("DUCC_PROCESSID");
		duccProcessUniqueId = System.getenv("DUCC_PROCESS_UNIQUEID");
		monitorPort = System.getenv("DUCC_STATE_UPDATE_PORT");

		analysisEngineDescriptorPath = args[0];
	}
}

