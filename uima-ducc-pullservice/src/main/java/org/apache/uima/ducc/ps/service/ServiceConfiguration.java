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
	private String assignedJmxPort;
	private String customRegistryClass;
	private String customProcessorClass;
	private String serviceJmxConnectURL;
	
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
		if (clientURL == null) {
			throw new ServiceInitializationException("Client URL not provided - terminating service");
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
