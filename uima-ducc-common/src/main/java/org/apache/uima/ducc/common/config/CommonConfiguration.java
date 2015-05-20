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
package org.apache.uima.ducc.common.config;

import org.apache.camel.CamelContext;
import org.apache.camel.impl.DefaultCamelContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
public class CommonConfiguration {
	// Create once and reuse
  private static final CamelContext context = new DefaultCamelContext();

  //	fetch the locale language
	@Value("#{ systemProperties['ducc.locale.language'] }")
	public String localeLanguage;
	//	fetch the locale country
	@Value("#{ systemProperties['ducc.locale.country'] }")
	public String localeCountry;
	
	//	fetch the cluster name
	@Value("#{ systemProperties['ducc.cluster.name'] }")
	public String clusterName;
	
	//	fetch the url of the broker (relevant when using jms transport only)
	@Value("#{ systemProperties['ducc.broker.url'] }")
	public String brokerUrl;

	//	fetch broker name
	@Value("#{ systemProperties['ducc.broker.name'] }")
	public String brokerName;
	
	//	fetch the rate at which the Db Component should post its state
	@Value("#{ systemProperties['ducc.db.state.publish.rate'] }")
	public String dbComponentStatePublishRate;
	
	//	fetch the name of an endpoint where the Db Component should post state updates
	@Value("#{ systemProperties['ducc.db.state.update.endpoint'] }")
	public String dbComponentStateUpdateEndpoint;
	
	//	fetch the signature required switch (on/off)
	@Value("#{ systemProperties['ducc.signature.required'] }")
	public String signatureRequired;
	
	//	fetch the orchestrator checkpoint switch (on/off)
	@Value("#{ systemProperties['ducc.orchestrator.checkpoint'] }")
	public String orchestratorCheckpoint;
	
	//	fetch the orchestrator start type (cold/warm/hot)
	@Value("#{ systemProperties['ducc.orchestrator.start.type'] }")
	public String orchestratorStartType;
	
	//	fetch the orchestrator retain completed hours
	@Value("#{ systemProperties['ducc.orchestrator.retain.completed.hours'] }")
	public String orchestratorRetainCompletedHours;
	
	//	fetch the name of Process Manager endpoint where the JM should send requests
	@Value("#{ systemProperties['ducc.pm.request.endpoint'] }")
	public String pmRequestEndpoint;
	
	//	fetch the rate at which the Process Manager should post its state
	@Value("#{ systemProperties['ducc.pm.state.publish.rate'] }")
	public String pmStatePublishRate;
	
	//	fetch the name of an endpoint where the Process Manager should post state updates
	@Value("#{ systemProperties['ducc.pm.state.update.endpoint'] }")
	public String pmStateUpdateEndpoint;
	
	//	fetch the name of Resource Manager endpoint where the JM expects state updates
	@Value("#{ systemProperties['ducc.rm.state.update.endpoint'] }")
	public String rmStateUpdateEndpoint;
	
	@Value("#{ systemProperties['ducc.rm.class.definitions'] }")
    public String classDefinitionFile;
	
	//	fetch the name of an endpoint where the Orchestrator should post state updates
	@Value("#{ systemProperties['ducc.orchestrator.state.update.endpoint'] }")
	public String orchestratorStateUpdateEndpoint;
	
	//	fetch the rate at which the Orchestrator should post its state
	@Value("#{ systemProperties['ducc.orchestrator.state.publish.rate'] }")
	public String orchestratorStatePublishRate;
	
	//	fetch the name of an endpoint where the Job Driver should post state updates
	@Value("#{ systemProperties['ducc.jd.state.update.endpoint'] }")
	public String jdStateUpdateEndpoint;
	
	//	fetch the rate at which the JD should post its state
	@Value("#{ systemProperties['ducc.jd.state.publish.rate'] }")
	public String jdStatePublishRate;
	
	//	fetch the name prefix of an endpoint where the JD should queue job CASes 
	//  example: if prefix is "ducc.jd.queue." and job number is "2701" then endpoint is ducc.jd.queue.2701
	@Value("#{ systemProperties['ducc.jd.queue.prefix'] }")
	public String jdQueuePrefix;
	
	//	fetch the name of an endpoint where the Service Manager should post state updates
	@Value("#{ systemProperties['ducc.sm.state.update.endpoint'] }")
	public String smStateUpdateEndpoint;
	
	//	fetch the rate at which the SM should post its state
	@Value("#{ systemProperties['ducc.sm.state.publish.rate'] }")
	public String smStatePublishRate;

	@Value("#{ systemProperties['ducc.agent.request.endpoint'] }")
	public String agentRequestEndpoint;

	@Value("#{ systemProperties['ducc.agent.node.metrics.endpoint'] }")
	public String nodeMetricsEndpoint;

	@Value("#{ systemProperties['ducc.agent.node.metrics.publish.rate'] }")
	public String nodeMetricsPublishRate;
	
	@Value("#{ systemProperties['ducc.agent.transport.override'] }")
	String transportOverride;
	
	@Value("#{ systemProperties['ducc.agent.node.inventory.endpoint'] }")
	public String nodeInventoryEndpoint;

	@Value("#{ systemProperties['ducc.agent.node.inventory.publish.rate'] }")
	public String nodeInventoryPublishRate;

	@Value("#{ systemProperties['ducc.rm.state.publish.rate'] }")
	public String rmStatePublishRate;

	@Value("#{ systemProperties['ducc.uima-as.endpoint'] }")
	public String managedServiceEndpoint;

  @Value("#{ systemProperties['ducc.uima-as.endpoint.type'] }")
  public String managedServiceEndpointType;

  @Value("#{ systemProperties['ducc.uima-as.endpoint.params'] }")
  public String managedServiceEndpointParams;

  @Value("#{ systemProperties['ducc.agent.managed.process.state.update.endpoint'] }")
	public String managedProcessStateUpdateEndpoint;
	
  @Value("#{ systemProperties['ducc.agent.managed.process.state.update.endpoint.type'] }")
  public String managedProcessStateUpdateEndpointType;

  @Value("#{ systemProperties['ducc.agent.managed.process.state.update.endpoint.params'] }")
  public String managedProcessStateUpdateEndpointParams;

  
  
  @Value("#{ systemProperties['ducc.uima-as.deployment.descriptor'] }")
	public String  deploymentDescriptorPath;

	@Value("#{ systemProperties['ducc.uima-as.process.id'] }")
	public String duccProcessId;

	@Value("#{ systemProperties['ducc.uima-as.saxon.jar.path'] }")
	public String saxonJarPath;
	
	@Value("#{ systemProperties['ducc.uima-as.dd2spring.xsl.path'] }")
	public String dd2SpringXslPath;	
	
	@Value("#{ systemProperties['ducc.ws.ipaddress'] }")
	public String wsIpAddress;	
	
	@Value("#{ systemProperties['ducc.ws.port'] }")
	public String wsPort;	
	
	@Value("#{ systemProperties['ducc.ws.port.ssl'] }")
	public String wsPortSsl;	
	
	@Value("#{ systemProperties['ducc.agent.launcher.process.stop.timeout'] }")
	public String processStopTimeout;	

	// Fetch max amount of time to wait for a reply from the JD
	@Value("#{ systemProperties['ducc.process.request.timeout'] }")
	public String processRequestTimeout;

	// Fetch the UIMA-AS container class
	@Value("#{ systemProperties['ducc.process.uima.as.container.class'] }")
	public String uimaASProcessContainerClass;
	
	// Fetch the UIMA container class
	@Value("#{ systemProperties['ducc.process.uima.container.class'] }")
	public String uimaProcessContainerClass;

	// Fetch the JP framework error threshold
	@Value("#{ systemProperties['ducc.process.framework.error.limit'] }")
	public String jpFrameworkErrorLimit;

	// Fetch the container class
	@Value("#{ systemProperties['ducc.process.thread.sleep.time'] }")
	public String processThreadSleepTime;
	
	
	@Value("#{ systemProperties['ducc.orchestrator.http.port'] }")
    public String duccORHttpPort; 

	@Value("#{ systemProperties['ducc.driver.jetty.max.threads'] }")
    public String jettyMaxThreads; 
	
	@Value("#{ systemProperties['ducc.driver.jetty.thread.idletime'] }")
	public String jettyThreadIdleTime; 

  public CamelContext camelContext() {
	    context.setAutoStartup(false);
		return context;
	}

}
