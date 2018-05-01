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
package org.apache.uima.ducc.ps.service.main;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.concurrent.ExecutionException;

import org.apache.uima.UIMAFramework;
import org.apache.uima.ducc.ps.service.IService;
import org.apache.uima.ducc.ps.service.ServiceConfiguration;
import org.apache.uima.ducc.ps.service.builders.PullServiceStepBuilder;
import org.apache.uima.ducc.ps.service.errors.ServiceException;
import org.apache.uima.ducc.ps.service.errors.ServiceInitializationException;
import org.apache.uima.ducc.ps.service.jmx.JMXAgent;
import org.apache.uima.ducc.ps.service.processor.IServiceProcessor;
import org.apache.uima.ducc.ps.service.processor.uima.UimaServiceProcessor;
import org.apache.uima.ducc.ps.service.registry.IRegistryClient;
import org.apache.uima.util.Level;
import org.apache.uima.util.Logger;

public class ServiceWrapper {
	private Logger logger = UIMAFramework.getLogger(ServiceWrapper.class);
	private IService service = null;
	private ServiceConfiguration serviceConfiguration =
			new ServiceConfiguration();
	private JMXAgent jmxAgent;
	
	private void addShutdownHook() {
		ServiceShutdownHook shutdownHook = new ServiceShutdownHook(this, logger);
	    Runtime.getRuntime().addShutdownHook(shutdownHook);
	}
	private String startJmxAgent() throws ServiceInitializationException {
		jmxAgent = new JMXAgent(serviceConfiguration.getAssignedJmxPort(), logger);
		int rmiRegistryPort = jmxAgent.initialize();
		return jmxAgent.start(rmiRegistryPort);
		
	}
	private IServiceProcessor createProcessor(String analysisEngineDescriptorPath) 
	throws ServiceInitializationException{
		if ( serviceConfiguration.getCustomProcessorClass() != null ) {
			try {
			Class<?> clz = Class.forName(serviceConfiguration.getCustomProcessorClass());
			if ( !IServiceProcessor.class.isAssignableFrom(clz) ) {
				throw new ServiceInitializationException(serviceConfiguration.getCustomProcessorClass()+" Processor Class does not implement IServiceProcessor ");
			}
			return (IServiceProcessor) clz.newInstance();
			} catch( Exception e) {
				logger.log(Level.WARNING,"",e);
				throw new ServiceInitializationException("Unable to instantiate Custom Processor from class:"+serviceConfiguration.getCustomProcessorClass());
			}
		} else {
			return new UimaServiceProcessor(analysisEngineDescriptorPath, serviceConfiguration);
		}
	}
	public void initialize(String[] args) throws ServiceInitializationException, ServiceException {
		// collect -Ds and env vars
		serviceConfiguration.collectProperties(args);
		serviceConfiguration.validateProperties();
		addShutdownHook();
		
		String analysisEngineDescriptorPath = 
				serviceConfiguration.getAnalysisEngineDescriptorPath();

		// create JMX agent
		String serviceJmxConnectString = startJmxAgent();
		
		serviceConfiguration.setServiceJmxConnectURL(serviceJmxConnectString);
		
		IServiceProcessor processor = 
				createProcessor(analysisEngineDescriptorPath);
		IRegistryClient registryClient= null;
		
		if ( serviceConfiguration.getCustomRegistryClass() != null ) {
			try {
				Class<?> clz = Class.forName(serviceConfiguration.getCustomRegistryClass()) ;
				if ( !IRegistryClient.class.isAssignableFrom(clz)) {
					throw new ServiceInitializationException(serviceConfiguration.getCustomRegistryClass()+" Registry Client Class does not implement IRegistryClient ");
				}
				try {
					// constructor with client URL argument
					Constructor<?> ctor = clz.getConstructor(String.class);
					registryClient = (IRegistryClient) ctor.newInstance(serviceConfiguration.getClientURL());
				} catch(NoSuchMethodException ee) {
					// zero arg constructor. User must initialize this registry via custom -D's or environment 
					registryClient = (IRegistryClient) clz.newInstance();
				}
				
				
			} catch( Exception e) {
				logger.log(Level.WARNING,"",e);
				throw new ServiceInitializationException("Unable to instantiate Custom Registry Client from class:"+serviceConfiguration.getCustomRegistryClass());
				
			}
			service = PullServiceStepBuilder.newBuilder()
					.withProcessor(processor)
					.withRegistry(registryClient)
					.withType(serviceConfiguration.getServiceType())
					.withScaleout(Integer.valueOf(serviceConfiguration.getThreadCount()))
					.withOptionalsDone().build();

		} else {
			service = PullServiceStepBuilder.newBuilder()
					.withProcessor(processor)
					.withClientURL(serviceConfiguration.getClientURL())
					.withType(serviceConfiguration.getServiceType())
					.withScaleout(Integer.valueOf(serviceConfiguration.getThreadCount()))
					.withOptionalsDone().build();

		}
		
		// String tasURL = "http://localhost:8080/test";


		service.initialize();

	}

	public void start() throws ServiceException, ExecutionException {
		service.start();
	}

	public void stop() {
		service.stop();
		try {
			jmxAgent.stop();
		} catch( IOException e ) {
			
		}
		
	}

	public static void main(String[] args) {
		ServiceWrapper wrapper = new ServiceWrapper();
		try {
			
			wrapper.initialize(args);
			wrapper.start();
		} catch( Exception e) {
			UIMAFramework.getLogger().log(Level.WARNING, "", e);
			wrapper.stop();
		}
	}
	 static class ServiceShutdownHook extends Thread {
		    private ServiceWrapper serviceWrapper;
		    private Logger logger;
		    
		    public ServiceShutdownHook(ServiceWrapper serviceWrapper, Logger logger ) {
		      this.serviceWrapper = serviceWrapper;
		      this.logger = logger;
		    }

		    public void run() {
		      try {
		          logger.log(Level.INFO, "Pull Service Caught SIGTERM Signal - Stopping ...");

		        serviceWrapper.stop();

		      } catch (Exception e) {
		    	  logger.log(Level.WARNING,"", e);
		      }
		    }
		  }
}
