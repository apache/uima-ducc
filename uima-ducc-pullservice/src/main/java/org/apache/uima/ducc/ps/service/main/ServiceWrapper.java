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


import java.io.File;
import java.lang.reflect.Constructor;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

import org.apache.uima.UIMAFramework;
import org.apache.uima.ducc.ps.service.IService;
import org.apache.uima.ducc.ps.service.ServiceConfiguration;
import org.apache.uima.ducc.ps.service.builders.PullServiceStepBuilder;
import org.apache.uima.ducc.ps.service.dgen.DeployableGeneration;
import org.apache.uima.ducc.ps.service.errors.IServiceErrorHandler;
import org.apache.uima.ducc.ps.service.errors.ServiceException;
import org.apache.uima.ducc.ps.service.errors.ServiceInitializationException;
import org.apache.uima.ducc.ps.service.errors.builtin.WindowBasedErrorHandler;
import org.apache.uima.ducc.ps.service.jmx.JMXAgent;
import org.apache.uima.ducc.ps.service.processor.IServiceProcessor;
import org.apache.uima.ducc.ps.service.processor.uima.UimaAsServiceProcessor;
import org.apache.uima.ducc.ps.service.processor.uima.UimaServiceProcessor;
import org.apache.uima.ducc.ps.service.registry.IRegistryClient;
import org.apache.uima.util.Level;
import org.apache.uima.util.Logger;

public class ServiceWrapper implements Application {
	private Logger logger = UIMAFramework.getLogger(ServiceWrapper.class);
	private IService service = null;
	// holds -D's and env variables needed at runtime
	private ServiceConfiguration serviceConfiguration =
			new ServiceConfiguration();
	// jmx agent to configure rmi registry so that jconsole clients can connect
	private JMXAgent jmxAgent;

	private void addShutdownHook() {
		ServiceShutdownHook shutdownHook = new ServiceShutdownHook(this, logger);
	    Runtime.getRuntime().addShutdownHook(shutdownHook);
	}
	private String startJmxAgent() throws ServiceInitializationException {
		jmxAgent = new JMXAgent(serviceConfiguration.getServiceJmxConnectURL(), logger);
		int rmiRegistryPort = jmxAgent.initialize();
		return jmxAgent.start(rmiRegistryPort);

	}

	/**
	 * Creates instance of IServiceProcessor. It checks -Dducc.deploy.JpType to determine which
	 * service type is being deployed. For 'uima' type, the method instantiates and returns
	 * UimaServiceProcessor and for 'uima-as' it returns UimaAsServiceProcessor. If none of
	 * the above is specified and -Dducc.deploy.custom.processor.class=XX is defined, the code
	 * instatiates user provided ServiceProcessor.
	 *
	 * @param analysisEngineDescriptorPath path to the AE descriptor
	 * @return IServiceProcessor instance
	 * @throws ServiceInitializationException
	 */
	private IServiceProcessor createProcessor(String analysisEngineDescriptorPath, String[] args)
	throws ServiceInitializationException{
		IServiceProcessor serviceProcessor=null;
		if ( serviceConfiguration.getCustomProcessorClass() != null ) {
			try {
			Class<?> clz = Class.forName(serviceConfiguration.getCustomProcessorClass());
			// custom processor must implement IServiceProcessor
			if ( !IServiceProcessor.class.isAssignableFrom(clz) ) {
				throw new ServiceInitializationException(serviceConfiguration.getCustomProcessorClass()+" Processor Class does not implement IServiceProcessor ");
			}
			serviceProcessor = (IServiceProcessor) clz.newInstance();
			int maxErrors = 0;
			int windowSize = 0;
			if ( serviceConfiguration.getMaxErrors() != null ) {
				maxErrors = Integer.parseInt(serviceConfiguration.getMaxErrors());
			}
			if ( serviceConfiguration.getMaxErrors() != null ) {
				windowSize = Integer.parseInt(serviceConfiguration.getErrorWindowSize());
			}
			serviceProcessor.setErrorHandlerWindow(maxErrors, windowSize);

			} catch( Exception e) {
				logger.log(Level.WARNING,"",e);
				throw new ServiceInitializationException("Unable to instantiate Custom Processor from class:"+serviceConfiguration.getCustomProcessorClass());
			}
		} else {
			if  ( "uima".equals(serviceConfiguration.getJpType() ) ) {
				serviceProcessor = new UimaServiceProcessor(analysisEngineDescriptorPath, serviceConfiguration);

			} else if ( "uima-as".equals(serviceConfiguration.getJpType()) ) {
				serviceProcessor = new UimaAsServiceProcessor(args, serviceConfiguration);

			} else {
				throw new RuntimeException("Invalid deployment. Set either -Dducc.deploy.JpType=[uima,uima-as] or provide -Dducc.deploy.custom.processor.class=XX where XX implements IServiceProcessor ");
			}
		}
		return serviceProcessor;
	}
	private IServiceErrorHandler getErrorHandler() {
		int maxErrors = 1;
		int windowSize = 1;

		if ( serviceConfiguration.getMaxErrors() != null ) {
			maxErrors = Integer.parseInt(serviceConfiguration.getMaxErrors());
		}
		if ( serviceConfiguration.getErrorWindowSize() != null ) {
			windowSize = Integer.parseInt(serviceConfiguration.getErrorWindowSize());
		}
		// Error handler which terminates service on the 1st error
		return	new WindowBasedErrorHandler()
				.withMaxFrameworkErrors(maxErrors)
				.withProcessErrorWindow(windowSize).build();
	}
	/**
	 * Check if AE descriptor is provided or we need to create it from parts
	 *
	 * @param serviceConfiguration
	 * @return
	 */
	private boolean isPiecesParts(ServiceConfiguration serviceConfiguration ) {
		return ( "uima".equals(serviceConfiguration.getJpType()) );
	}
	public void initialize(String[] args ) throws ServiceInitializationException, ServiceException {
		// collect -Ds and env vars
		serviceConfiguration.collectProperties(args);
		serviceConfiguration.validateProperties();
		addShutdownHook();
		// validateProperties() call above checked if a user provided AE descriptor path
		String analysisEngineDescriptorPath;

		// create JMX agent
		String serviceJmxConnectString = startJmxAgent();
		logger.log(Level.INFO, "Deploying service with JMX enabled - clients can connect using jmx URL:"+serviceJmxConnectString);
		serviceConfiguration.setServiceJmxConnectURL(serviceJmxConnectString);
		IServiceProcessor processor;
		if ( isPiecesParts(serviceConfiguration) || serviceConfiguration.getAnalysisEngineDescriptorPath() == null ) {
			if ( !new File(serviceConfiguration.getAnalysisEngineDescriptorPath()).canRead()) {
				DeployableGeneration dg = new DeployableGeneration(serviceConfiguration);
				try {
					analysisEngineDescriptorPath = dg.generate(true);
					logger.log(Level.INFO, "Deploying UIMA based service using generated (pieces-parts) AE descriptor "+analysisEngineDescriptorPath);
				} catch( Exception e) {
					throw new ServiceException("Unable to generate AE descriptor from parts");
				}
			} else {
				analysisEngineDescriptorPath = serviceConfiguration.getAnalysisEngineDescriptorPath();
			}
		} else {
			analysisEngineDescriptorPath = serviceConfiguration.getAnalysisEngineDescriptorPath();
			if ( analysisEngineDescriptorPath != null ) {
				logger.log(Level.INFO, "Deploying UIMA based service using provided descriptor "+analysisEngineDescriptorPath);
			}
		}
		processor = createProcessor(analysisEngineDescriptorPath, args);

		Objects.requireNonNull(processor, "Unable to instantiate IServiceProcessor");

		if ( serviceConfiguration.getCustomRegistryClass() != null ) {
			service = PullServiceStepBuilder.newBuilder(this)
					.withProcessor(processor)
					.withRegistry(getRegistryClient())
					.withType(serviceConfiguration.getServiceType())
					.withScaleout(Integer.valueOf(serviceConfiguration.getThreadCount()))
					.withWaitOnNoTask(serviceConfiguration.getWaitTime())
					.withOptionalsDone().build();

		} else {
			service = PullServiceStepBuilder.newBuilder(this)
					.withProcessor(processor)
					.withClientURL(serviceConfiguration.getClientURL())
					.withType(serviceConfiguration.getServiceType())
					.withScaleout(Integer.valueOf(serviceConfiguration.getThreadCount()))
					.withWaitOnNoTask(serviceConfiguration.getWaitTime())
					.withOptionalsDone().build();

		}


		service.initialize();

	}
	private IRegistryClient getRegistryClient() throws ServiceInitializationException {
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
		}
		return registryClient;
	}
	public void start() throws ServiceException, ExecutionException {
		service.start();
	}
    public void stopJmx() {
    	try {
    		if ( Objects.nonNull(jmxAgent)) {
     			jmxAgent.stop();
    		}
    	} catch( Exception e) {

    	}
    }
	public void stop() {
		try {
			service.stop();
			System.out.println(">>>>>> ServiceWrapper stopping JMX Agent");
			jmxAgent.stop();
		} catch( Exception e ) {
			logger.log(Level.WARNING,"",e);

		}

	}
	public void quiesceAndStop() {
		try {

			service.quiesceAndStop();
			logger.log(Level.INFO,"Stoppng JMX Agent");
			System.out.println("Stopping JMX Agent");
			jmxAgent.stop();
		} catch( Exception e ) {
			logger.log(Level.WARNING,"",e);

		}

	}
	public static void main(String[] args) {
		ServiceWrapper wrapper = null;
		try {
			wrapper = new ServiceWrapper();
			wrapper.initialize(args);
			wrapper.start();
		} catch( Throwable e) {
			UIMAFramework.getLogger().log(Level.WARNING, "", e);
			if ( wrapper != null ) {
				wrapper.stop();
			}
			System.exit(-1);
		}
	}
	 static class ServiceShutdownHook extends Thread {
		    private ServiceWrapper serviceWrapper;
		    private Logger logger;

		    public ServiceShutdownHook(ServiceWrapper serviceWrapper, Logger logger ) {
		      this.serviceWrapper = serviceWrapper;
		      this.logger = logger;
		    }
		    @Override
		    public void run() {
		      try {
		    	// Use System.out here since the logger may have already closed
		    	// its streams. Logger's shutdown hook could have run by now.
		    	System.out.println("Pull Service Caught SIGTERM Signal - Stopping (Quiescing) ...");
		    	//logger.log(Level.INFO, "Pull Service Caught SIGTERM Signal - Stopping (Quiescing) ...");
		        serviceWrapper.quiesceAndStop();

		      } catch (Exception e) {
		    	  logger.log(Level.WARNING,"", e);
		      }
		    }
		  }
	@Override
	public void onServiceStop() {
		stopJmx();

	}
}

