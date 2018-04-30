package org.apache.uima.ducc.ps.service.main;

import java.io.IOException;
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
import org.apache.uima.util.Level;
import org.apache.uima.util.Logger;

public class ServiceWrapper {
	private Logger logger = UIMAFramework.getLogger(ServiceWrapper.class);
	private IService service = null;
	private ServiceConfiguration serviceConfiguration =
			new ServiceConfiguration();
	private JMXAgent jmxAgent;
	
	private String createAEdescriptorFromParts() {
		return "";
	}
	private void addShutdownHook() {
		ServiceShutdownHook shutdownHook = new ServiceShutdownHook(this, logger);
	    Runtime.getRuntime().addShutdownHook(shutdownHook);
	}
	private String startJmxAgent() throws ServiceInitializationException {
		jmxAgent = new JMXAgent(serviceConfiguration.getAssignedJmxPort(), logger);
		int rmiRegistryPort = jmxAgent.initialize();
		return jmxAgent.start(rmiRegistryPort);
		
	}
	public void initialize(String[] args) throws ServiceInitializationException, ServiceException {
		serviceConfiguration.collectProperties(args);
		serviceConfiguration.validateProperties();
		addShutdownHook();
		String analysisEngineDescriptorPath = 
				serviceConfiguration.getAnalysisEngineDescriptorPath();
		if ( analysisEngineDescriptorPath == null) {
			//analysisEngineDescriptorPath = createAEdescriptorFromParts();
		}
//		jmxAgent = new JMXAgent(serviceConfiguration.getAssignedJmxPort(), logger);
//		int rmiRegistryPort = jmxAgent.initialize();
//		String serviceJmxConnectString = jmxAgent.start(rmiRegistryPort);
//		
		String serviceJmxConnectString = startJmxAgent();
		
		serviceConfiguration.setServiceJmxConnectURL(serviceJmxConnectString);
		
		IServiceProcessor processor = 
				new UimaServiceProcessor(analysisEngineDescriptorPath, serviceConfiguration);
		
		// String tasURL = "http://localhost:8080/test";

		service = PullServiceStepBuilder.newBuilder()
				.withProcessor(processor)
				.withClientURL(serviceConfiguration.getClientURL())
				.withType(serviceConfiguration.getServiceType())
				.withScaleout(Integer.valueOf(serviceConfiguration.getThreadCount()))
				.withOptionalsDone().build();

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
