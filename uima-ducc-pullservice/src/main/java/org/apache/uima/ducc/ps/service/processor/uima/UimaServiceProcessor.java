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
package org.apache.uima.ducc.ps.service.processor.uima;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.uima.UIMAFramework;
import org.apache.uima.analysis_engine.AnalysisEngine;
import org.apache.uima.analysis_engine.metadata.AnalysisEngineMetaData;
import org.apache.uima.cas.CAS;
import org.apache.uima.ducc.ps.service.IScaleable;
import org.apache.uima.ducc.ps.service.IServiceState;
import org.apache.uima.ducc.ps.service.ServiceConfiguration;
import org.apache.uima.ducc.ps.service.errors.IServiceErrorHandler;
import org.apache.uima.ducc.ps.service.errors.IServiceErrorHandler.Action;
import org.apache.uima.ducc.ps.service.jmx.JmxAEProcessInitMonitor;
import org.apache.uima.ducc.ps.service.monitor.IServiceMonitor;
import org.apache.uima.ducc.ps.service.monitor.builtin.RemoteStateObserver;
import org.apache.uima.ducc.ps.service.processor.IProcessResult;
import org.apache.uima.ducc.ps.service.processor.IServiceProcessor;
import org.apache.uima.ducc.ps.service.processor.IServiceResultSerializer;
import org.apache.uima.ducc.ps.service.processor.uima.utils.PerformanceMetrics;
import org.apache.uima.ducc.ps.service.processor.uima.utils.UimaMetricsGenerator;
import org.apache.uima.ducc.ps.service.processor.uima.utils.UimaResultDefaultSerializer;
import org.apache.uima.ducc.ps.service.utils.UimaSerializer;
import org.apache.uima.ducc.ps.service.utils.UimaUtils;
import org.apache.uima.resource.Resource;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.ResourceManager;
import org.apache.uima.resource.ResourceSpecifier;
import org.apache.uima.util.CasPool;
import org.apache.uima.util.Level;
import org.apache.uima.util.Logger;
import org.apache.uima.util.XMLInputSource;

public class UimaServiceProcessor implements IServiceProcessor, IScaleable {
	public static final String IMPORT_BY_NAME_PREFIX = "*importByName:";
	Logger logger = UIMAFramework.getLogger(UimaServiceProcessor.class);
   // Map to store DuccUimaSerializer instances. Each has affinity to a thread
	private Map<Long, UimaSerializer> serializerMap =
				new HashMap<>();
	private IServiceResultSerializer resultSerializer;
	// stores AE instance pinned to a thread
	private ThreadLocal<AnalysisEngine> threadLocal = 
			new ThreadLocal<> ();
    private ReentrantLock initStateShutdownLock = new ReentrantLock();
	private ResourceManager rm = 
			UIMAFramework.newDefaultResourceManager();;
    private CasPool casPool = null;
	private int scaleout=1;
	private JmxAEProcessInitMonitor initStateMonitor;
    private String analysisEngineDescriptor;
    private AnalysisEngineMetaData analysisEngineMetadata;
	// Platform MBean server if one is available (Java 1.5 only)
	private static Object platformMBeanServer;
	private ServiceConfiguration serviceConfiguration;
	private ScheduledThreadPoolExecutor executor = null;
	private IServiceMonitor monitor;
	static {
		// try to get platform MBean Server (Java 1.5 only)
		try {
			Class<?> managementFactory = Class.forName("java.lang.management.ManagementFactory");
			Method getPlatformMBeanServer = managementFactory.getMethod("getPlatformMBeanServer", new Class[0]);
			platformMBeanServer = getPlatformMBeanServer.invoke(null, (Object[]) null);
		} catch (Exception e) {
			platformMBeanServer = null;
		}
	}	
	
	public UimaServiceProcessor(String analysisEngineDescriptor) {
		this(analysisEngineDescriptor,  new UimaResultDefaultSerializer(), new ServiceConfiguration());
	}
	public UimaServiceProcessor(String analysisEngineDescriptor, ServiceConfiguration serviceConfiguration) {
		this(analysisEngineDescriptor,  new UimaResultDefaultSerializer(), serviceConfiguration);
	}
	public UimaServiceProcessor(String analysisEngineDescriptor, IServiceResultSerializer resultSerializer, ServiceConfiguration serviceConfiguration) {
		this.analysisEngineDescriptor = analysisEngineDescriptor;
		this.resultSerializer = resultSerializer;
		this.serviceConfiguration = serviceConfiguration;
		// start a thread which will collect AE initialization state
		launchStateInitializationCollector();
	}
	
	private void launchStateInitializationCollector() {
		monitor =
				new RemoteStateObserver(serviceConfiguration, logger);
		
		executor = new ScheduledThreadPoolExecutor(1);
		executor.prestartAllCoreThreads();
		// Instantiate a UIMA AS jmx monitor to poll for status of the AE.
		// This monitor checks if the AE is initializing or ready.
		initStateMonitor = 
				new JmxAEProcessInitMonitor(monitor, logger);
		/*
		 * This will run UimaAEJmxMonitor every 30
		 * seconds with an initial delay of 20 seconds. This monitor polls
		 * initialization status of AE.
		 */
		executor.scheduleAtFixedRate(initStateMonitor, 20, 30, TimeUnit.SECONDS);

	}
	public void setScaleout(int howManyThreads) {
		this.scaleout = howManyThreads;
	}
	public int getScaleout() {
		return scaleout;
	}

	@Override
	public void initialize() {
		if ( logger.isLoggable(Level.FINE)) {
			logger.log(Level.FINE, "Process Thread:"+ Thread.currentThread().getName()+" Initializing AE");
			
		}
		
	    HashMap<String,Object> paramsMap = new HashMap<>();
        paramsMap.put(Resource.PARAM_RESOURCE_MANAGER, rm);
	    paramsMap.put(AnalysisEngine.PARAM_MBEAN_SERVER, platformMBeanServer);

		try {
			
			XMLInputSource is = 
					UimaUtils.getXMLInputSource(analysisEngineDescriptor);
			String aed = is.getURL().toString();
			ResourceSpecifier rSpecifier =
			    UimaUtils.getResourceSpecifier(aed); //analysisEngineDescriptor);
			
			AnalysisEngine ae = UIMAFramework.produceAnalysisEngine(rSpecifier,
					paramsMap);
			// pin AE instance to this thread
			threadLocal.set(ae);
			
			synchronized(UimaServiceProcessor.class) {
		    	if ( casPool == null ) {
		    		initializeCasPool(ae.getAnalysisEngineMetaData());
		    	}
			}
			
			// every process thread has its own uima deserializer
			serializerMap.put(Thread.currentThread().getId(), new UimaSerializer());

		} catch (Exception e) {
			monitor.onStateChange(IServiceState.State.FailedInitialization.toString(), new Properties());
			throw new RuntimeException(e);

		} 
		if ( logger.isLoggable(Level.INFO)) {
			logger.log(Level.INFO, "Process Thread:"+ Thread.currentThread().getName()+" Done Initializing AE");
			
		}
	}

	private void initializeCasPool(AnalysisEngineMetaData aeMeta) throws ResourceInitializationException {
		Properties props = new Properties();
		props.setProperty(UIMAFramework.CAS_INITIAL_HEAP_SIZE, "1000");

		analysisEngineMetadata = aeMeta;
		casPool = new CasPool(scaleout, analysisEngineMetadata, rm);
	}

	private UimaSerializer getUimaSerializer() {
		
	   	return serializerMap.get(Thread.currentThread().getId());
	}
	@Override
	public IProcessResult process(String serializedTask) {
		AnalysisEngine ae = null;
		// Dont publish AE initialization state. We are in running state if
		// process is being called
		try {
			initStateShutdownLock.lockInterruptibly();
			if ( !executor.isTerminating() && !executor.isTerminated() && !executor.isShutdown() ) {
				// send final AE initialization report before we stop the collecting thread
				initStateMonitor.updateAgentWhenRunning();
				
				executor.shutdown();
				executor.awaitTermination(0, TimeUnit.SECONDS);
				monitor.onStateChange(IServiceState.State.Running.toString(), new Properties());
			}

		} catch( InterruptedException e) {
			Thread.currentThread().interrupt();
		} finally {
			initStateShutdownLock.unlock();
		}
		
		CAS cas = casPool.getCas();
		IProcessResult result;
		
		try {
			// deserialize the task into the CAS
			getUimaSerializer().deserializeCasFromXmi(serializedTask, cas);

			// check out AE instance pinned to this thread
			ae = threadLocal.get();
			// get AE metrics before calling process(). Needed for
			// computing a delta
			List<PerformanceMetrics> beforeAnalysis = 
					UimaMetricsGenerator.get(ae);
			
			// *****************************************************
			// PROCESS
			// *****************************************************
			ae.process(cas);
			
			// *****************************************************
			// No exception in process() , fetch metrics 
			// *****************************************************
			List<PerformanceMetrics> afterAnalysis = 
					UimaMetricsGenerator.get(ae);

			// get the delta
			List<PerformanceMetrics> casMetrics = 
					UimaMetricsGenerator.getDelta( afterAnalysis, beforeAnalysis);

//			StringBuilder sb = new StringBuilder("{");
//			
//			for (AnalysisEnginePerformanceMetrics metrics : casMetrics) {
//				sb.append(resultSerializer.serialize(metrics)).append("}");
//			}
//			sb.append("}");
			
			return new UimaProcessResult(resultSerializer.serialize(casMetrics));
		} catch( Exception e ) {
			logger.log(Level.WARNING,"",e);
			result = new UimaProcessResult(e, Action.TERMINATE);
			return result;
 		}
		finally {
			
			if (cas != null) {
				casPool.releaseCas(cas);
			}
		}
	}

	
	public void setErrorHandler(IServiceErrorHandler errorHandler) {

	}

	@Override
	public void stop() {
		logger.log(Level.INFO,this.getClass().getName()+" stop() called");
		try {
			AnalysisEngine ae = threadLocal.get();
			if ( ae != null ) {
				ae.destroy();
			}
		} catch( Exception e) {
			logger.log(Level.WARNING, "stop", e);
		} 
	}
	

}
