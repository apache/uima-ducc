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

package org.apache.uima.ducc.transport.configurator.jp;

import java.lang.reflect.Method;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.camel.CamelContext;
import org.apache.uima.ducc.common.component.AbstractDuccComponent;
import org.apache.uima.ducc.common.component.IJobProcessor;
import org.apache.uima.ducc.common.container.FlagsHelper;
import org.apache.uima.ducc.common.main.DuccService;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.transport.event.common.IProcessState.ProcessState;

public class JobProcessComponent extends AbstractDuccComponent 
implements IJobProcessor{

	
	private JobProcessConfiguration configuration=null;
	private String jmxConnectString="";
	private AgentSession agent = null;
	protected ProcessState currentState = ProcessState.Undefined;
	protected ProcessState previousState = ProcessState.Undefined;
	protected static DuccLogger logger;
	protected String saxonJarPath;
	protected String dd2SpringXslPath;
	protected String dd;
	private int timeout = 30000;  // default socket timeout for HTTPClient
	private int threadSleepTime = 5000; // time to sleep between GET requests if JD sends null CAS
	private CountDownLatch workerThreadCount = null;
	private CountDownLatch threadReadyCount=null;
	ScheduledThreadPoolExecutor executor = null;
	ExecutorService tpe = null;
    private volatile boolean uimaASJob=false;
    
	private DuccHttpClient httpClient = null;
    private Object processorInstance=null;
//    private String[] args = null;
	public JobProcessComponent(String componentName, CamelContext ctx,JobProcessConfiguration jpc) {
		super(componentName,ctx);
		this.configuration = jpc;
		jmxConnectString = super.getProcessJmxUrl();
		
	}
	public void setProcessor(Object pc, String[] args ) {
		this.processorInstance = pc;
	}
	public void setState(ProcessState state) {
		if ( !state.equals(currentState)) {
			currentState = state;
			agent.notify(currentState, super.getProcessJmxUrl());
		} 
	}
    public void setThreadSleepTime(int sleepTime) {
    	threadSleepTime = sleepTime;
    }
    public int getThreadSleepTime() {
    	return threadSleepTime;
    }
	protected void setDD(String dd) {
		this.dd = dd;
	}
	public void setDd2SpringXslPath( String dd2SpringXslPath ) {
		this.dd2SpringXslPath = dd2SpringXslPath;
	}
	public void setSaxonJarPath( String saxonJarPath) {
		this.saxonJarPath = saxonJarPath;
	}	
	
	protected void setAgentSession(AgentSession session ) {
		agent = session;
	}
	public String getProcessJmxUrl() {
		return jmxConnectString;
	}
	
	public DuccLogger getLogger() {
		if ( logger == null ) {
			logger = new DuccLogger(JobProcessComponent.class);
		}
		return logger;
	}
	public void setTimeout(int timeout) {
		this.timeout = timeout;
	}
	public int getTimeout() {
		return this.timeout;
	}
	/**
	 * This method is called by super during ducc framework boot
	 * sequence. It creates all the internal components and worker threads
	 * and initiates processing. When threads exit, this method shuts down
	 * the components and returns.
	 */
	public void start(DuccService service, String[] args) throws Exception {
		super.start(service, args);
		
		try {
			if ( args == null || args.length ==0 || args[0] == null || args[0].trim().length() == 0) {
				logger.warn("start", null, "Missing Deployment Descriptor - the JP Requires argument. Add DD for UIMA-AS job or AE descriptor for UIMA jobs");
                throw new RuntimeException("Missing Deployment Descriptor - the JP Requires argument. Add DD for UIMA-AS job or AE descriptor for UIMA jobs");
			}
			String processJmxUrl = super.getProcessJmxUrl();
			// tell the agent that this process is initializing
			agent.notify(ProcessState.Initializing, processJmxUrl);
			
			try {
				executor = new ScheduledThreadPoolExecutor(1);
				executor.prestartAllCoreThreads();
				// Instantiate a UIMA AS jmx monitor to poll for status of the AE.
				// This monitor checks if the AE is initializing or ready.
				JmxAEProcessInitMonitor monitor = new JmxAEProcessInitMonitor(agent);
				/*
				 * This will execute the UimaAEJmxMonitor continuously for every 15
				 * seconds with an initial delay of 20 seconds. This monitor polls
				 * initialization status of AE deployed in UIMA AS.
				 */
				executor.scheduleAtFixedRate(monitor, 20, 30, TimeUnit.SECONDS);
                // the JobProcessConfiguration class already checked for 
				// existence of -DDucc.Job.Type
				String jobType = System.getProperty(FlagsHelper.Name.JpType.pname()); 

				String[] jpArgs;
                // check if this is DD job
				if ( "uima-as".equals(jobType)) {
					uimaASJob = true;
                	// dd - deployment descriptor. Will use UIMA-AS
					jpArgs = new String[] { "-dd",args[0],"-saxonURL",saxonJarPath,
    						"-xslt",dd2SpringXslPath};
                } else if ( "uima".equals(jobType)) {
                	// this is pieces-parts job
                	String scaleout = System.getProperty(FlagsHelper.Name.JpThreadCount.pname());
                	if ( scaleout == null ) {
                		scaleout = "1";
                	}
                	// aed - analysis engine descriptor. Will use UIMA core only
                	jpArgs = new String[] { "-aed",args[0], "-t", scaleout};
                } else if ( "user".equals(jobType)) {
                	jpArgs = args;  
                } else {
                	throw new RuntimeException("Unsupported JP deployment mode. Check a value provided for -D"+FlagsHelper.Name.JpType.pname()+". Supported modes: [uima-as|uima|user]");
                }
				Properties props = new Properties();
				// Using java reflection, initialize instance of IProcessContainer
				Method initMethod = processorInstance.getClass().
						getDeclaredMethod("initialize", Properties.class, String[].class);
				int scaleout = (Integer)initMethod.invoke(processorInstance, props, jpArgs);
				
				getLogger().info("start", null,"Ducc JP JobType="+jobType);
				httpClient = new DuccHttpClient();
				String jdURL="";
				try {
					jdURL = System.getProperty(FlagsHelper.Name.JdURL.pname());
					// initialize http client. It tests the connection and fails
					// if unable to connect
					httpClient.initialize(jdURL);
					logger.info("start", null,"The JP Connected To JD Using URL "+jdURL);
				} catch( Exception ee ) {
					if ( ee.getCause() != null && ee instanceof java.net.ConnectException ) {
						logger.error("start", null, "JP Process Unable To Connect to the JD Using Provided URL:"+jdURL+" Unable to Continue - Shutting Down JP");
					}
					throw ee;
				}
                // Setup latch which will be used to determine if worker threads
				// initialized properly. The threads will not fetch WIs from the JD
				// until the latch is open (all threads complete initialization)
				threadReadyCount = new CountDownLatch(scaleout);
				// Setup Thread Factory 
				UimaServiceThreadFactory tf = new UimaServiceThreadFactory(Thread
						.currentThread().getThreadGroup());
				workerThreadCount = new CountDownLatch(scaleout); //uimaProcessor.getScaleout());
				// Setup Thread pool with thread count = scaleout
				tpe = Executors.newFixedThreadPool(scaleout, tf); //uimaProcessor.getScaleout(), tf);

				// initialize http client's timeout
				httpClient.setTimeout(timeout);
				
				System.out.println("JMX Connect String:"+ processJmxUrl);
		    	getLogger().info("start", null, "Starting "+scaleout+" Process Threads - JMX Connect String:"+ processJmxUrl);
				
		    	// Create and start worker threads that pull Work Items from the JD
		    	Future<?>[] threadHandles = new Future<?>[scaleout];
				for (int j = 0; j < scaleout; j++) {
					threadHandles[j] = tpe.submit(new HttpWorkerThread(this, httpClient, processorInstance, workerThreadCount, threadReadyCount));
				}
				// wait until all process threads initialize
				threadReadyCount.await();
                // if initialization was successful, tell the agent that the JP is running 
				if ( !currentState.equals(ProcessState.FailedInitialization )) {
			    	// pipelines deployed and initialized. This process is Ready
			    	currentState = ProcessState.Running;
					// Update agent with the most up-to-date state of the pipeline
					// all is well, so notify agent that this process is in Running state
					agent.notify(currentState, processJmxUrl);
				}
				for( Future<?> future : threadHandles ) {
					future.get();   // wait for each worker thread to exit run()
				}
		    } catch( Exception ee) {
		    	ee.printStackTrace();
		    	currentState = ProcessState.FailedInitialization;
		    	getLogger().info("start", null, ">>> Failed to Deploy UIMA Service. Check UIMA Log for Details");
				agent.notify(ProcessState.FailedInitialization);
				
		    } finally {
				// Stop executor. It was only needed to poll AE initialization status.
				// Since deploy() completed
				// the UIMA AS service either succeeded initializing or it failed. In
				// either case we no longer
				// need to poll for initialization status
		    	if ( executor != null ) {
			    	executor.shutdownNow();
		    	}
		    	if ( tpe != null ) {
		    		tpe.shutdown();
		    		tpe.awaitTermination(0, TimeUnit.MILLISECONDS);
		    	}
		    	
		    	if ( workerThreadCount != null ) {
			    	workerThreadCount.await();
			    	
			    	// Determine if the process container requires thread affinity to AE instance.
			    	// If it does, the worker thread has already called stop() which in
			    	// turn called AE.destroy(). If the process container has no thread 
			    	// affinity, call stop() here to make sure the cleanup code shuts down
			    	// internal components.
			    	Method useThreadAffinityMethod = processorInstance.getClass().getDeclaredMethod("useThreadAffinity");	
					boolean useThreadAffinity =
							(Boolean)useThreadAffinityMethod.invoke(processorInstance);
					if ( !useThreadAffinity) {
						Method deployMethod = processorInstance.getClass().getDeclaredMethod("stop");
						deployMethod.invoke(processorInstance);
					}
			    	
			    	
			    	// Stop process container
					Method stopMethod = processorInstance.getClass().getDeclaredMethod("stop");
					stopMethod.invoke(processorInstance);
		    	}
				stop();
		    }
		} catch( Exception e) {
			currentState = ProcessState.FailedInitialization;
			agent.notify(currentState);
			e.printStackTrace();
			stop();
		} 

	}
	
	public void setRunning() {
		currentState = ProcessState.Running;
	}
	public boolean isRunning() {
		return currentState.equals(ProcessState.Running);
	}
	public boolean isUimaASJob() {
	   return uimaASJob;
	}
	public void stop() {
		currentState = ProcessState.Stopping;
		agent.notify(currentState);
		if ( super.isStopping() ) {
			return;  // already stopping - nothing to do
		}

		System.out.println("... JobProcessComponent - Stopping Service Adapter");
	    try {
	    	if ( workerThreadCount != null ) {
	        	// block until all worker threads exit run()
	        	workerThreadCount.await();
	    	}
        	
			// Stop executor. It was only needed to poll AE initialization status.
			// Since deploy() completed
			// the UIMA AS service either succeeded initializing or it failed. In
			// either case we no longer
			// need to poll for initialization status
	    	if ( executor != null ) {
		    	executor.shutdownNow();
	    	}
	    	if ( tpe != null ) {
	    		tpe.shutdown();
	    		tpe.awaitTermination(0, TimeUnit.MILLISECONDS);
	    	}

        	if ( agent != null) {
            	agent.stop();
        	}
	    } catch( Exception e) {
	    	e.printStackTrace();
	    } finally {
	    	try {
		    	super.stop();
	    	} catch( Exception ee) {}
	    }
	}

}
