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

package org.apache.uima.ducc.transport.configuration.jp;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.camel.CamelContext;
import org.apache.camel.Route;
import org.apache.uima.aae.UimaAsVersion;
import org.apache.uima.ducc.common.component.AbstractDuccComponent;
import org.apache.uima.ducc.common.container.FlagsHelper;
import org.apache.uima.ducc.common.main.DuccService;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.container.jp.JobProcessManager;
import org.apache.uima.ducc.container.jp.iface.IUimaProcessor;
import org.apache.uima.ducc.transport.event.common.JobManagerStateReconciler;
import org.apache.uima.ducc.transport.event.common.IProcessState.ProcessState;

public class JobProcessComponent extends AbstractDuccComponent{

	
	private JobProcessConfiguration configuration=null;
	private String jmxConnectString="";
	private AgentSession agent = null;
	private JobProcessManager jobProcessManager = null;
	protected ProcessState currentState = ProcessState.Undefined;
	protected ProcessState previousState = ProcessState.Undefined;
	protected static DuccLogger logger;
	protected String saxonJarPath;
	protected String dd2SpringXslPath;
	protected String dd;
	private int timeout = 30000;  // default socket timeout for HTTPClient
	private int threadSleepTime = 5000; // time to sleep between GET requests if JD sends null CAS
//	private IUimaProcessor uimaProcessor = null; 
	private CountDownLatch workerThreadCount = null;
	private CountDownLatch threadReadyCount=null;
	ScheduledThreadPoolExecutor executor = null;
	ExecutorService tpe = null;
    private volatile boolean uimaASJob=false;
    
	// define default class to use to invoke methods via reflection
	private String containerClass = "org.apache.uima.ducc.user.jp.UimaProcessContainer";
;
	
	public JobProcessComponent(String componentName, CamelContext ctx,JobProcessConfiguration jpc) {
		super(componentName,ctx);
		this.configuration = jpc;
		jmxConnectString = super.getProcessJmxUrl();
		
	}
    public void setThreadSleepTime(int sleepTime) {
    	threadSleepTime = sleepTime;
    }
    public int getThreadSleepTime() {
    	return threadSleepTime;
    }
	public void setContainerClass(String clz) {
		if ( clz != null ) {
			containerClass = clz;
		}
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
	protected void setJobProcessManager(JobProcessManager jobProcessManager) {
		this.jobProcessManager = jobProcessManager;
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
		getLogger().info("start", null,"Ducc UIMA-AS Version:"+UimaAsVersion.getFullVersionString());
		System.out.println("... Starting Component");
		super.start(service, args);
		
		try {
			if ( args == null || args.length ==0 || args[0] == null || args[0].trim().length() == 0) {
				logger.warn("start", null, "Missing Deployment Descriptor - the JP Requires argument. Add DD for UIMA-AS job or AE descriptor for UIMA jobs");
                throw new RuntimeException("Missing Deployment Descriptor - the JP Requires argument. Add DD for UIMA-AS job or AE descriptor for UIMA jobs");
			}
			// the JobProcessConfiguration checked if the below property exists
			String jps = System.getProperty(FlagsHelper.Name.UserClasspath.pname());

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
				containerClass = configuration.getUserContainerClassForJob(jobType);
				String[] jpArgs;
				if ( "uima-as".equals(jobType)) {
					uimaASJob = true;
                	// dd - deployment descriptor. Will use UIMA-AS
					jpArgs = new String[] { "-dd",args[0],"-saxonURL",saxonJarPath,
    						"-xslt",dd2SpringXslPath};
                } else if ( "uima".equals(jobType)) {
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
				getLogger().info("start", null,"Ducc JP JobType="+jobType);
				final DuccHttpClient client = new DuccHttpClient();
				String jdURL="";
				try {
					jdURL = System.getProperty(FlagsHelper.Name.JdURL.pname());
					String url = jdURL.substring(jdURL.indexOf("http://")+7 );  // skip protocol
					String host = url.substring(0, url.indexOf(":"));
					String port = url.substring(url.indexOf(":") + 1);
					String target = "";
					if (port.indexOf("/") > -1) {
						target = port.substring(port.indexOf("/"));
						port = port.substring(0, port.indexOf("/"));
					}
					// initialize http client. It tests the connection and fails
					// if unable to connect
//					client.intialize(host, Integer.valueOf(port), target);
					client.initialize(jdURL);
					logger.info("start", null,"The JP Connected To JD Using URL "+jdURL);
				} catch( Exception ee ) {
					if ( ee.getCause() != null && ee instanceof java.net.ConnectException ) {
						logger.error("start", null, "JP Process Unable To Connect to the JD Using Provided URL:"+jdURL+" Unable to Continue - Shutting Down JP");
					}
					throw ee;
				}

				// Deploy UIMA pipelines. This blocks until the pipelines initializes or
		    	// there is an exception. The IUimaProcessor is a wrapper around
		    	// processing container where the analysis is being done.
		    	int scaleout =	jobProcessManager.initialize(jps, jpArgs, containerClass);
		    	// initialize latch to count number of threads which initialized successfully
		    	threadReadyCount = new CountDownLatch(scaleout);
		    	
//		    	uimaProcessor =	jobProcessManager.deploy(jps, uimaAsArgs, containerClass);

				// Setup Thread Factory 
				UimaServiceThreadFactory tf = new UimaServiceThreadFactory(Thread
						.currentThread().getThreadGroup());
				workerThreadCount = new CountDownLatch(scaleout); //uimaProcessor.getScaleout());
				// Setup Thread pool with thread count = scaleout
				tpe = Executors.newFixedThreadPool(scaleout, tf); //uimaProcessor.getScaleout(), tf);

				// initialize http client
				client.setTimeout(timeout);
//				client.setScaleout(scaleout);//uimaProcessor.getScaleout());
				
				System.out.println("JMX Connect String:"+ processJmxUrl);
                // Create thread pool and begin processing
		    	getLogger().info("start", null, "Starting "+scaleout+" Process Threads - JMX Connect String:"+ processJmxUrl);
				
		    	// Create and start worker threads that pull Work Items from the JD
		    	Future<?>[] threadHandles = new Future<?>[scaleout];
//		    	Future<?>[] threadHandles = new Future<?>[uimaProcessor.getScaleout()];
//				for (int j = 0; j < uimaProcessor.getScaleout(); j++) {
				for (int j = 0; j < scaleout; j++) {
					threadHandles[j] = tpe.submit(new HttpWorkerThread(this, client, jobProcessManager, workerThreadCount, threadReadyCount));
				}
				// wait until all process threads initialize
				threadReadyCount.await();
                // if there was init error setState() method
				if ( !currentState.equals(ProcessState.FailedInitialization )) {
			    	// pipelines deployed and initialized. This process is Ready
			    	currentState = ProcessState.Running;
					// Update agent with the most up-to-date state of the pipeline
					// all is well, so notify agent that this process is in Running state
					agent.notify(currentState, processJmxUrl);
				}

				getLogger().info("start", null, "All Http Worker Threads Started - Waiting For All Threads to Exit");

				for( Future<?> f : threadHandles ) {
					if ( f != null ) {
						f.get();  // wait for worker threads to exit
					}
				}
		    	getLogger().info("start", null, "All Http Worker Threads Terminated");
		    	jobProcessManager.stop();
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
		    }
		} catch( Exception e) {
			currentState = ProcessState.FailedInitialization;
			agent.notify(currentState);

			
		}

	}
	public void setState(ProcessState state) {
		if ( !currentState.equals(ProcessState.FailedInitialization)) {
			if ( state.equals(ProcessState.FailedInitialization)) {
				currentState = state;
				agent.notify(currentState, super.getProcessJmxUrl());
			}
		} else if ( currentState.equals(ProcessState.Initializing) ){
			currentState = state;
		}
	}
	public boolean isRunning() {
		return currentState.equals(ProcessState.Running);
	}
	public boolean isUimaASJob() {
	   return uimaASJob;
	}
	public void stop() {
		currentState = ProcessState.Stopping;
		if ( super.isStopping() ) {
			return;  // already stopping - nothing to do
		}

		System.out.println("... AbstractManagedService - Stopping Service Adapter");
	    try {
        	if (getContext() != null) {
    			for (Route route : getContext().getRoutes()) {

    				route.getConsumer().stop();
    				System.out.println(">>> configFactory.stop() - stopped route:"
    						+ route.getId());
    			}
    		}
        	// block for worker threads to exit run()
        	workerThreadCount.await();
        	
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

//        	if ( uimaProcessor != null ) {
//            	uimaProcessor.stop();
//        	}
        	if ( agent != null) {
            	agent.stop();
        	}
			super.stop();
			
	    } catch( Exception e) {
	    	e.printStackTrace();
	    }
	}
}
