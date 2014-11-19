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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.camel.CamelContext;
import org.apache.camel.Route;
import org.apache.uima.aae.UimaAsVersion;
import org.apache.uima.ducc.common.component.AbstractDuccComponent;
import org.apache.uima.ducc.common.main.DuccService;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.container.jp.JobProcessManager;
import org.apache.uima.ducc.container.jp.iface.IUimaProcessor;
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
	private int timeout;  // socket timeout for HTTPClient
	private IUimaProcessor uimaProcessor = null; 
	
	public JobProcessComponent(String componentName, CamelContext ctx,JobProcessConfiguration jpc) {
		super(componentName,ctx);
		this.configuration = jpc;
		jmxConnectString = super.getProcessJmxUrl();
		
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
		logger = new DuccLogger(JobProcessComponent.class);
		return logger;
	}
	public void setTimeout(int timeout) {
		this.timeout = timeout;
	}
	public void start(DuccService service, String[] args) throws Exception {
		super.start(service, args);
		
		try {
			String jps = System.getProperty("org.apache.uima.ducc.userjarpath");
			if (null == jps) {
				System.err
						.println("Missing the -Dorg.apache.uima.ducc.userjarpath=XXXX property");
				System.exit(1);
			}
			String processJmxUrl = super.getProcessJmxUrl();
			agent.notify(ProcessState.Initializing, processJmxUrl);
			
			ScheduledThreadPoolExecutor executor = null;
			
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

				System.out.println("Ducc UIMA-AS Version:"+UimaAsVersion.getFullVersionString());
				String[] uimaAsArgs = { "-dd",args[0],"-saxonURL",saxonJarPath,
						"-xslt",dd2SpringXslPath
					};
				final DuccHttpClient client = new DuccHttpClient();

				String jdURL = System.getProperty("jdURL");
				String url = jdURL.substring(jdURL.indexOf("http://")+7 );  // skip protocol
				String host = url.substring(0, url.indexOf(":"));
				String port = url.substring(url.indexOf(":") + 1);
				String target = "";
				if (port.indexOf("/") > -1) {
					target = port.substring(port.indexOf("/"));
					port = port.substring(0, port.indexOf("/"));
				}
//				client.intialize(host, Integer.valueOf(port), target, uimaProcessor.getScaleout(), timeout);
				client.intialize(host, Integer.valueOf(port), target, 2, timeout);

				UimaServiceThreadFactory tf = new UimaServiceThreadFactory(Thread
						.currentThread().getThreadGroup());

				final ExecutorService tpe = Executors.newFixedThreadPool(2, tf);
//				final ExecutorService tpe = Executors.newFixedThreadPool(uimaProcessor.getScaleout(), tf);

				
				// Deploy UIMA pipelines. This blocks until the pipelines initializes or
		    	// there is an exception. The IUimaProcessor is a wrapper around
		    	// processing container where the analysis is being done.
		    	uimaProcessor =	jobProcessManager.deploy(jps, uimaAsArgs, "org.apache.uima.ducc.user.jp.UimaProcessContainer");
				
		    	// pipelines deployed and initialized. This process is Ready
		    	currentState = ProcessState.Running;
				// Update agent with the most up-to-date state of the pipeline
			//	monitor.run();
				// all is well, so notify agent that this process is in Running state
				agent.notify(currentState, processJmxUrl);
                // Create thread pool and begin processing
				
				//for (int j = 0; j < uimaProcessor.getScaleout(); j++) {

				for (int j = 0; j < uimaProcessor.getScaleout(); j++) {
					tpe.submit(new HttpWorkerThread(this, client, uimaProcessor));
				}
				
				
				
		    } catch( Exception ee) {
		    	ee.printStackTrace();
		    	currentState = ProcessState.FailedInitialization;
				System.out
						.println(">>> Failed to Deploy UIMA Service. Check UIMA Log for Details");
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
		    	
		    }
			


		} catch( Exception e) {
			currentState = ProcessState.FailedInitialization;
			agent.notify(currentState);

			
		}

	}
	public void stop() {
		if ( super.isStopping() ) {
			return;  // already stopping - nothing to do
		}
		//configuration.stop();
		System.out.println("... AbstractManagedService - Stopping Service Adapter");
//		serviceAdapter.stop();
		System.out.println("... AbstractManagedService - Calling super.stop() ");
	    try {
        	if (getContext() != null) {
    			for (Route route : getContext().getRoutes()) {

    				route.getConsumer().stop();
    				System.out.println(">>> configFactory.stop() - stopped route:"
    						+ route.getId());
    			}
    		}
        	//jobProcessManager.
			//agent.stop();
        	uimaProcessor.stop();
        	agent.stop();
			super.stop();
	    } catch( Exception e) {
	    	e.printStackTrace();
	    }
	}
}
