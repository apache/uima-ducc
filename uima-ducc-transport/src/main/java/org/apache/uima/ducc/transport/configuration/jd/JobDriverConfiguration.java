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
package org.apache.uima.ducc.transport.configuration.jd;

import java.io.BufferedReader;
import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.uima.ducc.common.NodeIdentity;
import org.apache.uima.ducc.common.config.CommonConfiguration;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.Utils;
import org.apache.uima.ducc.common.utils.XStreamUtils;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.container.jd.mh.MessageHandler;
import org.apache.uima.ducc.ps.net.iface.IMetaTaskTransaction;
import org.apache.uima.ducc.ps.net.iface.IMetaTaskTransaction.Direction;
import org.apache.uima.ducc.ps.net.impl.MetaTaskTransaction;
import org.apache.uima.ducc.transport.DuccTransportConfiguration;
import org.apache.uima.ducc.transport.configuration.jd.iface.IJobDriverComponent;
import org.apache.uima.ducc.transport.dispatcher.ProcessStateDispatcher;
import org.apache.uima.ducc.transport.event.common.IProcessState.ProcessState;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

	/**
	 * A {@link JobDriverConfiguration} to configure JobDriver component. Depends on 
	 * properties loaded by a main program into System properties. 
	 * 
	 */
	@Configuration
	@Import({DuccTransportConfiguration.class,CommonConfiguration.class})
	public class JobDriverConfiguration {
		
		private static DuccLogger logger = DuccLoggerComponents.getJdOut(JobDriverConfiguration.class.getName());
		private static DuccId jobid = null;
		private static int port = 0;
		
		//	use Spring magic to autowire (instantiate and bind) CommonConfiguration to a local variable
		@Autowired CommonConfiguration common;
		//	use Spring magic to autowire (instantiate and bind) DuccTransportConfiguration to a local variable
		@Autowired DuccTransportConfiguration jobDriverTransport;
		
		/**
		 * Instantiate {@link JobDriverEventListener} which will handle incoming messages.
		 * 
		 * @param jd - {@link JobDriverComponent} instance
		 * @return - {@link JobDriverEventListener}
		 */
		public JobDriverEventListener jobDriverDelegateListener(IJobDriverComponent jdc) {
			JobDriverEventListener jdel =  new JobDriverEventListener(jdc);
			return jdel;
		}
		/**
		 * Create a Router to handle incoming messages from a given endpoint. All messages are delegated
		 * to a provided listener. Note: Camel uses introspection to determine which method to call when
		 * delegating a message. The name of the method doesnt matter it is the argument that needs
		 * to match the type of object in the message. If there is no method with a matching argument
		 * type the message will not be delegated.
		 * 
		 * @param endpoint - endpoint where messages are expected
		 * @param delegate - {@link JobDriverEventListener} instance
		 * @return - initialized {@link RouteBuilder} instance
		 * 
		 */
		public synchronized RouteBuilder routeBuilderForIncomingRequests(final String endpoint, final JobDriverEventListener delegate) {
	        return new RouteBuilder() {
	            public void configure() {
	            	from(endpoint)
	            	.bean(delegate);
	            }
	        };
	    }

		public Server createServer(int port, String app, IJobDriverComponent jdc) throws Exception {
			
			 //HTTP port                                                                                                                                        
            int portHttp = port;
            // Server thread pool
			QueuedThreadPool threadPool = new QueuedThreadPool();
			// Max cores
			int cores = Runtime.getRuntime().availableProcessors();

			if ( common.jettyMaxThreads != null) {
				try {
					int maxThreads = Integer.parseInt(common.jettyMaxThreads.trim());
				    if ( maxThreads < threadPool.getMinThreads()) {
						logger.warn("JobDriver", jobid, "Invalid value for jetty MaxThreads("+maxThreads+") - it should be greater or equal to "+threadPool.getMinThreads()+". Defaulting to jettyMaxThreads="+threadPool.getMaxThreads());
				    } else {
						threadPool.setMaxThreads(maxThreads);
				    }
				} catch( NumberFormatException e) {
					logger.warn("JobDriver", jobid, "Invalid value for jetty MaxThreads - check ducc.properties - defaulting to "+threadPool.getMaxThreads());
				}
			}
			if ( cores > threadPool.getMaxThreads() ) {
				logger.warn("JobDriver", jobid, "Invalid value for jetty MaxThreads("+threadPool.getMaxThreads()+") - it should be greater or equal to "+cores+". Defaulting to Number of CPU Cores="+cores);
				threadPool.setMaxThreads(cores);
			} // Server                                                                                                                                          
            Server server = new Server(threadPool);

            // Server connector                                                                                                                                
            ServerConnector connector = new ServerConnector(server);
            connector.setPort(portHttp);
            server.setConnectors(new Connector[] { connector });

			 ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
		     context.setContextPath("/");
		     server.setHandler(context);
		 
		    context.addServlet(new ServletHolder(new JDServlet(jdc)),app);
		    logger.info("JobDriver",jobid,"Jetty Configuration - Port: "+port+" Threads: "+threadPool.getMinThreads()+"-"+threadPool.getMaxThreads());

		    return server;
		}
		
		public static class JobDriverProcessor  implements Processor {
			private 	IJobDriverComponent jdc;
			
			private JobDriverProcessor(IJobDriverComponent jdc) {
				this.jdc = jdc;
			}
		    public void process(Exchange exchange) throws Exception {
		        // Get the transaction object sent by the JP
		    	IMetaTaskTransaction imt = 
		        		exchange.getIn().getBody(MetaTaskTransaction.class);
		        
		    	// process JP's request
		    	jdc.handleJpRequest(imt);
		    	
		    	// setup reply 
		    	imt.setDirection(Direction.Response);

		        exchange.getOut().setHeader("content-type", "text/xml");
		        // ship it!
		        exchange.getOut().setBody(imt);
		    }
		} 
		
		/**
		 * Creates and initializes {@link JobDriverComponent} instance. @Bean annotation identifies {@link JobDriverComponent}
		 * as a Spring framework Bean which will be managed by Spring container.  
		 * 
		 * @return {@link JobDriverComponent} instance
		 * 
		 * @throws Exception
		 */
		@Bean 
		public JobDriverComponent jobDriver() throws Exception {
			String location = "jobDriver";
			try {
				// Dispatcher needed to notify Agent of this process state
				// changes.
				ProcessStateDispatcher stateNotifier =
						new ProcessStateDispatcher();
				initializing(stateNotifier);
				
				JobDriverComponent jdc = new JobDriverComponent("JobDriver", common.camelContext(), this);
		        //	Instantiate delegate listener to receive incoming messages. 
		        JobDriverEventListener delegateListener = this.jobDriverDelegateListener(jdc);
				//	Inject a dispatcher into the listener in case it needs to send
				//  a message to another component
		        delegateListener.setDuccEventDispatcher(jobDriverTransport.duccEventDispatcher(common.orchestratorStateUpdateEndpoint, jdc.getContext()));
				try {
					NodeIdentity nodeIdentity = new NodeIdentity();
					jdc.setNode(nodeIdentity.getIp());
				}
				catch(Exception e) {
					logger.error(location, jobid, e);
				}
				port = Utils.findFreePort();
				jdc.setPort(port);
				String jdUniqueId = "/jdApp";
	            Server server = createServer(port, jdUniqueId, jdc);
				server.start();
				logger.info(location,jobid,"Jetty Started - Port: "+port);
				running(stateNotifier);
				
				return jdc;
			}
			catch(Exception e) {
				logger.error(location, jobid, e);
				int code = 55;
				logger.warn(location, jobid, "halt code="+code);
				Runtime.getRuntime().halt(code);
				throw e;
			}
		}
		
		private void initializing(ProcessStateDispatcher stateNotifier) throws Exception{
			String location = "initializing";
			String args = "";
			stateNotifier.sendStateUpdate(ProcessState.Initializing.name());
			logger.info(location, jobid, args);
		}
		
		private void running(ProcessStateDispatcher stateNotifier) throws Exception {
			String location = "running";
			String args = "";
			stateNotifier.sendStateUpdate(ProcessState.Running.name());
			logger.info(location, jobid, args);
		}
		
		public class JDServlet extends HttpServlet
		{
			private static final long serialVersionUID = 1L;
			private IJobDriverComponent jdc;
			public JDServlet(IJobDriverComponent jdc) {
				this.jdc = jdc;
			}
		    protected void doPost(HttpServletRequest request, HttpServletResponse response) 
		    		throws ServletException, IOException
		    {
		    	try {
			    	long post_stime = System.nanoTime();
					StringBuilder sb = new StringBuilder();
					BufferedReader reader = request.getReader();
					String line;
					while ((line = reader.readLine()) != null ) {
						sb.append(line);
					}
					//char[] content = new char[request.getContentLength()];
					String content = sb.toString().trim();

					//char[] content = new char[request.getContentLength()];

					//request.getReader().read(content);
					logger.debug("doPost",jobid, "Http Request Body:::"+String.valueOf(content));
					
					IMetaTaskTransaction imt=null;
					//String t = String.valueOf(content);
						
//					imt = (IMetaCasTransaction) XStreamUtils
//									.unmarshall(t.trim());
					imt = (IMetaTaskTransaction) XStreamUtils
							.unmarshall(content);
					MessageHandler.accumulateTimes("Unmarshall", post_stime);
			        
			    	// process JP's request
			    	jdc.handleJpRequest(imt);
			    	
			    	long marshall_stime = System.nanoTime();
			    	// setup reply 
			    	imt.setDirection(Direction.Response);

					response.setStatus(HttpServletResponse.SC_OK);

					response.setHeader("content-type", "text/xml");
					String body = XStreamUtils.marshall(imt);
						
					response.getWriter().write(body);
					
					// When debugging accumulate times taken by each stage of the message processing
					MessageHandler.accumulateTimes("Marshall", marshall_stime);
			        MessageHandler.accumulateTimes("Post", post_stime);
		    	} catch (Throwable e) {
		    		throw new ServletException(e);
		    	}
		    }
		    

		}
		
}
