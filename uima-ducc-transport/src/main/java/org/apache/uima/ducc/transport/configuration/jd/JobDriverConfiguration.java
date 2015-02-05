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
import org.apache.uima.ducc.container.net.iface.IMetaCasTransaction;
import org.apache.uima.ducc.container.net.iface.IMetaCasTransaction.Direction;
import org.apache.uima.ducc.container.net.impl.MetaCasTransaction;
import org.apache.uima.ducc.transport.DuccTransportConfiguration;
import org.apache.uima.ducc.transport.configuration.jd.iface.IJobDriverComponent;
import org.apache.uima.ducc.transport.event.JdStateDuccEvent;
import org.eclipse.jetty.server.Server;
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

		
		/**
		 * Creates Camel router that will publish Dispatched Job state at regular intervals.
		 * 
		 * @param targetEndpointToReceiveJdStateUpdate - endpoint where to publish Jd state 
		 * @param statePublishRate - how often to publish state
		 * @return
		 * @throws Exception
		 */
		private RouteBuilder routeBuilderForJdStatePost(final IJobDriverComponent jdc, final String targetEndpointToReceiveJdStateUpdate, final int statePublishRate) throws Exception {
			final JobDriverStateProcessor jdsp =  // an object responsible for generating the state 
				new JobDriverStateProcessor(jdc);
			
			return new RouteBuilder() {
			      public void configure() {
			        from("timer:jdStateDumpTimer?fixedRate=true&period=" + statePublishRate)
			                .process(jdsp)
			                .to(targetEndpointToReceiveJdStateUpdate);
			      }
			    };

		}
		/*
		private RouteBuilder routeBuilderForJpIncomingRequests(final JobDriverComponent jdc, final int port, final String app) throws Exception {
		    return new RouteBuilder() {
		        public void configure() throws Exception {
		        	CamelContext camelContext = jdc.getContext();
		            JettyHttpComponent jetty = new JettyHttpComponent();
		            jetty.setMaxThreads(10);  // Need to parameterize
		            jetty.setMinThreads(1);
		            
		            camelContext.addComponent("jetty", jetty);
		            // listen on all interfaces.
		            from("jetty:http://0.0.0.0:" + port + "/"+app)
		            .unmarshal().xstream().
		            process(new JobDriverProcessor(jdc)).marshal().xstream();
		        }
		    };
		}
		*/
		public Server createServer(int port, String app, IJobDriverComponent jdc) throws Exception {
			Server server = new Server(port);
			QueuedThreadPool threadPool = new QueuedThreadPool();
			threadPool.setMaxThreads(10);
			server.setThreadPool(threadPool);
			
			 ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
		     context.setContextPath("/");
		     server.setHandler(context);
		 
		    context.addServlet(new ServletHolder(new JDServlet(jdc)),app);
			return server;
		}
		
		public static class JobDriverProcessor  implements Processor {
			private 	IJobDriverComponent jdc;
			
			private JobDriverProcessor(IJobDriverComponent jdc) {
				this.jdc = jdc;
			}
		    public void process(Exchange exchange) throws Exception {
		        // Get the transaction object sent by the JP
		    	IMetaCasTransaction imt = 
		        		exchange.getIn().getBody(MetaCasTransaction.class);
		        
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
		 * Camel Processor responsible for generating Dispatched Job's state.
		 * 
		 */
		private class JobDriverStateProcessor implements Processor {
			private IJobDriverComponent jdc;
			
			private JobDriverStateProcessor(IJobDriverComponent jdc) {
				this.jdc = jdc;
			}
			public void process(Exchange exchange) throws Exception {
				// Fetch new state from Dispatched Job
				JdStateDuccEvent sse = jdc.getState();
				//	Add the state object to the Message
				exchange.getIn().setBody(sse);
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
				JobDriverComponent jdc = new JobDriverComponent("JobDriver", common.camelContext(), this);
		        //	Instantiate delegate listener to receive incoming messages. 
		        JobDriverEventListener delegateListener = this.jobDriverDelegateListener(jdc);
				//	Inject a dispatcher into the listener in case it needs to send
				//  a message to another component
		        delegateListener.setDuccEventDispatcher(jobDriverTransport.duccEventDispatcher(common.orchestratorStateUpdateEndpoint, jdc.getContext()));
				//	Inject Camel Router that will delegate messages to JobDriver delegate listener
				jdc.getContext().addRoutes(this.routeBuilderForIncomingRequests(common.orchestratorAbbreviatedStateUpdateEndpoint, delegateListener));
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
//				jdc.getContext().addRoutes(this.routeBuilderForJpIncomingRequests(jdc, port, jdUniqueId));
	            Server server = createServer(port, jdUniqueId, jdc);
				server.start();
				logger.info(location,jobid,"Jetty Running - Port:"+port);
				logger.info(location, jobid, "port: "+port+" "+"endpoint: "+common.jdStateUpdateEndpoint+" "+"rate: "+common.jdStatePublishRate);

				jdc.getContext().addRoutes(this.routeBuilderForJdStatePost(jdc, common.jdStateUpdateEndpoint, Integer.parseInt(common.jdStatePublishRate)));
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
					
					IMetaCasTransaction imt=null;
					//String t = String.valueOf(content);
						
//					imt = (IMetaCasTransaction) XStreamUtils
//									.unmarshall(t.trim());
					imt = (IMetaCasTransaction) XStreamUtils
							.unmarshall(content);
			        
			    	// process JP's request
			    	jdc.handleJpRequest(imt);
			    	
			    	// setup reply 
			    	imt.setDirection(Direction.Response);

					response.setStatus(HttpServletResponse.SC_OK);

					response.setHeader("content-type", "text/xml");
					String body = XStreamUtils.marshall(imt);
						
					response.getWriter().write(body);
		    		
		    	} catch (Throwable e) {
		    		throw new ServletException(e);
		    	}
		    }
		}
}
