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
package org.apache.uima.ducc.ps;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.uima.UIMAFramework;
import org.apache.uima.cas.CAS;
import org.apache.uima.ducc.ps.net.iface.IMetaTask;
import org.apache.uima.ducc.ps.net.iface.IMetaTaskTransaction;
import org.apache.uima.ducc.ps.net.iface.IMetaTaskTransaction.Direction;
import org.apache.uima.ducc.ps.net.impl.MetaTask;
import org.apache.uima.ducc.ps.service.transport.XStreamUtils;
import org.apache.uima.ducc.ps.service.utils.UimaSerializer;
import org.apache.uima.resource.metadata.impl.TypeSystemDescription_impl;
import org.apache.uima.util.CasCreationUtils;
import org.apache.uima.util.Level;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.junit.After;

public class Client {
	private Server server;
	private boolean block = false;
	private AtomicLong errorCount = new AtomicLong();
	private final static String app="test";
	private int httpPort = 12222;
	private int maxThreads = 50;
	private static UimaSerializer uimaSerializer = new UimaSerializer();
	private AtomicInteger correlationIdCounter = 
			new AtomicInteger(0);
	private AtomicInteger atomicCounter =
			new AtomicInteger(1);
	private AtomicInteger atomicErrorCounter =
			new AtomicInteger(10);
	private volatile boolean noMoreErrors = false;
	protected String getApp() {
		return app;
	}
	protected int getJettyPort() {
		while(true) {
			ServerSocket socket=null;
			try {
				socket = new ServerSocket(httpPort);
				break;
			} catch( IOException e) {
				httpPort++;
			} finally {
				if ( socket != null ) {
					try {
						socket.close();
					} catch( Exception ee) {}
					
				}
			}
		}
		return httpPort;
	}
	protected int getPort() {

		return httpPort;
	}
	   
	public void startJetty(boolean block) throws Exception {
		this.block = block;
	    	
	        
			QueuedThreadPool threadPool = new QueuedThreadPool();
			if (maxThreads < threadPool.getMinThreads()) {
				System.out.println(
				"Invalid value for jetty MaxThreads("+maxThreads+") - it should be greater or equal to "+threadPool.getMinThreads()+". Defaulting to jettyMaxThreads="+threadPool.getMaxThreads());
				threadPool.setMaxThreads(threadPool.getMinThreads());
			} else {
				threadPool.setMaxThreads(maxThreads);
			}

		    server = new Server(threadPool);

			// Server connector
			ServerConnector connector = new ServerConnector(server);
			connector.setPort(getJettyPort());
			server.setConnectors(new Connector[] { connector });
	        System.out.println("launching Jetty on Port:"+connector.getPort());
			ServletContextHandler context = new ServletContextHandler(
					ServletContextHandler.SESSIONS);
			context.setContextPath("/");
			server.setHandler(context);

			context.addServlet(new ServletHolder(new TaskHandlerServlet()), "/"+app);
	        
	        
			server.start();
	        System.out.println("Jetty Started - Waiting for Messages ...");
	    }

	    @After
	    public void stopJetty()
	    {
	        try
	        {
	        	if ( server != null ) {
		        	UIMAFramework.getLogger().log(Level.INFO, "Stopping Jetty");
		            server.stop();
	        		
	        	}
	        }
	        catch (Exception e)
	        {
	            e.printStackTrace();
	        }
	        UIMAFramework.getLogger().log(Level.INFO,"Jetty Stopped");
	    }
		public class TaskHandlerServlet extends HttpServlet {
			private static final long serialVersionUID = 1L;

			public TaskHandlerServlet() {
			}

			protected void doPost(HttpServletRequest request,
					HttpServletResponse response) throws ServletException,
					IOException {
				try {
					//System.out.println("Handling HTTP Post Request");
					//long post_stime = System.nanoTime();
					StringBuilder sb = new StringBuilder();
					BufferedReader reader = request.getReader();
					String line;
					while ((line = reader.readLine()) != null) {
						sb.append(line);
					}
					String content = sb.toString().trim();

					//System.out.println( "Http Request Body:::"+String.valueOf(content));
					
					
		    		 String nodeIP = request.getHeader("IP");
		             String nodeName = request.getHeader("Hostname");
		             String threadID = request.getHeader("ThreadID");
		             String pid = request.getHeader("PID");
				//	System.out.println( "Sender ID:::Node IP"+nodeIP+" Node Name:"+nodeName+" PID:"+pid+" ThreadID:"+threadID);

					IMetaTaskTransaction imt = null;

					imt = (IMetaTaskTransaction) XStreamUtils.unmarshall(content);
					IMetaTaskTransaction.Type type = imt.getType();
					switch(type) {
					case Get:
						System.out.println("---- Driver handling GET Request -- Thread:"+Thread.currentThread().getId());
						imt.setMetaTask(getMetaMetaCas());
						imt.getMetaTask().setAppData("CorrelationID-"+correlationIdCounter.incrementAndGet());
						if ( System.getProperty("simulate.no.work") == null || noMoreErrors) {
							imt.getMetaTask().setUserSpaceTask(getSerializedCAS());
						} else {
							imt.getMetaTask().setUserSpaceTask(null);
							if ( atomicErrorCounter.decrementAndGet() == 0 ) {
								noMoreErrors = true;
							}
						}
					//	handleMetaCasTransationGet(trans, taskConsumer);
						break;
					case Ack:
						System.out.println("---- Driver handling ACK Request - ");
						//handleMetaCasTransationAck(trans, taskConsumer);
						break;
					case End:
						System.out.println("---- Driver handling END Request - "+imt.getMetaTask().getAppData());
						//handleMetaCasTransationEnd(trans, taskConsumer);
						if ( imt.getMetaTask().getUserSpaceException() != null ) {
							System.out.println("Client received error#"+errorCount.incrementAndGet());
						}
						break;
					case InvestmentReset:
					//	handleMetaCasTransationInvestmentReset(trans, rwt);
						break;
					default:
						break;
					}
					// process service request
					//taskProtocolHandler.handle(imt);

					//long marshall_stime = System.nanoTime();
					// setup reply
					
					imt.setDirection(Direction.Response);

					response.setStatus(HttpServletResponse.SC_OK);

					response.setHeader("content-type", "text/xml");
					String body = XStreamUtils.marshall(imt);

					if (block ) {
						synchronized(this) {
							this.wait(0);
						}
						
					}
					System.out.println("Sending response");
					response.getWriter().write(body);
					
					
					//response.getWriter().write(content);
				} catch( InterruptedException e) {
					Thread.currentThread().interrupt();
				}
				catch (Throwable e) {
					e.printStackTrace();
					throw new ServletException(e);
				}
			}

		}
		public long getErrorCount() {
			return errorCount.get();
		}
		private IMetaTask getMetaCas(String serializedCas) {
			if ( serializedCas == null ) {
				return null;
			}
			return new MetaTask(atomicCounter.incrementAndGet(), "", serializedCas);
		}

		private IMetaTask getMetaMetaCas() {
			//IMetaMetaCas mmc = new MetaMetaCas();
					
			String serializedCas = "Bogus";

			IMetaTask metaCas = getMetaCas(serializedCas);
			
		//	mmc.setMetaCas(metaCas);
			//return mmc;
			return metaCas;
		}
		public String getSerializedCAS() {
			//logger.log(Level.INFO,"getSerializedCAS() Call "+seqno.incrementAndGet()
			//        + " - from "+taskConsumer.getType()+":"+taskConsumer.getHostName()+"-"+taskConsumer.getPid()+"-"+taskConsumer.getThreadId() );
			String serializedCas = null;
			try {
				CAS cas = null;
				cas = CasCreationUtils.createCas(new TypeSystemDescription_impl(), null, null);
				cas.setDocumentLanguage("en");
				
				//logger.log(Level.INFO,"delivering: " + text);
				cas.setDocumentText("TEST");
//				cas.setDocumentText("100 "+seqno.incrementAndGet()+" 1000 0");

				serializedCas = serialize(cas);
				cas.reset();
				cas.release();

			} catch( Exception e) {
				//logger.log(Level.WARNING,"Error",e);
			}

			return serializedCas;
		}
		private String serialize(CAS cas) throws Exception {
			String serializedCas = uimaSerializer.serializeCasToXmi(cas);
			return serializedCas;
		}
}
