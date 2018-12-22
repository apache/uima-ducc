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
package org.apache.uima.ducc.ps.transport;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.ServerSocket;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.uima.ducc.ps.service.registry.DefaultRegistryClient;
import org.apache.uima.ducc.ps.service.transport.ITargetURI;
import org.apache.uima.ducc.ps.service.transport.http.HttpServiceTransport;
import org.apache.uima.ducc.ps.service.transport.http.HttpServiceTransport.HttpClientExceptionGenerator.ERROR;
import org.apache.uima.ducc.ps.service.transport.target.HttpTargetURI;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class JunitTransportTestCase {
	private Server server;
	private final static String app="test";
	private int httpPort = 12222;
	private int maxThreads = 20;
	private int getJettyPort() {
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
	private int getPort() {

		return httpPort;
	}
    @Before
    public void startJetty() throws Exception
    {

        
		QueuedThreadPool threadPool = new QueuedThreadPool();
		if (maxThreads < threadPool.getMinThreads()) {
			// logger.warn("JobDriver", jobid,
			// "Invalid value for jetty MaxThreads("+maxThreads+") - it should be greater or equal to "+threadPool.getMinThreads()+". Defaulting to jettyMaxThreads="+threadPool.getMaxThreads());
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
            server.stop();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        System.out.println("Jetty Stopped");
    }
    private void wait(DefaultRegistryClient registryClient) {
    	synchronized(registryClient) {
    		try {
        		registryClient.wait(5*1000); 
    			
    		} catch( InterruptedException e) {}
    	}
    }
    @Test
    public void testTransportBasicConnectivity() throws Exception
    { 
    	int scaleout = 12;
    	ITargetURI targetUrl = new HttpTargetURI("http://localhost:"+getPort()+"/"+app);
    	DefaultRegistryClient registryClient =
    			new DefaultRegistryClient(targetUrl);
    	HttpServiceTransport transport = new HttpServiceTransport(registryClient, scaleout);
    	transport.initialize();
    	//String response = transport.getWork("Test");
    	//System.out.println("Test Received Response:"+response);

//        assertThat("Response Code", http.getResponseCode(), (equal((HttpStatus.OK_200)));
    }
    @Test
    public void testTransportIOException() throws Exception
    { 
    	System.out.println(".... Test::testTransportIOException");
    	int scaleout = 12;
    	ITargetURI targetUrl = new HttpTargetURI("http://localhost:"+getPort()+"/"+app);
    	DefaultRegistryClient registryClient =
    			new DefaultRegistryClient(targetUrl);
    	HttpServiceTransport transport = new HttpServiceTransport(registryClient, scaleout);
    	transport.initialize();
    	System.setProperty("MockHttpPostError", ERROR.IOException.name());
    	transport.dispatch("Dummy Message");
 
    	wait(registryClient);
    }
    @Test
    public void testTransportNoRoutToHostException() throws Exception
    { 
    	System.out.println(".... Test::testTransportNoRoutToHostException");
    	int scaleout = 12;
    	ITargetURI targetUrl = new HttpTargetURI("http://localhost:"+getPort()+"/"+app);
    	DefaultRegistryClient registryClient =
    			new DefaultRegistryClient(targetUrl);
    	HttpServiceTransport transport = new HttpServiceTransport(registryClient, scaleout);
    	transport.initialize();
    	System.setProperty("MockHttpPostError", ERROR.NoRouteToHostException.name());
    	transport.dispatch("Dummy Message");
    	wait(registryClient);

    }
    @Test
    public void testTransportURISyntaxException() throws Exception
    { 
    	System.out.println(".... Test::testTransportURISyntaxException");
    	int scaleout = 12;
    	ITargetURI targetUrl = new HttpTargetURI("http://localhost:"+getPort()+"/"+app);
    	DefaultRegistryClient registryClient =
    			new DefaultRegistryClient(targetUrl);
    	HttpServiceTransport transport = new HttpServiceTransport(registryClient, scaleout);
    	transport.initialize();
    	System.setProperty("MockHttpPostError", ERROR.URISyntaxException.name());
    	transport.dispatch("Dummy Message");
    	wait(registryClient);

    }
	public class TaskHandlerServlet extends HttpServlet {
		private static final long serialVersionUID = 1L;

		public TaskHandlerServlet() {
		}

		protected void doPost(HttpServletRequest request,
				HttpServletResponse response) throws ServletException,
				IOException {
			try {
				System.out.println("Handling HTTP Post Request");
				//long post_stime = System.nanoTime();
				StringBuilder sb = new StringBuilder();
				BufferedReader reader = request.getReader();
				String line;
				while ((line = reader.readLine()) != null) {
					sb.append(line);
				}
				String content = sb.toString().trim();

				System.out.println( "Http Request Body:::"+String.valueOf(content));
				
				
	    		 String nodeIP = request.getHeader("IP");
	             String nodeName = request.getHeader("Hostname");
	             String threadID = request.getHeader("ThreadID");
	             String pid = request.getHeader("PID");
				System.out.println( "Sender ID:::Node IP"+nodeIP+" Node Name:"+nodeName+" PID:"+pid+" ThreadID:"+threadID);

				response.getWriter().write(content);
			} catch (Throwable e) {
				e.printStackTrace();
				throw new ServletException(e);
			}
		}

	}
}
