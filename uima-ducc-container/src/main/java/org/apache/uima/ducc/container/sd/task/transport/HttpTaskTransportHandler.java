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

package org.apache.uima.ducc.container.sd.task.transport;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Properties;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.uima.UIMAFramework;
import org.apache.uima.ducc.common.utils.XStreamUtils;
import org.apache.uima.ducc.container.net.iface.IMetaCasTransaction;
import org.apache.uima.ducc.container.net.iface.IMetaCasTransaction.Direction;
import org.apache.uima.ducc.container.sd.iface.ServiceDriver;
import org.apache.uima.ducc.container.sd.task.error.TaskProtocolException;
import org.apache.uima.ducc.container.sd.task.iface.TaskProtocolHandler;
import org.apache.uima.ducc.container.sd.task.transport.iface.TaskTransportHandler;
import org.apache.uima.util.Level;
import org.apache.uima.util.Logger;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.thread.QueuedThreadPool;

public class HttpTaskTransportHandler implements TaskTransportHandler {
	Logger logger = UIMAFramework.getLogger(HttpTaskTransportHandler.class);
	// Jetty
	private Server server = null;
	// Delegate to handle incoming messages
	private TaskProtocolHandler taskProtocolHandler = null;
    private volatile boolean running = false;
    // mux is used to synchronize start()
    private Object mux = new Object();
	public HttpTaskTransportHandler() {
	}

	public void setTaskProtocolHandler(TaskProtocolHandler taskProtocolHandler) {
		this.taskProtocolHandler = taskProtocolHandler;
	}

	public String start() throws Exception {
		synchronized( mux ) {
			if ( !running ) {
				if ( taskProtocolHandler == null ) {
					throw new TaskProtocolException("start() called before initialize() - task protocol handler not started");
				}
				if ( server == null ) {
					throw new TaskProtocolException("start() called before initialize() - Jetty not started yet");
				}

				server.start();
				logger.log(Level.INFO, "Jetty Started - Waiting for Messages ...");
				running = true;
			}
		}
		return "";
	}

	public void stop() throws Exception {
		synchronized( mux ) {
			if ( server != null && server.isRunning() ) {
				server.stop();
			}
		}
	}

	public Server createServer(int httpPort, int maxThreads, String app,
			TaskProtocolHandler handler) throws Exception {

		// Server thread pool
		QueuedThreadPool threadPool = new QueuedThreadPool();
		if (maxThreads < threadPool.getMinThreads()) {
			// logger.warn("JobDriver", jobid,
			// "Invalid value for jetty MaxThreads("+maxThreads+") - it should be greater or equal to "+threadPool.getMinThreads()+". Defaulting to jettyMaxThreads="+threadPool.getMaxThreads());
			threadPool.setMaxThreads(threadPool.getMinThreads());
		} else {
			threadPool.setMaxThreads(maxThreads);
		}

		Server server = new Server(threadPool);

		// Server connector
		ServerConnector connector = new ServerConnector(server);
		connector.setPort(httpPort);
		server.setConnectors(new Connector[] { connector });

		ServletContextHandler context = new ServletContextHandler(
				ServletContextHandler.SESSIONS);
		context.setContextPath("/");
		server.setHandler(context);

		context.addServlet(new ServletHolder(new TaskHandlerServlet(handler)),
				app);
		logger.log(Level.INFO,"Service Driver URL: "+ context.getServer().getURI().toString()+httpPort+app);//"Jetty URL: http://localhost:"+httpPort+app);
		return server;
	}

	@Override
	public void initialize(Properties properties) throws TaskTransportException {
		// TODO Auto-generated method stub
		// Max cores
		int cores = Runtime.getRuntime().availableProcessors();
		String portString = (String) properties.get(ServiceDriver.Port);
		String maxThreadsString = (String) properties
				.get(ServiceDriver.MaxThreads);
		String appName = (String) properties
				.get(ServiceDriver.Application);

		int maxThreads = cores;
		int httpPort = -1;
		if (maxThreadsString != null) {
			try {
				maxThreads = Integer.parseInt(maxThreadsString.trim());
			} catch (NumberFormatException e) {
				logger.log(Level.WARNING,"Error",e);
			}
		}
		if (cores > maxThreads) {
			// logger.warn("JobDriver", jobid,
			// "Invalid value for jetty MaxThreads("+threadPool.getMaxThreads()+") - it should be greater or equal to "+cores+". Defaulting to Number of CPU Cores="+cores);
			maxThreads = cores;
		}
		if (portString != null) {
			try {
				httpPort = Integer.parseInt(portString.trim());
			} catch (NumberFormatException e) {
				logger.log(Level.WARNING,"Error",e);
				throw new TaskTransportException("Unable to start Server using provided port:"+httpPort);
			}
		}
		if (appName == null) {
			throw new TaskTransportException("The required "
					+ ServiceDriver.Application
					+ " property is not specified");
		}
		try {
			// create and initialize Jetty Server
			server = createServer(httpPort, maxThreads, appName, taskProtocolHandler);
		} catch (Exception e) {
			throw new TaskTransportException(e);
		}

	}



	public class TaskHandlerServlet extends HttpServlet {
		private static final long serialVersionUID = 1L;
		TaskProtocolHandler taskProtocolHandler = null;

		public TaskHandlerServlet(TaskProtocolHandler handler) {
			this.taskProtocolHandler = handler;
		}

		protected void doPost(HttpServletRequest request,
				HttpServletResponse response) throws ServletException,
				IOException {
			try {
				//long post_stime = System.nanoTime();
				StringBuilder sb = new StringBuilder();
				BufferedReader reader = request.getReader();
				String line;
				while ((line = reader.readLine()) != null) {
					sb.append(line);
				}
				// char[] content = new char[request.getContentLength()];
				String content = sb.toString().trim();

				// char[] content = new char[request.getContentLength()];

				// request.getReader().read(content);
				// logger.debug("doPost",jobid,
				// "Http Request Body:::"+String.valueOf(content));

				IMetaCasTransaction imt = null;
				// String t = String.valueOf(content);

				// imt = (IMetaCasTransaction) XStreamUtils
				// .unmarshall(t.trim());
				imt = (IMetaCasTransaction) XStreamUtils.unmarshall(content);
				//MessageHandler.accumulateTimes("Unmarshall", post_stime);

				// process service request
				taskProtocolHandler.handle(imt);

				//long marshall_stime = System.nanoTime();
				// setup reply
				imt.setDirection(Direction.Response);

				response.setStatus(HttpServletResponse.SC_OK);

				response.setHeader("content-type", "text/xml");
				String body = XStreamUtils.marshall(imt);

				response.getWriter().write(body);

				// When debugging accumulate times taken by each stage of the
				// message processing
			//	MessageHandler.accumulateTimes("Marshall", marshall_stime);
			//	MessageHandler.accumulateTimes("Post", post_stime);
			} catch (Throwable e) {
				throw new ServletException(e);
			}
		}

	}
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}
}
