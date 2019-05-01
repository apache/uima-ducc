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

package org.apache.uima.ducc.ps.sd.task.transport;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.HashMap;
import java.util.Properties;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.uima.UIMAFramework;

import org.apache.uima.ducc.ps.sd.iface.ServiceDriver;
import org.apache.uima.ducc.ps.sd.task.error.TaskProtocolException;
import org.apache.uima.ducc.ps.sd.task.iface.TaskProtocolHandler;
import org.apache.uima.ducc.ps.sd.task.transport.iface.TaskTransportHandler;
import org.apache.uima.ducc.ps.net.iface.IMetaTaskTransaction;
import org.apache.uima.ducc.ps.net.iface.IMetaTaskTransaction.Direction;
import org.apache.uima.ducc.ps.service.transport.XStreamUtils;
import org.apache.uima.util.Level;
import org.apache.uima.util.Logger;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.thread.QueuedThreadPool;

import com.thoughtworks.xstream.XStream;

public class HttpTaskTransportHandler implements TaskTransportHandler {
  Logger logger = UIMAFramework.getLogger(HttpTaskTransportHandler.class);

  // Jetty
  private Server server = null;

  // Delegate to handle incoming messages
  private TaskProtocolHandler taskProtocolHandler = null;

  private volatile boolean running = false;

  // mux is used to synchronize start()
  private Object mux = new Object();

  // Create ThreadLocal Map containing instances of XStream for each thread
  private ThreadLocal<HashMap<Long, XStream>> threadLocalXStream = new ThreadLocal<HashMap<Long, XStream>>() {
    @Override
    protected HashMap<Long, XStream> initialValue() {
      return new HashMap<Long, XStream>();
    }
  };

  public HttpTaskTransportHandler() {
  }

  public void setTaskProtocolHandler(TaskProtocolHandler taskProtocolHandler) {
    this.taskProtocolHandler = taskProtocolHandler;
  }

  public String start() throws Exception {
    synchronized (mux) {
      if (!running) {
        if (taskProtocolHandler == null) {
          throw new TaskProtocolException(
                  "start() called before initialize() - task protocol handler not started");
        }
        if (server == null) {
          throw new TaskProtocolException(
                  "start() called before initialize() - Jetty not started yet");
        }

        server.start();
        logger.log(Level.INFO, "Jetty Started - Waiting for Messages ...");
        running = true;
      }
    }
    return "";
  }

  public void stop() throws Exception {
    synchronized (mux) {
      if (server != null && server.isRunning()) {
        server.stop();
      }
    }
  }

  public Server createServer(int httpPort, int maxThreads, String app, TaskProtocolHandler handler)
          throws Exception {

    // Server thread pool
    QueuedThreadPool threadPool = new QueuedThreadPool();
    if (maxThreads < threadPool.getMinThreads()) {
      // logger.warn("JobDriver", jobid,
      // "Invalid value for jetty MaxThreads("+maxThreads+") - it should be greater or equal to
      // "+threadPool.getMinThreads()+". Defaulting to
      // jettyMaxThreads="+threadPool.getMaxThreads());
      threadPool.setMaxThreads(threadPool.getMinThreads());
    } else {
      threadPool.setMaxThreads(maxThreads);
    }

    Server server = new Server(threadPool);

    // Server connector
    ServerConnector connector = new ServerConnector(server);
    connector.setPort(httpPort);
    server.setConnectors(new Connector[] { connector });

    ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
    context.setContextPath("/");
    server.setHandler(context);

    context.addServlet(new ServletHolder(new TaskHandlerServlet(handler)), "/" + app);

    return server;
  }

  public int findFreePort() {
    ServerSocket socket = null;
    int port = 0;
    try {
      // by passing 0 as an arg, let ServerSocket choose an arbitrary
      // port that is available.
      socket = new ServerSocket(0);
      port = socket.getLocalPort();
    } catch (IOException e) {
    } finally {
      try {
        // Clean up
        if (socket != null) {
          socket.close();
        }
      } catch (Exception ex) {
        ex.printStackTrace();
      }
    }
    return port;
  }

  @Override
  public String initialize(Properties properties) throws TaskTransportException {
    // Max cores
    int cores = Runtime.getRuntime().availableProcessors();
    String maxThreadsString = (String) properties.get(ServiceDriver.MaxThreads);
    String appName = (String) properties.get(ServiceDriver.Application);

    int maxThreads = cores;
    int httpPort = 0;
    if (maxThreadsString != null) {
      try {
        maxThreads = Integer.parseInt(maxThreadsString.trim());
      } catch (NumberFormatException e) {
        logger.log(Level.WARNING, "Error", e);
      }
    }
    if (cores > maxThreads) {
      maxThreads = cores;
    }

    String portString = (String) properties.get(ServiceDriver.Port);
    if (portString != null) {
      try {
        httpPort = Integer.parseInt(portString.trim());
      } catch (NumberFormatException e) {
        logger.log(Level.WARNING, "Error", e);
        throw new TaskTransportException("Unable to start Server using provided port:" + httpPort);
      }
    }
    if (httpPort == 0) { // Use any free port if none or 0 specified
      httpPort = findFreePort();
    }
    if (appName == null) {
      appName = "test";
      logger.log(Level.WARNING,
              "The " + ServiceDriver.Application + " property is not specified - using " + appName);
    }
    try {
      // create and initialize Jetty Server
      server = createServer(httpPort, maxThreads, appName, taskProtocolHandler);
    } catch (Exception e) {
      throw new TaskTransportException(e);
    }

    // Establish the URL we could register for our customers
    String taskUrl = server.getURI().toString();
    if (taskUrl.endsWith("/")) {
      taskUrl = taskUrl.substring(0, taskUrl.length() - 1);
    }
    taskUrl += ":" + httpPort + "/" + appName;
    logger.log(Level.INFO, "Service Driver URL: " + taskUrl);

    return taskUrl;
  }

  public class TaskHandlerServlet extends HttpServlet {
    private static final long serialVersionUID = 1L;

    TaskProtocolHandler taskProtocolHandler = null;

    public TaskHandlerServlet(TaskProtocolHandler handler) {
      this.taskProtocolHandler = handler;
    }

    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
      try {
        StringBuilder sb = new StringBuilder();
        BufferedReader reader = request.getReader();
        String line;
        while ((line = reader.readLine()) != null) {
          sb.append(line);
        }
        String content = sb.toString().trim();
        // check ThreadLocal for a Map entry for this thread id. If not found, create
        // dedicated XStream instance for this thread which will be useed to serialize/deserialize
        // this thread's tasks

        if (threadLocalXStream.get().get(Thread.currentThread().getId()) == null) {
          threadLocalXStream.get().put(Thread.currentThread().getId(),
                  XStreamUtils.getXStreamInstance());
        }

        IMetaTaskTransaction imt = null;

        // imt = (IMetaTaskTransaction) XStreamUtils.unmarshall(content);
        // Use dedicated instance of XStream to deserialize request
        imt = (IMetaTaskTransaction) threadLocalXStream.get().get(Thread.currentThread().getId())
                .fromXML(content);

        // process service request
        taskProtocolHandler.handle(imt);

        // setup reply
        imt.setDirection(Direction.Response);

        response.setStatus(HttpServletResponse.SC_OK);

        response.setHeader("content-type", "text/xml");
        // String body = XStreamUtils.marshall(imt);
        // Use dedicated instance of XStream to serialize reply
        String body = threadLocalXStream.get().get(Thread.currentThread().getId()).toXML(imt);
        response.getWriter().write(body);

      } catch (Throwable e) {
        throw new ServletException(e);
      }
    }

  }

  public static void main(String[] args) {
    // TODO Auto-generated method stub

  }
}
