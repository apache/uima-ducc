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
package org.apache.uima.ducc.ws.server;

import java.io.File;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.jasper.servlet.JspServlet;
import org.apache.uima.ducc.common.IDuccEnv;
import org.apache.uima.ducc.common.config.CommonConfiguration;
import org.apache.uima.ducc.common.internationalization.Messages;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.DuccPropertiesResolver;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.ws.DuccPlugins;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.NCSARequestLog;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.RequestLogHandler;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.server.session.SessionHandler;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.eclipse.jetty.util.thread.ScheduledExecutorScheduler;


public class DuccWebServer {
	private static DuccLogger logger = DuccLoggerComponents.getWsLogger(DuccWebServer.class.getName());
	private static Messages messages = Messages.getInstance();
	
	private static DuccId jobid = null;
	
	
	
	 public enum ConfigValue {
		  MaxThreads("500"),
		  IdleTimeout("30000"),
		  PortHttp("42133"),
		  PortHttps("42155");
		  private String defaultValue;
		  private ConfigValue(String value) {
		      defaultValue = value;
		  }
		  public int getInt(String property) {
		      String location = "getInt";
		      int retVal = Integer.parseInt(defaultValue);
		      String desc = "[default]";
		      if(property != null) {
		         property = property.trim();
		         if(property.length() > 0) {
		            retVal = Integer.parseInt(property);
		            desc = "";
		  
		         }
		      }
		      String text = name()+"="+retVal+" "+desc;
		      logger.info(location, jobid, text.trim());
		      return retVal;
		  }
	 }
	
	/**
	 * To support https, do the following:
	 * 
	 * 1. use 'keytool -keystore keystore -alias jetty -genkey -keyalg RSA -validity 10000' to create
	 *    keystore in ducc_web/etc directory
	 * 2. in ducc.properties set SSL port, for example:
	 * 		ducc.ws.port.ssl = 42155
	 */
	
	private String rootDir = "?";
	private Server server;
	private CommonConfiguration commonConfiguration;
	
	public DuccWebServer(CommonConfiguration commonConfiguration) {
		this.commonConfiguration = commonConfiguration;
		init();
	}
	
	/**
	 * The DUCC cluster name can be set in the ducc.properties file, for example:
	 * 		ducc.cluster.name=Watson!
	 */
	public String getClusterName() {
		return commonConfiguration.clusterName;
	}
	
	/**
	 * The DUCC class definition file can be set in the ducc.properties file, for example:
	 * 		ducc.rm.class.definitions = blade.classes
	 */
	public String getClassDefinitionFile() {
		return commonConfiguration.classDefinitionFile;
	}
	
	public int getPortSsl() {
		String property = DuccPropertiesResolver.get(DuccPropertiesResolver.ducc_ws_port_https);
        int portHttps = ConfigValue.PortHttps.getInt(property);
        return portHttps;
	}
	
	public String getRootDir() {
		return rootDir;
	}

	public String getKeyStorePassword() {
		return DuccWebServerHelper.getKeyStorePassword();
	}

	public String getKeyManagerPassword() {
		return DuccWebServerHelper.getKeyManagerPassword();
	}
	
	private void init() {
		String methodName = "init";
		logger.trace(methodName, null, messages.fetch("enter"));
		logger.info(methodName, null, messages.fetchLabel("cluster name")+getClusterName());
		logger.info(methodName, null, messages.fetchLabel("class definition file")+getClassDefinitionFile());
		
		String property;

        /**                                                                                                                                                        
         * Determine server idle timeout                                                                                                                           
         * ducc.ws.idle.timeout                                                                                                                                    
         */
        property = DuccPropertiesResolver.get(DuccPropertiesResolver.ducc_ws_idle_timeout);
        int idleTimeout = ConfigValue.IdleTimeout.getInt(property);

        /**                                                                                                                                                        
         * Determine server max threads                                                                                                                            
         * ducc.ws.max.threads                                                                                                                                     
         */
        property = DuccPropertiesResolver.get(DuccPropertiesResolver.ducc_ws_max_threads);
        int maxThreads = ConfigValue.MaxThreads.getInt(property);

        /**                                                                                                                                                        
         * Determine server http port                                                                                                                              
         * ducc.ws.port                                                                                                                                            
         */
        property = DuccPropertiesResolver.get(DuccPropertiesResolver.ducc_ws_port);
        int portHttp = ConfigValue.PortHttp.getInt(property);

        /**                                                                                                                                                        
          * Determine server https port                                                                                                                             
          * ducc.ws.port.https                                                                                                                                      
          */
        property = DuccPropertiesResolver.get(DuccPropertiesResolver.ducc_ws_port_https);
        int portHttps = ConfigValue.PortHttps.getInt(property);

        try {
        	InetAddress inetAddress = InetAddress.getLocalHost();
            String host = inetAddress.getCanonicalHostName();
            DuccWebMonitor.getInstance().register(host, ""+portHttp);
        }
        catch(Exception e) {
            logger.error(methodName, jobid, e);
        }

        // === jetty.xml ===                                                                                                                                       

        // Setup Threadpool                                                                                                                                        
        QueuedThreadPool threadPool = new QueuedThreadPool();
        threadPool.setMaxThreads(maxThreads);

        // Server                                                                                                                                                          
        server = new Server(threadPool);

        // Scheduler                                                                                                                                                       
        server.addBean(new ScheduledExecutorScheduler());

        // === jetty-http.xml ===                                                                                                                                          
        ServerConnector http = new ServerConnector(server, new HttpConnectionFactory());
        http.setPort(portHttp);
        http.setIdleTimeout(idleTimeout);
        server.addConnector(http);

        // === jetty-https.xml ===                                                                                                                                         
        // SSL Context Factory                                                                                                                                             
        SslContextFactory sslContextFactory = new SslContextFactory();
        String keystore = DuccWebServerHelper.getDuccWebKeyStore();

        logger.info(methodName, jobid, "keystore="+keystore);
        HttpConfiguration http_config = new HttpConfiguration();
 		http_config.setSecureScheme("https");
 		http_config.setSecurePort(portHttps);
 		logger.info(methodName, jobid, "portHttps="+portHttps);
        HttpConfiguration https_config = new HttpConfiguration(http_config);
        https_config.addCustomizer(new SecureRequestCustomizer());
         
        ServerConnector https = new ServerConnector(server,
             new SslConnectionFactory(sslContextFactory,"http/1.1"),
             new HttpConnectionFactory(https_config));

        https.setPort(portHttps);
        sslContextFactory.setKeyStorePath(keystore);
        String pw = getKeyStorePassword();
        logger.trace(methodName, jobid, "pw="+pw);
        sslContextFactory.setKeyStorePassword(getKeyStorePassword());    
        sslContextFactory.setKeyManagerPassword(getKeyManagerPassword());
        
        server.setConnectors(new Connector[] { http });
        server.addConnector(https);
        
        // JSP
         
        ServletContextHandler jspHandler = new ServletContextHandler(ServletContextHandler.SESSIONS);
        jspHandler.setContextPath("/");
        jspHandler.setResourceBase("root");
        jspHandler.setClassLoader(Thread.currentThread().getContextClassLoader());
        jspHandler.addServlet(DefaultServlet.class, "/");
        ServletHolder jsp = jspHandler.addServlet(JspServlet.class, "*.jsp");
        jsp.setInitParameter("classpath", jspHandler.getClassPath());
        //
        ResourceHandler resourceHandler = new ResourceHandler();
        resourceHandler.setDirectoriesListed(true);
        resourceHandler.setWelcomeFiles(new String[]{ "index.html" });
        rootDir = DuccWebServerHelper.getDuccWebRoot();
        resourceHandler.setResourceBase(rootDir);
        //
        try {
			Properties properties = DuccWebProperties.get();
			String ducc_runmode = properties.getProperty("ducc.runmode","Production");
			logger.debug(methodName, null, "ducc.runmode:"+ducc_runmode);
			logger.debug(methodName, null, "rootdir:"+rootDir);
			String $runmode_jsp = rootDir+File.separator+"$banner"+File.separator+"$runmode.jsp";
			logger.debug(methodName, null, "$runmode_jsp:"+$runmode_jsp);
			File $runmodeFile = new File($runmode_jsp);
			logger.debug(methodName, null, "path:"+$runmodeFile.getAbsolutePath());
			$runmodeFile.delete();
			String text;
			if(ducc_runmode.equalsIgnoreCase("Test")) {
				text = "<html><%@ include file=\"$runmode.test.jsp\" %></html>";
			}
			else {
				text = "<html><%@ include file=\"$runmode.production.jsp\" %></html>";
			}
			PrintWriter out = new PrintWriter($runmodeFile);
			out.println(text);
			out.flush();
			out.close();
        }
        catch(Exception e) {
			logger.info(methodName, null, e);
        }
        //
        HandlerList handlers = new HandlerList();
		
        String key = "ducc.ws.requestLog.RetainDays";
        int dflt = 0;
        int requestLogRetainDays = DuccPropertiesResolver.get(key, dflt);
        logger.info(methodName, jobid, "requestLogRetainDays="+requestLogRetainDays);
        if(requestLogRetainDays > 0) {
			String requestLogTimeZone = "GMT";
			String requestLogFmt = "yyyy_MM_dd";
			String requestLogFile = IDuccEnv.DUCC_LOGS_WEBSERVER_DIR+requestLogFmt+".request.log";
			NCSARequestLog requestLog = new NCSARequestLog();
		    requestLog.setFilename(requestLogFile);
		    requestLog.setFilenameDateFormat(requestLogFmt);
		    requestLog.setRetainDays(requestLogRetainDays);
		    requestLog.setAppend(true);
		    requestLog.setExtended(true);
		    requestLog.setLogCookies(false);
		    requestLog.setLogTimeZone(requestLogTimeZone);
		    RequestLogHandler requestLogHandler = new RequestLogHandler();
		    requestLogHandler.setRequestLog(requestLog);
		    handlers.addHandler(requestLogHandler);
		    logger.info(methodName, jobid, "requestLogFile="+requestLogFile);
        }
		
        DuccHandler duccHandler = new DuccHandler(this);
        ArrayList<Handler> localHandlers = DuccPlugins.getInstance().gethandlers(this);
        DuccHandlerClassic duccHandlerClassic = new DuccHandlerClassic(this);
        DuccHandlerJsonFormat duccHandlerJson = new DuccHandlerJsonFormat(this);
        DuccHandlerProxy duccHandlerProxy = new DuccHandlerProxy();
        DuccHandlerViz duccHandlerViz = new DuccHandlerViz();
        DuccHandlerUserAuthentication duccHandlerUserAuthentication = new DuccHandlerUserAuthentication();
        SessionHandler sessionHandler = new SessionHandler();
        handlers.addHandler(sessionHandler);
        handlers.addHandler(duccHandlerUserAuthentication);
        for(Handler handler: localHandlers) {
        	handlers.addHandler(handler);
        }
        handlers.addHandler(duccHandlerJson);
        handlers.addHandler(duccHandlerProxy);
        handlers.addHandler(duccHandlerClassic);
        handlers.addHandler(duccHandlerViz);
        handlers.addHandler(duccHandler);
        handlers.addHandler(jspHandler);
        handlers.addHandler(resourceHandler);
        handlers.addHandler(new DefaultHandler());
        server.setHandler(handlers);
		
        logger.trace(methodName, null, messages.fetch("exit"));
	}
	
	public void start() throws Exception {
		server.start();
	}
	
	public void join() throws Exception {
		server.join();
	}
	
	public void stop() throws Exception {
		server.stop();
	}

}
