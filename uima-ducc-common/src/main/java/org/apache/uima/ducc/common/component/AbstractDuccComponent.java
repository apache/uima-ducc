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
package org.apache.uima.ducc.common.component;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.rmi.registry.LocateRegistry;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.activemq.camel.component.ActiveMQComponent;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.Route;
import org.apache.camel.builder.RouteBuilder;
import org.apache.uima.ducc.common.admin.event.DuccAdminEvent;
import org.apache.uima.ducc.common.admin.event.DuccAdminEventKill;
import org.apache.uima.ducc.common.crypto.Crypto;
import org.apache.uima.ducc.common.crypto.Crypto.AccessType;
import org.apache.uima.ducc.common.exception.DuccComponentInitializationException;
import org.apache.uima.ducc.common.exception.DuccConfigurationException;
import org.apache.uima.ducc.common.main.DuccService;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccProperties;
import org.apache.uima.ducc.common.utils.LinuxUtils;
import org.apache.uima.ducc.common.utils.Utils;

/**
 * Abstract class which every Ducc component should extend from. Provides support for loading
 * property files and resolving any placeholders in property values. Creates a special purpose Camel
 * router to receive system wide DuccAdminEvent messages.
 */
public abstract class AbstractDuccComponent implements DuccComponent,
        Thread.UncaughtExceptionHandler, AbstractDuccComponentMBean {
  private CamelContext context;

  private final String componentName;

  private JMXConnectorServer jmxConnector = null;

  private String processJmxUrl = null;

  private volatile boolean stopping;

  private DuccService service;

  private Object monitor = new Object();

  private DuccLogger logger;

  public AbstractDuccComponent(String componentName) {
    this(componentName, null);
  }

  public AbstractDuccComponent(String componentName, CamelContext context) {
    this.componentName = componentName;
    setContext(context);
    logger = getLogger();           // local to avoid global references
    if ( logger == null ) {
        System.out.println("Component '" + componentName + "' returned null logger; cannot boot.");
        System.exit(1);
    }
    DuccService.setDuccLogger(logger);          // sets the global logger
    logger.setAdditionalAppenders();           // add appenders to the non-ducc stuff in log4j.config
  }

  /**
   * Creates Camel Router for Ducc admin events. Any event arriving on this channel will be handled
   * by this abstract class.
   * 
   * @param endpoint
   *          - ducc admin endpoint
   * @param delegate
   *          - who to call when admin event arrives
   * @throws Exception
   */
  private void startAdminChannel(final String endpoint, final AbstractDuccComponent delegate)
          throws Exception {
    context.addRoutes(new RouteBuilder() {
      public void configure() {
          logger.info("configure", null, "Configuring Admin Channel on Endpoint:" + endpoint);
          onException(Exception.class).handled(true).process(new ErrorProcessor());
          
          from(endpoint).routeId("AdminRoute").unmarshal().xstream()
              .process(new AdminEventProcessor(delegate));
      }
    });
    
    logger.info("startAdminChannel", null, "Admin Channel Activated on endpoint:" + endpoint);
  }

  public class ErrorProcessor implements Processor {

      public void process(Exchange exchange) throws Exception {
          // the caused by exception is stored in a property on the exchange
          Throwable caused = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Throwable.class);
          caused.printStackTrace();
          logger.error("ErrorProcessor.process()", null, caused);
          
      }
  }

  /**
   * Loads named property file
   * 
   * @param componentProperties
   *          - property file to read
   * @throws Exception
   */
  public void loadProperties(String componentProperties) throws Exception {
    DuccProperties duccProperties = new DuccProperties();
    duccProperties.load((String) System.getProperty(componentProperties));
    // resolve any placeholders
    enrichSystemPropertiesWith(duccProperties);
    // Compose Broker URL from parts defined in ducc.properties
    composeBrokerUrl();
  }

  public void reloadProperties(String componentProperties) throws Exception {
    DuccProperties duccProperties = new DuccProperties();
    duccProperties.load((String) System.getProperty(componentProperties));
    Properties sysprops = System.getProperties();
    for (Map.Entry<Object, Object> entry : duccProperties.entrySet()) {
        String key = ((String) entry.getKey()).trim();
        sysprops.remove(key);
    }
    // resolve any placeholders
    enrichSystemPropertiesWith(duccProperties);
    // Compose Broker URL from parts defined in ducc.properties
    composeBrokerUrl();
  }

  /**
   * Resolve any placeholders in property values in provided DuccProperties
   * Adjust the *endpoint ones before copying to the System properties
   * 
   * @param duccProperties
   *          - properties to resolve
   * @throws Exception 
   */
  private void enrichSystemPropertiesWith(DuccProperties duccProperties) throws Exception {
    Properties props = System.getProperties();
    for (Map.Entry<Object, Object> entry : duccProperties.entrySet()) {
      String key = ((String) entry.getKey()).trim();
      if (!System.getProperties().containsKey(key)) {
        String value = (String) entry.getValue();
        value = Utils.resolvePlaceholderIfExists(value, duccProperties).trim();
        value = Utils.resolvePlaceholderIfExists(value, props).trim();
        if (key.endsWith(".endpoint")) {
          value = adjustTransportEndpoint(value, duccProperties.getProperty(key + ".type"));
        }
        System.setProperty(key, value);
      }
    }
  }

  /**
   * ducc.properties provides broker URL in pieces as follows: - ducc.broker.protocol -
   * ducc.broker.hostname - ducc.broker.port - ducc.broker.url.decoration Assemble the above into a
   * complete URL
   * 
   * @throws Exception
   */
  private void composeBrokerUrl() throws Exception {
    String duccBrokerProtocol;
    String duccBrokerHostname;
    String duccBrokerPort;
    String duccBrokerUrlDecoration;
    if ((duccBrokerProtocol = System.getProperty("ducc.broker.protocol")) == null) {
      throw new DuccConfigurationException(
              "Ducc Configuration Exception. Please add ducc.broker.protocol property to ducc.propeties");
    } else {
      int pos;
      // we dont expect "://" in the protocol. Strip it.
      if ((pos = duccBrokerProtocol.indexOf(":")) > -1) {
        duccBrokerProtocol = duccBrokerProtocol.substring(0, pos);
      }
    }
    if ((duccBrokerHostname = System.getProperty("ducc.broker.hostname")) == null) {
      throw new DuccConfigurationException(
              "Ducc Configuration Exception. Please add ducc.broker.hostname property to ducc.propeties");
    }
    if ((duccBrokerPort = System.getProperty("ducc.broker.port")) == null) {
      throw new DuccConfigurationException(
              "Ducc Configuration Exception. Please add ducc.broker.port property to ducc.propeties");
    }
    // broker url decoration (params) is optional
    duccBrokerUrlDecoration = System.getProperty("ducc.broker.url.decoration");
    if (duccBrokerUrlDecoration != null && duccBrokerUrlDecoration.startsWith("?")) {
      duccBrokerUrlDecoration = duccBrokerUrlDecoration.substring(1,
              duccBrokerUrlDecoration.length());
    }
    StringBuffer burl = new StringBuffer();
    burl.append(duccBrokerProtocol).append("://").append(duccBrokerHostname).append(":")
            .append(duccBrokerPort);
    if (duccBrokerUrlDecoration != null && duccBrokerUrlDecoration.trim().length() > 0) {
      burl.append("?").append(duccBrokerUrlDecoration);
    }
    System.setProperty("ducc.broker.url", burl.toString());
    // UIMA-4142 (remove annoying debug statement) 
    // logger.info("composeBrokerUrl", null, "Ducc Composed Broker URL:" + System.getProperty("ducc.broker.url"));
  }

  // Jira 3943 - Adjust endpoints only on those in ducc.properties
  public String adjustTransportEndpoint(String endpointValue, String endpointType) throws Exception {
    if (endpointType == null) {
      throw new DuccComponentInitializationException(
              "Endpoint type not specified in component properties. Specify vm, queue, or topic type value for endpoint: "
                      + endpointValue);
    } else if (endpointType.equals("vm")) {
      endpointValue = "vm:" + endpointValue;
    } else if (endpointType.equals("topic") || endpointType.equals("queue")) {
      endpointValue = "activemq:" + endpointType + ":" + endpointValue;
    } else if (endpointType.equals("socket")) {
      endpointValue = "mina:tcp://localhost:";
    } else {
      throw new DuccComponentInitializationException("Provided Endpoint type is invalid:"
              + endpointType + ". Specify vm, queue, or topic type value for endpoint: "
              + endpointValue);
    }
    return endpointValue;
  }

  public void setContext(CamelContext context) {
    this.context = context;
  }

  public CamelContext getContext() {
    return context;
  }

    /**
     * Is the event apparently issue by a DUCC registered admin or not?
     */
    public boolean validateAdministrator(DuccAdminEvent event)
    {
    	String methodName = "validate_user";
                
        String user = event.getUser();                
        byte[] auth_block= event.getAuthBlock();

        try {
            String userHome = null;
            userHome = LinuxUtils.getUserHome(user);
            
            Crypto crypto = new Crypto(user, userHome,AccessType.READER);
            String signature = (String)crypto.decrypt(auth_block);
        
            if ( ! user.equals(signature ))  {
                return false;
            }

        } catch ( Throwable t ) {
            logger.error(methodName, null, "Crypto failure:", t.toString());
            return false;
        }
       
        if ( ! user.equals(System.getProperty("user.name")) ) {
            logger.warn(methodName, null, user, "is not DUCC process owner.");
            return false;
        }

        return true;
    }

  /**
   * Called when DuccAdminEvent is received on the Ducc Admin Channel
   * 
   * @param event
   *          - admin event
   */
  public void onDuccAdminKillEvent(DuccAdminEvent event) throws Exception 
    {
	  String methodName = "onDuccAdminKillEvent";
        logger.info("onDuccAdminKillEvent", null,"\n\tDucc Process:" + componentName);
        if ( ! validateAdministrator(event) ) {
            logger.info(methodName, null, "Failed authentication/authorization Ignoring shutdown event.");
            return;
        }
        logger.info(methodName, null, "Received Kill Event - Cleaning Up and Stopping");
        stop();
        System.exit(2);
  }

  public void start(DuccService service) throws Exception {
    start(service, null);
  }
  
    public void start(DuccService service, String[] args) throws Exception {
	    String endpoint = null;
	    this.service = service;
	    if (System.getProperty("ducc.deploy.components") != null
	            && !System.getProperty("ducc.deploy.components").equals("uima-as")
	             && !System.getProperty("ducc.deploy.components").equals("jd")
	            && (endpoint = System.getProperty("ducc.admin.endpoint")) != null) {
	        logger.info("start", null, ".....Starting Admin Channel on endpoint:" + endpoint);
            startAdminChannel(endpoint, this);
            logger.info("start", null, "Admin Channel started on endpoint:" + endpoint);
	    }
        logger.info("start",null, ".....Starting Camel Context");
	    // Start Camel
	    context.start();
	    List<Route> routes  = context.getRoutes();
	   
	    for( Route route : routes ) {
	    	 context.startRoute(route.getId());
             logger.info("start",null, "---OR Route in Camel Context-"+route.getEndpoint().getEndpointUri()+" Route State:"+context.getRouteStatus(route.getId()));
	    }
        logger.info("start",null, "..... Camel Initialized and Started");

	    // Instrument this process with JMX Agent. The Agent will
	    // find an open port and start JMX Connector allowing
	    // jmx clients to connect to this jvm using standard
	    // jmx connect url. This process does not require typical
	    // -D<jmx params> properties. Currently the JMX does not
	    // use security allowing all clients to connect.

        logger.info("start",null, "..... Starting JMX Agent");
	    processJmxUrl = startJmxAgent();
	    if (processJmxUrl != null && processJmxUrl.trim().length() > 0 ) {
	        logger.info("start",null, "..... JMX Agent Ready");
	        logger.info("start",null, "Connect jConsole to this process using JMX URL:" + processJmxUrl);
	      }
	    System.getProperties().setProperty("ducc.jmx.url", processJmxUrl);

	    if ( !System.getProperty("ducc.deploy.components").equals("uima-as")
	             && !System.getProperty("ducc.deploy.components").equals("jd")) {
		    ServiceShutdownHook shutdownHook = new ServiceShutdownHook(this, logger);
		    Runtime.getRuntime().addShutdownHook(shutdownHook);
	    }
	    // Register Ducc Component MBean with JMX.
	    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

	    ObjectName name = new ObjectName(
	            "org.apache.uima.ducc.service.admin.jmx:type=DuccComponentMBean,name="
	                    + getClass().getSimpleName());
	    mbs.registerMBean(this, name);
  }

  protected String getProcessJmxUrl() {
    return processJmxUrl;
  }

    public void stop() 
        throws Exception 
    {
        String methodName = "stop";

	    synchronized (monitor) {
	        if (stopping) {
                return;
	        }
	        stopping = true;
        }
        logger.info(methodName, null, "----------stop() called");
 
        try {
        	logger.info(methodName, null, "Stopping Camel Routes");
            List<Route> routes = context.getRoutes();
            for (Route route : routes) {
                if ( !route.getId().startsWith("mina")) {
                    logger.info(methodName, null, "Stopping Route:"+route.getId());
                    route.getConsumer().stop();
                    route.getEndpoint().stop();
                }
            }

            ActiveMQComponent amqc = (ActiveMQComponent) context.getComponent("activemq");
            amqc.stop();
            amqc.shutdown();

            logger.info(methodName, null, "Stopping Camel Context");
            context.stop();
            logger.info(methodName, null, "Camel Context Stopped");
            

            ObjectName name = new ObjectName(
                                             "org.apache.uima.ducc.service.admin.jmx:type=DuccComponentMBean,name="
                                             + getClass().getSimpleName());
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            Set<?> set = mbs.queryMBeans(name, null);
            if (set.size() > 0) {
                mbs.unregisterMBean(name);
            }

            if (jmxConnector != null) {
                jmxConnector.stop();
            }

            if (service != null) {
                service.stop();
            }
            logger.info(methodName, null, "Component cleanup completed - terminating process");

        } catch (Exception e) {
            // It's a sensitive time, let's emit twice just for luck
            System.out.println("----------------------------------------------------------------------------------------------------");
            e.printStackTrace();
            System.out.println("----------------------------------------------------------------------------------------------------");
            logger.error(methodName, null, e);
        }
        
        long waitTime=0;
        if ( System.getProperty("WaitTime") != null) {
            try {
                synchronized( this ) {
                    waitTime = Long.valueOf(System.getProperty("WaitTime"));
                    if ( waitTime > 0) {
                        wait(waitTime);
                    }
                }
            } catch( Exception e) {
		   
            }
	   
        }
        //  System.exit(0);
    }

  public void handleUncaughtException(Exception e) {
    e.printStackTrace();
  }

  /** 
   * Start RMI registry so the JMX clients can connect to the JVM via JMX.
   * 
   * @return JMX connect URL
   * @throws Exception
   */
  public String startJmxAgent() throws Exception {
      int rmiRegistryPort = 2099; // start with a default port setting
      if (System.getProperty("ducc.jmx.port") != null) {
        try {
          int tmp = Integer.valueOf(System.getProperty("ducc.jmx.port"));
          rmiRegistryPort = tmp;
        } catch (NumberFormatException nfe) {
          // default to 2099
        	nfe.printStackTrace();
        }
      }
      boolean done = false;
      JMXServiceURL url = null;
      
      // retry until a valid rmi port is found
      while (!done) {
        	try {
             	//	 set up RMI registry on a given port
        		LocateRegistry.createRegistry(rmiRegistryPort);
                done = true;
                // Got a valid port
        	} catch( Exception exx) {
                // Try again with a different port
                rmiRegistryPort++;
        	}
      } // while

      try {
        	MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
         
            final String hostname = InetAddress.getLocalHost().getHostName();
            String s = String.format("service:jmx:rmi:///jndi/rmi://%s:%d/jmxrmi", hostname,
                    rmiRegistryPort);
            url = new JMXServiceURL(s);
            jmxConnector = JMXConnectorServerFactory.newJMXConnectorServer(url, null, mbs);
            jmxConnector.start();
        } catch (Exception e) {
        	url = null;
            logger.error("startJmxAgent", null, "Unable to Start JMX Connector. Running with *No* JMX Connectivity");
        }
      if ( url == null ) {
    	  return "";    // empty string
      } else {
          return url.toString();
      }
  }

  public void cleanup(Throwable e) {
    e.printStackTrace();
  }

  public void uncaughtException(final Thread t, final Throwable e) {
    e.printStackTrace();
    System.exit(1);
  }

  public class AdminEventProcessor implements Processor {
    final AbstractDuccComponent delegate;

    public AdminEventProcessor(final AbstractDuccComponent delegate) {
      this.delegate = delegate;
    }

    public void process(final Exchange exchange) throws Exception {
        logger.info("AdminEventProcessor.process()", null, "Received Admin Message of Type:"
                    + exchange.getIn().getBody().getClass().getName());
        
        if (exchange.getIn().getBody() instanceof DuccAdminEventKill) {
            // start a new thread to process the admin kill event. Need to do this
        // so that Camel thread associated with admin channel can go back to
        // its pool. Otherwise, we will not be able to stop the admin channel.
        Thread th = new Thread(new Runnable() {
          public void run() {
            try {
              delegate.onDuccAdminKillEvent((DuccAdminEventKill) exchange.getIn().getBody());
            } catch (Exception e) {

            }
          }
        });
        th.start();
      } else {
        handleAdminEvent((DuccAdminEvent) exchange.getIn().getBody());
      }
    }
  }

  /**
   * Components interested in receiving DuccAdminEvents should override this method
   */
  public void handleAdminEvent(DuccAdminEvent event) throws Exception {
  }

  static class ServiceShutdownHook extends Thread {
    private AbstractDuccComponent duccProcess;
    private DuccLogger logger;
    
    public ServiceShutdownHook(AbstractDuccComponent service, DuccLogger logger ) {
      this.duccProcess = service;
      this.logger = logger;
    }

    public void run() {
      try {
          logger.info("start",null, "DUCC Service Caught Kill Signal - Registering Killer Task and Stopping ...");

        // schedule a kill task which will kill this process after 1 minute
        Timer killTimer = new Timer();
        killTimer.schedule(new KillerThreadTask(logger), 60 * 1000);

        // try to stop the process cleanly
        duccProcess.stop();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  // This task will run if stop() fails to stop the process within 1 minute
  static class KillerThreadTask extends TimerTask {
    DuccLogger logger;
    public KillerThreadTask(DuccLogger logger) {
      this.logger = logger;
    }
    public void run() {
      try {
          logger.info("start",null,"Process is about to kill itself via Runtime.getRuntime().halt()");

        // Take the jvm down hard. This call will not
        // invoke registered ShutdownHooks and just
        // terminates the jvm.
        Runtime.getRuntime().halt(-1);
        
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  public void setLogLevel(String clz, String level) {
    service.setLogLevel(clz, level);
  }

  public void setLogLevel(String level) {
    service.setLogLevel(getClass().getCanonicalName(), level);
  }

  public String getLogLevel() {
    return service.getLogLevel(getClass().getCanonicalName());
  }

  public boolean isStopping() {
    return stopping;
  }
}
