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
package org.apache.uima.ducc.common.main;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;

import org.apache.camel.CamelContext;
import org.apache.camel.main.Main;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.uima.ducc.common.NodeIdentity;
import org.apache.uima.ducc.common.component.AbstractDuccComponent;
import org.apache.uima.ducc.common.exception.DuccComponentInitializationException;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

/**
 * 
 * Main program to launch Ducc Component process. Launch configuration is provided
 * in properties file via -Dducc.deploy.configuration=<file.properties>. Entries
 * in this file are used to enrich System properties. Any value in the properties
 * file can be overriden with a -D<key>=<value> where <key> matches a key in 
 * properties file. To launch multiple Ducc components in the same jvm, list them
 * in ducc.deploy.components property. For example,
 * 
 *  ducc.deploy.components=jm,rm
 *  
 *  The above will launch Job Manager and Resource Manager in the same jvm.
 *  
 *
 */
public class DuccService extends AbstractDuccComponent {
	public static final String DUCC_PROPERTY_FILE="ducc.deploy.configuration";
	public static final String DUCC_DEPLOY_COMPONENTS="ducc.deploy.components";

	private Main main;
  private DuccLogger logger;
  private ApplicationContext context;
  Map<String,AbstractDuccComponent> duccComponents = null;
	public DuccService() {
		super("");

        /**
         * Initialize the DuccLogger AFTER the properties are set.
         */
        logger = new DuccLogger(DuccService.class);		
        
        //	Plugin UncaughtExceptionHandler to handle OOM and any 
        //  unhandled runtime exceptions 
        Thread.currentThread().setUncaughtExceptionHandler(this);
	}
	/**
	 * Exits process if given configuration class is either null or not set (empty).
	 *  
	 * @param classToVerify - class name to check
	 * @param reason - 
	 */
	private void exitIfInvalid( String componentName, String classToVerify, String reason) {
		if ( classToVerify == null || classToVerify.trim().length() == 0) {
			System.out.println("Unable to start Component: "+componentName+". Missing "+reason+".\nUsage: java -DServiceConfigurationClass=<Configuration.class> ...");
			System.exit(-1);
		}
	}
	/**
	 * Extracts component configuration classes from System properties
	 * 
	 * @return - array of configuration classes
	 * 
	 * @throws DuccComponentInitializationException - if no components provided for loading
	 */
	private Class<?>[] getComponentsToLoad() throws Exception {
		String[] componentsToLoad = System.getProperty(DUCC_DEPLOY_COMPONENTS).split(",");
		if ( componentsToLoad == null || componentsToLoad.length == 0 ) {
			throw new DuccComponentInitializationException("Ducc Component not specified. Provide Ducc Component(s) to Load via -D"+DUCC_DEPLOY_COMPONENTS+" System property");
		}
		List<Class<?>> components = new ArrayList<Class<?>>(); 
		for( String componentToLoad : componentsToLoad ) {
			String configurationClassName = System.getProperty("ducc."+componentToLoad+".configuration.class");
			exitIfInvalid(componentToLoad, configurationClassName, "Configuration Class Name");
			components.add(Class.forName(configurationClassName));
		}
		Class<?>[] configClasses = new Class[components.size()];
		components.toArray(configClasses);
		return configClasses;
	}
	/**
	 * Initializes Ducc component(s) based on provided configuration.  
	 * 
	 * @throws Exception
	 */
	public void boot(String[] args) throws Exception {
        String methodName = "boot";
        // create a Main instance
        main = new Main();
        // enable hangup support so you can press ctrl + c to terminate the JVM
        main.enableHangupSupport();
        //	Load ducc properties file and enrich System properties. It supports
        //  overrides for entries in ducc properties file. Any key in the ducc 
        //	property file can be overriden with -D<key>=<value>
		loadProperties(DUCC_PROPERTY_FILE);

		System.out.println(System.getProperties());
		//	Extract component configuration classes available in System properties 
		Class<?>[] configClasses = getComponentsToLoad();
		//	Configure via Spring DI using named Spring's Java Config magic.
		//	Multiple component configurations can be loaded into a Spring container. 
		context = 
			new AnnotationConfigApplicationContext(configClasses);
		//	Extract all Ducc components from Spring container
//		Map<String,AbstractDuccComponent> duccComponents = 
		duccComponents =
			context.getBeansOfType(AbstractDuccComponent.class);
		//	Start all components
		for(Map.Entry<String, AbstractDuccComponent> duccComponent: duccComponents.entrySet()) {
			logger.info(methodName, null, "... Starting Component: ", duccComponent.getKey());
			if ( args != null && args.length > 0 ) {
				duccComponent.getValue().start(this,args);
			} else {
				duccComponent.getValue().start(this);
			}
			logger.info(methodName, null, "... Component started: ", duccComponent.getKey());
		}
		
		Runtime.getRuntime().addShutdownHook( 
            new Thread(
                    new Runnable() {
                            public void run() {
                                    System.out.println( "Shutdown hook running" );
                                    for(Map.Entry<String, AbstractDuccComponent> duccComponent: duccComponents.entrySet()) {
                                      try {
                                        duccComponent.getValue().stop();
                                      } catch ( Exception e) {
                                        logger.warn("ShutdownHook.run", null, e);
                                      }
                                      logger.info("ShutdownHook.run", null, "... Stopping Component: ", duccComponent.getKey());
                                    }
                            }       
                    }
            )
    );
		
		
    // run until you terminate the JVM
    logger.info(methodName, null, "Starting Camel. Use ctrl + c to terminate the JVM.\n");
    main.run();
       
  }
	public AbstractDuccComponent getComponentInstance(String componentKey) {
    //  Extract all Ducc components from Spring container
    Map<String,AbstractDuccComponent> duccComponents = 
      context.getBeansOfType(AbstractDuccComponent.class);
    for(Map.Entry<String, AbstractDuccComponent> duccComponent: duccComponents.entrySet()) {
      if ( componentKey.equals(duccComponent.getKey())) {
        return duccComponent.getValue();
      }
    }
	  return null;
	}
	public void stop() throws Exception {
		if ( main.isStarted() ) {
			List<CamelContext> ctxList = main.getCamelContexts();
			for( CamelContext ctx : ctxList ) {
				ctx.stop();
			}
			main.stop();
			logger.shutdown();
		}
	}
	
	public static void main(String[] args) {
		DuccService duccService = null;
		try {
			if ( System.getenv("DUCC_HOME") == null && System.getProperty("DUCC_HOME") == null ) {
				System.out.println("Unable to Launch Ducc Service - DUCC_HOME not defined. Add it to your environment or provide it with -DDUCC_HOME=<path>");
				System.exit(-1);
			}
            //	make sure DUCC_HOME is in the System properties. Needed to resolve
            //  any place holders that may exist in paths. If System properties doesnt
            //  contain this property it must exist in the environment. The DuccService.java
            //  enforces existence of DUCC_HOME in either the env or System properties. 
            if ( System.getProperty("DUCC_HOME") == null ) {
                System.setProperty("DUCC_HOME",System.getenv("DUCC_HOME"));
            }

            if ( System.getenv("IP") == null ) {
                NodeIdentity ni = new NodeIdentity();
                System.setProperty("IP", ni.getIp());
        		System.setProperty("NodeName", ni.getName());

            } else {
                System.setProperty("IP", System.getenv("IP"));
        		System.setProperty("NodeName", System.getenv("NodeName"));
            }

			duccService = new DuccService();
			duccService.boot(args);
			
		} catch( DuccComponentInitializationException e) {
			e.printStackTrace();
			if ( duccService != null ) {
				try {
					duccService.stop();
				} catch( Exception stopException) {
					stopException.printStackTrace();
				}
			}
		} catch( Exception e) {
			e.printStackTrace();
			
		}
	}
	public void setLogLevel(String clz, String level) {
		@SuppressWarnings("unchecked")
		Enumeration<Logger> loggers = LogManager.getCurrentLoggers();
        while (loggers.hasMoreElements()) {
            Logger logger = (Logger) loggers.nextElement();
            if ( logger.getName().equals(clz)) 
                logger.setLevel(Level.toLevel(level));
            	System.out.println("---------Set New Log Level:"+level+" For Logger:"+clz);
            }
        }
	public String getLogLevel(String clz) {
		@SuppressWarnings("unchecked")
		Enumeration<Logger> loggers = LogManager.getCurrentLoggers();
    	//System.out.println("---------Fetching Log Level For Logger:"+clz);
        while (loggers.hasMoreElements()) {
            Logger logger = (Logger) loggers.nextElement();
        	//System.out.println("---------Current Logger:"+logger.getName());
            if ( logger.getName().equals(clz)) {
            	//System.out.println("---------Found Log Level ("+logger.getLevel()+") ForLogger:"+logger.getName());
               if ( logger.getLevel() == null ) {
            	   logger.setLevel(Level.toLevel("info"));
               } else {
               	return logger.getLevel().toString();
               }
            }
        }
		return "";
	}
}
