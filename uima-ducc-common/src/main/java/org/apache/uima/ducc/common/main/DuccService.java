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

import java.lang.reflect.Method;
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
import org.apache.uima.ducc.common.component.IJobProcessor;
import org.apache.uima.ducc.common.exception.DuccComponentInitializationException;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.Utils;
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
    //private Investment investment = null;
	private Main main;
    private static DuccLogger globalLogger = null;
    private ApplicationContext context;
    Map<String,AbstractDuccComponent> duccComponents = null;
    
    private Object investmentInstance;
    
    private String[] args = null;
	public DuccService() {
		super("");
        
        //	Plugin UncaughtExceptionHandler to handle OOM and any 
        //  unhandled runtime exceptions 
        Thread.currentThread().setUncaughtExceptionHandler(this);
	}

    public static DuccLogger getDuccLogger()
    {
        return globalLogger;
    }

    public static DuccLogger getDuccLogger(String claz)
    {
        return globalLogger.getLoggerFor(claz);
    }

    public static DuccLogger getDuccLogger(@SuppressWarnings("rawtypes") Class claz)
    {
        return getDuccLogger(claz.getName());
    }

    public static void setDuccLogger(DuccLogger l)
    {
        globalLogger = l;
    }

    public DuccLogger getLogger()
    {
        if ( getDuccLogger() == null ) {
            return new DuccLogger(DuccService.class);
        } else {
            return getDuccLogger();
        }
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
		duccComponents =
			context.getBeansOfType(AbstractDuccComponent.class);
		//	Start all components except for JP component
		for(Map.Entry<String, AbstractDuccComponent> duccComponent: duccComponents.entrySet()) {
			// The job-process is what used to be uima-as to identify the type of component
			// The new job-process is the JP component. JP components are started differently
			// from the rest of Ducc daemons.
			if ( !(duccComponent.getValue() instanceof IJobProcessor) ) {
				getDuccLogger().info(methodName, null, "... Starting Component: ", duccComponent.getKey());
				if ( args != null && args.length > 0 ) {
					duccComponent.getValue().start(this,args);
				} else {
					duccComponent.getValue().start(this);
				}
				getDuccLogger().info(methodName, null, "... Component started: ", duccComponent.getKey());
			}
		}
		System.out.println("Starting Camel. Use ctrl + c to terminate the JVM.\n");
        // run until you terminate the JVM
        getDuccLogger().info(methodName, null, "Starting Camel. Use ctrl + c to terminate the JVM.\n");
        main.start();
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
	/**
	 * This method returns an instance of IJobProcessor which would only exist
	 * in a JP and UIMA-based AP.
	 * 
	 * @return - IJobProcessor instance
	 */
	public IJobProcessor getJobProcessorComponent() {
	    //  Extract all Ducc components from Spring container
	    Map<String,AbstractDuccComponent> duccComponents = 
	      context.getBeansOfType(AbstractDuccComponent.class);
	    // scan for component which implements IJobProcessor interface.
	    for(Map.Entry<String, AbstractDuccComponent> duccComponent: duccComponents.entrySet()) {
	      if ( duccComponent.getValue() instanceof IJobProcessor) {
	        return (IJobProcessor)duccComponent.getValue();
	      }
	    }
		return null;
	}
	/**
	 * This method is only called when launching a JP.
	 * @param instanceType
	 * @return
	 */
	public AbstractDuccComponent getComponentByInstanceType(Class<?> instanceType) {
	    //  Extract all Ducc components from Spring container
	    Map<String,AbstractDuccComponent> duccComponents = 
	      context.getBeansOfType(AbstractDuccComponent.class);
	    for(Map.Entry<String, AbstractDuccComponent> duccComponent: duccComponents.entrySet()) {
	        return duccComponent.getValue();
	    }
	    return null;
	}
    /**
	 * This method is only called when launching a JP.
     * @param ipc - instance of IProcessContainer
     * @param args - program args
     * @throws Exception
     */
	public void setProcessor(Object ipc, String[] args) throws Exception {
		AbstractDuccComponent duccComponent = getComponentByInstanceType(IJobProcessor.class);
		if ( duccComponent instanceof IJobProcessor ) {
			// store program args which will be used in start() method below
			this.args = args;
			// hand-off instance of IProcessContainer to the IJobProcessor
			((IJobProcessor)duccComponent).setProcessor(ipc, args);
		}
	}
	/**
	 * This method is only called when launching a JP. It starts the JP including
	 * initialization and starts all worker threads.
     *
	 * @throws Exception
	 */
	public void start() throws Exception {
		AbstractDuccComponent duccComponent = getComponentByInstanceType(IJobProcessor.class);
		if ( duccComponent instanceof IJobProcessor ) {
			// initialize JP and start work threads to begin processing
			duccComponent.start(this, args);
			//getDuccLogger().info("setProcessor", null, "... Component started: job-process");
		}
		
	}
	public void stop() throws Exception {
		if ( main.isStarted() ) {
			List<CamelContext> ctxList = main.getCamelContexts();
			for( CamelContext ctx : ctxList ) {
				ctx.stop();
			}
			main.stop();
			getDuccLogger().shutdown();
		}
	}
	
	public static void main(String[] args) {
		DuccService duccService = null;
		try {
			if ( Utils.findDuccHome() == null ) {
                //findDuccHome places it into System.properties
				System.out.println("Unable to Launch Ducc Service - DUCC_HOME not defined. Add it to your environment or provide it with -DDUCC_HOME=<path>");
				System.exit(-1);
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
            if ( logger.getName().equals(clz)) {
                logger.setLevel(Level.toLevel(level));
                System.out.println("---------Set New Log Level:"+level+" For Logger:"+clz);
            }
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
	public void registerInvestmentInstance(Object instance) {
		this.investmentInstance = instance;
	}
	public void registerInvestmentResetCallback(Object o, Method m) throws Exception {
		Method investmentInstanceMethod = 
				investmentInstance.getClass().getDeclaredMethod("setJobComponent", Object.class, Method.class);
		investmentInstanceMethod.invoke(investmentInstance, o,m);
	}
}
