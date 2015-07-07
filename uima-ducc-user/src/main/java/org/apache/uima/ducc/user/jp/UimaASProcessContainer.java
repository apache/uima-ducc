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

package org.apache.uima.ducc.user.jp;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.BindException;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.uima.UIMAFramework;
import org.apache.uima.aae.UimaASApplicationEvent.EventTrigger;
import org.apache.uima.aae.UimaAsVersion;
import org.apache.uima.aae.client.UimaASProcessStatus;
import org.apache.uima.aae.client.UimaAsBaseCallbackListener;
import org.apache.uima.aae.client.UimaAsynchronousEngine;
import org.apache.uima.aae.monitor.statistics.AnalysisEnginePerformanceMetrics;
import org.apache.uima.adapter.jms.client.BaseUIMAAsynchronousEngine_impl;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.analysis_engine.impl.AnalysisEngineManagementImpl;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.impl.XmiSerializationSharedData;
import org.apache.uima.collection.EntityProcessStatus;
import org.apache.uima.impl.UimaVersion;
import org.apache.uima.util.Level;
import org.apache.uima.util.Logger;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

public class UimaASProcessContainer  extends DuccAbstractProcessContainer {
	private String endpointName;
	private String saxonURL = null;
	private String xslTransform = null;
	private static BaseUIMAAsynchronousEngine_impl uimaASClient = null;
	private static final CountDownLatch brokerLatch = new CountDownLatch(1);
	private static Object brokerInstance = null;
	private static Class<?> classToLaunch=null;
	private static volatile boolean brokerRunning = false;
	protected Object initializeMonitor = new Object();
	public volatile boolean initialized = false;
	private static final Class<?> CLASS_NAME = UimaASProcessContainer.class;
	private static final char FS = System.getProperty("file.separator").charAt(
			0);
	// use this map to pin each thread to its own instance of UimaSerializer
//	private static Map<Long, UimaSerializer> serializerMap = new HashMap<Long, UimaSerializer>();
    private String[] deploymentDescriptors = null;
	private String[] ids = null;
    private String duccHome=null;
    
    private volatile boolean threadAffinity=false;
	boolean enablePerformanceBreakdownReporting = false;

	
	public boolean useThreadAffinity() {
	  return threadAffinity;
	}	
	private int generateDescriptorsAndGetScaleout(String[] args) throws Exception {
		deploymentDescriptors = getDescriptors(args);
		ids = new String[deploymentDescriptors.length];
		return scaleout;
	}
	public int doInitialize(Properties props, String[] args) throws Exception {
		duccHome = System.getProperty("DUCC_HOME"); 
        String pid = getPID("Queue");
		if ( System.getenv("NodeName") != null) {
			endpointName = System.getenv("NodeName")+pid;
		} else {
			endpointName = InetAddress.getLocalHost().getCanonicalHostName()+pid;
		}
		// Needed to resolve ${queue.name} placeholder in DD generated 
		// by DUCC
		System.setProperty("queue.name", endpointName);
		String jobType = System.getProperty("ducc.deploy.JpType"); 
		if ( "uima-as".equals(jobType)) {
			System.out.println("UIMA-AS Version:"+UimaAsVersion.getFullVersionString());
        } 
		// enable performance breakdown reporting for uima sdk
		// version 2.6+ or higher. These versions include changes
		// to make performance aggregation thread safe. Fields
		// to accumulate performance are AtomicLong's instead of long.
		try {
			if ( UimaVersion.getMajorVersion() >=2 && UimaVersion.getMinorVersion() >= 6) {
				AnalysisEngineManagementImpl aemi = 
						new AnalysisEngineManagementImpl();
				// use reflection to make sure the new uima sdk is being used.
				// The new code will use AtomicLong instead of long for
				// analysisTime field
				Field f = aemi.getClass().getDeclaredField("analysisTime");
				f.setAccessible(true);
	            if ( f.getType().isAssignableFrom(AtomicLong.class)) {
	         	    enablePerformanceBreakdownReporting = true;
		        }
			}
		} catch( Throwable t) {
			
		}

		
		// generate Spring context file once
		synchronized( UimaASProcessContainer.class) {
			if ( !initialized ) {
				generateDescriptorsAndGetScaleout(args);
				initialized = true;
			}
			return scaleout;
		}
	}
	public byte[] getLastSerializedError() throws Exception {

		if (lastError != null) {

			return super.serialize(lastError);
		}
		return null;

	}
	/**
	 * This method is called by each worker thread before entering  
	 * process loop in run(). Each work thread shares instance of
	 * this class (IProcessContainer). IN this method a single instance
	 * of a co-located broker is created. This broker is deployed in 
	 * a fenced container using a classloader initialized with a classpath
	 * built at runtime which includes just the AMQ jars. Once the broker is
	 * deployed, this method also creates a shared instance of UIMA-AS client
	 * which is used to deploy UIMA-AS based service. 
	 * 
	 * @param duccHome - DUCC HOME needed to find AMQ jars 
	 * @return
	 * @throws Exception
	 */
	public void doDeploy() throws Exception {
		// deploy singleUIMA-AS Version instance of embedded broker
		synchronized( UimaASProcessContainer.class) {
			try {
				// below code runs once to create broker, uima-as client and
				// uima-as service
				if ( brokerInstance == null ) {
					
					System.out.println("UIMA-AS Version::"+UimaAsVersion.getFullVersionString());
					// isolate broker by loading it in its own Class Loader
					// Sets the brokerInstance
					deployBroker(duccHome);
					// Broker is running 
					brokerRunning = true;
					// create a shared instance of UIMA-AS client
					uimaASClient = new BaseUIMAAsynchronousEngine_impl();

					int i = 0;
					// Deploy UIMA-AS services
					for (String dd : deploymentDescriptors) {
						// Deploy UIMA-AS service. Keep the deployment id so 
						// that we can undeploy uima-as service on stop.
						ids[i] = deployService(dd);
					}
					// send GetMeta to UIMA-AS service and wait for a reply
					initializeUimaAsClient(endpointName);
				}
				
			} catch ( Throwable e) {
				Logger logger = UIMAFramework.getLogger();
				logger.log(Level.WARNING, "UimaProcessContainer", e);
				e.printStackTrace();
				throw new RuntimeException(e);

			} finally {
			}
			//	Pin thread to its own CAS serializer
//			serializerMap.put( Thread.currentThread().getId(), new UimaSerializer());
		}
	}
	  public static void dump(ClassLoader cl, int numLevels) {
		    int n = 0;
		    for (URLClassLoader ucl = (URLClassLoader) cl; ucl != null && ++n <= numLevels; ucl = (URLClassLoader) ucl.getParent()) {
		      System.out.println("Class-loader " + n + " has " + ucl.getURLs().length + " urls:");
		      for (URL u : ucl.getURLs()) {
		        System.out.println("  " + u );
		      }
		    }
		  }

	private void deployBroker(String duccHome) throws Exception {
		// Save current context class loader. When done loading the broker jars
		// this class loader will be restored
		ClassLoader currentCL = Thread.currentThread().getContextClassLoader();

		try {
			// setup a classpath for Ducc broker
			String[] brokerClasspath = new String[] {
				duccHome+File.separator+"apache-uima"+File.separator+"apache-activemq"+File.separator+"lib"+File.separator+"*",
				duccHome+File.separator+"apache-uima"+File.separator+"apache-activemq"+File.separator+"lib"+File.separator+"optional"+File.separator+"*"
			};
			
			// isolate broker in its own Class loader
			URLClassLoader ucl = create(brokerClasspath);
			Thread.currentThread().setContextClassLoader(ucl);
			
			classToLaunch = ucl.loadClass("org.apache.activemq.broker.BrokerService");
			dump(ucl, 4);
			brokerInstance = classToLaunch.newInstance();
			
			Method setDedicatedTaskRunnerMethod = classToLaunch.getMethod("setDedicatedTaskRunner", boolean.class);
			setDedicatedTaskRunnerMethod.invoke(brokerInstance, false);
			
			Method setPersistentMethod = classToLaunch.getMethod("setPersistent", boolean.class);
			setPersistentMethod.invoke(brokerInstance, false);
			
			int port = 61626;  // try to start the colocated broker with this port first
			String brokerURL = "tcp://localhost:";
			// loop until a valid port is found for the broker
			while (true) {
				try {
					Method addConnectorMethod = classToLaunch.getMethod("addConnector", String.class);
					addConnectorMethod.invoke(brokerInstance, brokerURL+port);
					
					Method startMethod = classToLaunch.getMethod("start");
					startMethod.invoke(brokerInstance);
					
					Method waitUntilStartedMethod = classToLaunch.getMethod("waitUntilStarted");
					waitUntilStartedMethod.invoke(brokerInstance);
					System.setProperty("DefaultBrokerURL", brokerURL + port);
					System.setProperty("BrokerURI", brokerURL + port);
					// Needed to resolve ${broker.name} placeholder in DD generated 
					// by DUCC
					System.setProperty("broker.name", brokerURL + port);
					

					break;   // got a valid port for the broker
				} catch (Exception e) {
					if ( isBindException(e)) {
						port++;
					} else {
						throw new RuntimeException(e);
					}
				}
			}

		} catch ( Exception e) {
			throw e;
		} finally {
			// restore context class loader
			Thread.currentThread().setContextClassLoader(currentCL);
			brokerLatch.countDown();
		}
		
	}
	private boolean isBindException(Throwable e) {
		if ( e == null ) {
			return false;
		}
		
		if (e instanceof BindException) {
			return true;
		} else if ( e.getCause() != null ) {
			return isBindException(e.getCause());				
		} else {
			return false;
		}
	}
	  public static URLClassLoader create(String[] classPathElements) throws MalformedURLException {
		    ArrayList<URL> urlList = new ArrayList<URL>(classPathElements.length);
		    for (String element : classPathElements) {
		      if (element.endsWith("*")) {
		        File dir = new File(element.substring(0, element.length() - 1));
		        File[] files = dir.listFiles();   // Will be null if missing or not a dir
		        if (files != null) {
		          for (File f : files) {
		            if (f.getName().endsWith(".jar")) {
		              urlList.add(f.toURI().toURL());
		            }
		          }
		        }
		      } else {
		        File f = new File(element);
		        if (f.exists()) {
		          urlList.add(f.toURI().toURL());
		        }
		      }
		    }
		    URL[] urls = new URL[urlList.size()];
		    return new URLClassLoader(urlList.toArray(urls), ClassLoader.getSystemClassLoader().getParent());
		  }
	/** 
	 * This method is called via reflection and stops the UIMA-AS service,
	 * the client, and the colocated broker.
	 * 
	 * @throws Exception
	 */
	public void doStop() throws Exception {
		try {
			synchronized(UimaASProcessContainer.class) {
				if ( brokerRunning ) {
					System.out.println("Stopping UIMA_AS Client");
					try {
						// Prevent UIMA-AS from exiting
						System.setProperty("dontKill", "true");
						uimaASClient.stop();

					} catch (Exception e) {
						e.printStackTrace();
					}
					
					System.out.println("Stopping Broker");

					Method stopMethod = classToLaunch.getMethod("stop");
					stopMethod.invoke(brokerInstance);
					
					Method waitMethod = classToLaunch.getMethod("waitUntilStopped");
					waitMethod.invoke(brokerInstance);

					brokerRunning = false;
				}
			}
			
		} finally {
		}
	}
	/**
	 * This method is called via reflection and delegates processing to the colocated
	 * UIMA-AS service via synchronous call to sendAndReceive()
	 * 
	 * @param xmi - serialized CAS
	 * @throws Exception
	 */
	public List<Properties> doProcess(Object xmi) throws Exception {
		CAS cas = uimaASClient.getCAS();   // fetch a new CAS from the client's Cas Pool
		try {
			// reset last error
			lastError = null;
			XmiSerializationSharedData deserSharedData = new XmiSerializationSharedData();
			// Use thread dedicated UimaSerializer to de-serialize the CAS
			super.getUimaSerializer().
				deserializeCasFromXmi((String)xmi, cas, deserSharedData, true,-1);

			/*
			 * The following code commented for now. Re-enable when uima-as
			 * performance metric collection is fixed. There is a bug in 
			 * the uima-as which causes metrics to be invalid.
			List<AnalysisEnginePerformanceMetrics> perfMetrics = new ArrayList<AnalysisEnginePerformanceMetrics>();
			*/
			List<Properties> metricsList = new ArrayList<Properties>(); 

			if ( enablePerformanceBreakdownReporting ) {
				List<AnalysisEnginePerformanceMetrics> perfMetrics = 
						new ArrayList<AnalysisEnginePerformanceMetrics>();
				uimaASClient.sendAndReceiveCAS(cas, perfMetrics);

				for( AnalysisEnginePerformanceMetrics metrics : perfMetrics ) {
					Properties p = new Properties();
					p.setProperty("name", metrics.getName());
					p.setProperty("uniqueName", metrics.getUniqueName());
					p.setProperty("analysisTime",String.valueOf(metrics.getAnalysisTime()) );
					p.setProperty("numProcessed",String.valueOf(metrics.getNumProcessed()) );
					metricsList.add(p);
				}
				
			} else {
				// delegate processing to the UIMA-AS service and wait for a reply
				uimaASClient.sendAndReceiveCAS(cas);//, perfMetrics);
				// convert UIMA-AS metrics into properties so that we can return this
				// data in a format which doesnt require UIMA-AS to digest
				Properties p = new Properties();
				p.setProperty("name", "Performance Metrics Not Supported For DD Jobs and UIMA-AS <= v2.6.0 ");
				p.setProperty("uniqueName","Performance Metrics Not Supported For DD Jobs and UIMA-AS <= v2.6.0 " );
				p.setProperty("analysisTime","0" );
				p.setProperty("numProcessed","0" );
				metricsList.add(p);
			}
			/*
			 * The following code commented for now. Re-enable when uima-as
			 * performance metric collection is fixed. There is a bug in 
			 * the uima-as which causes metrics to be invalid.

			for( AnalysisEnginePerformanceMetrics metrics : perfMetrics ) {
				Properties p = new Properties();
				p.setProperty("name", metrics.getName());
				p.setProperty("uniqueName", metrics.getUniqueName());
				p.setProperty("analysisTime",String.valueOf(metrics.getAnalysisTime()) );
				p.setProperty("numProcessed",String.valueOf(metrics.getNumProcessed()) );
				metricsList.add(p);
			}
			*/
			return metricsList;
		} catch( Throwable e ) {
			lastError = e;
			Logger logger = UIMAFramework.getLogger();
			logger.log(Level.WARNING, "UimaProcessContainer", e);
			e.printStackTrace();
			throw new AnalysisEngineProcessException();
		} finally {
			if ( cas != null) {
				cas.release();
			}
		}
	}
	private String getPID(final String fallback) {
		// the following code returns '<pid>@<hostname>'
		String name = ManagementFactory.getRuntimeMXBean().getName();
		int pos = name.indexOf('@');

		if (pos < 1) {
			// pid not found
			return fallback;
		}

		try {
			return Long.toString(Long.parseLong(name.substring(0, pos)));
		} catch (NumberFormatException e) {
			// ignore
		}
		return fallback;
	}
	private void initializeUimaAsClient(String endpoint) throws Exception {
		String brokerURL = System.getProperty("DefaultBrokerURL");
		Map<String, Object> appCtx = new HashMap<String, Object>();
		appCtx.put(UimaAsynchronousEngine.ServerUri, brokerURL);
		appCtx.put(UimaAsynchronousEngine.ENDPOINT, endpoint);
		appCtx.put(UimaAsynchronousEngine.CasPoolSize, scaleout);
		appCtx.put(UimaAsynchronousEngine.Timeout, 0);
		appCtx.put(UimaAsynchronousEngine.GetMetaTimeout, 0);
		appCtx.put(UimaAsynchronousEngine.CpcTimeout, 1100);
		UimaAsTestCallbackListener listener = new UimaAsTestCallbackListener();

		uimaASClient.addStatusCallbackListener(listener);
		uimaASClient.initialize(appCtx);
        // blocks until the client initializes
		waitUntilInitialized();
	}

	private void waitUntilInitialized() throws Exception {
		synchronized (initializeMonitor) {
			while (!initialized) {
				initializeMonitor.wait();
			}
		}
	}

	private String deployService(String aDeploymentDescriptorPath)
			throws Exception {

		Map<String, Object> appCtx = new HashMap<String, Object>();
		appCtx.put(UimaAsynchronousEngine.DD2SpringXsltFilePath,
				xslTransform.replace('/', FS));
		appCtx.put(UimaAsynchronousEngine.SaxonClasspath,
				saxonURL.replace('/', FS));
		appCtx.put(UimaAsynchronousEngine.CasPoolSize, scaleout);
		
		String containerId = null;
		try {
			// use UIMA-AS client to deploy the service using provided
			// Deployment Descriptor
		System.out.println("---------------- BROKER URL:::"+System.getProperty("broker.name"));
        ClassLoader duccCl = Thread.currentThread().getContextClassLoader();
		ClassLoader cl = this.getClass().getClassLoader();
		Thread.currentThread().setContextClassLoader(cl);
		containerId = uimaASClient
					.deploy(aDeploymentDescriptorPath, appCtx);
		Thread.currentThread().setContextClassLoader(duccCl);

		} catch (Exception e) {
			// Any problem here should be fatal
			throw e;
		}
		return containerId;
	}
	/**
	 * Extract descriptors from arg list. Also extract xsl processor and saxon url.
	 * Parse the DD to fetch scaleout property.
	 * 
	 * @param args - java argument list 
	 * @return - an array of DDs
	 * 
	 * @throws Exception
	 */
	private String[] getDescriptors(String[] args) throws Exception {
		UIMAFramework.getLogger(CLASS_NAME).log(Level.INFO,
				"UIMA-AS version " + UimaAsVersion.getFullVersionString());

		int nbrOfArgs = args.length;
		String[] deploymentDescriptors = ArgsParser.getMultipleArg("-d", args);
		if (deploymentDescriptors.length == 0) {
			// allow multiple args for one key
			deploymentDescriptors = ArgsParser.getMultipleArg2("-dd", args);
		}
		saxonURL = ArgsParser.getArg("-saxonURL", args);
		xslTransform = ArgsParser.getArg("-xslt", args);
		endpointName = ArgsParser.getArg("-q", args);

		if (nbrOfArgs < 1
				|| (deploymentDescriptors.length == 0 || saxonURL.equals("") || xslTransform
						.equals(""))) {
			printUsageMessage();
			return null; // Done here
		}
		parseDD(deploymentDescriptors[0]);
		return deploymentDescriptors;
	}
    /**
     * Parses given Deployment Descriptor to extract scaleout
     * 
     * @param ddPath - path to the DD
     * @throws Exception
     */
	public void parseDD(String ddPath) throws Exception {
		SAXParserFactory parserFactor = SAXParserFactory.newInstance();
		SAXParser parser = parserFactor.newSAXParser();
		SAXHandler handler = new SAXHandler();
		parser.parse(new File(ddPath), handler);

	}

	class SAXHandler extends DefaultHandler {

		String content = null;

		@Override
		// Triggered when the start of tag is found.
		public void startElement(String uri, String localName, String qName,
				Attributes attributes) throws SAXException {
			if (qName.equals("inputQueue")) {
				endpointName = attributes.getValue("endpoint");
			} else if (qName.equals("scaleout")) {
				scaleout = Integer.parseInt(attributes
						.getValue("numberOfInstances"));
			}

		}

		@Override
		public void endElement(String uri, String localName, String qName)
				throws SAXException {

		}

	}


	protected void finalize() {
		System.err.println(this + " finalized");
	}

	private static void printUsageMessage() {
		System.out
				.println(" Arguments to the program are as follows : \n"
						+ "-d path-to-UIMA-Deployment-Descriptor [-d path-to-UIMA-Deployment-Descriptor ...] \n"
						+ "-saxon path-to-saxon.jar \n"
						+ "-q top level service queue name \n"
						+ "-xslt path-to-dd2spring-xslt\n"
						+ "   or\n"
						+ "path to Spring XML Configuration File which is the output of running dd2spring\n");
	}

	protected class UimaAsTestCallbackListener extends
			UimaAsBaseCallbackListener {

		public void onBeforeProcessCAS(UimaASProcessStatus status,
				String nodeIP, String pid) {
			// System.out
			// .println("runTest: onBeforeProcessCAS() Notification - CAS:"
			// + status.getCasReferenceId()
			// + " is being processed on machine:"
			// + nodeIP
			// + " by process (PID):" + pid);
		}

		public synchronized void onBeforeMessageSend(UimaASProcessStatus status) {
			// casSent = status.getCasReferenceId();
			// System.out
			// .println("runTest: Received onBeforeMessageSend() Notification With CAS:"
			// + status.getCasReferenceId());
		}

		public void onUimaAsServiceExit(EventTrigger cause) {
			System.out
					.println("runTest: Received onUimaAsServiceExit() Notification With Cause:"
							+ cause.name());
		}

		public synchronized void entityProcessComplete(CAS aCAS,
				EntityProcessStatus aProcessStatus,
				List<AnalysisEnginePerformanceMetrics> componentMetricsList) {
		}

		/**
		 * Callback method which is called by Uima EE client when a reply to
		 * process CAS is received. The reply contains either the CAS or an
		 * exception that occurred while processing the CAS.
		 */
		public synchronized void entityProcessComplete(CAS aCAS,
				EntityProcessStatus aProcessStatus) {
		}

		/**
		 * Callback method which is called by Uima EE client when the
		 * initialization of the client is completed successfully.
		 */
		public void initializationComplete(EntityProcessStatus aStatus) {
			synchronized (initializeMonitor) {
				initialized = true;
				initializeMonitor.notifyAll();
			}
		}

		/**
		 * Callback method which is called by Uima EE client when a CPC reply is
		 * received OR exception occured while processing CPC request.
		 */
		public void collectionProcessComplete(EntityProcessStatus aStatus) {
		}
	}

}
