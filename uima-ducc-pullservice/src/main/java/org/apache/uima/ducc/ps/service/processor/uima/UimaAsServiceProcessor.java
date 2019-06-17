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
package org.apache.uima.ducc.ps.service.processor.uima;

import java.io.File;
import java.io.FileInputStream;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import org.apache.uima.UIMAFramework;
import org.apache.uima.ducc.ps.service.IScaleable;
import org.apache.uima.ducc.ps.service.IServiceState;
import org.apache.uima.ducc.ps.service.ServiceConfiguration;
import org.apache.uima.ducc.ps.service.dgen.DeployableGenerator;
import org.apache.uima.ducc.ps.service.dgen.DuccUimaReferenceByName;
import org.apache.uima.ducc.ps.service.errors.IServiceErrorHandler;
import org.apache.uima.ducc.ps.service.errors.IServiceErrorHandler.Action;
import org.apache.uima.ducc.ps.service.errors.ServiceInitializationException;
import org.apache.uima.ducc.ps.service.metrics.IWindowStats;
import org.apache.uima.ducc.ps.service.metrics.builtin.ProcessWindowStats;
import org.apache.uima.ducc.ps.service.monitor.IServiceMonitor;
import org.apache.uima.ducc.ps.service.monitor.builtin.RemoteStateObserver;
import org.apache.uima.ducc.ps.service.processor.IProcessResult;
import org.apache.uima.ducc.ps.service.processor.IServiceProcessor;
import org.apache.uima.ducc.ps.service.processor.IServiceResultSerializer;
import org.apache.uima.ducc.ps.service.processor.uima.utils.PerformanceMetrics;
import org.apache.uima.ducc.ps.service.processor.uima.utils.UimaResultDefaultSerializer;
import org.apache.uima.ducc.ps.service.utils.UimaSerializer;
import org.apache.uima.ducc.ps.service.utils.Utils;
import org.apache.uima.util.Level;
import org.apache.uima.util.Logger;

public class UimaAsServiceProcessor extends AbstractServiceProcessor implements IServiceProcessor, IScaleable {
	private static final Class<?> CLASS_NAME = UimaAsServiceProcessor.class;

	Logger logger = UIMAFramework.getLogger(UimaServiceProcessor.class);
	// Map to store DuccUimaSerializer instances. Each has affinity to a thread
	private static Object brokerInstance = null;
	private String saxonURL = null;
	private String xslTransform = null;
	protected Object initializeMonitor = new Object();
	private static volatile boolean brokerRunning = false;
	private static final char FS = System.getProperty("file.separator").charAt(0);

	private static final CountDownLatch brokerLatch = new CountDownLatch(1);
	public static final String brokerPropertyName = "ducc.broker.name";
	public static final String queuePropertyName = "ducc.queue.name";
	public static final String duccNodeName = "DUCC_NODENAME";
	private IServiceResultSerializer resultSerializer;
	private static Class<?> classToLaunch = null;
	private String aeName = "";
	private int scaleout = 1;
	private static Object platformMBeanServer;
	private ServiceConfiguration serviceConfiguration;
	private IServiceMonitor monitor;
	private IServiceErrorHandler errorHandler;
	public volatile boolean initialized = false;
	private String[] deploymentDescriptors = null;
	private String[] ids = null;
	private String duccHome = null;
	boolean enablePerformanceBreakdownReporting = false;
	private AtomicInteger numberOfInitializedThreads = new AtomicInteger();
	
	public static final String AE_NAME = "AeName";
	public static final String AE_CONTEXT = "AeContext";
	public static final String AE_ANALYSIS_TIME = "AeAnalysisTime";
	public static final String AE_CAS_PROCESSED = "AeProcessedCasCount";


	private static String M_PROCESS="process";
	private static String M_STOP="stop";
	private static String M_INITIALIZE="initialize";

	private Method processMethod;
	private Method stopMethod;
	private Object processorInstance;

	static {
		// try to get platform MBean Server (Java 1.5 only)
		try {
			Class<?> managementFactory = Class.forName("java.lang.management.ManagementFactory");
			Method getPlatformMBeanServer = managementFactory.getMethod("getPlatformMBeanServer", new Class[0]);
			platformMBeanServer = getPlatformMBeanServer.invoke(null, (Object[]) null);
		} catch (Exception e) {
			platformMBeanServer = null;
		}
	}
	private String[] args;


	public UimaAsServiceProcessor(String[] args, ServiceConfiguration serviceConfiguration) {
		this.args = args;
		this.serviceConfiguration = serviceConfiguration;

		// start a thread which will collect AE initialization state
		launchStateInitializationCollector();
	}

	@Override
	public void setScaleout(int howManyThreads) {
		scaleout = howManyThreads;
	}

	@Override
	public int getScaleout() {
		return scaleout;
	}
	public void setErrorHandlerWindow(int maxErrors, int windowSize) {
		this.maxErrors = maxErrors;
		this.windowSize = windowSize;
	}
	private int generateDescriptorsAndGetScaleout(String[] args) throws Exception {
		deploymentDescriptors = getDescriptors(args);
		ids = new String[deploymentDescriptors.length];
		return scaleout;
	}

	private void enableMetricsIfNewUimaAs() throws Exception{
		// inner class hiding java reflection code to access uima-as class
		UimaAsVersionWrapper uimaVersion = new UimaAsVersionWrapper();
		
		int majorVersion = uimaVersion.getMajor(); 
		int minorVersion = uimaVersion.getMinor(); 
		int buildRevision = uimaVersion.getRevision(); 

		// enable performance breakdown reporting when support is added in the next UIMA
		// AS release after 2.6.0
		// (assumes the fix will be after the current 2.6.1-SNAPSHOT level)
		if (majorVersion > 2 || (majorVersion == 2 && (minorVersion > 6 || (minorVersion == 6 && buildRevision > 1)))) {
			enablePerformanceBreakdownReporting = true;
		}
	}
	private void launchStateInitializationCollector() {
		monitor =
				new RemoteStateObserver(serviceConfiguration, logger);
	}
	
	public void dump(ClassLoader cl, int numLevels) {
		int n = 0;
		for (URLClassLoader ucl = (URLClassLoader) cl; ucl != null
				&& ++n <= numLevels; ucl = (URLClassLoader) ucl.getParent()) {
			System.out.println("Class-loader " + n + " has "
					+ ucl.getURLs().length + " urls:");
			for (URL u : ucl.getURLs()) {
				System.out.println("  " + u);
			}
		}
	}
	
	@Override
	public void initialize() throws ServiceInitializationException {
	   	// Save current context cl and inject System classloader as
		// a context cl before calling user code. 
		ClassLoader savedCL = Thread.currentThread().getContextClassLoader();

		try {
			duccHome = System.getProperty("DUCC_HOME");
			String pid = Utils.getPID("Queue");
			String endpointName; // This is for the local within-process UIMA-AS client/server
			if (System.getenv(duccNodeName) != null) {
				endpointName = System.getenv(duccNodeName) + pid;
			} else {
				endpointName = InetAddress.getLocalHost().getCanonicalHostName() + pid;
			}
			// Needed to resolve ${queue.name} placeholder in DD generated by DUCC
			System.setProperty(queuePropertyName, endpointName);
			errorHandler = getErrorHandler(logger);

			// generate Spring context file once
			synchronized (UimaAsServiceProcessor.class) {
				// every process thread has its own uima deserializer
				serializerMap.put(Thread.currentThread().getId(), new UimaSerializer());
				if (!initialized) {
					monitor.onStateChange(IServiceState.State.Initializing.toString(), new Properties());

					Properties duccProperties = loadDuccProperties();
					String saxonPath = 
							resolvePlaceholders(duccProperties.getProperty("ducc.uima-as.saxon.jar.path"), System.getProperties() ); 
					String xsltPath = 
							resolvePlaceholders(duccProperties.getProperty("ducc.uima-as.dd2spring.xsl.path"), System.getProperties() ); 
					List<String> argList = new ArrayList<>();
					argList.add("-d");
					argList.add(args[0]);
					argList.add("-saxonURL");
					argList.add(saxonPath);
					argList.add("-xslt");
					argList.add(xsltPath);
					
					if ( System.getProperty("ducc.deploy.JpThreadCount") != null ) {
						argList.add("-t");
						argList.add(System.getProperty("ducc.deploy.JpThreadCount"));
					}
					
					resultSerializer = new UimaResultDefaultSerializer();
					
					Thread.currentThread().setContextClassLoader(ClassLoader.getSystemClassLoader());
					dump(ClassLoader.getSystemClassLoader(), 3);
					enableMetricsIfNewUimaAs();
					
					// load proxy class from uima-ducc-user.jar to access uima classes. The UimaWrapper is a convenience/wrapper
					// class so that we dont have to use reflection on Uima classes.
					Class<?> classToLaunch = 
							ClassLoader.getSystemClassLoader().loadClass("org.apache.uima.ducc.user.common.main.UimaAsWrapper");
		
					processorInstance = classToLaunch.newInstance();

					Method initMethod = processorInstance.getClass().getMethod(M_INITIALIZE, String.class,String.class,String.class, int.class, boolean.class);

					processMethod = processorInstance.getClass().getMethod(M_PROCESS, new Class[] {String.class});
					
					stopMethod = processorInstance.getClass().getMethod(M_STOP);
					//public void initialize(String analysisEngineDescriptor, String xslTransform, String saxonURL,  int scaleout,  boolean deserialize) throws Exception {


					
					
					
					
					
					
				//	uimaASClient = new UimaAsClientWrapper(); 
					scaleout = generateDescriptorsAndGetScaleout(argList.toArray(new String[argList.size()])); // Also converts the DD if necessary
					if ( scaleout == 0 ) {
						scaleout = 1;
					}
					// initialize AE via UimaWrapper
					initMethod.invoke(processorInstance, deploymentDescriptors[0], xsltPath, saxonPath, scaleout, (serviceConfiguration.getJpType() != null));

					initialized = true;
				}
				//doDeploy();
			}
			if ( numberOfInitializedThreads.incrementAndGet() == scaleout ) {
				super.delay(logger, DEFAULT_INIT_DELAY);
				monitor.onStateChange(IServiceState.State.Running.toString(), new Properties());
			}

			
		} catch( Exception e) {
			logger.log(Level.WARNING, null, e);
			monitor.onStateChange(IServiceState.State.FailedInitialization.toString(), new Properties());
			throw new ServiceInitializationException("",e);
		} finally {
			Thread.currentThread().setContextClassLoader(savedCL);

		}

	}
	public String resolvePlaceholders(String contents, Properties props ) 
    {
        //  Placeholders syntax ${<placeholder>}
        Pattern placeHolderPattern = Pattern.compile("\\$\\{(.*?)\\}");
      
        java.util.regex.Matcher matcher = 
            placeHolderPattern.matcher(contents); 

        StringBuffer sb = new StringBuffer();
        while (matcher.find()) {
            // extract placeholder
            final String key = matcher.group(1);
            //  Find value for extracted placeholder. 
            String placeholderValue = props.getProperty(key);
            if (placeholderValue == null) {
                placeholderValue = System.getProperty(key);
                if (placeholderValue == null) {
                    throw new IllegalArgumentException("Missing value for placeholder: " + key);
                }
            }
            matcher.appendReplacement(sb, placeholderValue);        
        }
        matcher.appendTail(sb);
        return sb.toString();
	}
	
	@Override
	public IProcessResult process(String serializedTask) {
	//	CAS cas = null;
		IProcessResult result;
	   	// Save current context cl and inject System classloader as
		// a context cl before calling user code. 
		ClassLoader savedCL = Thread.currentThread().getContextClassLoader();

		Thread.currentThread().setContextClassLoader(ClassLoader.getSystemClassLoader());

		try {

			List<PerformanceMetrics> casMetrics = new ArrayList<>();

			if (enablePerformanceBreakdownReporting) {
				try {
					// Use basic data structures for returning performance metrics from 
					// a processor process(). The PerformanceMetrics class is not 
					// visible in the code packaged in uima-ducc-user.jar so return
					// metrics in Properties object for each AE.
					List<Properties> metrics;

					// *****************************************************
					// PROCESS
					// *****************************************************
					metrics = (List<Properties>) processMethod.invoke(processorInstance, serializedTask);
					for( Properties p : metrics ) {
						// there is Properties object for each AE, so create an
						// instance of PerformanceMetrics and initialize it
						PerformanceMetrics pm = 
								new PerformanceMetrics(p.getProperty(AE_NAME), 
										p.getProperty(AE_CONTEXT),
										Long.valueOf(p.getProperty(AE_ANALYSIS_TIME)), 
										Long.valueOf(p.getProperty(AE_CAS_PROCESSED)));
						casMetrics.add(pm);
					}
					successCount.incrementAndGet();
					errorCountSinceLastSuccess.set(0);

				} catch (Exception t) {
					logger.log(Level.WARNING, "", t);
					IWindowStats stats = 
							new ProcessWindowStats(errorCount.incrementAndGet(), 
									successCount.get(), 
									errorCountSinceLastSuccess.incrementAndGet());
					Action action = 
							errorHandler.handleProcessError(t, this, stats);
					result = new UimaProcessResult(t, action);
					return result;
				}

			}
			return new UimaProcessResult(resultSerializer.serialize(casMetrics));

		} catch (Exception t) {
			logger.log(Level.WARNING, "", t);
			result = new UimaProcessResult(t, Action.TERMINATE);
			return result;
		} finally {
			Thread.currentThread().setContextClassLoader(savedCL);
		}
	}

	private Properties loadDuccProperties() throws Exception {
		Properties duccProperties = new Properties();
		duccProperties.load(new FileInputStream(System.getProperty("ducc.deploy.configuration")));
		return duccProperties;
	}
	/**
	 * Extract descriptors from arg list. Also extract xsl processor and saxon url.
	 * Parse the DD to fetch scaleout property & convert the DD if necessary.
	 *
	 * @param args
	 *            - java argument list
	 * @return - an array of DDs
	 *
	 * @throws Exception
	 */
	private String[] getDescriptors(String[] args) throws Exception {
		int nbrOfArgs = args.length;
		String[] deploymentDescriptors = Utils.getMultipleArg("-d", args);
		if (deploymentDescriptors.length == 0) {
			// allow multiple args for one key
			deploymentDescriptors = Utils.getMultipleArg2("-dd", args);
		}
		for( String arg : args) {
			logger.log(Level.INFO,"+++++++++++++++ arg:"+arg);
		}
		saxonURL = Utils.getArg("-saxonURL", args);
		xslTransform = Utils.getArg("-xslt", args);
		String threadCount = Utils.getArg("-t", args); // Will be null if ducc.deploy.JpThreadCount is undefined
		if ( threadCount.isEmpty()) {
			threadCount = "1";  // default
		}
		if (nbrOfArgs < 1 || (deploymentDescriptors.length == 0 || saxonURL.equals("") || xslTransform.equals(""))) {
			printUsageMessage();
			return null; // Done here
		}
		deploymentDescriptors[0] = parseDD(deploymentDescriptors[0], threadCount);
		return deploymentDescriptors;
	}

	/**
	 * Parses given Deployment Descriptor to extract scaleout and to check if it has
	 * placeholders for the queue & broker. Generates a converted one if necessary,
	 * e.g. for "pull services. The scaleout is used for "pull" services and should
	 * become the default for DD jobs.
	 * 
	 * @param ddPath
	 *            - path to the DD
	 * @param threadCount
	 *            - pipeline scaleout value null if undefined)
	 * @return original or converted DD
	 * @throws Exception
	 */
	public String parseDD(String ddPath, String threadCount) throws Exception {
		// For "custom" pull-services must convert the DD so it can be deployed locally
		String logfile=System.getenv("DUCC_PROCESS_LOG_PREFIX");
		// For JPs this should return the same file, already converted by the JD
		String logDir = new File(logfile).getParent();

		DeployableGenerator deployableGenerator = new DeployableGenerator(logDir);
	    DuccUimaReferenceByName configuration = new DuccUimaReferenceByName(0, ddPath);
	    String ddNew = deployableGenerator.generateDd(configuration, System.getenv("DUCC_JOBID"), true);
		
		if (ddNew != ddPath) {
			logger.log(Level.INFO, "Generated " + ddNew + " from " + ddPath);
		}

		int ddScaleout = deployableGenerator.getScaleout();
		if (threadCount == null) {
			scaleout = ddScaleout;
			logger.log(Level.INFO, "DD specifies a scaleout of " + scaleout);
		} else {
			scaleout = Integer.parseInt(threadCount);
			if (ddScaleout != scaleout) {
				logger.log(Level.WARNING,
						"Scaleout specified as " + scaleout + " but the DD is configured for " + ddScaleout);
			}
		}

		return ddNew;
	}

	@Override
	public void stop() {
		synchronized (UimaAsServiceProcessor.class) {
			
			logger.log(Level.INFO,this.getClass().getName()+" stop() called");
		   	// save current context cl and inject System classloader as
			// a context cl before calling user code. This is done in 
			// user code needs to load resources 
			ClassLoader savedCL = Thread.currentThread().getContextClassLoader();
			Thread.currentThread().setContextClassLoader(ClassLoader.getSystemClassLoader());
			if ( logger.isLoggable(Level.FINE)) {
				logger.log(Level.FINE,"",">>>> stop():::Context Classloader Switch - Executing code from System Classloader");
			}
			try {
				System.setProperty("dontKill", "true");
				stopMethod.invoke(processorInstance);
				super.stop();

			} catch( Exception e) {
				logger.log(Level.WARNING, "stop", e);
			} finally {
				Thread.currentThread().setContextClassLoader(savedCL);
				if ( logger.isLoggable(Level.FINE)) {
					logger.log(Level.FINE,"",">>>> stop():::Context Classloader Switch - Restored Ducc Classloader");
				}
			}

		}

	}

	private static void printUsageMessage() {
		System.out.println(" Arguments to the program are as follows : \n"
				+ "-d path-to-UIMA-Deployment-Descriptor [-d path-to-UIMA-Deployment-Descriptor ...] \n"
				+ "-saxon path-to-saxon.jar \n" + "-q top level service queue name \n"
				+ "-xslt path-to-dd2spring-xslt\n" + "   or\n"
				+ "path to Spring XML Configuration File which is the output of running dd2spring\n");
	}

	private class UimaAsVersionWrapper {
		Class<?> clz = null;
		Method m = null;

		
		public UimaAsVersionWrapper() throws Exception {
			clz = 
					Thread.currentThread().getContextClassLoader().loadClass("org.apache.uima.aae.UimaAsVersion");
				//	Class.forName("org.apache.uima.aae.UimaAsVersion");
		}

		public String getFullVersion() throws Exception {
			m = clz.getDeclaredMethod("getFullVersionString");
			return (String)m.invoke(null);
		}
		public int getMajor() throws Exception {
			Method majorVersionMethod = clz.getDeclaredMethod("getMajorVersion");
			return (short) majorVersionMethod.invoke(null);
		}
		public int getMinor() throws Exception {
			Method minorVersionMethod = clz.getDeclaredMethod("getMinorVersion");
			return (short) minorVersionMethod.invoke(null);
		}
		public int getRevision() throws Exception {
			Method buildRevisionMethod = clz.getDeclaredMethod("getBuildRevision");
			return (short) buildRevisionMethod.invoke(null);
		}

	}
}
