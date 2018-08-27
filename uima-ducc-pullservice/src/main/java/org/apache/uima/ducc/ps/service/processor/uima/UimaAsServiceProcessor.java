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
import java.net.BindException;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import org.apache.uima.UIMAFramework;
import org.apache.uima.cas.CAS;
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
import org.apache.uima.resource.metadata.impl.TypeSystemDescription_impl;
import org.apache.uima.util.CasCreationUtils;
import org.apache.uima.util.Level;
import org.apache.uima.util.Logger;

public class UimaAsServiceProcessor extends AbstractServiceProcessor implements IServiceProcessor, IScaleable {
	private static final Class<?> CLASS_NAME = UimaAsServiceProcessor.class;

	Logger logger = UIMAFramework.getLogger(UimaServiceProcessor.class);
	// Map to store DuccUimaSerializer instances. Each has affinity to a thread
	private static Object brokerInstance = null;
	private UimaAsClientWrapper uimaASClient = null;
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
	@Override
	public void initialize() throws ServiceInitializationException {
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
				
					enableMetricsIfNewUimaAs();
					resultSerializer = new UimaResultDefaultSerializer();
					uimaASClient = new UimaAsClientWrapper(); 
					scaleout = generateDescriptorsAndGetScaleout(argList.toArray(new String[argList.size()])); // Also converts the DD if necessary
					if ( scaleout == 0 ) {
						scaleout = 1;
					}
					initialized = true;
				}
				doDeploy();
			}
			if ( numberOfInitializedThreads.incrementAndGet() == scaleout ) {
				super.delay(logger, DEFAULT_INIT_DELAY);
				monitor.onStateChange(IServiceState.State.Running.toString(), new Properties());
			}

			
		} catch( Exception e) {
			logger.log(Level.WARNING, null, e);
			monitor.onStateChange(IServiceState.State.FailedInitialization.toString(), new Properties());
			throw new ServiceInitializationException("",e);
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

	private CAS getCAS(String serializedTask) throws Exception {
		CAS cas = uimaASClient.getCAS();
		// DUCC JP services are given a serialized CAS ... others just the doc-text for
		// a CAS
		if (serviceConfiguration.getJpType() != null) {
			// Use thread dedicated UimaSerializer to de-serialize the CAS
			getUimaSerializer().deserializeCasFromXmi(serializedTask, cas);
		} else {
			cas.setDocumentText(serializedTask);
			cas.setDocumentLanguage("en");
		}
		return cas;
	}

	
	@Override
	public IProcessResult process(String serializedTask) {
		CAS cas = null;
		IProcessResult result;
		try {

			cas = getCAS(serializedTask);
			List<PerformanceMetrics> casMetrics = new ArrayList<>();

			if (enablePerformanceBreakdownReporting) {
				List<?> perfMetrics = new ArrayList<>();

				try {
					uimaASClient.sendAndReceive(cas, perfMetrics);
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

				for (Object metrics : perfMetrics) {
					Method nameMethod = metrics.getClass().getDeclaredMethod("getName");
					String name = (String) nameMethod.invoke(metrics);
					Method uniqueNameMethod = metrics.getClass().getDeclaredMethod("getUniqueName");
					String uniqueName = (String) uniqueNameMethod.invoke(metrics);
					Method analysisTimeMethod = metrics.getClass().getDeclaredMethod("getAnalysisTime");
					long analysisTime = (long) analysisTimeMethod.invoke(metrics);

					boolean aggregate = uniqueName.startsWith("/" + name);
					int pos = uniqueName.indexOf("/", 1);
					if (pos > -1 && scaleout > 1 && name != null && aggregate) {
						String st = uniqueName.substring(pos);
						uniqueName = "/" + name + st;
					}
					PerformanceMetrics pm = new PerformanceMetrics(name, uniqueName, analysisTime);
					casMetrics.add(pm);
				}
			} else {
				// delegate processing to the UIMA-AS service and wait for a reply
				try {
					
					uimaASClient.sendAndReceive(cas);
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
				PerformanceMetrics pm = new PerformanceMetrics(
						"Performance Metrics Not Supported For DD Jobs and UIMA-AS <= v2.6.0",
						"Performance Metrics Not Supported For DD Jobs and UIMA-AS <= v2.6.0 ", 0);
				casMetrics.add(pm);

			}
			return new UimaProcessResult(resultSerializer.serialize(casMetrics));

		} catch (Exception t) {
			logger.log(Level.WARNING, "", t);
			result = new UimaProcessResult(t, Action.TERMINATE);
			return result;
		} finally {
			if (cas != null) {
				cas.release();
			}
		}
	}
	private void doDeploy() throws Exception {
		// deploy singleUIMA-AS Version instance of embedded broker
			try {
				// below code runs once to create broker, uima-as client and
				// uima-as service
				if (brokerInstance == null) {
					deployBroker(duccHome);
					// Broker is running
					brokerRunning = true;

					int i = 0;
					// Deploy UIMA-AS services
					for (String dd : deploymentDescriptors) {
						// Deploy UIMA-AS service. Keep the deployment id so
						// that we can undeploy uima-as service on stop.
						ids[i] = uimaASClient.deployService(dd);
					}
					// send GetMeta to UIMA-AS service and wait for a reply
					uimaASClient.initialize(); 
				}

			} catch (Throwable e) {
				logger.log(Level.WARNING, "UimaAsServiceProcesser", e);
				throw new RuntimeException(e);

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
			if (brokerRunning) {
				logger.log(Level.INFO, "Stopping UIMA-AS Client");
				System.out.println("Stopping UIMA-AS Client");
				try {
					// Prevent UIMA-AS from exiting
					System.setProperty("dontKill", "true");
					uimaASClient.stop();
					System.out.println("UIMA-AS Client Stopped");
					Method brokerStopMethod = classToLaunch.getMethod("stop");
					brokerStopMethod.invoke(brokerInstance);

					Method waitMethod = classToLaunch.getMethod("waitUntilStopped");
					waitMethod.invoke(brokerInstance);
					brokerRunning = false;
					System.out.println("Internal Broker Stopped");
					super.stop();

				} catch (Exception e) {
					logger.log(Level.WARNING, "stop", e);
				}

			}
		}

	}

	private void deployBroker(String duccHome) throws Exception {
		// Save current context class loader. When done loading the broker jars
		// this class loader will be restored
		ClassLoader currentCL = Thread.currentThread().getContextClassLoader();
		HashMap<String, String> savedPropsMap = null;

		try {
			// setup a classpath for Ducc broker
			String[] brokerClasspath = new String[] {
					duccHome + File.separator + "apache-uima" + File.separator + "apache-activemq" + File.separator
							+ "lib" + File.separator + "*",
					duccHome + File.separator + "apache-uima" + File.separator + "apache-activemq" + File.separator
							+ "lib" + File.separator + "optional" + File.separator + "*" };

			// isolate broker in its own Class loader
			URLClassLoader ucl = Utils.create(brokerClasspath);
			Thread.currentThread().setContextClassLoader(ucl);
			savedPropsMap = Utils.hideLoggingProperties(); // Ensure DUCC doesn't try to use the user's logging setup

			classToLaunch = ucl.loadClass("org.apache.activemq.broker.BrokerService");
			if (System.getProperty("ducc.debug") != null) {
				Utils.dump(ucl, 4);
			}
			brokerInstance = classToLaunch.newInstance();

			Method setDedicatedTaskRunnerMethod = classToLaunch.getMethod("setDedicatedTaskRunner", boolean.class);
			setDedicatedTaskRunnerMethod.invoke(brokerInstance, false);

			Method setPersistentMethod = classToLaunch.getMethod("setPersistent", boolean.class);
			setPersistentMethod.invoke(brokerInstance, false);

			int port = 61626; // try to start the colocated broker with this port first
			String brokerURL = "tcp://localhost:";
			// loop until a valid port is found for the broker
			while (true) {
				try {
					Method addConnectorMethod = classToLaunch.getMethod("addConnector", String.class);
					addConnectorMethod.invoke(brokerInstance, brokerURL + port);

					Method startMethod = classToLaunch.getMethod("start");
					startMethod.invoke(brokerInstance);

					Method waitUntilStartedMethod = classToLaunch.getMethod("waitUntilStarted");
					waitUntilStartedMethod.invoke(brokerInstance);
					System.setProperty("DefaultBrokerURL", brokerURL + port);
					System.setProperty("BrokerURI", brokerURL + port);
					// Needed to resolve ${broker.name} placeholder in DD generated by DUCC
					System.setProperty(brokerPropertyName, brokerURL + port);

					break; // got a valid port for the broker
				} catch (Exception e) {
					if (isBindException(e)) {
						port++;
					} else {
						throw new RuntimeException(e);
					}
				}
			}

		} catch (Exception e) {
			throw e;
		} finally {
			// restore context class loader
			Thread.currentThread().setContextClassLoader(currentCL);
			brokerLatch.countDown();
			Utils.restoreLoggingProperties(savedPropsMap); // May not be necessary as user's logger has been established
		}

	}

	private boolean isBindException(Throwable e) {
		if (e == null) {
			return false;
		}

		if (e instanceof BindException) {
			return true;
		} else if (e instanceof SocketException && "Address already in use".equals(e.getMessage())) {
			return true;
		} else if (e.getCause() != null) {
			return isBindException(e.getCause());
		} else {
			return false;
		}
	}

	private static void printUsageMessage() {
		System.out.println(" Arguments to the program are as follows : \n"
				+ "-d path-to-UIMA-Deployment-Descriptor [-d path-to-UIMA-Deployment-Descriptor ...] \n"
				+ "-saxon path-to-saxon.jar \n" + "-q top level service queue name \n"
				+ "-xslt path-to-dd2spring-xslt\n" + "   or\n"
				+ "path to Spring XML Configuration File which is the output of running dd2spring\n");
	}
	public static void main(String[] args) {
		try {
			UimaAsServiceProcessor processor = 
					new UimaAsServiceProcessor(args, null);
			processor.initialize();
			CAS cas = CasCreationUtils.createCas(new TypeSystemDescription_impl(), null, null);
			cas.setDocumentLanguage("en");
			cas.setDocumentText("Test");
			UimaSerializer serializer = 
					new UimaSerializer();
			String serializedCas = serializer.serializeCasToXmi(cas);

			IProcessResult result =
					processor.process(serializedCas);
			System.out.println("Client Received Result - Success:"+(result.getResult()!=null));
			processor.stop();
		} catch( Exception e) {
			e.printStackTrace();
		}
		
	}
	private class UimaAsClientWrapper {
		private Object uimaASClient;
		private Class<?> clientClz;

		public UimaAsClientWrapper() throws Exception {
			clientClz = Class
					.forName("org.apache.uima.adapter.jms.client.BaseUIMAAsynchronousEngine_impl");
			uimaASClient = clientClz.newInstance();
			
		}

		public String deployService(String aDeploymentDescriptorPath) throws Exception {

			Map<String, Object> appCtx = new HashMap<>();

			Class<?> clz = Class.forName("org.apache.uima.aae.client.UimaAsynchronousEngine");

			appCtx.put((String) clz.getField("DD2SpringXsltFilePath").get(uimaASClient), xslTransform.replace('/', FS));
			appCtx.put((String) clz.getField("SaxonClasspath").get(uimaASClient), saxonURL.replace('/', FS));
			appCtx.put((String) clz.getField("CasPoolSize").get(uimaASClient), scaleout);

			String containerId = null;
			// use UIMA-AS client to deploy the service using provided
			// Deployment Descriptor
			ClassLoader duccCl = Thread.currentThread().getContextClassLoader();
			ClassLoader cl = this.getClass().getClassLoader();
			Thread.currentThread().setContextClassLoader(cl);
			Method deployMethod = uimaASClient.getClass().getDeclaredMethod("deploy", String.class, Map.class);
			containerId = (String) deployMethod.invoke(uimaASClient,
					new Object[] { aDeploymentDescriptorPath, appCtx });
			Thread.currentThread().setContextClassLoader(duccCl);
			return containerId;
		}

		private void initialize() throws Exception {

			String endpoint = System.getProperty(queuePropertyName);
			String brokerURL = System.getProperty(brokerPropertyName);
			Map<String, Object> appCtx = new HashMap<>();
			Class<?> clz = Class.forName("org.apache.uima.aae.client.UimaAsynchronousEngine");

			appCtx.put((String) clz.getField("ServerUri").get(uimaASClient), brokerURL);
			appCtx.put((String) clz.getField("ENDPOINT").get(uimaASClient), endpoint);
			appCtx.put((String) clz.getField("CasPoolSize").get(uimaASClient), scaleout);
			appCtx.put((String) clz.getField("Timeout").get(uimaASClient), 0);
			appCtx.put((String) clz.getField("GetMetaTimeout").get(uimaASClient), 0);
			appCtx.put((String) clz.getField("CpcTimeout").get(uimaASClient), 1100);
			Method initMethod = uimaASClient.getClass().getMethod("initialize", Map.class);
			initMethod.invoke(uimaASClient, new Object[] { appCtx });

			// blocks until the client initializes
			Method getMetaMethod = uimaASClient.getClass().getMethod("getMetaData");
			Object meta = getMetaMethod.invoke(uimaASClient);

			Method nameMethod = meta.getClass().getMethod("getName");
			aeName = (String) nameMethod.invoke(meta);
		}
		public CAS getCAS() throws Exception {
			Method getCasMethod = uimaASClient.getClass().getMethod("getCAS");
			return (CAS) getCasMethod.invoke(uimaASClient);
		}
		public void sendAndReceive(CAS cas, List<?> perfMetrics) throws Exception {
			Method sendMethod = uimaASClient.getClass().getMethod("sendAndReceiveCAS", CAS.class,
					List.class);
			sendMethod.invoke(uimaASClient, new Object[] { cas, perfMetrics });
		}
		public void sendAndReceive(CAS cas) throws Exception {
			Method sendMethod = uimaASClient.getClass().getDeclaredMethod("sendAndReceiveCAS", CAS.class);
			sendMethod.invoke(uimaASClient, new Object[] { cas });
		}
		public void stop() throws Exception {
			Method clientStopMethod = uimaASClient.getClass().getDeclaredMethod("stop");
			clientStopMethod.invoke(uimaASClient);
		}
		

	}
	private class UimaAsVersionWrapper {
		Class<?> clz = null;
		Method m = null;

		
		public UimaAsVersionWrapper() throws Exception {
			clz = Class.forName("org.apache.uima.aae.UimaAsVersion");
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
