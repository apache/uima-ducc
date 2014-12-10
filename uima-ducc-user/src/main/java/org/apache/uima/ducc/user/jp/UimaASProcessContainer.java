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
import java.io.IOException;
import java.net.BindException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.activemq.broker.BrokerService;
import org.apache.uima.UIMAFramework;
import org.apache.uima.aae.UimaAsVersion;
import org.apache.uima.aae.UimaSerializer;
import org.apache.uima.aae.UimaASApplicationEvent.EventTrigger;
import org.apache.uima.aae.client.UimaASProcessStatus;
import org.apache.uima.aae.client.UimaAsBaseCallbackListener;
import org.apache.uima.aae.client.UimaAsynchronousEngine;
import org.apache.uima.aae.monitor.statistics.AnalysisEnginePerformanceMetrics;
import org.apache.uima.adapter.jms.client.BaseUIMAAsynchronousEngine_impl;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.impl.XmiSerializationSharedData;
import org.apache.uima.collection.EntityProcessStatus;
import org.apache.uima.ducc.user.jp.iface.IProcessContainer;
import org.apache.uima.util.Level;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

public class UimaASProcessContainer implements IProcessContainer {
	String endpointName;
	protected int scaleout;
	String saxonURL = null;
	String xslTransform = null;
	String uimaAsDebug = null;
	static final BaseUIMAAsynchronousEngine_impl uimaASClient = new BaseUIMAAsynchronousEngine_impl();
	private static final CountDownLatch brokerLatch = new CountDownLatch(1);
	
	protected Object initializeMonitor = new Object();
	public volatile boolean initialized = false;
	private static final Class<?> CLASS_NAME = UimaProcessContainer.class;
	private static final char FS = System.getProperty("file.separator").charAt(
			0);
	public static BrokerService broker = null;
	private UimaSerializer uimaSerializer = new UimaSerializer();
    private String[] deploymentDescriptors = null;
	private String[] ids = null;
    
	public int initialize(String[] args) throws Exception {
		// Get DDs and also extract scaleout property from DD
		deploymentDescriptors = getDescriptors(args);
		ids = new String[deploymentDescriptors.length];
	    
		return scaleout;
	}

	/**
	 * This method is called via reflection and deploys both the colocated AMQ broker
	 * and UIMA-AS service.
	 * 
	 * @param args - command line args
	 * @return
	 * @throws Exception
	 */
//	public void deploy(String[] args) throws Exception {
	public void deploy() throws Exception {
		System.out.println("UIMA-AS Version::"+UimaAsVersion.getFullVersionString());
		synchronized( UimaASProcessContainer.class) {
			if ( broker == null ) {
				broker = new BrokerService();
				broker.setDedicatedTaskRunner(false);
				broker.setPersistent(false);
				int port = 61626;  // try to start the colocated broker with this port first
				String brokerURL = "tcp://localhost:";
				// loop until a valid port is found for the broker
				while (true) {
					try {
						broker.addConnector(brokerURL + port);
						broker.start();
						broker.waitUntilStarted();
						System.setProperty("DefaultBrokerURL", brokerURL + port);
						break;   // got a valid port for the broker
					} catch (IOException e) {
						if (e.getCause() instanceof BindException) {
							port++;   // choose the next port and retry
						} else {
							throw new RuntimeException(e);
						}

					}

				}
				brokerLatch.countDown();
			}
		}
		brokerLatch.await();   // all threads must wait for the broker to start and init
		// deploy colocated UIMA-AS services from provided deployment
		// descriptors
		int i = 0;
		for (String dd : deploymentDescriptors) {
			// keep the container id so that we can un-deploy it when shutting
			// down
			ids[i] = deployService(dd);
		}
		initializeUimaAsClient(endpointName);
/*
		// Get DDs and also extract scaleout property from DD
		String[] deploymentDescriptors = getDescriptors(args);
		// deploy colocated UIMA-AS services from provided deployment
		// descriptors
		String[] ids = new String[deploymentDescriptors.length];
		int i = 0;
		for (String dd : deploymentDescriptors) {
			// keep the container id so that we can un-deploy it when shutting
			// down
			ids[i] = deployService(dd);
		}
		// initialize and start UIMA-AS client. This sends GetMeta request to
		// deployed top level service and waits for a reply
		initializeUimaAsClient(endpointName);
	
		return scaleout;
*/
	}

	/** 
	 * This method is called via reflection and stops the UIMA-AS service,
	 * the client, and the colocated broker.
	 * 
	 * @throws Exception
	 */
	public void stop() throws Exception {
		System.out.println("Stopping UIMA_AS Client");
		try {
			System.setProperty("dontKill", "true");
			uimaASClient.stop();

		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("Stopping Broker");
		broker.stop();
		broker.waitUntilStopped();
	}
	/**
	 * This method is called via reflection and delegates processing to the colocated
	 * UIMA-AS service via synchronous call to sendAndReceive()
	 * 
	 * @param xmi - serialized CAS
	 * @throws Exception
	 */
	public List<Properties> process(Object xmi) throws Exception {
		CAS cas = uimaASClient.getCAS();   // fetch a new CAS from the client's Cas Pool
		try {
			XmiSerializationSharedData deserSharedData = new XmiSerializationSharedData();
			// deserialize the CAS
			uimaSerializer.deserializeCasFromXmi((String)xmi, cas, deserSharedData, true,
					-1);
			List<AnalysisEnginePerformanceMetrics> perfMetrics = new ArrayList<AnalysisEnginePerformanceMetrics>();
			// delegate processing to the UIMA-AS service and wait for a reply
			uimaASClient.sendAndReceiveCAS(cas, perfMetrics);
			// convert UIMA-AS metrics into properties so that we can return this
			// data in a format which doesnt require UIMA-AS to digest
			List<Properties> metricsList = new ArrayList<Properties>(); 
			for( AnalysisEnginePerformanceMetrics metrics : perfMetrics ) {
				Properties p = new Properties();
				p.setProperty("name", metrics.getName());
				p.setProperty("uniqueName", metrics.getUniqueName());
				p.setProperty("analysisTime",String.valueOf(metrics.getAnalysisTime()) );
				p.setProperty("numProcessed",String.valueOf(metrics.getNumProcessed()) );
				metricsList.add(p);
			}
			return metricsList;
		} finally {
			if ( cas != null) {
				cas.release();
			}
		}
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
			containerId = uimaASClient
					.deploy(aDeploymentDescriptorPath, appCtx);

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
		uimaAsDebug = ArgsParser.getArg("-uimaEeDebug", args);
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
			String casReferenceId = ((UimaASProcessStatus) aProcessStatus)
					.getCasReferenceId();

			// if (aProcessStatus instanceof UimaASProcessStatus) {
			// if (aProcessStatus.isException()) {
			// System.out
			// .println("--------- Got Exception While Processing CAS"
			// + casReferenceId);
			// } else {
			// System.out.println("Client Received Reply - CAS:"
			// + casReferenceId);
			// }
			// }
		}

		/**
		 * Callback method which is called by Uima EE client when a reply to
		 * process CAS is received. The reply contains either the CAS or an
		 * exception that occurred while processing the CAS.
		 */
		public synchronized void entityProcessComplete(CAS aCAS,
				EntityProcessStatus aProcessStatus) {
			String casReferenceId = ((UimaASProcessStatus) aProcessStatus)
					.getCasReferenceId();

			// if (aProcessStatus instanceof UimaASProcessStatus) {
			// if (aProcessStatus.isException()) {
			// System.out
			// .println("--------- Got Exception While Processing CAS"
			// + casReferenceId);
			// } else {
			// System.out.println("Client Received Reply - CAS:"
			// + casReferenceId);
			// }
			// }

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
