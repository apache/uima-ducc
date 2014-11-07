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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.activemq.broker.BrokerService;
import org.apache.uima.UIMAFramework;
import org.apache.uima.aae.UimaASApplicationEvent.EventTrigger;
import org.apache.uima.aae.UimaAsVersion;
import org.apache.uima.aae.UimaSerializer;
import org.apache.uima.aae.client.UimaASProcessStatus;
import org.apache.uima.aae.client.UimaAsBaseCallbackListener;
import org.apache.uima.aae.client.UimaAsynchronousEngine;
import org.apache.uima.aae.monitor.statistics.AnalysisEnginePerformanceMetrics;
import org.apache.uima.adapter.jms.client.BaseUIMAAsynchronousEngine_impl;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.impl.XmiSerializationSharedData;
import org.apache.uima.collection.EntityProcessStatus;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.Level;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

public class UimaProcessContainer {
	String endpointName;
	protected int scaleout;
	String saxonURL = null;
	String xslTransform = null;
	String uimaAsDebug = null;
	static final BaseUIMAAsynchronousEngine_impl uimaASClient = new BaseUIMAAsynchronousEngine_impl();
	protected Object initializeMonitor = new Object();
	public volatile boolean initialized = false;
	private static final Class CLASS_NAME = UimaProcessContainer.class;
	private static final char FS = System.getProperty("file.separator").charAt(
			0);
	public static BrokerService broker = null;
	private UimaSerializer uimaSerializer = new UimaSerializer();

	public int deploy(String[] args) throws Exception {

		broker = new BrokerService();
		broker.setDedicatedTaskRunner(false);
		broker.setPersistent(false);
		int port = 61626;
		String brokerURL = "tcp://localhost:";
		while (true) {
			try {
				broker.addConnector(brokerURL + port);
				broker.start();
				broker.waitUntilStarted();
				System.setProperty("DefaultBrokerURL", brokerURL + port);
				break;
			} catch (IOException e) {
				if (e.getCause() instanceof BindException) {
					port++;
				} else {
					e.printStackTrace();
					break;
				}

			}

		}
		String[] deploymentDescriptors = initialize(args);
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
	}

	public void stop() throws Exception {
		System.out.println("Stopping UIMA_AS Client");
		try {
			uimaASClient.stop();

		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("Stopping Broker");
		broker.stop();
		broker.waitUntilStopped();
	}

	public void initializeUimaAsClient(String endpoint) throws Exception {

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

		waitUntilInitialized();

	}

	protected void waitUntilInitialized() throws Exception {
		synchronized (initializeMonitor) {
			while (!initialized) {
				initializeMonitor.wait();
			}
		}
	}

	protected String deployService(String aDeploymentDescriptorPath)
			throws Exception {

		Map<String, Object> appCtx = new HashMap<String, Object>();
		appCtx.put(UimaAsynchronousEngine.DD2SpringXsltFilePath,
				xslTransform.replace('/', FS));
		appCtx.put(UimaAsynchronousEngine.SaxonClasspath,
				saxonURL.replace('/', FS));
		String containerId = null;
		try {
			containerId = uimaASClient
					.deploy(aDeploymentDescriptorPath, appCtx);

		} catch (ResourceInitializationException e) {

			System.out.println(">>>>>>>>>>> Exception ---:"
					+ e.getClass().getName());
		} catch (Exception e) {
			System.out.println(">>>>>>>>>>> runTest: Exception:"
					+ e.getClass().getName());
			throw e;
		}
		return containerId;
	}

	public void process(String xmi) throws Exception {
		CAS cas = uimaASClient.getCAS();
		XmiSerializationSharedData deserSharedData = new XmiSerializationSharedData();

		uimaSerializer.deserializeCasFromXmi(xmi, cas, deserSharedData, true,
				-1);

		uimaASClient.sendAndReceiveCAS(cas);
		cas.release();
	}

	public String[] initialize(String[] args) throws Exception {
		UIMAFramework.getLogger(CLASS_NAME).log(Level.INFO,
				"UIMA-AS version " + UimaAsVersion.getFullVersionString());

		int nbrOfArgs = args.length;
		String[] deploymentDescriptors = getMultipleArg("-d", args);
		if (deploymentDescriptors.length == 0) {
			// allow multiple args for one key
			deploymentDescriptors = getMultipleArg2("-dd", args);
		}
		saxonURL = getArg("-saxonURL", args);
		xslTransform = getArg("-xslt", args);
		uimaAsDebug = getArg("-uimaEeDebug", args);
		endpointName = getArg("-q", args);

		if (nbrOfArgs < 1
				|| (deploymentDescriptors.length == 0 || saxonURL.equals("") || xslTransform
						.equals(""))) {
			printUsageMessage();
			return null; // Done here
		}
		parseDD(deploymentDescriptors[0]);
		return deploymentDescriptors;
	}

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

	/**
	 * scan args for a particular arg, return the following token or the empty
	 * string if not found
	 * 
	 * @param id
	 *            the arg to search for
	 * @param args
	 *            the array of strings
	 * @return the following token, or a 0 length string if not found
	 */
	private static String getArg(String id, String[] args) {
		for (int i = 0; i < args.length; i++) {
			if (id.equals(args[i]))
				return (i + 1 < args.length) ? args[i + 1] : "";
		}
		return "";
	}

	/**
	 * scan args for a particular arg, return the following token(s) or the
	 * empty string if not found
	 * 
	 * @param id
	 *            the arg to search for
	 * @param args
	 *            the array of strings
	 * @return the following token, or a 0 length string array if not found
	 */
	private static String[] getMultipleArg(String id, String[] args) {
		String[] retr = {};
		for (int i = 0; i < args.length; i++) {
			if (id.equals(args[i])) {
				String[] temp = new String[retr.length + 1];
				for (int s = 0; s < retr.length; s++) {
					temp[s] = retr[s];
				}
				retr = temp;
				retr[retr.length - 1] = (i + 1 < args.length) ? args[i + 1]
						: null;
			}
		}
		return retr;
	}

	/**
	 * scan args for a particular arg, return the following token(s) or the
	 * empty string if not found
	 * 
	 * @param id
	 *            the arg to search for
	 * @param args
	 *            the array of strings
	 * @return the following token, or a 0 length string array if not found
	 */
	private static String[] getMultipleArg2(String id, String[] args) {
		String[] retr = {};
		for (int i = 0; i < args.length; i++) {
			if (id.equals(args[i])) {
				int j = 0;
				while ((i + 1 + j < args.length)
						&& !args[i + 1 + j].startsWith("-")) {
					String[] temp = new String[retr.length + 1];
					for (int s = 0; s < retr.length; s++) {
						temp[s] = retr[s];
					}
					retr = temp;
					retr[retr.length - 1] = args[i + 1 + j++];
				}
				return retr;
			}
		}
		return retr;
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
