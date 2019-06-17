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
package org.apache.uima.ducc.user.common.main;

import java.net.BindException;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import org.apache.activemq.broker.BrokerService;
import org.apache.uima.aae.client.UimaAsynchronousEngine;
import org.apache.uima.aae.monitor.statistics.AnalysisEnginePerformanceMetrics;
import org.apache.uima.adapter.jms.client.BaseUIMAAsynchronousEngine_impl;
import org.apache.uima.cas.CAS;
import org.apache.uima.ducc.user.common.BasicUimaMetricsGenerator;
import org.apache.uima.resource.metadata.ProcessingResourceMetaData;

public class UimaAsWrapper extends AbstractWrapper {
	public static final String brokerPropertyName = "ducc.broker.name";
	public static final String queuePropertyName = "ducc.queue.name";
	public static final String duccNodeName = "DUCC_NODENAME";
	private static final char FS = System.getProperty("file.separator").charAt(0);

    private BrokerService broker = null;
    private UimaAsynchronousEngine client = null;
    private String serviceId = null;
    private String aeName = null;
	private boolean deserializeFromXMI;

	public void initialize(String analysisEngineDescriptor, String xslTransform, String saxonURL,  int scaleout,  boolean deserialize) throws Exception {
		synchronized (UimaAsWrapper.class) {
			deserializeFromXMI = deserialize;
			// A single thread should deploy a broker and a service.
			synchronized(UimaAsWrapper.class ) {
				if ( broker == null ) {
					deployBroker();
					Map<String, Object> appCtx = new HashMap<>();
					appCtx.put(UimaAsynchronousEngine.DD2SpringXsltFilePath, xslTransform.replace('/', FS));
					appCtx.put(UimaAsynchronousEngine.SaxonClasspath, saxonURL.replace('/', FS));
					appCtx.put(UimaAsynchronousEngine.CasPoolSize, scaleout);
					
					client = new BaseUIMAAsynchronousEngine_impl();
					
					serviceId = deployService(analysisEngineDescriptor, appCtx);
					ProcessingResourceMetaData meta = client.getMetaData();
					aeName = meta.getName();
				}
			}
		}

	}
	public synchronized List<Properties> process(String serializedTask) throws Exception {

		CAS cas = client.getCAS();
		cas.reset();
		try {
			if (deserializeFromXMI) {
				super.deserializeCasFromXmi(serializedTask, cas);
			} else {
				cas.setDocumentText(serializedTask);
				cas.setDocumentLanguage("en");
			}
			
			List<AnalysisEnginePerformanceMetrics> metrics = new ArrayList<>();
			
			client.sendAndReceiveCAS(cas,metrics);
			List<Properties> analysisManagementObjects = new ArrayList<>();
			for( AnalysisEnginePerformanceMetrics aeMetrics : metrics ) {
				Properties p = new Properties();
				p.setProperty(BasicUimaMetricsGenerator.AE_NAME, aeMetrics.getName());
				p.setProperty(BasicUimaMetricsGenerator.AE_CONTEXT, aeMetrics.getUniqueName());
				p.setProperty(BasicUimaMetricsGenerator.AE_ANALYSIS_TIME, String.valueOf(aeMetrics.getAnalysisTime()));
				p.setProperty(BasicUimaMetricsGenerator.AE_CAS_PROCESSED, String.valueOf(aeMetrics.getNumProcessed()));
				analysisManagementObjects.add(p);
			}
			return analysisManagementObjects;

		} finally {
			cas.release();
		}

	}
	public void stop() throws Exception {
		try {
			if ( Objects.nonNull(client)) {
				client.stop();
			}
		} finally {
			if ( Objects.nonNull(broker)) {
				broker.stop();
				broker.waitUntilStopped();
			}

		}
	}
	private HashMap<String,String> hideLoggingProperties() {
		String[] propsToSave = { "log4j.configuration", 
				                 "java.util.logging.config.file",
							     "java.util.logging.config.class",
							     "org.apache.uima.logger.class"};
		HashMap<String, String> savedPropsMap = new HashMap<String,String>();
		for (String prop : propsToSave) {
			String val = System.getProperty(prop);
			if (val != null) {
				savedPropsMap.put(prop,  val);
				System.getProperties().remove(prop);
				//System.out.println("!!!! Saved prop " + prop + " = " + val);
			}
		}
		return savedPropsMap;
	}

	private void restoreLoggingProperties(HashMap<String,String> savedPropsMap) {
		for (String prop : savedPropsMap.keySet()) {
			System.setProperty(prop, savedPropsMap.get(prop));
		}
	}

	private void deployBroker() throws Exception {
		HashMap<String, String> savedPropsMap = null;

		savedPropsMap = hideLoggingProperties(); // Ensure DUCC doesn't try to use the user's logging setup
        try {
    		broker = new BrokerService();
    		broker.setDedicatedTaskRunner(false); 
    		broker.setPersistent(false);
    		// try to first start the embedded broker on port 616226. If not
    		// available loop until a valid port is found
    		startBroker("tcp://localhost:", 61626);

        } finally {
			restoreLoggingProperties(savedPropsMap); // May not be necessary as user's logger has been established
        }
	}
	private String deployService(String analysisEngineDescriptor, Map<String, Object> appCtx) throws Exception {

		String containerId = client.deploy(analysisEngineDescriptor, appCtx);
		
		String endpoint = System.getProperty(queuePropertyName);
		String brokerURL = System.getProperty(brokerPropertyName);

		appCtx.put(UimaAsynchronousEngine.ServerUri, brokerURL);
		appCtx.put(UimaAsynchronousEngine.ENDPOINT, endpoint);
		appCtx.put(UimaAsynchronousEngine.Timeout, 0);
		appCtx.put(UimaAsynchronousEngine.GetMetaTimeout, 0);
		appCtx.put(UimaAsynchronousEngine.CpcTimeout, 1100);

		client.initialize(appCtx);
		
		return containerId;
	}
	private void startBroker(String brokerURL, int startPort) throws Exception {
		// loop until a valid port is found for the broker
		while (true) {
			
			try {
				broker.addConnector(brokerURL+startPort);
				broker.start();
				broker.waitUntilStarted();
				System.setProperty("DefaultBrokerURL", brokerURL + startPort);
				System.setProperty("BrokerURI", brokerURL + startPort);
				// Needed to resolve ${broker.name} placeholder in DD generated by DUCC
				System.setProperty(brokerPropertyName, brokerURL + startPort);

				break; // got a valid port for the broker
				
			} catch (Exception e) {
				if (isBindException(e)) {
					startPort++;
				} else {
					throw new RuntimeException(e);
				}
			}
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
}
