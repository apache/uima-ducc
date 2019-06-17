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

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import org.apache.uima.UIMAFramework;
import org.apache.uima.analysis_engine.AnalysisEngine;
import org.apache.uima.analysis_engine.metadata.AnalysisEngineMetaData;
import org.apache.uima.cas.CAS;
import org.apache.uima.ducc.user.common.BasicUimaMetricsGenerator;
import org.apache.uima.ducc.user.common.UimaUtils;
import org.apache.uima.resource.Resource;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.ResourceManager;
import org.apache.uima.resource.ResourceSpecifier;
import org.apache.uima.util.CasPool;

public class UimaWrapper extends AbstractWrapper {
	private CasPool casPool = null;
	private ResourceManager rm = UIMAFramework.newDefaultResourceManager();
	// Platform MBean server if one is available (Java 1.5 only)
	private static Object platformMBeanServer;
	private boolean deserializeFromXMI;

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

	public void initialize(String analysisEngineDescriptor, int scaleout, boolean deserialize,
			ThreadLocal<AnalysisEngine> threadLocal) throws Exception {

		ResourceSpecifier rSpecifier = UimaUtils.getResourceSpecifier(analysisEngineDescriptor);
		HashMap<String, Object> paramsMap = new HashMap<>();
		paramsMap.put(Resource.PARAM_RESOURCE_MANAGER, rm);
		paramsMap.put(AnalysisEngine.PARAM_MBEAN_SERVER, platformMBeanServer);
		deserializeFromXMI = deserialize;

		AnalysisEngine ae = UIMAFramework.produceAnalysisEngine(rSpecifier, paramsMap);
		// pin AE instance to this thread
		threadLocal.set(ae);

		// check out AE instance pinned to this thread

		synchronized (UimaWrapper.class) {
			if (casPool == null) {
				initializeCasPool(ae.getAnalysisEngineMetaData(), scaleout);
			}
		}
	}

	public List<Properties> process(String serializedTask, ThreadLocal<AnalysisEngine> threadLocal) throws Exception {

		CAS cas = casPool.getCas();
		try {
			if (deserializeFromXMI) {
				super.deserializeCasFromXmi(serializedTask, cas);
			} else {
				cas.setDocumentText(serializedTask);
				cas.setDocumentLanguage("en");

			}
			// check out AE instance pinned to this thread
			AnalysisEngine ae = threadLocal.get();
			List<Properties> preProcessMetrics = BasicUimaMetricsGenerator.get(ae);
			ae.process(cas);
			List<Properties> postProcessMetrics = BasicUimaMetricsGenerator.get(ae);
			return BasicUimaMetricsGenerator.getDelta(postProcessMetrics, preProcessMetrics);

		} finally {
			if (cas != null) {
				casPool.releaseCas(cas);
			}
		}

	}

	public void stop(ThreadLocal<AnalysisEngine> threadLocal) {
		AnalysisEngine ae = threadLocal.get();
		if (ae != null) {
			ae.destroy();
		}
	}

	private void initializeCasPool(AnalysisEngineMetaData analysisEngineMetadata, int scaleout)
			throws ResourceInitializationException {
		Properties props = new Properties();
		props.setProperty(UIMAFramework.CAS_INITIAL_HEAP_SIZE, "1000");
		casPool = new CasPool(scaleout, analysisEngineMetadata, rm);
	}

}
