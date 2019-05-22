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
package org.apache.uima.ducc.user.common;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.uima.UIMAFramework;
import org.apache.uima.analysis_engine.AnalysisEngine;
import org.apache.uima.analysis_engine.AnalysisEngineManagement;
import org.apache.uima.util.Level;
import org.apache.uima.util.Logger;

public class BasicUimaMetricsGenerator {
	public static final String AE_NAME = "AeName";
	public static final String AE_CONTEXT = "AeContext";
	public static final String AE_ANALYSIS_TIME = "AeAnalysisTime";
	public static final String AE_CAS_PROCESSED = "AeProcessedCasCount";

	private BasicUimaMetricsGenerator() {
		
	}
	public static void getLeafManagementObjects(AnalysisEngineManagement aem, List<Properties> result) {
		getLeafManagementObjects(aem, result, "");
	}

	public static List<Properties> get(AnalysisEngine ae) throws Exception {
		List<Properties> analysisManagementObjects = new ArrayList<>();
		synchronized (BasicUimaMetricsGenerator.class) {
			// Fetch AE's management information that includes per component
			// performance stats
			// These stats are internally maintained in a Map. If the AE is an
			// aggregate
			// the Map will contain AnalysisEngineManagement instance for each AE.
			AnalysisEngineManagement aem = ae.getManagementInterface();
			if (aem.getComponents().size() > 0) {
				// Flatten the hierarchy by recursively (if this AE is an aggregate)
				// extracting
				// primitive AE's AnalysisEngineManagement instance and placing it
				// in
				// afterAnalysisManagementObjects List.
				getLeafManagementObjects(aem, analysisManagementObjects);
			} else {
				String path = produceUniqueName(aem);
				analysisManagementObjects.add(deepCopyMetrics(aem, path));

			}

		}
		return analysisManagementObjects;
	}

	public static List<Properties> get(AnalysisEngineManagement aem) throws Exception {
		List<Properties> analysisManagementObjects = new ArrayList<>();
		synchronized (BasicUimaMetricsGenerator.class) {
			// Fetch AE's management information that includes per component
			// performance stats
			// These stats are internally maintained in a Map. If the AE is an
			// aggregate
			// the Map will contain AnalysisEngineManagement instance for each AE.
			// AnalysisEngineManagement aem = ae.getManagementInterface();
			if (aem.getComponents().size() > 0) {
				// Flatten the hierarchy by recursively (if this AE is an aggregate)
				// extracting
				// primitive AE's AnalysisEngineManagement instance and placing it
				// in
				// afterAnalysisManagementObjects List.
				getLeafManagementObjects(aem, analysisManagementObjects);
			} else {
				String path = produceUniqueName(aem);
				analysisManagementObjects.add(deepCopyMetrics(aem, path));

			}

		}
		return analysisManagementObjects;
	}

	/**
	 * Recursively
	 * 
	 * @param aem
	 * @param result
	 * @param uimaFullyQualifiedAEContext
	 */
	private static void getLeafManagementObjects(AnalysisEngineManagement aem, List<Properties> result,
			String uimaFullyQualifiedAEContext) {
		if (aem.getComponents().isEmpty()) {
			// skip Flow Controller
			if (!aem.getName().equals("Fixed Flow Controller")) {
				// is this primitive AE delegate in an aggregate. If so the
				// mbean unique name will have "p0=" string. An examples mbean
				// name looks like this:
				// org.apache.uima:type=ee.jms.services,s=Top Level Aggregate
				// TAE Uima EE Service,p0=Top Level Aggregate TAE
				// Components,p1=SecondLevelAggregateCM
				// Components,p2=ThirdLevelAggregateCM
				// Components,name=Multiplier1
				if (aem.getUniqueMBeanName().indexOf("p0=") > -1) {
					int p1indx = aem.getUniqueMBeanName().indexOf("p1=");
					if (p1indx > -1) {
						String tmp = aem.getUniqueMBeanName().substring(p1indx);
						String[] parts = tmp.split(",");
						for (String part : parts) {
							if (part.startsWith("name=")) {
								uimaFullyQualifiedAEContext += "/" + part.substring(5);
								break;
							}
						}
					} else {
						uimaFullyQualifiedAEContext = "";
					}

				}
				result.add(deepCopyMetrics(aem, uimaFullyQualifiedAEContext));
			}
		} else {
			for (AnalysisEngineManagement child : (Iterable<AnalysisEngineManagement>) aem.getComponents().values()) {
				getLeafManagementObjects(child, result, produceUniqueName(aem));
			}
		}
	}

	private static String produceUniqueName(AnalysisEngineManagement aem) {
		String[] parts = aem.getUniqueMBeanName().split(",");
		StringBuffer sb = new StringBuffer();
		for (String part : parts) {
			int pos;
			if ((pos = part.indexOf("=")) > -1 && part.startsWith("p")) {
				String n = part.substring(pos + 1, part.indexOf(" Components"));
				if (part.startsWith("p0=") && n.indexOf(" ") > -1) {
					String indx = n.substring(n.lastIndexOf(" "));
					if (indx != null) {
						int instanceNumber = -1;
						try {
							instanceNumber = Integer.parseInt(indx.trim());
							sb.append(instanceNumber).append(" Components ");
							n = n.substring(0, n.lastIndexOf(" "));
						} catch (NumberFormatException nfe) {
						}
					}
				}
				sb.append("/").append(n.trim());
			} else if (part.trim().startsWith("name=") || part.trim().startsWith("org.apache.uima:name=")) {
				sb.append("/").append(part.substring(part.trim().indexOf("=") + 1));
			}
		}
		return sb.toString();
	}

	private static Properties deepCopyMetrics(AnalysisEngineManagement aem, String uimaFullyQualifiedAEContext) {
		String index = "";

		// Create a unique name with each AE name is separated with "/". Prepend
		// "X Components" where
		// X is a instance number of a scaled AE. Also, strip the X from the AE
		// name. The instance number
		// is added to each scaled up component during initialization of the
		// uima-as. We need to prepend
		// "X Components" to allow DUCC JD to parse the unique name correctly (
		// basically for backwards
		// compatibility.
		int pos = aem.getUniqueMBeanName().lastIndexOf("name=");
		if (pos > -1) {
			// get the name of the component. In case of nested component this
			// will be the KEY from AE descriptor
			String tmp = aem.getUniqueMBeanName().substring(pos + 5);
			// in case this is the top level AE, check if it has been scaled up
			// by extracting its instance number.For example,
			// NoOpAnnotator 2.
			int last = tmp.lastIndexOf(" ");
			if (last == -1) {
				index = "1";
			} else {
				index = tmp.substring(last).trim();
			}
			if (uimaFullyQualifiedAEContext.trim().length() > 0 && last > -1) {
				// extract instance number

				try {
					// check if the instance number is a number. If not silently
					// handle the exception.
					Integer.parseInt(index);
					// strip the instance number from the AE name
					uimaFullyQualifiedAEContext = uimaFullyQualifiedAEContext.substring(0, last + 1);
				} catch (NumberFormatException nfe) {

				} catch (Exception e) {
				}
			} else {

				if (!uimaFullyQualifiedAEContext.endsWith(tmp)) {
					uimaFullyQualifiedAEContext += "/" + tmp;
				}
			}
		}
		Properties p = new Properties();
		p.setProperty(AE_NAME, aem.getName());
		p.setProperty(AE_CONTEXT, uimaFullyQualifiedAEContext);
		p.setProperty(AE_ANALYSIS_TIME, String.valueOf(aem.getAnalysisTime()));
		p.setProperty(AE_CAS_PROCESSED, String.valueOf(aem.getNumberOfCASesProcessed()));

		return p;
	}

	public static List<Properties> getDelta(List<Properties> afterAnalysisManagementObjects,
			List<Properties> beforeAnalysisManagementObjects) throws Exception {

		List<Properties> deltaMetrics = new ArrayList<>();
		// Diff the before process() performance metrics with post process performance
		// metrics
		for (Properties after : afterAnalysisManagementObjects) {
			for (Properties before : beforeAnalysisManagementObjects) {
				String uniqueName = (String) after.get(AE_CONTEXT);
				if (((String) before.get(AE_CONTEXT)).equals(uniqueName)) {
					long postProcessCasCount = Long.valueOf(after.getProperty(AE_CAS_PROCESSED));
					long preProcessCasCount = Long.valueOf(before.getProperty(AE_CAS_PROCESSED));

					long deltaProcessCasCount = postProcessCasCount - preProcessCasCount;

					long afterAnalysisTime = Long.valueOf(after.getProperty(AE_ANALYSIS_TIME)).longValue();
					long beforeAnalysisTime = Long.valueOf(before.getProperty(AE_ANALYSIS_TIME)).longValue();

					long deltaAnalysisTime = afterAnalysisTime - beforeAnalysisTime;
					if (deltaAnalysisTime < 0) {
						Logger logger = UIMAFramework.getLogger();
						logger.log(Level.WARNING, "Thread:" + Thread.currentThread()
								+ " UimaProcessContainer.getAEMetricsForCAS() - Unexpected negative result for analysis time:"
								+ (afterAnalysisTime - beforeAnalysisTime) + " Component:" + uniqueName + " before="
								+ beforeAnalysisTime + " after=" + afterAnalysisTime);
					}
					Properties metrics = new Properties();
					metrics.setProperty(AE_NAME, after.getProperty(AE_NAME));
					metrics.setProperty(AE_CONTEXT, uniqueName);
					metrics.setProperty(AE_ANALYSIS_TIME, String.valueOf(deltaAnalysisTime));
					metrics.setProperty(AE_CAS_PROCESSED, String.valueOf(deltaProcessCasCount));
					deltaMetrics.add(metrics);
					break;
				}
			}
		}
		return deltaMetrics;

	}
}
