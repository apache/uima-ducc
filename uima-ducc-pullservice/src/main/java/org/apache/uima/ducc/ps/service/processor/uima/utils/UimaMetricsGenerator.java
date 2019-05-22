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
package org.apache.uima.ducc.ps.service.processor.uima.utils;

import java.util.ArrayList;
import java.util.List;

import org.apache.uima.UIMAFramework;
import org.apache.uima.analysis_engine.AnalysisEngine;
import org.apache.uima.analysis_engine.AnalysisEngineManagement;
import org.apache.uima.util.Level;
import org.apache.uima.util.Logger;

public class UimaMetricsGenerator {
	
	private UimaMetricsGenerator() {}
	
	public static void getLeafManagementObjects(AnalysisEngineManagement aem,
			List<PerformanceMetrics> result) {
		getLeafManagementObjects(aem, result, "");
	}
	   public static List<PerformanceMetrics> get(AnalysisEngine ae)
			throws Exception {
		List<PerformanceMetrics> analysisManagementObjects = new ArrayList<PerformanceMetrics>();
		synchronized(UimaMetricsGenerator.class) {
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
				// System.out.println("-----------------Unique1:"+aem.getUniqueMBeanName());
				// System.out.println("-----------------Simple1:"+aem.getName());
			} else {
				String path = produceUniqueName(aem);
//				 System.out.println(Thread.currentThread().getId()+" -----------------Unique2:"+aem.getUniqueMBeanName());
//				 System.out.println(Thread.currentThread().getId()+" -----------------Simple2:"+aem.getName());
//				 System.out.println(Thread.currentThread().getId()+" -----------------Path:"+path);
				analysisManagementObjects.add(deepCopyMetrics(aem, path));

			}
			
		}
		return analysisManagementObjects;
	}
	   public static List<PerformanceMetrics> get(AnalysisEngineManagement aem)
			throws Exception {
		List<PerformanceMetrics> analysisManagementObjects = new ArrayList<PerformanceMetrics>();
		synchronized(UimaMetricsGenerator.class) {
			// Fetch AE's management information that includes per component
			// performance stats
			// These stats are internally maintained in a Map. If the AE is an
			// aggregate
			// the Map will contain AnalysisEngineManagement instance for each AE.
			//AnalysisEngineManagement aem = ae.getManagementInterface();
			if (aem.getComponents().size() > 0) {
				// Flatten the hierarchy by recursively (if this AE is an aggregate)
				// extracting
				// primitive AE's AnalysisEngineManagement instance and placing it
				// in
				// afterAnalysisManagementObjects List.
				getLeafManagementObjects(aem, analysisManagementObjects);
				// System.out.println("-----------------Unique1:"+aem.getUniqueMBeanName());
				// System.out.println("-----------------Simple1:"+aem.getName());
			} else {
				String path = produceUniqueName(aem);
//				 System.out.println(Thread.currentThread().getId()+" -----------------Unique2:"+aem.getUniqueMBeanName());
//				 System.out.println(Thread.currentThread().getId()+" -----------------Simple2:"+aem.getName());
//				 System.out.println(Thread.currentThread().getId()+" -----------------Path:"+path);
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
	private static void getLeafManagementObjects(AnalysisEngineManagement aem,
			List<PerformanceMetrics> result,
			String uimaFullyQualifiedAEContext) {
//		System.out.println("----------- 1 getLeafManagementObjects() - Unique Name:"+aem.getUniqueMBeanName()+" UniqueContext:"+uimaFullyQualifiedAEContext);
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
				    if ( p1indx > -1 ) {
				    	String tmp = aem.getUniqueMBeanName().substring(p1indx);
				    	String[] parts = tmp.split(",");
				    	for( String part : parts ) {
				    		if ( part.startsWith("name=") ) {
				    			uimaFullyQualifiedAEContext += "/"+part.substring(5);
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
			for (AnalysisEngineManagement child : (Iterable<AnalysisEngineManagement>) aem
					.getComponents().values()) {
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
				sb.append("/").append(
						part.substring(part.trim().indexOf("=") + 1));
			}
		}
		return sb.toString();
	}

	private static PerformanceMetrics deepCopyMetrics(
			AnalysisEngineManagement aem, String uimaFullyQualifiedAEContext) {
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
				if ( last == -1 ) {
					index = "1";	
				} else {
					index = tmp.substring(last).trim();
				}
//				System.out.println("uimaFullyQualifiedAEContext.trim().length()="+uimaFullyQualifiedAEContext.trim().length() );
				if (uimaFullyQualifiedAEContext.trim().length() > 0 && last > -1) {
					// extract instance number
					

					try {
						// check if the instance number is a number. If not silently
						// handle the exception.
						Integer.parseInt(index);
//						System.out.println("deepCopyMetrics - context:"+uimaFullyQualifiedAEContext+" last="+last);
						// strip the instance number from the AE name
						uimaFullyQualifiedAEContext = uimaFullyQualifiedAEContext
								.substring(0, last + 1);
					} catch (NumberFormatException nfe) {

					} catch( Exception e) {
//						System.out.println(Thread.currentThread().getId()+" deepCopyMetrics - context:"+uimaFullyQualifiedAEContext+" last="+last);
					}
				} else {

					if (!uimaFullyQualifiedAEContext.endsWith(tmp)) {
						uimaFullyQualifiedAEContext += "/" + tmp;
					}
				}
			}
			// Primitive AE will not have "X Components" prefix, but it is required
			// by the DUCC JD to be there. Prepend it to the unique name.
			/*
			if (uimaFullyQualifiedAEContext.indexOf(" Components ") == -1) {
				uimaFullyQualifiedAEContext = index + " Components "
						+ uimaFullyQualifiedAEContext;
			}
			*/
			return new PerformanceMetrics(aem.getName(),
					uimaFullyQualifiedAEContext, aem.getAnalysisTime(), aem.getNumberOfCASesProcessed());
			
		
	}

	public static List<PerformanceMetrics> getDelta(
			List<PerformanceMetrics> afterAnalysisManagementObjects,
			List<PerformanceMetrics> beforeAnalysisManagementObjects)
			throws Exception {
		// Create a List to hold per CAS analysisTime and total number of CASes processed by each AE. 
	    // This list will be serialized and sent to the client
		List<PerformanceMetrics> performanceList = new ArrayList<PerformanceMetrics>();
		// Diff the before process() performance metrics with post process performance metrics
		for (PerformanceMetrics after : afterAnalysisManagementObjects) {
			for (PerformanceMetrics before : beforeAnalysisManagementObjects) {
				String uniqueName = after.getUniqueName();
				if (before.getUniqueName().equals(after.getUniqueName())) {
					
					if ( (after.getAnalysisTime() - before.getAnalysisTime()) < 0 ) {
						Logger logger = UIMAFramework.getLogger();
						logger.log(Level.WARNING, "Thread:"+Thread.currentThread()+" UimaProcessContainer.getAEMetricsForCAS() - Unexpected negative result for analysis time:"+(after.getAnalysisTime()-before.getAnalysisTime())+" Component:"+uniqueName+" before="+before.getAnalysisTime()+" after="+after.getAnalysisTime());
					}
					PerformanceMetrics metrics = new PerformanceMetrics(
							after.getName(), uniqueName,
							after.getAnalysisTime() - before.getAnalysisTime(),
							after.getNumberOfTasksProcessed() - before.getNumberOfTasksProcessed());
					performanceList.add(metrics);
					break;
				}
			}
		}
		return performanceList;

	}
	
}
