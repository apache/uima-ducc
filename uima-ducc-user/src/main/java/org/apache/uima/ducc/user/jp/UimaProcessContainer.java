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


import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.uima.UIMAFramework;
import org.apache.uima.analysis_engine.AnalysisEngine;
import org.apache.uima.analysis_engine.AnalysisEngineManagement;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.impl.XmiSerializationSharedData;
import org.apache.uima.ducc.user.common.UimaUtils;
import org.apache.uima.ducc.user.jp.uima.UimaAnalysisEngineInstancePoolWithThreadAffinity;
import org.apache.uima.impl.UimaVersion;
import org.apache.uima.resource.Resource;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.ResourceManager;
import org.apache.uima.resource.ResourceSpecifier;
import org.apache.uima.util.CasPool;
import org.apache.uima.util.Level;
import org.apache.uima.util.Logger;


public class UimaProcessContainer extends DuccAbstractProcessContainer {
	public static final String IMPORT_BY_NAME_PREFIX = "*importByName:";
//	private DuccUimaSerializer uimaSerializer = new DuccUimaSerializer();

    private static ResourceManager rm=null;
	Semaphore sharedInitSemaphore = new Semaphore(1);
	// this map enforces thread affinity to specific thread. Needed to make
	// sure that a thread used to initialized the AE is used to call process().
	// Some AEs depend on ThreadLocal storage.
	UimaAnalysisEngineInstancePoolWithThreadAffinity instanceMap = new UimaAnalysisEngineInstancePoolWithThreadAffinity();
   
	private static CasPool casPool = null;
	  AtomicInteger counter = new AtomicInteger();
	  private String analysisEngineDescriptor=null;
	  private static CountDownLatch latch = new CountDownLatch(1);
	  /**
	   * The platform MBean server if one is available (Java 1.5 only)
	   */
	  private static Object platformMBeanServer;

	  static {
	    // try to get platform MBean Server (Java 1.5 only)
	    try {
	      Class<?> managementFactory = Class.forName("java.lang.management.ManagementFactory");
	      Method getPlatformMBeanServer = managementFactory.getMethod("getPlatformMBeanServer",
	              new Class[0]);
	      platformMBeanServer = getPlatformMBeanServer.invoke(null, (Object[]) null);
	    } catch (Exception e) {
	      platformMBeanServer = null;
	    }
	  }

	    // maintain thread affinity to specific instance of AE
	  private volatile boolean threadAffinity=true;
	    /*
	  private String gen(int length) {
		  StringBuffer sb = new StringBuffer();
		  for(int i=length; i > 0; i -= 12) {
			  int n = Math.min(12, Math.abs(i));
			  sb.append(org.apache.commons.lang.StringUtils.leftPad(Long.toString(Math.round(Math.random()*Math.pow(36,n)),n),'0'));
		  }
		  return sb.toString();
	  }
	  */
	  public boolean useThreadAffinity() {
	   	return threadAffinity;
	  }

    private int configureAndGetScaleout(String[] args ) throws Exception {
	    analysisEngineDescriptor = ArgsParser.getArg("-aed", args);
		scaleout = Integer.valueOf(ArgsParser.getArg("-t", args));
		String jobType = System.getProperty("ducc.deploy.JpType"); 
		if ( "uima".equals(jobType)) {
		  System.out.println("UIMA Version:"+UimaVersion.getFullVersionString());
		} 
        return scaleout;		  
	}
	public byte[] getLastSerializedError() throws Exception {

		if (lastError != null) {

			return super.serialize(lastError);
		}
		return null;

	}

	public int doInitialize(Properties props, String[] args) throws Exception {
			return configureAndGetScaleout(args);
	}
	public void doDeploy() throws Exception {
	
		System.out.println("....... UimaProcessContainer.doDeploy()");
		ResourceSpecifier rSpecifier = null;
	    HashMap<String,Object> paramsMap = 
				new HashMap<String,Object>();
	    synchronized(UimaProcessContainer.class) {
	    	if ( rm == null ) {
	    		rm = UIMAFramework.newDefaultResourceManager();
	    	}
	    }
	     paramsMap.put(Resource.PARAM_RESOURCE_MANAGER, rm);
	     paramsMap.put(AnalysisEngine.PARAM_MBEAN_SERVER, platformMBeanServer);

		try {
			// Acquire single-permit semaphore to serialize instantiation of
			// AEs. This is done to control access to non-thread safe 
			// structures in the core. The sharedInitSemaphore is a static 
			// and is shared by all instances of this class.
			sharedInitSemaphore.acquire();
			// Parse the descriptor in the calling thread.
			rSpecifier = UimaUtils.getResourceSpecifier(analysisEngineDescriptor);
			AnalysisEngine ae = UIMAFramework.produceAnalysisEngine(rSpecifier,
					paramsMap);

			instanceMap.checkin(ae);
			if (instanceMap.size() == scaleout) {
				try {
					Properties props = new Properties();
			        props.setProperty(UIMAFramework.CAS_INITIAL_HEAP_SIZE, "1000");

					analysisEngineMetadata = ae.getAnalysisEngineMetaData();
					casPool = new CasPool(scaleout, analysisEngineMetadata,rm);
					latch.countDown();
				} catch (Exception e) {
					throw new ResourceInitializationException(e);
				}
			}

		} catch (Throwable e) {
			latch.countDown();
			Logger logger = UIMAFramework.getLogger();
			logger.log(Level.WARNING, "UimaProcessContainer", e);
			e.printStackTrace();
			throw new RuntimeException(e);

		} finally {
			sharedInitSemaphore.release();
		}
		
	}

	public void doStop() throws Exception {
		try {
			AnalysisEngine ae = instanceMap.checkout();
			if ( ae != null ) {
				ae.destroy();
			}
		} catch( Exception e) {
	        e.printStackTrace();		
		} finally {
		}
	}

	public List<Properties> doProcess(Object xmi) throws Exception {
		AnalysisEngine ae = null;
		latch.await();
		CAS cas = casPool.getCas();
		try {
			// reset last error
			lastError = null;
			XmiSerializationSharedData deserSharedData = new XmiSerializationSharedData();
			// deserialize the CAS
			super.getUimaSerializer().
			    deserializeCasFromXmi((String)xmi, cas, deserSharedData, true,-1);

			// the following checks out AE instance pinned to this thread
			ae = instanceMap.checkout();
			List<AnalysisEnginePerformanceMetrics> beforeAnalysis = getMetrics(ae);
			ae.process(cas);
			List<AnalysisEnginePerformanceMetrics> afterAnalysis = getMetrics(ae);

			// get the delta
			List<AnalysisEnginePerformanceMetrics> casMetrics = getAEMetricsForCAS(
					 afterAnalysis, beforeAnalysis);

			// convert UIMA-AS metrics into properties so that we can return
			// this
			// data in a format which doesnt require UIMA-AS to digest
			List<Properties> metricsList = new ArrayList<Properties>();
			
			for (AnalysisEnginePerformanceMetrics metrics : casMetrics) {
				Properties p = new Properties();
				
				p.setProperty("name", metrics.getName());
				p.setProperty("uniqueName", metrics.getUniqueName());
				p.setProperty("analysisTime",
						String.valueOf(metrics.getAnalysisTime()));
				p.setProperty("numProcessed",
						String.valueOf(metrics.getNumProcessed()));
				metricsList.add(p);
			}
			
			return metricsList;
		} catch( Throwable e ) {
			super.lastError = e;
			Logger logger = UIMAFramework.getLogger();
			logger.log(Level.WARNING, "UimaProcessContainer", e);
			e.printStackTrace();
			throw new AnalysisEngineProcessException();
		}
		finally {
			if (ae != null) {
				instanceMap.checkin(ae);
			}
			if (cas != null) {
				casPool.releaseCas(cas);
			}
		}
	}
	   private List<AnalysisEnginePerformanceMetrics> getMetrics(AnalysisEngine ae)
			throws Exception {
		List<AnalysisEnginePerformanceMetrics> analysisManagementObjects = new ArrayList<AnalysisEnginePerformanceMetrics>();
		synchronized(UimaProcessContainer.class) {
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

	private void getLeafManagementObjects(AnalysisEngineManagement aem,
			List<AnalysisEnginePerformanceMetrics> result) {
		getLeafManagementObjects(aem, result, "");
	}

	/**
	 * Recursively
	 * 
	 * @param aem
	 * @param result
	 * @param uimaFullyQualifiedAEContext
	 */
	private void getLeafManagementObjects(AnalysisEngineManagement aem,
			List<AnalysisEnginePerformanceMetrics> result,
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
					uimaFullyQualifiedAEContext = "";
/*
					// check id the parent aggregate has been scaled up by
					// looking at the last char in its name. If it is a number
					// strip it from the name
					if (Character.isDigit(uimaFullyQualifiedAEContext
							.charAt(uimaFullyQualifiedAEContext.length() - 1))
							&& uimaFullyQualifiedAEContext.lastIndexOf(" ") > -1) {
						String indx = uimaFullyQualifiedAEContext
								.substring(uimaFullyQualifiedAEContext
										.lastIndexOf(" "));
						if (indx != null) {
							int value = -1;
							try {
								value = Integer.parseInt(indx.trim());
								// Prepend "X Components" to the unique name
								// with X stripped.
								uimaFullyQualifiedAEContext = value
										+ " Components "
										+ uimaFullyQualifiedAEContext
												.substring(
														0,
														uimaFullyQualifiedAEContext
																.lastIndexOf(" "));
							} catch (NumberFormatException ex) {

							}
						}
					}
					*/
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

	private String produceUniqueName(AnalysisEngineManagement aem) {
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

	private AnalysisEnginePerformanceMetrics deepCopyMetrics(
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
			return new AnalysisEnginePerformanceMetrics(aem.getName(),
					uimaFullyQualifiedAEContext, aem.getAnalysisTime(),
					aem.getNumberOfCASesProcessed());
			
		
	}

	private List<AnalysisEnginePerformanceMetrics> getAEMetricsForCAS(
			List<AnalysisEnginePerformanceMetrics> afterAnalysisManagementObjects,
			List<AnalysisEnginePerformanceMetrics> beforeAnalysisManagementObjects)
			throws Exception {
		// Create a List to hold per CAS analysisTime and total number of CASes
		// processed
		// by each AE. This list will be serialized and sent to the client
		List<AnalysisEnginePerformanceMetrics> performanceList = new ArrayList<AnalysisEnginePerformanceMetrics>();
		// Diff the before process() performance metrics with post process
		// performance
		// metrics
		for (AnalysisEnginePerformanceMetrics after : afterAnalysisManagementObjects) {
			for (AnalysisEnginePerformanceMetrics before : beforeAnalysisManagementObjects) {
				if (before.getUniqueName().equals(after.getUniqueName())) {

					AnalysisEnginePerformanceMetrics metrics = new AnalysisEnginePerformanceMetrics(
							after.getName(), after.getUniqueName(),
							after.getAnalysisTime() - before.getAnalysisTime(),
							after.getNumProcessed());
					// System.out.println("********************"+metrics.getUniqueName());
					// System.out.println("********************"+metrics.getName());
					performanceList.add(metrics);
					break;
				}
			}
		}
		return performanceList;

	}
	private static class AnalysisEnginePerformanceMetrics {
		  
		  private String name;
		  private String uniqueName;
		  private long analysisTime;
		  private long numProcessed;
		  
		  /**
		   * Creates a performance metrics instance
		   * 
		   */
		  public AnalysisEnginePerformanceMetrics(String name, String uimaContextPath, long analysisTime, long numProcessed ) {
		    this.name = name;
		    this.uniqueName = uimaContextPath;
		    this.analysisTime = analysisTime;
		    this.numProcessed = numProcessed;
		  }

		  /**
		   * Gets the local name of the component as specified in the aggregate
		   * 
		   * @return the name
		   */
		  public String getName() {
		    return name;
		  }

		  /**
		   * Gets the unique name of the component reflecting its location in the aggregate hierarchy
		   * 
		   * @return the unique name
		   */
		  public String getUniqueName() {
		    if ( uniqueName != null && uniqueName.trim().length() > 0 && !uniqueName.trim().equals("Components")) {
//		    	if ( !uimaContextPath.endsWith(getName())) {
//		    		return uimaContextPath+"/"+getName();
//		    	}
		      return uniqueName;
		    } else {
		      return getName();
		    }
		  }

		  /**
		   * Gets the elapsed time the CAS spent analyzing this component
		   * 
		   * @return time in milliseconds
		   */
		  public long getAnalysisTime() {
		    return analysisTime;
		  }

		  /**
		   * Gets the total number of CASes processed by this component so far
		   * 
		   * @return number processed
		   */
		  public long getNumProcessed() {
		    return numProcessed;
		  }
		  
		}

}
