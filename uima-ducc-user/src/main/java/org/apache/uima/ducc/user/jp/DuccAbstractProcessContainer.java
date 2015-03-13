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

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.uima.UIMAFramework;
import org.apache.uima.analysis_engine.metadata.AnalysisEngineMetaData;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.impl.XmiSerializationSharedData;
import org.apache.uima.ducc.CasHelper;
import org.apache.uima.ducc.user.common.DuccUimaSerializer;
import org.apache.uima.ducc.user.jp.iface.IProcessContainer;
import org.apache.uima.resource.metadata.FsIndexDescription;
import org.apache.uima.resource.metadata.TypePriorities;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.CasCreationUtils;

public abstract class DuccAbstractProcessContainer implements IProcessContainer{
	// Container implementation must implement the following methods
    protected abstract void doDeploy() throws Exception;
    protected abstract int doInitialize(Properties p, String[] arg) throws Exception;
    protected abstract void doStop() throws Exception;
    protected abstract List<Properties>  doProcess(Object subject) throws Exception;
    protected 	AnalysisEngineMetaData analysisEngineMetadata;

	protected Throwable lastError = null;
    protected int scaleout=1;
    // Map to store DuccUimaSerializer instances. Each has affinity to a thread
	protected static Map<Long, DuccUimaSerializer> serializerMap =
			new HashMap<Long, DuccUimaSerializer>();
	
	protected final boolean debug = System.getProperty("ducc.debug") != null;

	/**
	 * This method is called to fetch a WorkItem ID from a given CAS which
	 * is required to support investment reset. 
	 *
	 */
	public String getKey(String xmi) throws Exception {
		if ( analysisEngineMetadata == null ) {
			// WorkItem ID (key) is only supported for pieces 'n parts 
			return null;
		} 
		Properties props = new Properties();
        props.setProperty(UIMAFramework.CAS_INITIAL_HEAP_SIZE, "1000");

		TypeSystemDescription tsd = analysisEngineMetadata.getTypeSystem();
		TypePriorities tp = analysisEngineMetadata.getTypePriorities();
		FsIndexDescription[] fsid = analysisEngineMetadata.getFsIndexes();
		CAS cas;
		synchronized( CasCreationUtils.class) {
			cas = CasCreationUtils.createCas(tsd, tp, fsid, props);
		}
		// deserialize the CAS
		XmiSerializationSharedData deserSharedData = new XmiSerializationSharedData();
		getUimaSerializer().
		    deserializeCasFromXmi((String)xmi, cas, deserSharedData, true,-1);
		
		String key = CasHelper.getId(cas);
		cas.release();
		return key;
	}
    public int getScaleout( ){
		return scaleout;
	}

    protected DuccUimaSerializer getUimaSerializer() {
    	return serializerMap.get(Thread.currentThread().getId());
    }

    private void restoreLog4j() {
		String log4jConfigurationFile="";
		if ( (log4jConfigurationFile = System.getProperty("ducc.user.log4j.saved.configuration")) != null ) {
			System.setProperty("log4j.configuration", log4jConfigurationFile);
		}
    }
    public int initialize(Properties p, String[] arg) throws Exception {
    	System.out.println("DuccAbstractProcessContainer.initialize() >>>>>>>>> Initializing User Container");
		// save current context cl and inject System classloader as
		// a context cl before calling user code. This is done in 
		// user code needs to load resources 
    	// Restore user's log4j setting (was hidden from ducc-side logger)
		ClassLoader savedCL = Thread.currentThread().getContextClassLoader();
		restoreLog4j();
		Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
		try {
    		return doInitialize(p, arg);
        }finally {
			Thread.currentThread().setContextClassLoader(savedCL);
			System.getProperties().remove("log4j.configuration");
 	 		System.out.println("DuccAbstractProcessContainer.initialize() <<<<<<<< User Container initialized");
        }
    }
    public void deploy() throws Exception {

    	System.out.println("DuccAbstractProcessContainer.deploy() >>>>>>>>> Deploying User Container");
    	// save current context cl and inject System classloader as
 		// a context cl before calling user code. 
    	// Restore user's log4j setting (was hidden from ducc-side logger)
 		ClassLoader savedCL = Thread.currentThread().getContextClassLoader();
 		restoreLog4j();
 		Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
         try {
     		doDeploy();
         } finally {
 			Thread.currentThread().setContextClassLoader(savedCL);
 			//	Pin thread to its own CAS serializer instance
 			serializerMap.put( Thread.currentThread().getId(), new DuccUimaSerializer());
 			// remove log4j configuration property after calling user code
 			System.getProperties().remove("log4j.configuration");
			System.out.println("DuccAbstractProcessContainer.deploy() <<<<<<<< User Container deployed");
         }
     }
    public List<Properties> process(Object xmi) throws Exception {
    	if (debug) System.out.println("DuccAbstractProcessContainer.process() >>>>>>>>> Processing User Container");
 		// save current context cl and inject System classloader as
 		// a context cl before calling user code. 
    	// No need to restore log4j properties here
 		ClassLoader savedCL = Thread.currentThread().getContextClassLoader();
 		Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
         try {
     		return doProcess(xmi);
         }finally {
 			Thread.currentThread().setContextClassLoader(savedCL);
 			if (debug) System.out.println("DuccAbstractProcessContainer.process() <<<<<<<< User Container processed");
         }
     }
    public void stop() throws Exception {
    	if (debug) System.out.println("DuccAbstractProcessContainer.stop() >>>>>>>>> Stopping User Container");
 		// save current context cl and inject System classloader as
 		// a context cl before calling user code. 
 		ClassLoader savedCL = Thread.currentThread().getContextClassLoader();
 		Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
         
 		try {
     		doStop();
         }finally {
 			Thread.currentThread().setContextClassLoader(savedCL);
 	 		if (debug) System.out.println("DuccAbstractProcessContainer.stop() <<<<<<<< User Container stopped");
         }
     }

    
    protected byte[] serialize(Throwable t) throws Exception {
		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(baos);
			oos.writeObject(t);
			oos.close();
			return baos.toByteArray();
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
	}
}
