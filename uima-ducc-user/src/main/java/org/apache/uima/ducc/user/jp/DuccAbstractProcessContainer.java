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

import org.apache.uima.ducc.user.common.DuccUimaSerializer;
import org.apache.uima.ducc.user.jp.iface.IProcessContainer;

public abstract class DuccAbstractProcessContainer implements IProcessContainer{
	// Container implementation must implement the following methods
    protected abstract void doDeploy() throws Exception;
    protected abstract int doInitialize(Properties p, String[] arg) throws Exception;
    protected abstract void doStop() throws Exception;
    protected abstract List<Properties>  doProcess(Object subject) throws Exception;

	protected Throwable lastError = null;
    protected int scaleout=1;
    // Map to store DuccUimaSerializer instances. Each has affinity to a thread
	protected static Map<Long, DuccUimaSerializer> serializerMap =
			new HashMap<Long, DuccUimaSerializer>();

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
		// save current context cl and inject System classloader as
		// a context cl before calling user code. This is done in 
		// user code needs to load resources 
		ClassLoader savedCL = Thread.currentThread().getContextClassLoader();
		restoreLog4j();
 		System.out.println("DuccAbstractProcessContainer.initialize() >>>>>>>>> Crossing from Ducc Container to User Container");
		Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
		try {

    		return doInitialize(p, arg);
        }finally {
			Thread.currentThread().setContextClassLoader(savedCL);
 	 		System.out.println("DuccAbstractProcessContainer.initialize() <<<<<<<< Returning from User Container to Ducc Container");
			System.setProperty("log4j.configuration", "");
        }
    }
    public void deploy() throws Exception {

    	// save current context cl and inject System classloader as
 		// a context cl before calling user code. 
 		ClassLoader savedCL = Thread.currentThread().getContextClassLoader();
 		restoreLog4j();
 		System.out.println("DuccAbstractProcessContainer.deploy() >>>>>>>>> Crossing from Ducc Container to User Container");
 		Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
         try {
     		doDeploy();
         } finally {
 			Thread.currentThread().setContextClassLoader(savedCL);
 	 		System.out.println("DuccAbstractProcessContainer.deploy() <<<<<<<< Returning from User Container to Ducc Container");
 			//	Pin thread to its own CAS serializer instance
 			serializerMap.put( Thread.currentThread().getId(), new DuccUimaSerializer());
			if ( System.getProperties().containsKey("log4j.configuration")) {
				// remove log4j configuration property after calling user code
				System.getProperties().remove("log4j.configuration");
			}
         }
     }
    public List<Properties> process(Object xmi) throws Exception {
 		// save current context cl and inject System classloader as
 		// a context cl before calling user code. 
 		ClassLoader savedCL = Thread.currentThread().getContextClassLoader();
 		Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
         try {
     		return doProcess(xmi);
         }finally {
 			Thread.currentThread().setContextClassLoader(savedCL);
         }
     }
    public void stop() throws Exception {
 		// save current context cl and inject System classloader as
 		// a context cl before calling user code. 
 		ClassLoader savedCL = Thread.currentThread().getContextClassLoader();
 		Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
 		System.out.println("DuccAbstractProcessContainer.stop() >>>>>>>>> Crossing from Ducc Container to User Container");
         
 		try {
     		doStop();
         }finally {
 			Thread.currentThread().setContextClassLoader(savedCL);
 	 		System.out.println("DuccAbstractProcessContainer.stop() <<<<<<<< Returning from User Container to Ducc Container");

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
