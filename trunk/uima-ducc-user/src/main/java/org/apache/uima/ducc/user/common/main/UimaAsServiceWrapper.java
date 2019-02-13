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
import java.net.URLClassLoader;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.uima.ducc.user.common.investment.Investment;
import org.apache.uima.ducc.user.jp.iface.IProcessContainer;

public class UimaAsServiceWrapper implements IServiceWrapper {
	private Method stopMethod;
	private Method startMethod;
	private Object duccContainerInstance;
	private Logger logger = Logger.getLogger(UimaAsServiceWrapper.class.getName());
	private Investment investment = new Investment();
	boolean DEBUG = false;

	public UimaAsServiceWrapper() {

	}

	@Override
	public void initialize(String[] args) throws Exception {
		try {

			// cache current context classloader
			ClassLoader sysCL = Thread.currentThread().getContextClassLoader();

			// Fetch a classpath for the fenced Ducc container
			String duccContainerClasspath = System.getProperty("ducc.deploy.DuccClasspath");
			URLClassLoader ucl = DuccJobService.create(duccContainerClasspath);
			if (System.getProperty("ducc.debug") != null) {
				DEBUG = true;
			}
			if (DEBUG) {
				DuccJobService.dump(ucl, 4);
			}

			// Load the DuccService class and find its methods of interest
			Class<?> duccServiceClass = ucl.loadClass("org.apache.uima.ducc.common.main.DuccService");
			Method bootMethod = duccServiceClass.getMethod("boot", String[].class);
			Method setProcessorMethod = duccServiceClass.getMethod("setProcessor", Object.class, String[].class);
			Method registerInvestmentInstanceMethod = duccServiceClass.getMethod("registerInvestmentInstance",
					Object.class);
			startMethod = duccServiceClass.getMethod("start");
			stopMethod = duccServiceClass.getMethod("stop");
			// Establish user's logger early to prevent the DUCC code from accidentally
			// doing so
			logger.log(Level.INFO, ">>>>>>>>> Booting Ducc Container");

			HashMap<String, String> savedPropsMap = hideLoggingProperties(); // Ensure DUCC doesn't try to use the
																				// user's logging setup

			// Construct & initialize Ducc fenced container.
			// It calls component's Configuration class
			Thread.currentThread().setContextClassLoader(ucl);
			duccContainerInstance = duccServiceClass.newInstance();
			bootMethod.invoke(duccContainerInstance, (Object) args);

			logger.log(Level.INFO, "<<<<<<<< Ducc Container booted");
			restoreLoggingProperties(savedPropsMap); // May not be necessary as user's logger has been established

			// below property is set by component's Configuration class. It can also
			// be provided on the command line in case a custom processor is needed.
			String processorClass = System.getProperty("ducc.deploy.JpProcessorClass");

			// Instantiate process container where the actual analysis will be done.
			// Currently there are three containers:
			// 1 - UimaProcessContainer - used for pieces parts (UIMA only)
			// 2 - UimaASProcessContainer - used for DD jobs
			// 3 - UimaASServiceContainer - used for UIMA-AS based services
			//
			// NOTE: the container class is loaded by the main System classloader
			// and requires uima-ducc-user jar to be in the System classpath.
			// --------------------------------------------------------------------
			// load the process container class using the initial system class loader
			Class<?> processorClz = sysCL.loadClass(processorClass);
			IProcessContainer pc = (IProcessContainer) processorClz.newInstance();

			logger.log(Level.INFO, ">>>>>>>>> Running Ducc Container class:" + pc.getClass().getName());

			// Call DuccService.setProcessor() to hand-off instance of the
			// process container to the component along with this process args
			setProcessorMethod.invoke(duccContainerInstance, pc, args);

			// Hand-off investment object
			registerInvestmentInstanceMethod.invoke(duccContainerInstance, investment);

			logger.log(Level.INFO, "<<<<<<<< Ducc Container ended");

		} catch (Throwable t) {
			t.printStackTrace();
			System.out.println("Exiting Process Due to Unrecoverable Error");
			Runtime.getRuntime().halt(99); // User Error
		}
	}

	@Override
	public void start() throws Exception {
		// Call DuccService.start() to initialize the process and begin processing
		startMethod.invoke(duccContainerInstance);

	}

	public static HashMap<String, String> hideLoggingProperties() {
		String[] propsToSave = { "log4j.configuration", "java.util.logging.config.file",
				"java.util.logging.config.class", "org.apache.uima.logger.class" };
		HashMap<String, String> savedPropsMap = new HashMap<String, String>();
		for (String prop : propsToSave) {
			String val = System.getProperty(prop);
			if (val != null) {
				savedPropsMap.put(prop, val);
				System.getProperties().remove(prop);
				// System.out.println("!!!! Saved prop " + prop + " = " + val);
			}
		}
		return savedPropsMap;
	}

	public static void restoreLoggingProperties(HashMap<String, String> savedPropsMap) {
		for (String prop : savedPropsMap.keySet()) {
			System.setProperty(prop, savedPropsMap.get(prop));
			// System.out.println("!!!! Restored prop " + prop + " = " +
			// System.getProperty(prop));
		}
	}

	@Override
	public void stop() throws Exception {
		try {
			stopMethod.invoke(duccContainerInstance);
		} catch (Throwable t) {
			logger.log(Level.SEVERE, "Stop failed");
			t.printStackTrace();
		}

	}

}
