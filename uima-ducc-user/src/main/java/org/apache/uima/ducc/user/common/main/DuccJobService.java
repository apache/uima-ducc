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

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.uima.ducc.user.common.investment.Investment;

/**
 * Main program that is used to launch Job Process(JP).
 * 
 */
public class DuccJobService {
	boolean DEBUG = false;
	private Investment investment = null;
	private Logger logger = Logger.getLogger(DuccJobService.class.getName());
	private IServiceWrapper service = null;

	public static URLClassLoader create(String classPath)
			throws MalformedURLException {
		return create(classPath.split(":"));
	}

	public static URLClassLoader create(String[] classPathElements)
			throws MalformedURLException {
		ArrayList<URL> urlList = new ArrayList<URL>(classPathElements.length);
		for (String element : classPathElements) {
			if (element.endsWith("*")) {
				File dir = new File(element.substring(0, element.length() - 1));
				File[] files = dir.listFiles(); // Will be null if missing or
												// not a dir
				if (files != null) {
					for (File f : files) {
						if (f.getName().endsWith(".jar")) {
							urlList.add(f.toURI().toURL());
						}
					}
				}
			} else {
				File f = new File(element);
				if (f.exists()) {
					urlList.add(f.toURI().toURL());
				}
			}
		}
		URL[] urls = new URL[urlList.size()];
		return new URLClassLoader(urlList.toArray(urls), ClassLoader
				.getSystemClassLoader().getParent());
	}

	/*
	 * Dump all the URLs
	 */
	public static void dump(ClassLoader cl, int numLevels) {
		int n = 0;
		for (URLClassLoader ucl = (URLClassLoader) cl; ucl != null
				&& ++n <= numLevels; ucl = (URLClassLoader) ucl.getParent()) {
			System.out.println("Class-loader " + n + " has "
					+ ucl.getURLs().length + " urls:");
			for (URL u : ucl.getURLs()) {
				System.out.println("  " + u);
			}
		}
	}


	public void start(String[] args) throws Exception {
		try {
	        investment = new Investment();

	        service = ServiceFactory.newService();
	        service.initialize(args);
	        service.start();
			logger.log(Level.INFO, "<<<<<<<< Ducc Container ended");
			
		} catch( Throwable t) {
			logger.log(Level.SEVERE, "<<<<<<<< Exiting Process Due to Unrecoverable Error:", t);
			Runtime.getRuntime().halt(99);   // User Error
		}

	}
	
	/*
	 * Terminate service connection
	 */
	public void stop() {
		try {
			service.stop();
		} catch( Throwable t) {
			logger.log(Level.SEVERE, "Stop failed");
		}
	}

	public static HashMap<String,String> hideLoggingProperties() {
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
			}
		}
		return savedPropsMap;
	}
	
	public static void restoreLoggingProperties(HashMap<String,String> savedPropsMap) {
		for (String prop : savedPropsMap.keySet()) {
			System.setProperty(prop, savedPropsMap.get(prop));
		}
	}
	
	public static void main(String[] args) {
		try {
			DuccJobService service = new DuccJobService();
			service.start(args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	

}
