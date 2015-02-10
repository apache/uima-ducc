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
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;

import org.apache.uima.ducc.user.jp.iface.IProcessContainer;

/**
 * Main program that is used to launch Job Process(JP).
 * 
 */
public class DuccJobService {
	boolean DEBUG = false;
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
        // cache current context classloader
		ClassLoader sysCL = Thread.currentThread().getContextClassLoader();
		// Fetch a classpath for the fenced Ducc container
		String duccContainerClasspath = System
				.getProperty("ducc.deploy.DuccClasspath");//"ducc.deploy.JpDuccClasspath");
		URLClassLoader ucl = create(duccContainerClasspath);
		if ( System.getProperty("DEBUG") != null ) {
			DEBUG = true;
		}
		if (DEBUG) {
			dump(ucl, 4);
		}

		Thread.currentThread().setContextClassLoader(ucl);
		// Create DuccService instance
		Class<?> classToLaunch = ucl.loadClass("org.apache.uima.ducc.common.main.DuccService");
		Object duccContainerInstance = classToLaunch.newInstance();

		// initialize Ducc fenced container. It calls component's Configuration class
		Method bootMethod = classToLaunch.getMethod("boot", String[].class);
		bootMethod.invoke(duccContainerInstance, (Object) args);

		// below property is set by component's Configuration class. It can also
		// be provided on the command line in case a custom processor is needed.
		String processorClass = System
				.getProperty("ducc.deploy.JpProcessorClass");

		// Instantiate process container where the actual analysis will be done.
		// Currently there are two containers:
		// 1 - UimaProcessContainer - used for pieces parts (UIMA only)
		// 2 - UimaASProcessContainer - used for DD jobs
		//
		// NOTE: the container class is loaded by the main System classloader
		//       and requires uima-ducc-user jar to be in the System classpath.
		// --------------------------------------------------------------------
		// load the process container class using the initial system class loader
		Class<?> processorClz = sysCL.loadClass(processorClass);
		IProcessContainer pc = (IProcessContainer) processorClz.newInstance();

		// Call DuccService.setProcessor() to hand-off instance of the 
		// process container to the component along with this process args
		Method setProcessorMethod = classToLaunch.getMethod("setProcessor",
				Object.class, String[].class);
		setProcessorMethod.invoke(duccContainerInstance, pc, args);
        // Call DuccService.start() to initialize worker threads and to
		// start fetching Work Items from a JD for processing.
		Method startMethod = classToLaunch.getMethod("start");// ,
		startMethod.invoke(duccContainerInstance);

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
