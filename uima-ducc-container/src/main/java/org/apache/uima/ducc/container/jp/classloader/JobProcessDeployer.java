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

package org.apache.uima.ducc.container.jp.classloader;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;

import org.apache.uima.ducc.container.jp.UimaProcessor;
import org.apache.uima.ducc.container.jp.iface.IJobProcessDeployer;
import org.apache.uima.ducc.container.jp.iface.IUimaProcessor;
import org.apache.uima.ducc.container.jp.iface.ServiceFailedInitialization;

public class JobProcessDeployer implements IJobProcessDeployer {
	private static boolean suppressClassPathDisplay;
	// declare methods to be called via reflection
	private static String M_DEPLOY="deploy";
	private static String M_PROCESS="process";
	private static String M_STOP="stop";
	
	public IUimaProcessor deploy(String userClasspath, String[] args, String clzToLoad) throws ServiceFailedInitialization {
		try {
			URL[] urls = getUrls(userClasspath);

			addUrlsToSystemLoader(urls);

			Class<?> classToLaunch = null;
			classToLaunch = ClassLoader.getSystemClassLoader().loadClass(clzToLoad);

			Method deployMethod = classToLaunch.getMethod(M_DEPLOY, String[].class);
			Method processMethod = classToLaunch.getMethod(M_PROCESS, String.class);
			Method stopMethod = classToLaunch.getMethod(M_STOP);

			int args2length = args.length - 1;
			if (args2length < 0) {
				args2length = 0;
			}
			
			Object uimaContainerInstance = classToLaunch.newInstance();
			// This blocks until Uima AS container is fully initialized
			Object scaleout = deployMethod.invoke(uimaContainerInstance,
					(Object) args);
			return new UimaProcessor(uimaContainerInstance,processMethod,stopMethod,(Integer)scaleout);
	
		} catch( Exception e) {
			throw new ServiceFailedInitialization(e);
		}

	}

	private static void addUrlsToSystemLoader(URL[] urls) throws IOException {
		URLClassLoader systemClassLoader = (URLClassLoader) ClassLoader
				.getSystemClassLoader();
		try {
			Method method = URLClassLoader.class.getDeclaredMethod("addURL",
					new Class[] { URL.class });
			method.setAccessible(true); // is normally "protected"
			for (URL url : urls) {
				method.invoke(systemClassLoader, new Object[] { url });
			}
		} catch (Throwable t) {
			t.printStackTrace();
			throw new IOException(
					"Error, could not add URL to system classloader");
		}
	}

	private URL[] getUrls(String jps) throws MalformedURLException, IOException,
			URISyntaxException {
//		String jps = System.getProperty("org.apache.uima.ducc.userjarpath");
//		if (null == jps) {
//			System.err
//					.println("Missing the -Dorg.apache.uima.jarpath=XXXX property");
//			System.exit(1);
//		}
		if (!suppressClassPathDisplay) {
			System.out.println("UimaBootstrap ClassPath:");
		}
		List<URL> urls = new ArrayList<URL>();
		String[] jpaths = jps.split(File.pathSeparator);
		for (String p : jpaths) {
			addUrlsFromPath(p, urls);
		}
		return urls.toArray(new URL[urls.size()]);
	}

	private static FilenameFilter jarFilter = new FilenameFilter() {
		public boolean accept(File dir, String name) {
			name = name.toLowerCase();
			return (name.endsWith(".jar"));
		}
	};

	private static void addUrlsFromPath(String p, List<URL> urls)
			throws MalformedURLException, IOException, URISyntaxException {
		File pf = new File(p);
		if (pf.isDirectory()) {
			File[] jars = pf.listFiles(jarFilter);
			if (jars.length == 0) {
				// this is the case where the user wants to include
				// a directory containing non-jar'd .class files
				add(urls, pf);
			} else {
				for (File f : jars) {
					add(urls, f);
				}
			}
		} else if (p.toLowerCase().endsWith(".jar")) {
			add(urls, pf);
		}
	}

	private static void add(List<URL> urls, File cp)
			throws MalformedURLException {
		URL url = cp.toURI().toURL();
		if (!suppressClassPathDisplay) {
			System.out.format(" %s%n", url.toString());
		}
		urls.add(url);
	}

}
