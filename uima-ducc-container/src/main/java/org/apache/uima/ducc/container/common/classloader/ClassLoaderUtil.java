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
package org.apache.uima.ducc.container.common.classloader;

import java.io.File;
import java.util.ArrayList;

import org.apache.uima.ducc.container.common.ContainerLogger;
import org.apache.uima.ducc.container.common.IContainerLogger;
import org.apache.uima.ducc.container.common.IEntityId;
import org.apache.uima.ducc.container.common.MessageBuffer;
import org.apache.uima.ducc.container.common.Standardize;

public class ClassLoaderUtil {

	private static IContainerLogger logger = ContainerLogger.getLogger(ClassLoaderUtil.class, IContainerLogger.Component.JD.name());
	
	public static ClassLoader getClassLoader() {
		return ClassLoader.getSystemClassLoader();
	}
	
	public static ClassLoader getClassLoaderParent() {
		return ClassLoader.getSystemClassLoader().getParent();
	}
	
	private static ArrayList<String> resolveClasspathWildcardsDirectory(String dName) {
		String location = "resolveClasspathWildcardsDirectory";
		ArrayList<String> retVal = new ArrayList<String>();
		if(dName != null) {
			try {
				retVal.add(dName);
				logger.debug(location, IEntityId.null_id, dName);
				File file = new File(dName);
				if (file.isDirectory()) {
					MessageBuffer mb = new MessageBuffer();
					mb.append(Standardize.Label.directory.get()+file.getName());
					logger.debug(location, IEntityId.null_id, mb);
					File[] jars = file.listFiles();
					for(File jar : jars) {
						String jarName = jar.getCanonicalPath();
						if(jarName.endsWith(".jar")) {
							retVal.add(jarName);
							logger.debug(location, IEntityId.null_id, jarName);
						}
					}
				}
			}
			catch(Exception e) {
				logger.error(location, IEntityId.null_id, e);
			}
		}
		return retVal;
	}
	
	private static ArrayList<String> resolveClasspathWildcards(ArrayList<String> list) {
		ArrayList<String> retVal = new ArrayList<String>();
		if(list != null) {
			for(String item : list) {
				ArrayList<String> xList = resolveClasspathWildcardsDirectory(item);
				for(String xItem : xList) {
					retVal.add(xItem);
				}
			}
		}
		return retVal;
	}
	
	private static String convertToString(ArrayList<String> list) {
		StringBuffer sb = new StringBuffer();
		if(list != null) {
			boolean first = true;
			for(String item : list) {
				if(first) {
					first = false;
				}
				else {
					sb.append(":");
				}
				sb.append(item);
			}
		}
		return sb.toString();
	}
	
	private static ArrayList<String> convertToList(String cp) {
		ArrayList<String> retVal = new ArrayList<String>();
		if(cp != null) {
			String[] array = cp.split(":");
			for(String item : array) {
				retVal.add(item);
			}
		}
		return retVal;
	}
	
	public static String resolveClasspathWildcards(String cp) {
		ArrayList<String> list = convertToList(cp);
		list = resolveClasspathWildcards(list);
		String retVal = convertToString(list);
		return retVal;
	}
}
