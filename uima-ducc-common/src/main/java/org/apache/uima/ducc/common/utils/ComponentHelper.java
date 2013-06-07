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
package org.apache.uima.ducc.common.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;


public class ComponentHelper {
	
	private static final DuccLogger logger = DuccLoggerComponents.getOrLogger(ComponentHelper.class.getName());
	
	/**
	 * Abort component (at start-up) if already running, indicated by the existence of
	 * file <componentName>.lock in <directory>.
	 * 
	 * @param directory
	 * @param componentName
	 */
	public static void oneInstance(String directory, String componentName) {
		String methodName = "oneInstance";
		try {
			IOHelper.mkdirs(directory);
			String filename = getLockFileNameWithPath(directory, componentName);
			File file = new File(filename);
			if(file.exists()) {
				logger.error(methodName, null, "found file "+filename);
				BufferedReader in = new BufferedReader(new FileReader(file));
				String hostname = in.readLine();
				logger.error(methodName, null, "already running on host "+hostname);
				System.exit(-1);
			}
			file.deleteOnExit();
			InetAddress addr = InetAddress.getLocalHost();
			String hostname = addr.getHostName();
			BufferedWriter out = new BufferedWriter(new FileWriter(file));
		    out.write(hostname+"\n");
		    out.close();
		} catch (IOException e) {
			logger.error(methodName, null, e);
		}
	}
	
	/**
	 * Determine lock status, indicated by the existence of
	 * file <componentName>.lock in <directory>.
	 * 
	 * @param directory
	 * @param componentName
	 */
	public static boolean isLocked(String directory, String componentName) {
		String filename = getLockFileNameWithPath(directory, componentName);
		File file = new File(filename);
		return (file.exists());
	}
	
	/**
	 * Return lock file name with path.
	 * 
	 * @param directory
	 * @param componentName
	 */
	public static String getLockFileNameWithPath(String directory, String componentName) {
		return directory+componentName+".lock";
	}
	
	/**
	 * Return lock file name.
	 * 
	 * @param directory
	 * @param componentName
	 */
	public static String getLockFileName(String directory, String componentName) {
		return componentName+".lock";
	}
}
