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
package org.apache.uima.ducc.ws.utils.alien;

import java.util.TreeMap;

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.Utils;
import org.apache.uima.ducc.common.utils.id.DuccId;

public class OsProxy {
	
	private static DuccLogger logger = DuccLogger.getLogger(OsProxy.class);
	private static DuccId jobid = null;
	
	private static String ducc_ling = 
			Utils.resolvePlaceholderIfExists(
					System.getProperty("ducc.agent.launcher.ducc_spawn_path"),System.getProperties());
	
	public static TreeMap<String, FileInfo> getFilesInDirectory(EffectiveUser eu, String directory) {
		return getFilesInDirectory(eu, directory, false);
	}
	
	public static TreeMap<String, FileInfo> getFilesInDirectory(EffectiveUser eu, String directory, boolean recursive) {
		String location = "getFilesInDirectory";
		TreeMap<String, FileInfo> map = new TreeMap<String, FileInfo>();
		
		try {
			AlienDirectory alienDirectory = new AlienDirectory(eu, directory, ducc_ling, recursive);
			map = alienDirectory.getMap();
		}
		catch(Exception e) {
			// no worries
			logger.error(location, jobid, e);
		}
		return map;
	}
}
