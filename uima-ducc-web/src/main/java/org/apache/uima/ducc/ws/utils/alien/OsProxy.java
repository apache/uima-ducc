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

import java.io.File;
import java.util.Map;
import java.util.TreeMap;

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.id.DuccId;

public class OsProxy {
	
	private static DuccLogger logger = DuccLoggerComponents.getWsLogger(OsProxy.class.getName());
	private static DuccId jobid = null;
	
	private static String newline = "\n";
	private static String whitespace = "\\s+";
	
	public static Map<String, FileInfo> getFilesInDirectory(String ducc_ling, String user, String directory) {
		String location = "getFilesInDirectory";
		TreeMap<String, FileInfo> map = new TreeMap<String, FileInfo>();
		if(ducc_ling == null) {
			try {
				File filedir = new File(directory);
				File[] filelist = filedir.listFiles();
				for(File file : filelist) {
					String name = file.getName();
					String fullname = file.getAbsolutePath();
					long length = file.length();
					FileInfo fileInfo = new FileInfo(fullname,length);
					map.put(name, fileInfo);
				}
			}
			catch(Exception e) {
				// no worries
				logger.debug(location, jobid, e);
			}
		}
		else {
			try {
				AlienDirectory alienDirectory = new AlienDirectory(user, directory, ducc_ling);
				String result = null;
				try {
					result = alienDirectory.getString();
				}
				catch(Exception e) {
					logger.debug(location, jobid, e);
				}
				logger.debug(location, jobid, result);
				if(result != null) {
					String[] lines = result.split(newline);
					for(String line : lines) {
						logger.debug(location, jobid, line);
						String[] element = line.split(whitespace);
						String name = element[8];
						String fullname = directory+name;
						if(!name.startsWith(".")) {
							if(!directory.endsWith(File.separator)) {
								fullname = directory+File.separator+name;
							}
							long length = Long.parseLong(element[4]);
							FileInfo fileInfo = new FileInfo(fullname,length);
							map.put(name, fileInfo);
						}
					}
				}
			}
			catch(Exception e) {
				// no worries
				logger.debug(location, jobid, e);
			}
		}
		return map;
	}
}
