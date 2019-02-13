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
package org.apache.uima.ducc.ws.server;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccPropertiesResolver;
import org.apache.uima.ducc.common.utils.Utils;
import org.apache.uima.ducc.common.utils.id.DuccId;

public class DuccWebProperties {

	private static DuccLogger duccLogger = DuccLogger.getLogger(DuccWebProperties.class);
	private static DuccId jobid = null;
	
	private static String dir_home = Utils.findDuccHome();
	private static String dir_resources = "resources";
	private static String ducc_properties_filename = dir_home+File.separator+dir_resources+File.separator+"ducc.properties";
	
	private static long default_ducc_ws_monitored_daemon_down_millis_expiry = 120000;
	
	public static Properties get() {
		Properties currentProperties = new Properties();
		try {
			File file = new File(ducc_properties_filename);
			FileInputStream fis;
			fis = new FileInputStream(file);
			currentProperties.load(fis);
			fis.close();
		} 
		catch (FileNotFoundException e) {
			e.printStackTrace();
		} 
		catch (IOException e) {
			e.printStackTrace();
		}
		return currentProperties;
	}
	
	public static String getProperty(String key, String defaultValue) {
		String value = defaultValue;
		Properties currentProperties = get();
		if(currentProperties != null) {
			if(currentProperties.containsKey(key)) {
				value = currentProperties.getProperty(key).trim();
			}
		}
		return value;
	}
	
	public static String getProperty(String key) {
		return getProperty(key, null);
	}
	
	public static long getPropertyLong(String key, long defaultValue) {
		String location = "getPropertyLong";
		long retVal = defaultValue;
		try {
			String pval = getProperty(key);
			if(pval != null) {
				retVal = Long.parseLong(pval);
			}
		}
		catch(Exception e) {
			duccLogger.error(location, jobid, e);
		}
		return retVal;
	}
	
	public static long get_ducc_ws_monitored_daemon_down_millis_expiry() {
		String key = DuccPropertiesResolver.ducc_ws_monitored_daemon_down_millis_expiry;
		long dval = default_ducc_ws_monitored_daemon_down_millis_expiry;
		long retVal = getPropertyLong(key,dval);
		return retVal;
	}
}
