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
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.ws.standardize.WsStandardize;

public class DuccWebServerHelper {

	private static DuccLogger logger = DuccLoggerComponents.getWsLogger(DuccWebServerHelper.class.getName());

	private static DuccId jobid = null;
	
	/**
	 * DUCC_WEB should be set as an environment variable.  This is the webserver's
	 * base directory where it will find web pages to serve from sub-directory root, 
	 * and SSL data in sub-directory etc.
	 */
	public static String ducc_web_default = ".";
	
	public static String getDuccWeb() {
		String location = "getDuccWeb";
		String ducc_web = ducc_web_default;
		String ducc_web_property = System.getProperty("DUCC_WEB");
		String ducc_web_env = System.getenv("DUCC_WEB");
		if(ducc_web_property != null) {
			ducc_web = ducc_web_property;
			logger.info(location, jobid, WsStandardize.Label.DUCC_WEB_PROPERTY.get()+ducc_web);
		}
		else if(ducc_web_env != null) {
			ducc_web = ducc_web_env;
			logger.info(location, jobid, WsStandardize.Label.DUCC_WEB_ENV.get()+ducc_web);
		}
		else {
			logger.info(location, jobid, WsStandardize.Label.DUCC_WEB.get()+ducc_web);
		}
		return ducc_web;
	}
	
	public static String getDuccWebRoot() {
		String rootDir = getDuccWeb()+File.separator+"root";
		return rootDir;
	}
	
	public static String getDuccWebKeyStore() {
		String rootDir = getDuccWeb()+File.separator+"etc"+File.separator+"keystore";
		return rootDir;
	}
	
	private static boolean exists(String fileName) {
		boolean retVal = false;
		try {
			File file = new File(fileName);
			if(file.exists()) {
				retVal = true;
			}
		}
		catch(Exception e) {
			
		}
		return retVal;
	}
	
	public static String getImageFileName(String key) {
		String location = "getImageFileName";
		String retVal = null;
		if(key != null) {
			String relativeFileName = "resources"+File.separator+"image-map.properties";
			try {
				String fileName = getDuccWebRoot()+File.separator+relativeFileName;
				File file = new File(fileName);
				FileInputStream fis = new FileInputStream(file);
				Properties properties = new Properties();
				properties.load(fis);
				fis.close();
				String value = properties.getProperty(key);
				if(exists(value)) {
					retVal = value;
					logger.debug(location, jobid, key+"="+retVal);
				}
				else {
					logger.debug(location, jobid, value+" not found");
				}
			}
			catch (FileNotFoundException e) {
				logger.debug(location, jobid, relativeFileName+" not found");
			} 
			catch (IOException e) {
				logger.debug(location, jobid, relativeFileName+" load error");
			}
		}
		return retVal;
	}
}
