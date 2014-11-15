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
*/package org.apache.uima.ducc.common.config;

import java.io.File;
import java.util.Properties;

public class SystemPropertiesHelper {

	private static SystemPropertiesHelper instance = new SystemPropertiesHelper();
	
	public static SystemPropertiesHelper getInstance() {
		return instance;
	}
	
	public enum Name {
		
		CollectionReaderCfg(false),
		CollectionReaderXml(true),
		UserClasspath(true),
		UserErrorHandlerClassname(false),
		UserErrorHandlerCfg(false),
		;
		
		private boolean requiredFlag;
		
		private Name(boolean requiredFlag) {
			setRequired(requiredFlag);
		}
		
		private void setRequired(boolean value) {
			requiredFlag = value;
		}
		
		public boolean isRequired() {
			return requiredFlag;
		}
	}
	
	public String getCollectionReaderCfg() {
		Properties properties = System.getProperties();
		return properties.getProperty(Name.CollectionReaderCfg.name());
	}
	
	public String getCollectionReaderCfgDashD(String value) {
		return toDashD(Name.CollectionReaderCfg.name(),value);
	}
	
	public String getCollectionReaderXml() {
		Properties properties = System.getProperties();
		return properties.getProperty(Name.CollectionReaderXml.name());
	}
	
	public String getCollectionReaderXmlDashD(String value) {
		return toDashD(Name.CollectionReaderXml.name(),value);
	}
	
	public String getUserClasspath() {
		Properties properties = System.getProperties();
		return properties.getProperty(Name.UserClasspath.name());
	}
	
	public String getUserClasspathDashD(String value) {
		return toDashD(Name.UserClasspath.name(),value);
	}
	
	public String getUserErrorHandlerClassname() {
		Properties properties = System.getProperties();
		return properties.getProperty(Name.UserErrorHandlerClassname.name());
	}
	
	public String getUserErrorHandlerClassnameDashD(String value) {
		return toDashD(Name.UserErrorHandlerClassname.name(),value);
	}
	
	public String getUserErrorHandlerCfg() {
		Properties properties = System.getProperties();
		return properties.getProperty(Name.UserErrorHandlerCfg.name());
	}
	
	public String getUserErrorHandlerCfgDashD(String value) {
		return toDashD(Name.UserErrorHandlerCfg.name(),value);
	}
	
	private String toDashD(String key, String value) {
		String retVal = "";
		if(key != null) {
			if(value != null) {
				String trimmedKey = key.trim();
				String trimmedValue = value.trim();
				if(trimmedKey.length() > 0) {
					if(trimmedValue.length() > 0) {
						retVal = "-D"+trimmedKey+"="+trimmedValue;
					}
				}
			}
		}
		return retVal;
	}
	
	public String[] stringToArray(String classpath) {
		String[] retVal = null;
		if(classpath != null) {
			retVal = classpath.split(File.pathSeparator);
		}
		return retVal;
	}
	
}
