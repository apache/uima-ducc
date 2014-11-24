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
*/package org.apache.uima.ducc.common.container;

import java.io.File;
import java.util.Properties;

public class FlagsHelper {

	private static FlagsHelper instance = new FlagsHelper();
	
	public static FlagsHelper getInstance() {
		return instance;
	}
	
	public enum Name {
		
		CollectionReaderCfg(false),
		CollectionReaderXml(true),
		JobId(true),
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
		
		public String pname() {
			return "ducc.deploy."+name();
		}
		
		public String dname() {
			return "-D"+pname();
		}
		
		public String arg(String value) {
			String retVal = "";
			if(value != null) {
				String trimmedValue = value.trim();
				if(trimmedValue.length() > 0) {
					retVal = dname()+"="+trimmedValue;
				}
			}
			return retVal;
		}
	}
	
	public String getCollectionReaderCfg() {
		Properties properties = System.getProperties();
		return properties.getProperty(Name.CollectionReaderCfg.pname());
	}
	
	public String getCollectionReaderCfgDashD(String value) {
		return Name.CollectionReaderCfg.arg(value);
	}
	
	public String getCollectionReaderXml() {
		Properties properties = System.getProperties();
		return properties.getProperty(Name.CollectionReaderXml.pname());
	}
	
	public String getCollectionReaderXmlDashD(String value) {
		return Name.CollectionReaderXml.arg(value);
	}
	
	public String getJobId() {
		Properties properties = System.getProperties();
		return properties.getProperty(Name.JobId.pname());
	}
	
	public String getJobIdDashD(String value) {
		return Name.JobId.arg(value);
	}
	
	public String getUserClasspath() {
		Properties properties = System.getProperties();
		return properties.getProperty(Name.UserClasspath.pname());
	}
	
	public String getUserClasspathDashD(String value) {
		return Name.UserClasspath.arg(value);
	}
	
	public String getUserErrorHandlerClassname() {
		Properties properties = System.getProperties();
		return properties.getProperty(Name.UserErrorHandlerClassname.pname());
	}
	
	public String getUserErrorHandlerClassnameDashD(String value) {
		return Name.UserErrorHandlerClassname.arg(value);
	}
	
	public String getUserErrorHandlerCfg() {
		Properties properties = System.getProperties();
		return properties.getProperty(Name.UserErrorHandlerCfg.pname());
	}
	
	public String getUserErrorHandlerCfgDashD(String value) {
		return Name.UserErrorHandlerCfg.arg(value);
	}
	
	public String[] stringToArray(String classpath) {
		String[] retVal = null;
		if(classpath != null) {
			retVal = classpath.split(File.pathSeparator);
		}
		return retVal;
	}
	
}
