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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

public class FlagsHelper {

	private static FlagsHelper instance = new FlagsHelper();
	
	public static FlagsHelper getInstance() {
		return instance;
	}
	
	public enum Name {
		
		CollectionReaderCfg,
		CollectionReaderXml,
		JdURL,						// http://<node>:<port>/jdApp
		JobId,
		UserClasspath,				// path1.class:path2.jar:path3/*:...
		UserErrorHandlerClassname,
		UserErrorHandlerCfg,
		WorkItemTimeout,			// maximum milliseconds that any work item should take to process
		;
		
		private static ArrayList<Name> requiredJd = new ArrayList<Name>(Arrays.asList(CollectionReaderCfg, JobId, UserClasspath));
		private static ArrayList<Name> requiredJp = new ArrayList<Name>(Arrays.asList(JdURL, JobId, UserClasspath));
		
		private Name() {
		}
		
		public boolean isRequiredJd() {
			boolean retVal = false;
			if(requiredJd.contains(this)) {
				retVal = true;
			}
			return retVal;
		}
		
		public boolean isRequiredJp() {
			boolean retVal = false;
			if(requiredJp.contains(this)) {
				retVal = true;
			}
			return retVal;
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
	
	public String getJdURL() {
		Properties properties = System.getProperties();
		return properties.getProperty(Name.JdURL.pname());
	}
	
	public String getJdURLDashD(String value) {
		return Name.JdURL.arg(value);
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
	
	public String getWorkItemTimeout() {
		Properties properties = System.getProperties();
		return properties.getProperty(Name.WorkItemTimeout.pname());
	}
	
	public String getWorkItemTimeoutDashD(String value) {
		return Name.WorkItemTimeout.arg(value);
	}
	
	public String[] stringToArray(String classpath) {
		String[] retVal = null;
		if(classpath != null) {
			retVal = classpath.split(File.pathSeparator);
		}
		return retVal;
	}
	
}
