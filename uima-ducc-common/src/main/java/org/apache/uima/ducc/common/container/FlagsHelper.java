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
		JobDirectory,
		JobId,
		JpAeDescriptor,
		JpAeOverrides,
		JpCcDescriptor,
		JpCcOverrides,
		JpCmDescriptor,
		JpCmOverrides,
		JpDd,
		JpDdBrokerEndpoint("ducc.local.queue"),
		JpDdBrokerURL("${DefaultBrokerURL}"),
		JpDdDescription,
		JpDdName,
		JpThreadCount,
		JpType,						// { uima, uima-as }
		UserClasspath,				// path1.class:path2.jar:path3/*:...
		UserErrorHandlerClassname,
		UserErrorHandlerCfg,
		WorkItemTimeout,			// maximum milliseconds that any work item should take to process
		;
		
		private static ArrayList<Name> requiredJd = new ArrayList<Name>(Arrays.asList(CollectionReaderCfg, JobId, UserClasspath));
		private static ArrayList<Name> requiredJp = new ArrayList<Name>(Arrays.asList(JdURL, JobId, UserClasspath));
		
		private String defaultValue = null;
		
		private Name() {
		}
		
		private Name(String value) {
			defaultValue = value;
		}
		
		public String getDefaultValue() {
			return defaultValue;
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
	
	public String getJobDirectory() {
		Properties properties = System.getProperties();
		return properties.getProperty(Name.JobDirectory.pname());
	}
	
	public String getJobDirectoryDashD(String value) {
		return Name.JobDirectory.arg(value);
	}
	
	public String getJobId() {
		Properties properties = System.getProperties();
		return properties.getProperty(Name.JobId.pname());
	}
	
	public String getJobIdDashD(String value) {
		return Name.JobId.arg(value);
	}
	
	public String getJpAeDescriptor() {
		Properties properties = System.getProperties();
		return properties.getProperty(Name.JpAeDescriptor.pname());
	}
	
	public String getJpAeDescriptorDashD(String value) {
		return Name.JpAeDescriptor.arg(value);
	}
	
	public String getJpAeOverrides() {
		Properties properties = System.getProperties();
		return properties.getProperty(Name.JpAeOverrides.pname());
	}
	
	public String getJpAeOverridesDashD(String value) {
		return Name.JpAeOverrides.arg(value);
	}
	
	public String getJpCcDescriptor() {
		Properties properties = System.getProperties();
		return properties.getProperty(Name.JpCcDescriptor.pname());
	}
	
	public String getJpCcDescriptorDashD(String value) {
		return Name.JpCcDescriptor.arg(value);
	}
	
	public String getJpCcOverrides() {
		Properties properties = System.getProperties();
		return properties.getProperty(Name.JpCcOverrides.pname());
	}
	
	public String getJpCcOverridesDashD(String value) {
		return Name.JpCcOverrides.arg(value);
	}
	
	public String getJpCmDescriptor() {
		Properties properties = System.getProperties();
		return properties.getProperty(Name.JpCmDescriptor.pname());
	}
	
	public String getJpCmDescriptorDashD(String value) {
		return Name.JpCmDescriptor.arg(value);
	}
	
	public String getJpCmOverrides() {
		Properties properties = System.getProperties();
		return properties.getProperty(Name.JpCmOverrides.pname());
	}
	
	public String getJpCmOverridesDashD(String value) {
		return Name.JpCmOverrides.arg(value);
	}
	
	public String getJpDd() {
		Properties properties = System.getProperties();
		return properties.getProperty(Name.JpDd.pname());
	}
	
	public String getJpDdDashD(String value) {
		return Name.JpDd.arg(value);
	}
	
	public String getJpDdBrokerEndpoint() {
		Properties properties = System.getProperties();
		return properties.getProperty(Name.JpDdBrokerEndpoint.pname());
	}
	
	public String getJpDdBrokerEndpointDashD(String value) {
		return Name.JpDdBrokerEndpoint.arg(value);
	}
	
	public String getJpDdBrokerURL() {
		Properties properties = System.getProperties();
		return properties.getProperty(Name.JpDdBrokerURL.pname());
	}
	
	public String getJpDdBrokerURLDashD(String value) {
		return Name.JpDdBrokerURL.arg(value);
	}
	
	public String getJpDdDescription() {
		Properties properties = System.getProperties();
		return properties.getProperty(Name.JpDdDescription.pname());
	}
	
	public String getJpDdDescriptionDashD(String value) {
		return Name.JpDdDescription.arg(value);
	}
	
	public String getJpDdName() {
		Properties properties = System.getProperties();
		return properties.getProperty(Name.JpDdName.pname());
	}
	
	public String getJpDdNameDashD(String value) {
		return Name.JpDdName.arg(value);
	}
	
	public String getJpThreadCount() {
		Properties properties = System.getProperties();
		return properties.getProperty(Name.JpThreadCount.pname());
	}
	
	public String getJpThreadCountDashD(String value) {
		return Name.JpThreadCount.arg(value);
	}
	
	public String getJpType() {
		Properties properties = System.getProperties();
		return properties.getProperty(Name.JpType.pname());
	}
	
	public String getJpTypeDashD(String value) {
		return Name.JpType.arg(value);
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
