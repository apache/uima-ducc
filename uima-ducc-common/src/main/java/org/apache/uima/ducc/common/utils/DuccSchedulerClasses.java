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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class DuccSchedulerClasses {
	
	public static final String FAIR_SHARE = "FAIR_SHARE";
	
	private static DuccSchedulerClasses instance = null;
	
	private String fileName = null;
	
	public static DuccSchedulerClasses getInstance() {
		if(instance == null) {
			instance = new DuccSchedulerClasses();
		}
		return instance;
	}
	
	public DuccSchedulerClasses() {
		String key = DuccPropertiesResolver.ducc_rm_class_definitions;
		String file_classes = DuccPropertiesResolver.getInstance().getFileProperty(key);
		String dir_home = Utils.findDuccHome();
		String dir_resources = "resources";
		fileName = dir_home+File.separator+dir_resources+File.separator+file_classes;
	}
	
	public String getProperty(Properties properties, String name) {
		String retVal = "";
		String property = properties.getProperty(name);
		if(property != null) {
			retVal = property.trim();
		}
		return retVal;
	}
	
	public DuccProperties getClasses() {
		DuccProperties properties = new DuccProperties();
		try {
			File file = new File(fileName);
			FileInputStream fis = new FileInputStream(file);
			properties.load(fis);
			fis.close();
		}
		catch(IOException e) {
			e.printStackTrace();
		}
		return properties;
	}
	
	public boolean isPreemptable(String class_name) {
		boolean retVal = false;
		Properties properties = getClasses();
		String policy = getProperty(properties,"scheduling.class."+class_name+".policy");
		if(policy.equals(FAIR_SHARE)) {
			retVal = true;
		}
		return retVal;
	}
	
	public String getDebugClassDefaultName() {
		String name = "";
		Properties properties = getClasses();
		String value = properties.getProperty("scheduling.default.name.debug");
		if(value != null) {
			name = value.trim();
		}
		return name;
	}
	
	public String getDebugClassSpecificName(String class_name) {
		String retVal = null;
		if(class_name != null) {
			Properties properties = getClasses();
			String name = properties.getProperty("scheduling.class."+class_name+".debug", "");
			if(name.equals("")) {
				name = getDebugClassDefaultName();
				if(name.equals("")) {
				}
				else {
					retVal = name;
				}
			}
			else {
				retVal = name;
			}
		}
		return retVal;
	}
	
	public String getDebugClassName(String class_name) {
		String retVal = null;
		if(isPreemptable(class_name)) {
			String name = getDebugClassSpecificName(class_name);
			if(name == null) {
				name = getDebugClassDefaultName();
			}
			retVal = name;
		}
		else {
			retVal = class_name;
		}
		return retVal;
	}
}
