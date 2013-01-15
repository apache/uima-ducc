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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.uima.ducc.common.utils.DuccProperties;


public class DuccWebSchedulerClasses {
	
	private String fileName;
	
	public DuccWebSchedulerClasses(String fileName) {
		this.fileName = fileName;
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
	
	public String getProperty(Properties properties, String name) {
		String retVal = "";
		String property = properties.getProperty(name);
		if(property != null) {
			retVal = property.trim();
		}
		return retVal;
	}
	
	public String getReserveClassDefaultName() {
		String name = "";
		Properties properties = getClasses();
		String value = properties.getProperty("scheduling.default.name.reserve");
		if(value != null) {
			name = value.trim();
		}
		return name;
	}
	
	public String[] getReserveClasses() {
		ArrayList<String> classList = new ArrayList<String>();
		Properties properties = getClasses();
		String class_set = properties.getProperty("scheduling.class_set");
		class_set.trim();
		if(class_set != null) {
			String[] class_array = StringUtils.split(class_set);
			for(int i=0; i<class_array.length; i++) {
				String class_name = class_array[i].trim();
				String policy = getProperty(properties,"scheduling.class."+class_name+".policy");
				if(policy.equals(DuccConstants.FIXED_SHARE)) {
					classList.add(class_name);
				}
				else if(policy.equals(DuccConstants.RESERVE) && !class_name.equals(DuccConstants.JobDriver)) {
					classList.add(class_name);
				}
			}
		}
		String[] retVal = classList.toArray(new String[0]);
		return retVal;
	}
	
	public String[] getJobClasses() {
		ArrayList<String> classList = new ArrayList<String>();
		Properties properties = getClasses();
		String class_set = properties.getProperty("scheduling.class_set");
		class_set.trim();
		if(class_set != null) {
			String[] class_array = StringUtils.split(class_set);
			for(int i=0; i<class_array.length; i++) {
				String class_name = class_array[i].trim();
				String policy = getProperty(properties,"scheduling.class."+class_name+".policy");
				if(policy.equals(DuccConstants.FAIR_SHARE)) {
					classList.add(class_name);
				}
			}
		}
		String[] retVal = classList.toArray(new String[0]);
		return retVal;
	}
	
}
