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
package org.apache.uima.ducc.orchestrator.authentication;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Properties;
import java.util.TreeMap;

import org.apache.uima.ducc.common.IDuccEnv;


public class DuccWebAdministrators {

	private String fileName = IDuccEnv.DUCC_ADMINISTRATORS_FILE;
	
	private static DuccWebAdministrators duccWebAdministrators = new DuccWebAdministrators();
	
	public static DuccWebAdministrators getInstance() {
		return duccWebAdministrators;
	}
	
	private Properties load() {
		Properties properties = new Properties();
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
	
	public Iterator<String> getSortedAuthorizedUserids() {
		TreeMap<String,String> map = new TreeMap<String,String>();
		Properties properties = load();
		if(!properties.isEmpty()) {
			Enumeration<?> enumeration = properties.propertyNames();
			while(enumeration.hasMoreElements()) {
				String name = (String)enumeration.nextElement();
				map.put(name, name);
			}
		}
		Iterator<String> iterator = map.keySet().iterator();
		return iterator;
	}
	
	public String getAuthorizationFileName() {
		return fileName;
	}
	
	public boolean isAdministrator(String userid) {
		boolean retVal = false;
		try {
			if(userid != null) {
				Properties properties = load();
				Iterator<Object> iterator = properties.keySet().iterator();
				while(iterator.hasNext()) {
					String authorizedUserid = ((String)(iterator.next())).trim();
					if(userid.trim().equals(authorizedUserid)) {
						retVal = true;
						break;
					}
				}
			}
		}
		catch(Exception e) {
		}
		return retVal;
	}

}
