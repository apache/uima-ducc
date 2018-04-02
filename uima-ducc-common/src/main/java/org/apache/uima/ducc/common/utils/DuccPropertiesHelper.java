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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class DuccPropertiesHelper {
	
	public static DuccPropertiesResolver configure() {
		String key = "DUCC_HOME";
		String value = System.getenv(key);
		if(value != null) {
			Properties properties = System.getProperties();
			properties.setProperty(key, value);
			System.setProperties(properties);
		}
		return DuccPropertiesResolver.getInstance();
	}
	
	private static String defaultDuccHead = "?";
	
	public static String getDuccHead() {
		String key = DuccPropertiesResolver.ducc_head;
		String value = DuccPropertiesResolver.get(key,defaultDuccHead);
		String retVal = value;
		return retVal;
	}
	
	private static String defaultDuccHeadVirtualIpAddress= "";
	
	public static String getDuccHeadVirtualIpAddress() {
		String key = DuccPropertiesResolver.ducc_head_virtual_ip_address;
		String value = DuccPropertiesResolver.get(key,defaultDuccHeadVirtualIpAddress);
		String retVal = value;
		return retVal;
	}
	
	private static String defaultDuccHeadVirtualIpDevice= "";
	
	public static String getDuccHeadVirtualIpDevice() {
		String key = DuccPropertiesResolver.ducc_head_virtual_ip_device;
		String value = DuccPropertiesResolver.get(key,defaultDuccHeadVirtualIpDevice);
		String retVal = value;
		return retVal;
	}
}
