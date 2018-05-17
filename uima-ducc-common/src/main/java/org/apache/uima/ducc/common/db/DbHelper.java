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

package org.apache.uima.ducc.common.db;

import org.apache.uima.ducc.common.utils.DuccPropertiesResolver;

/*
 * Class to help utilize database entries from ducc.properties file
 */
public class DbHelper {

	public static final String ducc_database_host = "ducc.database.host";
	
	// Note: comment out logging to prevent CLI console error messages,
	// due to requirement that log4j is needed in classpath.
	
	//private static DuccLogger logger = DuccLogger.getLogger(DbHelper.class);
	//private static DuccId jobid = null;
	
	private static final DuccPropertiesResolver dpr = DuccPropertiesResolver.getInstance();
	
	// enabled if not == null and not == --disabled--
	public static boolean isDbEnabled() {
		boolean retVal = true;
		
		String value;
		value = dpr.getProperty(DuccPropertiesResolver.ducc_database_host_list);
		if(value != null) {
			if(value.equalsIgnoreCase(DuccPropertiesResolver.ducc_database_disabled)) {
				retVal = false;
			}
		}
		else {
			value = dpr.getProperty(ducc_database_host);
			if(value != null) {
				if(value.equalsIgnoreCase(DuccPropertiesResolver.ducc_database_disabled)) {
					retVal = false;
				}
			}
			else {
				retVal = false;
			}
		}
		return retVal;
	}
	
	public static boolean isDbDisabled() {
		return !isDbEnabled();
	}
	
	public static String getHostListString() {
		String retVal = dpr.getProperty(DuccPropertiesResolver.ducc_database_host_list);
		if(retVal == null) {
			dpr.getProperty(ducc_database_host);
		}
		return retVal;
	}
	
	public static String getJxmHostString() {
		String retVal = dpr.getProperty(DuccPropertiesResolver.ducc_database_jmx_host);
		return retVal;
	}
	
	public static String getJxmPortString() {
		String retVal = dpr.getProperty(DuccPropertiesResolver.ducc_database_jmx_port);
		return retVal;
	}
	
	public static Integer getJxmPortInteger() {
		//String location = "getJxmPortInteger";
		Integer retVal = new Integer(7199);
		String jmx_port = dpr.getProperty(DuccPropertiesResolver.ducc_database_jmx_port);
		try {
			retVal = Integer.parseInt(jmx_port);
		}
		catch(Exception e) {
			//logger.error(location, jobid, e);
		}
		return retVal;
	}
	
	private static String[] emptyList = new String[0];
	
	public static String[] getHostList() {
		String[] retVal = emptyList;
		String hostListString = getHostListString();
		if(hostListString != null) {
			retVal = stringToArray(hostListString);
		}
		return retVal;
	}
	
    public static String[] stringToArray(String input) {
    	String[] output = null;
    	if(input == null) {
    		output = new String[0];
    	}
    	else {
    		output = input.split("\\s+");
    	}
    	return output;
    }
}
