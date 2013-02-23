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

import java.util.Properties;

public class DuccHandlerUtils {

	private static String stateAvailable = "Available";
	
	private static String valueTrue = "True";
	private static String valueFalse = "False";
	
	private static String valueYes = "Yes";
	private static String valueNo = "No";
	
	
	private static String valueGood = "Good";
	private static String valuePoor = "Poor";
	
	private static String decorateOpenNeutral = "<span class=\"health_neutral\">";
	private static String decorateOpenGreen = "<span class=\"health_green\">";
	private static String decorateOpenRed = "<span class=\"health_red\">";
	private static String decorateClose = "</span>";
	
	public static String getUninterpreted(Properties propertiesMeta, String key) {
		String retVal = "";
		if(propertiesMeta != null) {
			if(key != null) {
				if(propertiesMeta.containsKey(key)) {
					String value = propertiesMeta.getProperty(key);
					if(value != null) {
						retVal = value.trim();
					}
				}
			}
		}
		return retVal;
	}
	
	public static String getInterpretedYesNo(String state, Properties propertiesMeta, String key) {
		String retVal = "";
		if(state != null) {
			if(state.equals(stateAvailable)) {
				String value = getUninterpreted(propertiesMeta, key);
				value = value.trim();
				if(value.equalsIgnoreCase(valueTrue)) {
					retVal = valueYes;
				}
				else if(value.equalsIgnoreCase(valueFalse)) {
					retVal = valueNo;
				}
				else {
					retVal = value;
				}
			}
		}
		return retVal;
	}
	
	public static String getInterpretedGoodPoor(String state, Properties propertiesMeta, String key) {
		String retVal = "";
		if(state != null) {
			if(state.equals(stateAvailable)) {
				String value = getUninterpreted(propertiesMeta, key);
				value = value.trim();
				if(value.equalsIgnoreCase(valueTrue)) {
					retVal = valueGood;
				}
				else if(value.equalsIgnoreCase(valueFalse)) {
					retVal = valuePoor;
				}
				else {
					retVal = value;
				}
			}
		}
		return retVal;
	}
	
	public static String getDecorated(String value) {
		String retVal = "";
		if(value != null) {
			if(value.equalsIgnoreCase(valueYes)) {
				StringBuffer sb = new StringBuffer();
				sb.append(decorateOpenNeutral);
				sb.append(value);
				sb.append(decorateClose);
				retVal = sb.toString();
			}
			else if(value.equalsIgnoreCase(valueNo)) {
				StringBuffer sb = new StringBuffer();
				sb.append(decorateOpenRed);
				sb.append(value);
				sb.append(decorateClose);
				retVal = sb.toString();
			}
			else if(value.equalsIgnoreCase(valueGood)) {
				StringBuffer sb = new StringBuffer();
				sb.append(decorateOpenGreen);
				sb.append(value);
				sb.append(decorateClose);
				retVal = sb.toString();
			}
			else if(value.equalsIgnoreCase(valuePoor)) {
				StringBuffer sb = new StringBuffer();
				sb.append(decorateOpenRed);
				sb.append(value);
				sb.append(decorateClose);
				retVal = sb.toString();
			}
			else {
				retVal = value;
			}
		}
		return retVal;
	}
	
}
