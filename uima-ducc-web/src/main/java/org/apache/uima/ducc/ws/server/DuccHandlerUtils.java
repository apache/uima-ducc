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
	
	private static String valueUp = "Up";
	private static String valueDown = "Down";
	
	private static String valueGood = "Good";
	private static String valuePoor = "Poor";
	
	//private static String health_neutral = "health_neutral";
	private static String health_green = "health_green";
	private static String health_red = "health_red";
	
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
	
	public static String getInterpreted(String state, Properties propertiesMeta, String key, String xform4True, String xform4False) {
		String retVal = "";
		if(state != null) {
			if(state.equals(stateAvailable)) {
				String value = getUninterpreted(propertiesMeta, key);
				value = value.trim();
				if(value.equalsIgnoreCase(valueTrue)) {
					retVal = xform4True;
				}
				else if(value.equalsIgnoreCase(valueFalse)) {
					retVal = xform4False;
				}
				else {
					retVal = value;
				}
			}
		}
		return retVal;
	}
	
	public static String getInterpretedUpDown(String state, Properties propertiesMeta, String key) {
		return getInterpreted(state, propertiesMeta, key, valueUp, valueDown);
	}
	
	public static String getInterpretedGoodPoor(String state, Properties propertiesMeta, String key) {
		return getInterpreted(state, propertiesMeta, key, valueGood, valuePoor);
	}
	
	private static String openSpan(String spanClass, String spanTitle) {
		StringBuffer sb = new StringBuffer();
		sb.append("<");
		sb.append("span");
		if(spanClass != null) {
			sb.append(" ");
			sb.append("class=");
			sb.append("\"");
			sb.append(spanClass);
			sb.append("\"");
		}
		if(spanTitle != null) {
			sb.append(" ");
			sb.append("title=");
			sb.append("\"");
			sb.append(spanTitle);
			sb.append("\"");
		}
		sb.append(">");
		return sb.toString();
	}
	
	private static String closeSpan() {
		return "</span>";
	}
	
	public static String getDecorated(String value) {
		return getDecorated(value,null);
	}
	
	public static String getDecorated(String value, String popup) {
		String retVal = "";
		if(value != null) {
			String tValue = value.trim();
			if(tValue.equalsIgnoreCase(valueDown)) {
				StringBuffer sb = new StringBuffer();
				sb.append(openSpan(health_red, popup));
				sb.append(tValue);
				sb.append(closeSpan());
				retVal = sb.toString();
			}
			else if(tValue.equalsIgnoreCase(valueUp)) {
				StringBuffer sb = new StringBuffer();
				sb.append(openSpan(health_green, popup));
				sb.append(tValue);
				sb.append(closeSpan());
				retVal = sb.toString();
			}
			else if(tValue.equalsIgnoreCase(valuePoor)) {
				StringBuffer sb = new StringBuffer();
				sb.append(openSpan(health_red, popup));
				sb.append(tValue);
				sb.append(closeSpan());
				retVal = sb.toString();
			}
			else if(tValue.equalsIgnoreCase(valueGood)) {
				StringBuffer sb = new StringBuffer();
				sb.append(openSpan(health_green, popup));
				sb.append(tValue);
				sb.append(closeSpan());
				retVal = sb.toString();
			}
			else {
				retVal = value;
			}
		}
		return retVal;
	}
	
	public static String warn(String text) {
		StringBuffer sb = new StringBuffer();
		sb.append("<span class=\"health_red\""+">");
		sb.append(text);
		sb.append("</span>");
		return sb.toString();
	}
	
	public static String down() {
		StringBuffer sb = new StringBuffer();
		sb.append("<span class=\"health_red\""+">");
		sb.append("down");
		sb.append("</span>");
		return sb.toString();
	}
	
	public static String up() {
		StringBuffer sb = new StringBuffer();
		sb.append("<span class=\"health_green\""+">");
		sb.append("up");
		sb.append("</span>");
		return sb.toString();
	}
	
	public static String up_provisional(String text) {
		StringBuffer sb = new StringBuffer();
		sb.append("<span class=\"health_black\""+">");
		sb.append("up"+text);
		sb.append("</span>");
		return sb.toString();
	}
	
	public static String unknown() {
		StringBuffer sb = new StringBuffer();
		sb.append("<span class=\"health_black\""+">");
		sb.append("unknown");
		sb.append("</span>");
		return sb.toString();
	}
	
}
