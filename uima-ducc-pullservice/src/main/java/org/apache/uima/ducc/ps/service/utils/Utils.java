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
package org.apache.uima.ducc.ps.service.utils;

import java.util.Properties;
import java.util.regex.Pattern;

public class Utils {

	/**
	 * Resolves placeholders in provided contents using java's Matcher. Finds
	 * all occurances of ${<placeholder>} and resolves each using System
	 * properties which holds <placeholder>=<value> pairs.
	 * 
	 * @param contents
	 *            - target text containing placeholder(s)
	 * @param props
	 *            - Properties object holding key/value pairs
	 * @return - text with resolved placeholders
	 * 
	 * @throws Exception
	 */
	public static String resolvePlaceholders(String contents) {
		return resolvePlaceholders(contents, System.getProperties());
	}

	/**
	 * Resolves placeholders in provided contents using java's Matcher. Finds
	 * all occurances of ${<placeholder>} and resolves each using provided
	 * Properties object which holds <placeholder>=<value> pairs. If the
	 * placeholder not found then tries the System properties.
	 * 
	 * @param contents
	 *            - target text containing placeholder(s)
	 * @param props
	 *            - Properties object holding key/value pairs
	 * @return - text with resolved placeholders
	 * 
	 * @throws Exception
	 */
	public static String resolvePlaceholders(String contents, Properties props) {
		// Placeholders syntax ${<placeholder>}
		Pattern placeHolderPattern = Pattern.compile("\\$\\{(.*?)\\}");

		java.util.regex.Matcher matcher = placeHolderPattern.matcher(contents);

		StringBuffer sb = new StringBuffer();
		while (matcher.find()) {
			// extract placeholder
			final String key = matcher.group(1);
			// Find value for extracted placeholder.
			String placeholderValue = props.getProperty(key);
			if (placeholderValue == null) {
				placeholderValue = System.getProperty(key);
				if (placeholderValue == null) {
					throw new IllegalArgumentException(
							"Missing value for placeholder: " + key);
				}
			}
			matcher.appendReplacement(sb, placeholderValue);
		}
		matcher.appendTail(sb);
		return sb.toString();
	}

	/**
	 * Resolves placeholder using Spring Framework utility class
	 * 
	 * 
	 * @param value
	 * @param props
	 * @return
	 */
	public static String resolvePlaceholderIfExists(String value,
			Properties props) {
		String retVal = value;
		if (value != null && value.contains("${")) {
			retVal = resolvePlaceholders(value, props);
		}
		return retVal;
	}

}
