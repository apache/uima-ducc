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
package org.apache.uima.ducc.common.internationalization;

import java.util.HashMap;
import java.util.Locale;
import java.util.ResourceBundle;

public class Messages {

	private String resourceName = "Messages";
	
	private Locale locale = null;
	private ResourceBundle resourceBundle = null;
	
	private static String default_language = "en";
	private static String default_country = "us";
	
	private static HashMap<String,Messages> messagesMap = new HashMap<String,Messages>();
	
	public static Messages getInstance() {
		return getInstance(default_language,default_country);
	}
	
	public static Messages getInstance(String language, String country) {
		if(language == null) {
			language = default_language;
		}
		if(country == null) {
			country = default_country;
		}
		String key = language+'.'+country;
		if(!messagesMap.containsKey(key)) {
			Locale locale = new Locale(language,country);
			Messages messages = new Messages(locale);
			messagesMap.put(key,messages);
		}
		Messages instance = messagesMap.get(key);
		return instance;
	}
	
	private Messages(Locale locale) {
		assert(locale != null);
		this.locale = locale;
		init();
	}
	
	private void init() {
		assert(locale != null);
		try {
			resourceBundle = ResourceBundle.getBundle(resourceName,locale);
		}
		catch(Exception e) {
		}
		if(resourceBundle == null) {
			try {
				locale = new Locale(default_language,default_country);
				resourceBundle = ResourceBundle.getBundle(resourceName,locale);
			}
			catch(Exception e) {
				// Give up!
			}
		}
	}
	
	private String normalize(String key) {
		return key.replaceAll(" ","_");
	}
	
	public String fetch(String key) {
		String text;
		String normalizedKey = normalize(key);
		try {
			text = resourceBundle.getString(normalizedKey);
		}
		catch(Exception e) {
			//e.printStackTrace();
			text = key;
		}
		return text;
	}

	public String fetchLabel(String key) {
		return fetch(key)+Placeholders.label_sep;
	}
	
	public String fetch_exact(String key) {
		String text;
		String normalizedKey = normalize(key);
		try {
			text = resourceBundle.getString(normalizedKey);
		}
		catch(Exception e) {
			//e.printStackTrace();
			text = normalizedKey;
		}
		return text;
	}

}
