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
import java.util.MissingResourceException;
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
			locale = new Locale(default_language,default_country);
			resourceBundle = ResourceBundle.getBundle(resourceName,locale);
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
		catch(MissingResourceException e) {
			//e.printStackTrace();
			text = key;
		}
		return text;
	}

	public String fetchLabel(String key) {
		return fetch(key)+Placeholders.label_sep;
	}
	
	private String fetch_exact(String key) {
		String text;
		String normalizedKey = normalize(key);
		text = resourceBundle.getString(normalizedKey);
		return text;
	}
	
	/*
	 * <test>
	 */
	
	private static String[] keys = {
		"and away we go",
		"CAS.id",
		"CAS.size",
		"changing core pool size to",
		"client initialization complete",
		"client initialization failed",
		"client initialization in progress",
		"client terminated",
		"client termination failed",
		"client terminating",
		"collection reader initialization failed",
		"creating driver thread",
		"default",
		"driver begin",
		"driver end",
		"driver init",
		"enter",
		"exit",
		"file",
		"from",
		"host",
		"invalid",
		"job.broker",
		"job.queue",
		"kill thread for",
		"no",
		"not found",
		"log directory",
		"log directory (default)",
		"pending job termination",
		"pending processes termination",
		"permits to force thread terminations",
		"plist",
		"process count",
		"publishing state",
		"received",
		"releasing",
		"removed queue",
		"retry",
		"retry attempts exhausted",
		"running",
		"seqNo",
		"shares",
		"size zero request ignored",
		"terminate",
		"terminate driver thread",
		"terminating thread",
		"thread",
		"threads-per-share",
		"UIMA-AS",
		"unable to create user log appender",
		"user log",
		"work item monitor class",
	};

	public static void showMessages(Messages messages) {
		for(int i=0; i <keys.length; i++) {
			String key = keys[i];
			System.out.println(messages.fetch(key));
			messages.fetch_exact(key);
		}
		System.out.println(messages.fetch("foobar"));
	}
	
	public static void main(String[] args) {
		showMessages(Messages.getInstance());
		showMessages(Messages.getInstance("foo","bar"));
		showMessages(Messages.getInstance("","bar"));
		showMessages(Messages.getInstance(null,"bar"));
	}

	/*
	 * </test>
	 */
}
