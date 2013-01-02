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
package org.apache.uima.ducc.common.persistence;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.uima.ducc.common.internationalization.Messages;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;


public class PropertiesFileManager implements IPropertiesFileManager {
	
	private static Messages messages = Messages.getInstance();
	
	private DuccLogger logger = null;
	private String basedir = null;
	private String propertiesFileName = null;
	private Properties properties = new Properties();
	
	private boolean loadOnGet = false;
	private boolean storeOnSet = true;
	
	public PropertiesFileManager(String componentId, String basedir, String propertiesFileName) {
		this.logger = DuccLoggerComponents.makeLogger(PropertiesFileManager.class.getName(),componentId);
		this.basedir = basedir;
		this.propertiesFileName = propertiesFileName;
		init();
	}
	
	public PropertiesFileManager(String componentId, String basedir, String propertiesFileName, boolean loadOnGet, boolean storeOnSet) {
		this.logger = DuccLoggerComponents.makeLogger(PropertiesFileManager.class.getName(),componentId);
		this.basedir = basedir;
		this.propertiesFileName = propertiesFileName;
		this.loadOnGet = loadOnGet;
		this.storeOnSet = storeOnSet;
		init();
	}
	
	private String getFullFileName() {
		return basedir+File.separator+propertiesFileName;
	}
	
	private boolean mkdirs(String directory) {
		boolean created = false;
		File file = new File(directory);
		if(!file.exists()) {
			file.mkdirs();
			created = true;
		}
		return created;
	}
	
	private void init() {
		String methodName = "init";
		logger.trace(methodName, null, messages.fetch("enter"));
		logger.debug(methodName, null, messages.fetchLabel("logger")+logger);
		logger.debug(methodName, null, messages.fetchLabel("basedir")+basedir);
		logger.debug(methodName, null, messages.fetchLabel("propertiesFileName")+propertiesFileName);
		logger.debug(methodName, null, messages.fetchLabel("loadOnGet")+loadOnGet);
		logger.debug(methodName, null, messages.fetchLabel("storeOnSet")+storeOnSet);
		mkdirs(basedir);
		load();
		logger.info(methodName, null, getFullFileName());
		logger.trace(methodName, null, messages.fetch("exit"));
	}
	
	private void load() {
		String methodName = "load";
		logger.trace(methodName, null, messages.fetch("enter"));
		try {
			FileInputStream fis = new FileInputStream(getFullFileName());
			properties.load(fis);
			fis.close();
		}
		catch(IOException e) {
			logger.warn(methodName, null, messages.fetchLabel("load_failed")+getFullFileName());
		}
		logger.trace(methodName, null, messages.fetch("exit"));
	}
	
	private void store() {
		String methodName = "store";
		logger.trace(methodName, null, messages.fetch("enter"));
		try {
			FileOutputStream fos = new FileOutputStream(getFullFileName());
			properties.store(fos, null);
			fos.close();
		}
		catch(IOException e) {
			logger.error(methodName, null, e);
		}
		logger.trace(methodName, null, messages.fetch("exit"));
	}
	
	public String get(String key, String defaultValue) {
		String methodName = "get";
		synchronized(this) {
			logger.trace(methodName, null, messages.fetch("enter"));
			if(loadOnGet) {
				load();
			}
			String value = properties.getProperty(key, defaultValue);
			logger.debug(methodName, null, messages.fetchLabel("key")+key+" "+messages.fetchLabel("value")+value);
			logger.trace(methodName, null, messages.fetch("exit"));
			return value;
		}
	}
	
	public void set(String key, String value) {
		String methodName = "set";
		synchronized(this) {
			logger.trace(methodName, null, messages.fetch("enter"));
			properties.setProperty(key, value);
			logger.debug(methodName, null, messages.fetchLabel("key")+key+" "+messages.fetchLabel("value")+value);
			if(storeOnSet) {
				store();
			}
			logger.trace(methodName, null, messages.fetch("exit"));
		}
	}
	
	public int increment(String key) {
		String methodName = "increment";
		synchronized(this) {
			logger.trace(methodName, null, messages.fetch("enter"));
			int retVal = Integer.valueOf(get(key,"-1"));
			retVal++;
			logger.debug(methodName, null, messages.fetchLabel("key")+key+" "+messages.fetchLabel("value")+retVal);
			set(key,String.valueOf(retVal));
			logger.trace(methodName, null, messages.fetch("exit"));
			return retVal;
		}
	}
	
}
