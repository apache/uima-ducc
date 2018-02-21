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
package org.apache.uima.ducc.container.jd.user.error.classload;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.Utils;
import org.apache.uima.ducc.container.common.FlagsExtendedHelper;
import org.apache.uima.ducc.container.common.MessageBuffer;
import org.apache.uima.ducc.container.common.Standardize;
import org.apache.uima.ducc.container.common.classloader.ContextSwitch;
import org.apache.uima.ducc.container.common.logger.IComponent;
import org.apache.uima.ducc.container.common.logger.ILogger;
import org.apache.uima.ducc.container.common.logger.Logger;
import org.apache.uima.ducc.user.common.PrivateClassLoader;

public class ProxyUserErrorStringify {

	private static Logger logger = Logger.getLogger(ProxyUserErrorStringify.class, IComponent.Id.JD.name());
	
	private URLClassLoader urlClassLoader = null;

	private String[] requiredClasses = { 
			"org.apache.uima.ducc.user.error.iface.IStringify", 
			"org.apache.uima.ducc.user.error.iface.Stringify",
			"org.apache.uima.ducc.user.error.iface.StringifyUserError",
			};
	
	public ProxyUserErrorStringify() throws ProxyUserErrorException {
		initialize();
	}
	
	public String convert(
			Object userException
			) throws ProxyUserErrorException {
		String location = "convert";
		String retVal = null;
		try {
			Class<?> clazz = urlClassLoader.loadClass("org.apache.uima.ducc.user.error.iface.Stringify");
			Constructor<?> constructor = clazz.getConstructor();
			Object[] c_args = new Object[] { };
			Object instance = ContextSwitch.construct(urlClassLoader, constructor, c_args);
			Class<?>[] parameterTypes = { 
				Object.class,	// userException
				};
			Method method = clazz.getMethod("convert", parameterTypes);
			Object[] args = { 
				userException,
				};
			Object printableString = ContextSwitch.call(urlClassLoader, method, instance, args);
			retVal = (String)printableString;
		}
		catch(Exception e) {
			logger.trace(location, ILogger.null_id, e);
			throw new ProxyUserErrorException(e);
		}
		return retVal;
	}
	
	private String getUimaAsDirectory() throws Exception {
		String location = "getUimaAsDirectory";
		try {
			StringBuffer sb = new StringBuffer();
			String duccHome = Utils.findDuccHome();
			sb.append(duccHome);
			if(!duccHome.endsWith(File.separator)) {
				sb.append(File.separator);
			}
			sb.append("apache-uima");
			sb.append(File.separator);
			sb.append("lib");
			sb.append(File.separator);
			sb.append("*");
			String retVal = sb.toString();
			logger.debug(location, ILogger.null_id, retVal);
			return retVal;
		}
		catch(Exception e) {
			logger.error(location, ILogger.null_id, e);
			throw e;
		}
	}
	
	private String augmentUserClasspath() throws ProxyUserErrorException {
		String location = "augmentUserClasspath";
		try {
			StringBuffer sb = new StringBuffer();
			FlagsExtendedHelper feh = FlagsExtendedHelper.getInstance();
			String userClasspath = feh.getUserClasspath();
			sb.append(userClasspath);
			if(!userClasspath.endsWith(File.pathSeparator)) {
				sb.append(File.pathSeparator);
			}
			sb.append(getUimaAsDirectory());
			String retVal = sb.toString();
			logger.debug(location, ILogger.null_id, retVal);
			return retVal;
		}
		catch(Exception e) {
			logger.error(location, ILogger.null_id, e);
			throw new ProxyUserErrorException(e);
		}
	}
	
	private void initialize() throws ProxyUserErrorException {
		String userClasspath = augmentUserClasspath();
		urlClassLoader = createClassLoader(userClasspath);
		validate();
	}
	
	private URLClassLoader createClassLoader(String userClasspath) {
		String location = "createClassLoader";
		URLClassLoader retVal = null;
		try {
			retVal = PrivateClassLoader.create(userClasspath);
		}
		catch(Exception e) {
			logger.error(location, ILogger.null_id, e);
		}
		return retVal;
	}
	
	private void validate() throws ProxyUserErrorException {
		for(String className : requiredClasses) {
			loadClass(className);
		}
	}
	
	private void loadClass(String className) throws ProxyUserErrorException {
		String location = "loadClass";
		try {
			MessageBuffer mb1 = new MessageBuffer();
			mb1.append(Standardize.Label.loading.get()+className);
			logger.debug(location, ILogger.null_id, mb1.toString());
			URL[] urls = urlClassLoader.getURLs();
			for(URL url : urls) {
				logger.trace(location, ILogger.null_id, url);
			}
			Class<?> loadedClass = urlClassLoader.loadClass(className);
			MessageBuffer mb2 = new MessageBuffer();
			mb2.append(Standardize.Label.loaded.get()+loadedClass.getName());
			logger.trace(location, ILogger.null_id, mb2.toString());
		} 
		catch (Exception e) {
			DuccLogger duccLogger = DuccLogger.getLogger(ProxyUserErrorStringify.class, "JD");
			duccLogger.error(location, null, e);
			logger.error(location, ILogger.null_id, e);
			throw new ProxyUserErrorException(e);
		}
	}
}
