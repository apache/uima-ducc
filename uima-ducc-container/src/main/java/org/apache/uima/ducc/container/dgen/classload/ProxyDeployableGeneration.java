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
package org.apache.uima.ducc.container.dgen.classload;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.Utils;
import org.apache.uima.ducc.container.common.FlagsExtendedHelper;
import org.apache.uima.ducc.container.common.MessageBuffer;
import org.apache.uima.ducc.container.common.Standardize;
import org.apache.uima.ducc.container.common.classloader.ContextSwitch;
import org.apache.uima.ducc.container.common.classloader.PrivateClassLoader;
import org.apache.uima.ducc.container.common.classloader.ProxyException;
import org.apache.uima.ducc.container.common.classloader.ProxyLogger;
import org.apache.uima.ducc.container.common.logger.IComponent;
import org.apache.uima.ducc.container.common.logger.ILogger;
import org.apache.uima.ducc.container.common.logger.Logger;

public class ProxyDeployableGeneration {

	private static Logger logger = Logger.getLogger(ProxyDeployableGeneration.class, IComponent.Id.JD.name());
	
	private URLClassLoader urlClassLoader = null;

	private String[] requiredClasses = { 
			"org.apache.uima.ducc.user.dgen.iface.DeployableGeneration",
			"org.apache.uima.ducc.user.dgen.iface.IDeployableGeneration",
			};
	
	public ProxyDeployableGeneration() throws ProxyDeployableGenerationException {
		initialize();
	}
	
	private void show(String text) {
		String location = "show";
		logger.info(location, ILogger.null_id, text);
		System.out.println(text);
	}
	
	private void show(String name, String value) {
		show(name+"="+value);
	}
	
	private void show(String name, Integer value) {
		show(name+"="+value.toString());
	}
	
	private void show(String name, List<String> value) {
		if(value == null) {
			show(name+"="+value);
		}
		else {
			show(name+"="+value.toString());
		}
	}
	
	public String generate(
			String directory, 
			String id,
			String dgenName,
			String dgenDescription,
			Integer dgenThreadCount,
			String dgenBrokerURL,
			String dgenEndpoint,
			String dgenFlowController,
			String cmDescriptor,
			List<String> cmOverrides, 
			String aeDescriptor, 
			List<String> aeOverrides, 
			String ccDescriptor,
			List<String> ccOverrides
			) throws ProxyDeployableGenerationException, ProxyException
	{
		String retVal = null;
		try {
			show("directory", directory);
			show("id", id);
			show("dgenName", dgenName);
			show("dgenDescription", dgenDescription);
			show("dgenThreadCount", dgenThreadCount);
			show("dgenBrokerURL", dgenBrokerURL);
			show("dgenEndpoint", dgenEndpoint);
			show("degnFlowController", dgenFlowController);
			show("cmDescriptor", cmDescriptor);
			show("cmOverrides", cmOverrides);
			show("aeDescriptor", aeDescriptor);
			show("aeOverrides", aeOverrides);
			show("ccDescriptor", ccDescriptor);
			show("ccOverrides", ccOverrides);
			Class<?> clazz = urlClassLoader.loadClass("org.apache.uima.ducc.user.dgen.iface.DeployableGeneration");
			Constructor<?> constructor = clazz.getConstructor();
			Object instance = constructor.newInstance();
			Class<?>[] parameterTypes = { 
					String.class,	// directory
					String.class,	// id
					String.class,	// dgenName
					String.class,	// dgenDescription
					Integer.class,	// dgenThreadCount
					String.class,	// dgenBrokerURL
					String.class,	// dgenEndpoint
					String.class,	// dgenFlowController
					String.class,	// cmDescriptor
					List.class,		// cmOverrides
					String.class,	// aeDescriptor
					List.class,		// aeOverrides
					String.class,	// ccDescriptor
					List.class,		// ccOverrides
			};
			Method method = clazz.getMethod("generate", parameterTypes);
			Object[] args = { 
					directory, 
					id, 
					dgenName, 
					dgenDescription, 
					dgenThreadCount, 
					dgenBrokerURL, 
					dgenEndpoint,
					dgenFlowController,
					cmDescriptor,
					cmOverrides,
					aeDescriptor,
					aeOverrides,
					ccDescriptor,
					ccOverrides
					};
			String dgen = (String) ContextSwitch.call(urlClassLoader, method, instance, args);
			show("generated deployment descriptor", dgen);
			retVal = dgen;
		}
		catch(Exception e) {
			ProxyLogger.loggifyUserException(e);
			throw new ProxyException();
		}
		return retVal;
	}
	
	public String generate(
			String directory, 
			String id,
			String dgenName,
			String dgenDescription,
			Integer dgenThreadCount,
			String dgenBrokerURL,
			String dgenEndpoint,
			String dgenFlowController,
			String referenceByName
			) throws ProxyException
	{
		String retVal = null;
		try {
			show("directory", directory);
			show("id", id);
			show("dgenName", dgenName);
			show("dgenDescription", dgenDescription);
			show("dgenThreadCount", dgenThreadCount);
			show("dgenBrokerURL", dgenBrokerURL);
			show("dgenEndpoint", dgenEndpoint);
			show("degnFlowController", dgenFlowController);
			show("referenceByName", referenceByName);
			Class<?> clazz = urlClassLoader.loadClass("org.apache.uima.ducc.user.dgen.iface.DeployableGeneration");
			Constructor<?> constructor = clazz.getConstructor();
			Object instance = constructor.newInstance();
			Class<?>[] parameterTypes = { 
					String.class,	// directory
					String.class,	// id
					String.class,	// dgenName
					String.class,	// dgenDescription
					Integer.class,	// dgenThreadCount
					String.class,	// dgenBrokerURL
					String.class,	// dgenEndpoint
					String.class,	// dgenFlowController
					String.class,	// referenceByName
			};
			Method method = clazz.getMethod("generate", parameterTypes);
			Object[] args = { 
					directory, 
					id, 
					dgenName, 
					dgenDescription, 
					dgenThreadCount, 
					dgenBrokerURL, 
					dgenEndpoint,
					dgenFlowController,
					referenceByName
					};
			String dgen = (String) ContextSwitch.call(urlClassLoader, method, instance, args);
			show("generated deployment descriptor", dgen);
			retVal = dgen;
		}
		catch(Exception e) {
			ProxyLogger.loggifyUserException(e);
			throw new ProxyException();
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
			logger.info(location, ILogger.null_id, retVal);
			return retVal;
		}
		catch(Exception e) {
			logger.error(location, ILogger.null_id, e);
			throw e;
		}
	}
	
	private String augmentUserClasspath() throws ProxyDeployableGenerationException {
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
			logger.info(location, ILogger.null_id, retVal);
			return retVal;
		}
		catch(Exception e) {
			logger.error(location, ILogger.null_id, e);
			throw new ProxyDeployableGenerationException(e);
		}
	}
	
	private void initialize() throws ProxyDeployableGenerationException {
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
	
	private void validate() throws ProxyDeployableGenerationException {
		for(String className : requiredClasses) {
			loadClass(className);
		}
	}
	
	private void loadClass(String className) throws ProxyDeployableGenerationException {
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
			DuccLogger duccLogger = DuccLogger.getLogger(ProxyDeployableGeneration.class, "JD");
			duccLogger.error(location, null, e);
			logger.error(location, ILogger.null_id, e);
			throw new ProxyDeployableGenerationException(e);
		}
	}
}
