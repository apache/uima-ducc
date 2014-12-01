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
package org.apache.uima.ducc.container.dd.classload;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.container.common.FlagsExtendedHelper;
import org.apache.uima.ducc.container.common.MessageBuffer;
import org.apache.uima.ducc.container.common.Standardize;
import org.apache.uima.ducc.container.common.classloader.PrivateClassLoader;
import org.apache.uima.ducc.container.common.logger.IComponent;
import org.apache.uima.ducc.container.common.logger.ILogger;
import org.apache.uima.ducc.container.common.logger.Logger;

public class ProxyDDGenerate {

	private static Logger logger = Logger.getLogger(ProxyDDGenerate.class, IComponent.Id.JD.name());
	
	private URLClassLoader urlClassLoader = null;

	private String[] requiredClasses = { 
			"org.apache.uima.ducc.user.dd.iface.DDException", 
			"org.apache.uima.ducc.user.dd.iface.DDGenerate",
			"org.apache.uima.ducc.user.dd.iface.IDDGenerate",
			// implied:
			//"org.springframework.util.Assert",
			//"org.apache.xmlbeans.XmlBeans",
			};
	
	public ProxyDDGenerate() throws ProxyDDException {
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
			String ddName,
			String ddDescription,
			Integer ddThreadCount,
			String ddBrokerURL,
			String ddEndpoint,
			String cmDescriptor,
			List<String> cmOverrides, 
			String aeDescriptor, 
			List<String> aeOverrides, 
			String ccDescriptor,
			List<String> ccOverrides
			) throws ProxyDDException
	{
		String retVal = null;
		try {
			show("directory", directory);
			show("id", id);
			show("ddName", ddName);
			show("ddDescription", ddDescription);
			show("ddThreadCount", ddThreadCount);
			show("ddBrokerURL", ddBrokerURL);
			show("ddEndpoint", ddEndpoint);
			show("cmDescriptor", cmDescriptor);
			show("cmOverrides", cmOverrides);
			show("aeDescriptor", aeDescriptor);
			show("aeOverrides", aeOverrides);
			show("ccDescriptor", ccDescriptor);
			show("ccOverrides", ccOverrides);
			Class<?> clazz = urlClassLoader.loadClass("org.apache.uima.ducc.user.dd.iface.DDGenerate");
			Constructor<?> constructor = clazz.getConstructor();
			Object instance = constructor.newInstance();
			Class<?>[] parameterTypes = { 
					String.class,	// directory
					String.class,	// id
					String.class,	// ddName
					String.class,	// ddDescription
					Integer.class,	// ddThreadCount
					String.class,	// ddBrokerURL
					String.class,	// ddEndpoint
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
					ddName, 
					ddDescription, 
					ddThreadCount, 
					ddBrokerURL, 
					ddEndpoint,
					cmDescriptor,
					cmOverrides,
					aeDescriptor,
					aeOverrides,
					ccDescriptor,
					ccOverrides
					};
			String dd = (String) method.invoke(instance, args);
			show("generated deployment descriptor", dd);
		}
		catch(Exception e) {
			e.printStackTrace();
			throw new ProxyDDException(e.toString());
		}
		return retVal;
	}
	
	private void initialize() throws ProxyDDException {
		FlagsExtendedHelper feh = FlagsExtendedHelper.getInstance();
		String userClasspath = feh.getUserClasspath();
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
	
	private void validate() throws ProxyDDException {
		for(String className : requiredClasses) {
			loadClass(className);
		}
	}
	
	private void loadClass(String className) throws ProxyDDException {
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
			DuccLogger duccLogger = DuccLogger.getLogger(ProxyDDGenerate.class, "JD");
			duccLogger.error(location, null, e);
			logger.error(location, ILogger.null_id, e);
			throw new ProxyDDException(e);
		}
	}
}
