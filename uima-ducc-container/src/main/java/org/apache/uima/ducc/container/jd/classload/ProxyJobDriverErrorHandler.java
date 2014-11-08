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
package org.apache.uima.ducc.container.jd.classload;

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.net.URL;
import java.net.URLClassLoader;

import org.apache.uima.ducc.container.common.ContainerLogger;
import org.apache.uima.ducc.container.common.IContainerLogger;
import org.apache.uima.ducc.container.common.IEntityId;
import org.apache.uima.ducc.container.jd.JobDriverException;

public class ProxyJobDriverErrorHandler {

	private IContainerLogger logger = ContainerLogger.getLogger(ProxyJobDriverErrorHandler.class, IContainerLogger.Component.JD.name());
	
	private URLClassLoader classLoader = null;
	
	private Object objectInstance = null;
	private Method methodInstanceHandle = null;
	
	private Method methodInstanceIsKillJob = null;
	private Method methodInstanceIsKillProcess = null;
	private Method methodInstanceIsKillWorkItem = null;
	
	private static String packageName = "org.apache.uima.ducc.user.jd.iface.";
	private static String defaultClassName = packageName+"JdUserErrorHandler";
	private static String directiveInterfaceName = packageName+"IJdUserDirective";
	
	public ProxyJobDriverErrorHandler(String[] classPath) throws JobDriverException {
		String className = defaultClassName;
		String initializationData = null;
		initialize(classPath, className, initializationData);
	}
	
	public ProxyJobDriverErrorHandler(String[] classPath, String className, String initializationData) throws JobDriverException {
		if(className == null) {
			className = defaultClassName;
		}
		initialize(classPath, className, initializationData);
	}
	
	private void initialize(String[] classpath, String className, String initializationData) throws JobDriverException {
		String location = "initialize";
		try {
			URL[] classLoaderUrls = new URL[classpath.length];
			int i = 0;
			for(String item : classpath) {
				classLoaderUrls[i] = this.getClass().getResource(item);
				i++;
			}
			classLoader = new URLClassLoader(classLoaderUrls, ClassLoader.getSystemClassLoader().getParent());
			Class<?> classAnchor = classLoader.loadClass(className);
			objectInstance = classAnchor.newInstance();
			//
			String methodNameInitialize = "initialize";
			Method methodInstanceInitialize = classAnchor.getMethod(methodNameInitialize, String.class);
			methodInstanceInitialize.invoke(objectInstance, initializationData);
			//
			Method[] classMethods = classAnchor.getMethods();
			for(Method method : classMethods) {
				if(method.getName().equals("handle")) {
					Type[] types = method.getParameterTypes();
					if(types.length == 2) {
						if(types[0].toString().contains("String")) {
							if(types[1].toString().contains("Exception")) {
								methodInstanceHandle = method;
								break;
							}
						}
					}
				}
			}
			//
			Class<?> directiveAnchor = classLoader.loadClass(directiveInterfaceName);
			Method[] directiveMethods = directiveAnchor.getMethods();
			for(Method method : directiveMethods) {
				Type[] types = method.getParameterTypes();
				if(types.length == 0) {
					if(method.getName().equals("isKillJob")) {
					methodInstanceIsKillJob = method;
					}
					else if(method.getName().equals("isKillProcess")) {
						methodInstanceIsKillProcess = method;
					}
					else if(method.getName().equals("isKillWorkItem")) {
						methodInstanceIsKillWorkItem = method;
					}
				}
			}
		} 
		catch (Exception e) {
			logger.error(location, IEntityId.null_id, e);
			throw new JobDriverException(e);
		}
	}
	
	public ProxyJobDriverDirective handle(Object serializedCAS, Object exception) throws JobDriverException {
		String location = "handle";
		ProxyJobDriverDirective retVal = null;
		try {
			Object[] plist = new Object[2];
			plist[0] = serializedCAS;
			plist[1] = exception;
			Object directive = methodInstanceHandle.invoke(objectInstance, plist);
			boolean isKillJob = (Boolean) methodInstanceIsKillJob.invoke(directive);
			boolean isKillProcess = (Boolean) methodInstanceIsKillProcess.invoke(directive);
			boolean isKillWorkItem = (Boolean) methodInstanceIsKillWorkItem.invoke(directive);
			retVal = new ProxyJobDriverDirective(isKillJob, isKillProcess, isKillWorkItem);
		} 
		catch (Exception e) {
			logger.error(location, IEntityId.null_id, e);
			throw new JobDriverException(e);
		}
		return retVal;
	}
}
