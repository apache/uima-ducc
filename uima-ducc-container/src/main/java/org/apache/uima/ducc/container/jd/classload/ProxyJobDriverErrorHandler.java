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
import java.net.URLClassLoader;

import org.apache.uima.ducc.common.container.FlagsHelper;
import org.apache.uima.ducc.container.common.classloader.PrivateClassLoader;
import org.apache.uima.ducc.container.common.classloader.ProxyLogger;
import org.apache.uima.ducc.container.common.logger.IComponent;
import org.apache.uima.ducc.container.common.logger.ILogger;
import org.apache.uima.ducc.container.common.logger.Logger;
import org.apache.uima.ducc.container.jd.JobDriverException;

public class ProxyJobDriverErrorHandler {

	private static Logger logger = Logger.getLogger(ProxyJobDriverErrorHandler.class, IComponent.Id.JD.name());
	
	private URLClassLoader classLoader = null;
	
	private Object objectInstance = null;
	private Method methodInstanceHandle = null;
	
	private Method methodInstanceIsKillJob = null;
	private Method methodInstanceIsKillProcess = null;
	private Method methodInstanceIsKillWorkItem = null;
	
	private static String packageName = "org.apache.uima.ducc.user.jd.iface.";
	private static String defaultClassName = packageName+"JdUserErrorHandler";
	private static String directiveInterfaceName = packageName+"IJdUserDirective";
	
	public ProxyJobDriverErrorHandler() throws JobDriverException {
		try {
			initialize();
		}
		catch(Exception e) {
			ProxyLogger.loggifyUserException(e);
			throw new JobDriverException();
		}
	}
	
	private void initialize() throws Exception {
		String location = "initialize";
		FlagsHelper fh = FlagsHelper.getInstance();
		String userClasspath = fh.getUserClasspath();
		String[] classpath = fh.stringToArray(userClasspath);
		if(classpath != null) {
			for(String item : classpath) {
				logger.trace(location, ILogger.null_id, item);
			}
		}
		String className = fh.getUserErrorHandlerClassname();
		if(className == null) {
			className = defaultClassName;
		}
		String initializationData = fh.getUserErrorHandlerCfg();
		classLoader = createClassLoader(userClasspath);
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
						if(types[1].toString().contains("Object")) {
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
	
	private URLClassLoader createClassLoader(String userClasspath) throws Exception {
		URLClassLoader retVal = null;
		retVal = PrivateClassLoader.create(userClasspath);
		return retVal;
	}
	
	// Failed work item
	public ProxyJobDriverDirective handle(String serializedCAS, Object userException) throws JobDriverException {
		String location = "handle";
		ProxyJobDriverDirective retVal = new ProxyJobDriverDirective();
		try {
			Object[] plist = new Object[2];
			plist[0] = serializedCAS;
			plist[1] = userException;
			Object directive = methodInstanceHandle.invoke(objectInstance, plist);
			boolean isKillJob = (Boolean) methodInstanceIsKillJob.invoke(directive);
			boolean isKillProcess = (Boolean) methodInstanceIsKillProcess.invoke(directive);
			boolean isKillWorkItem = (Boolean) methodInstanceIsKillWorkItem.invoke(directive);
			retVal = new ProxyJobDriverDirective(isKillJob, isKillProcess, isKillWorkItem);
		} 
		catch (Exception e) {
			logger.error(location, ILogger.null_id, e);
		}
		return retVal;
	}
	
	// Failed process
	public ProxyJobDriverDirective handle(String serializedCAS) throws JobDriverException {
		String location = "handle";
		ProxyJobDriverDirective retVal = new ProxyJobDriverDirective();
		try {
			//TODO
			boolean isKillJob = false;
			boolean isKillProcess = false;
			boolean isKillWorkItem = false;
			retVal = new ProxyJobDriverDirective(isKillJob, isKillProcess, isKillWorkItem);
		} 
		catch (Exception e) {
			logger.error(location, ILogger.null_id, e);
		}
		return retVal;
	}
}
