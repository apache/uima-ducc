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
package org.apache.uima.ducc.user.common.main;

import java.lang.reflect.Method;

/*
 * Implements dynamic loading of a service class, initializes its instance
 * and starts it. 
 */

public class PullServiceWrapper implements IServiceWrapper{

	private Object instance = null;
	private Class<?> duccServiceClass =  null;
	
	public void initialize(String[] args) throws Exception{
		duccServiceClass = 
				ClassLoader.getSystemClassLoader().loadClass("org.apache.uima.ducc.ps.service.main.ServiceWrapper");
		instance = duccServiceClass.newInstance();
		Method initMethod = duccServiceClass.getMethod("initialize", String[].class);
		initMethod.invoke(instance, (Object) args);
	}
	public void start() throws Exception {
		Method startMethod = duccServiceClass.getMethod("start");
		startMethod.invoke(instance);
	}

	public void stop() throws Exception {
		Method stopMethod = duccServiceClass.getMethod("stop");
		stopMethod.invoke(instance);

	}
}
