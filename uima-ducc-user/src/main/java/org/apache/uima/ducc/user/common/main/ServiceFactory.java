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

public class ServiceFactory {
	
	private ServiceFactory() {}

	public static IServiceWrapper newService() throws ClassNotFoundException, InstantiationException, IllegalAccessException { 
		Class<?> serviceClass = null;
		String serviceClassName = null;
		// use custom service class or default. Custom class must implement ServiceWrapper
		if ( ( serviceClassName = System.getProperty("ducc.service.class")) != null ) {
			serviceClass = Thread.currentThread().getContextClassLoader().loadClass(serviceClassName);
			return (IServiceWrapper)serviceClass.newInstance();
		} else {
			// use default
			return new PullServiceWrapper();
		}
	}
}
