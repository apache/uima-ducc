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
package org.apache.uima.ducc.common.jd.plugin;

import java.util.Properties;

import org.apache.uima.cas.CAS;

public class JdProcessExceptionHandlerLoader {

	public static IJdProcessExceptionHandler load(String className) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
		IJdProcessExceptionHandler jdEventHandler = null;
		Class<?> pluginJdExceptionHandler = Class.forName(className);
		Object object = pluginJdExceptionHandler.newInstance();
		if(object instanceof IJdProcessExceptionHandler) {
			jdEventHandler = (IJdProcessExceptionHandler) object;
		}
		else {
			throw new InstantiationException("Implementation Error: "+className);
		}
		return jdEventHandler;
	}
	
	public static void main(String[] args) {

		try {
			IJdProcessExceptionHandler jdProcessExceptionHandler = JdProcessExceptionHandlerLoader.load(JdProcessExceptionHandler.class.getName());
			CAS cas = null;
			Exception e = null;
			Properties p = null;
			jdProcessExceptionHandler.handle("test001",cas, e, p);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
			
		try {
			IJdProcessExceptionHandler jdProcessExceptionHandler = JdProcessExceptionHandlerLoader.load("org.apache.uima.ducc.common.jd.plugin.example.ExampleJdProcessExceptionHandler");
			CAS cas = null;
			Exception e = null;
			Properties p = null;
			jdProcessExceptionHandler.handle("test002",cas, e, p);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		try {
			IJdProcessExceptionHandler jdProcessExceptionHandler = JdProcessExceptionHandlerLoader.load("org.apache.uima.ducc.common.jd.plugin.example.BadJdProcessExceptionHandler");
			CAS cas = null;
			Exception e = null;
			Properties p = null;
			jdProcessExceptionHandler.handle("test003",cas, e, p);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
