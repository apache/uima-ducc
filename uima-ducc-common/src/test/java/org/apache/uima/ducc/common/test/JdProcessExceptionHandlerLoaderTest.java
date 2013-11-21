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
package org.apache.uima.ducc.common.test;

import static org.junit.Assert.*;

import java.util.Properties;

import org.apache.uima.cas.CAS;
import org.apache.uima.ducc.common.jd.plugin.IJdProcessExceptionHandler;
import org.apache.uima.ducc.common.jd.plugin.JdProcessExceptionHandler;
import org.apache.uima.ducc.common.jd.plugin.JdProcessExceptionHandlerLoader;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class JdProcessExceptionHandlerLoaderTest {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test() {
		try {
			IJdProcessExceptionHandler jdProcessExceptionHandler = JdProcessExceptionHandlerLoader.load(JdProcessExceptionHandler.class.getName());
			CAS cas = null;
			Exception e = null;
			Properties p = null;
			jdProcessExceptionHandler.handle("test001",cas, e, p);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			fail("Exception");
		} catch (IllegalAccessException e) {
			e.printStackTrace();
			fail("Exception");
		} catch (InstantiationException e) {
			e.printStackTrace();
			fail("Exception");
		}
			
		try {
			IJdProcessExceptionHandler jdProcessExceptionHandler = JdProcessExceptionHandlerLoader.load("org.apache.uima.ducc.common.jd.plugin.example.ExampleJdProcessExceptionHandler");
			CAS cas = null;
			Exception e = null;
			Properties p = null;
			jdProcessExceptionHandler.handle("test002",cas, e, p);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			fail("Exception");
		} catch (IllegalAccessException e) {
			e.printStackTrace();
			fail("Exception");
		} catch (InstantiationException e) {
			e.printStackTrace();
			fail("Exception");
		}
		
		try {
			IJdProcessExceptionHandler jdProcessExceptionHandler = JdProcessExceptionHandlerLoader.load("org.apache.uima.ducc.common.jd.plugin.example.BadJdProcessExceptionHandler");
			CAS cas = null;
			Exception e = null;
			Properties p = null;
			jdProcessExceptionHandler.handle("test003",cas, e, p);
			fail("No Exception?");
		} catch (ClassNotFoundException e) {
			//Expected
		} catch (IllegalAccessException e) {
			e.printStackTrace();
			fail("Exception");
		} catch (InstantiationException e) {
			e.printStackTrace();
			fail("Exception");
		}
	}

}
