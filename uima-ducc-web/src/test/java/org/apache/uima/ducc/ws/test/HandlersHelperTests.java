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
package org.apache.uima.ducc.ws.test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.net.URL;

import org.apache.uima.ducc.common.main.DuccService;
import org.apache.uima.ducc.common.utils.AlienFile;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.ws.utils.HandlersHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Rudimentary file readability function tests
 */
public class HandlersHelperTests {

	private static DuccLogger logger = null;
	
	@Before
	public void before() {
		ducc_home();
		ducc_logger();
		ducc_config();
	}
	
	@After
	public void after() {
	}
	
	private void ducc_logger() {
		logger = new DuccLogger(HandlersHelperTests.class);
		DuccService.setDuccLogger(logger);
	}
	
	private void ducc_config() {
		try {
			DuccService.setDuccLogger(logger);
		}
		catch(Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	private String getResource(String name) {
		String retVal = name;
		try {
			URL urlXml = null;
			File file = null;
			String path = null;
			//
			urlXml = this.getClass().getResource(name);
			file = new File(urlXml.getFile());
			path = file.getAbsolutePath();
			System.out.println(path);
			retVal = path;
		}
		catch(Exception e) {
		}
		return retVal;
	}
	
	private void ducc_home() {
		String folder = "/ducc_runtime/resources";
		String value = getResource(folder);
		String key = "DUCC_HOME";
		System.setProperty(key, value);
	}
	
	String userSelf = System.getProperty("user.name");
	String pathReadable = "/etc/passwd";
	String pathUnreadable = "/etc/foobar";
	
	@Test
	public void test1() {
		try {
			String user = userSelf;
			String path = pathReadable;
			boolean result = HandlersHelper.isFileReadable(user, path);
			assertTrue(result);
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void test2() {
		try {
			String user = userSelf;
			String path = pathUnreadable;
			boolean result = HandlersHelper.isFileReadable(user, path);
			assertTrue(!result);
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void test3() {
		try {
			String user = userSelf;
			String path = pathReadable;
			AlienFile alienFile = new AlienFile(user, path);
			String result = alienFile.getString();
			assertTrue(result != null);
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void test4() {
		try {
			String user = userSelf;
			String path = pathUnreadable;
			AlienFile alienFile = new AlienFile(user, path);
			String result = alienFile.getString();
			assertTrue(result == null);
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
}
