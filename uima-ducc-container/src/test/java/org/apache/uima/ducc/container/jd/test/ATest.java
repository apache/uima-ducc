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
package org.apache.uima.ducc.container.jd.test;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.net.URL;
import java.util.Map;

import org.apache.uima.ducc.common.container.FlagsHelper;
import org.apache.uima.ducc.common.container.FlagsHelper.Name;
import org.apache.uima.ducc.container.jd.mh.MessageHandler;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

public abstract class ATest {
	
	private boolean disabled = false;
	private boolean verbose = false;
	private boolean warned = false;
	private boolean debug = false;
	
	public void clear() {
		for(Name name : FlagsHelper.Name.values()) {
			try {
				System.clearProperty(name.pname());
			}
			catch(Exception e) {
			}
			debug("clear: "+name.name());
		}
	}
	
	public String getResource(String name) {
		String retVal = null;
		URL urlXml = null;
		File file = null;
		String path = null;
		//
		urlXml = this.getClass().getResource(name);
		file = new File(urlXml.getFile());
		path = file.getAbsolutePath();
		retVal = path;
		return retVal;
	}
	
	public void ducc_home() {
		String folder = "/ducc_runtime";
		String file = "/resources/log4j.xml";
		String path = getResource(folder+file);
		String value = path.replace(file, "");
		String key = "DUCC_HOME";
		System.setProperty(key, value);
	}
	
	public void environment() {
		Map<String, String> map = System.getenv();
		if(map != null) {
			if(map.containsKey("JUNIT_VERBOSE")) {
				verbose = true;
			}
			if(map.containsKey("JUNIT_DEBUG")) {
				debug = true;
			}
		}
	}
	
	public boolean isDisabled(String name ) {
		if(disabled) {
			if(!warned) {
				System.err.println("Tests are disabled: "+name);
			}
			warned = true;
		}
		return disabled;
	}
	
	public boolean isVerbose() {
		return verbose;
	}
	
	public boolean isDebug() {
		return debug;
	}
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		ducc_home();
		environment();
		clear();
	}

	@After
	public void tearDown() throws Exception {
	}
	
	public void disabler() {
		MessageHandler.piggybackingDisable();
	}
	
	protected void out_println(String message) {
		System.out.println(message);
	}
	
	protected void debug(String message) {
		if(isDebug()) {
			out_println(message);
		}
	}
	
	protected void announce(String text) {
		if(isVerbose()) {
			String message = text;
			out_println(message);
		}
	}
	
	protected void asExpected(String text) {
		if(isVerbose()) {
			String message = "as expected: "+text;
			out_println(message);
		}
	}
	
	protected void asExpected(Exception e) {
		if(isVerbose()) {
			String message = "as expected: "+e.getMessage();
			out_println(message);
		}
	}
	
	protected void delete(File directory) {
		try {
			for(File file : directory.listFiles()) {
				debug("delete: "+file.getName());
				file.delete();
			}
			debug("delete: "+directory.getName());
			directory.delete();
		}
		catch(Exception e) {
			//e.printStackTrace();
		}
	}
	
	protected File mkWorkingDir() {
		URL url = this.getClass().getResource("/");
		File root = new File(url.getFile());
		String name = root.getAbsolutePath();
		debug(name);
		assertTrue(root.isDirectory());
		String nameWorking = name+File.separator+"working";
		File working = new File(nameWorking);
		delete(working);
		working.mkdir();
		return working;
	}
}
