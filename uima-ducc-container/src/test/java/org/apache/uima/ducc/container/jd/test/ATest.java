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

import org.apache.uima.ducc.container.common.ContainerLogger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

public abstract class ATest {
	
	private boolean disabled = false;
	private boolean verbose = true;
	private boolean warned = false;
	private boolean debug = true;
	
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
		if(!isVerbose()) {
			ContainerLogger.setSilentRunning();
		}
		else {
			ContainerLogger.resetSilentRunning();
		}
	}

	@After
	public void tearDown() throws Exception {
	}
	
	protected void out_println(String message) {
		System.out.println(message);
	}
	
	protected void debug(String message) {
		if(isDebug()) {
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
}
