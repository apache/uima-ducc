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

import org.apache.uima.ducc.common.json.MonitorInfo;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MonitorInfoTest {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test0() {
		MonitorInfo m0 = new MonitorInfo();
		assertFalse(m0.isRegression(null));
	}
	
	@Test
	public void test1() {
		MonitorInfo m0 = new MonitorInfo();
		MonitorInfo m1 = new MonitorInfo();
		assertFalse(m1.isRegression(m0));
	}
	
	@Test
	public void test2() {
		MonitorInfo m0 = new MonitorInfo();
		MonitorInfo m1 = new MonitorInfo();
		m0.total = "0";
		m1.total = "1";
		assertFalse(m1.isRegression(m0));
		m0.total = "1";
		m1.total = "0";
		assertTrue(m1.isRegression(m0));
	}
	
	@Test
	public void test3() {
		MonitorInfo m0 = new MonitorInfo();
		MonitorInfo m1 = new MonitorInfo();
		m0.done = "0";
		m1.done = "1";
		assertFalse(m1.isRegression(m0));
		m0.done = "1";
		m1.done = "0";
		assertTrue(m1.isRegression(m0));
	}
	
	@Test
	public void test4() {
		MonitorInfo m0 = new MonitorInfo();
		MonitorInfo m1 = new MonitorInfo();
		m0.error = "0";
		m1.error = "1";
		assertFalse(m1.isRegression(m0));
		m0.error = "1";
		m1.error = "0";
		assertTrue(m1.isRegression(m0));
	}
	
	@Test
	public void test5() {
		MonitorInfo m0 = new MonitorInfo();
		MonitorInfo m1 = new MonitorInfo();
		m0.retry = "0";
		m1.retry = "1";
		assertFalse(m1.isRegression(m0));
		m0.retry = "1";
		m1.retry = "0";
		assertTrue(m1.isRegression(m0));
	}
}
