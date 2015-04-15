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

import java.io.File;

import org.apache.uima.ducc.container.jd.JobDriver;
import org.apache.uima.ducc.container.jd.JobDriverException;
import org.apache.uima.ducc.container.jd.test.helper.Utilities;
import org.junit.Before;
import org.junit.Test;

public class Test1 extends TestSuite {
	
	protected JobDriver jd;
	
	@Before
    public void setUp() throws JobDriverException {
        initialize();
        jd = JobDriver.getInstance();
    }
	
	@Test
	public void test_11() {
		testIncludeAll();
	}
	
	@Test
	public void test_12() {
		testExcludeOne(2);
		String userClasspath = Utilities.getInstance().getUserCP();
		String[] cpParts = userClasspath.split(File.pathSeparator);
		for(int i=0; i<cpParts.length; i++) {
			testExcludeOne(i);
		}
	}
	
	//@Test
	public void test_13() {
		testNoXml();
	}
	
	@Test
	public void test_14() {
		getTotal();
	}
	
	@Test
	public void test_15() {
		getMetaCas();
	}
	
	@Test
	public void test_16() {
		getMetaCases(0);
	}
	
	@Test
	public void test_17() {
		getMetaCases(10);
	}

}
