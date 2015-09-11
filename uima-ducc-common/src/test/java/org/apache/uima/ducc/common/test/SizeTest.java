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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.uima.ducc.common.SizeBytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class SizeTest {

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
			SizeBytes size;
			SizeBytes.Type type;
			long value;
			//
			type = SizeBytes.Type.GBytes;
			value = 15;
			size = new SizeBytes(type,value);
			assertTrue(size.getGBytes() == value);
			//
			type = SizeBytes.Type.MBytes;
			value = 30;
			size = new SizeBytes(type,value);
			assertTrue(size.getMBytes() == value);
			//
			type = SizeBytes.Type.KBytes;
			value = 45;
			size = new SizeBytes(type,value);
			assertTrue(size.getKBytes() == value);
			//
			type = SizeBytes.Type.Bytes;
			value = 60;
			size = new SizeBytes(type,value);
			assertTrue(size.getBytes() == value);
		}
		catch(Exception e) {
			e.printStackTrace();
			fail("Exception");
		}
	}

}
