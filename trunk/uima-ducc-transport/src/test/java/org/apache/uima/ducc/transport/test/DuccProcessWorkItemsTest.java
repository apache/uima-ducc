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
package org.apache.uima.ducc.transport.test;

import static org.junit.Assert.fail;

import org.apache.uima.ducc.transport.event.common.DuccProcessWorkItemsV1;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class DuccProcessWorkItemsTest {

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
	
	private static void stats(DuccProcessWorkItemsV1 pwi) {
		System.out.println("     done: "+pwi.getCountDone());
		System.out.println("    error: "+pwi.getCountError());
		System.out.println("    retry: "+pwi.getCountRetry());
		System.out.println(" dispatch: "+pwi.getCountDispatch());
		System.out.println("avg(secs): "+pwi.getSecsAvg());
		System.out.println("min(secs): "+pwi.getSecsMin());
		System.out.println("max(secs): "+pwi.getSecsMax());
	}
	
	private static void done(DuccProcessWorkItemsV1 pwi, int msecs) {
		pwi.dispatch();
		pwi.done(msecs);
	}
	
	private static void error(DuccProcessWorkItemsV1 pwi) {
		pwi.dispatch();
		pwi.error();
	}
	
	private static void retry(DuccProcessWorkItemsV1 pwi) {
		pwi.dispatch();
		pwi.retry();
	}
	
	@Test
	public void test() {
		try {
			DuccProcessWorkItemsV1 pwi = new DuccProcessWorkItemsV1();
			pwi.dispatch();
			done(pwi,30000);
			done(pwi,40000);
			for(int i=0; i<9; i++) {
				error(pwi);
			}
			for(int i=0; i<4; i++) {
				retry(pwi);
			}
			stats(pwi);
		}
		catch(Exception e) {
			fail("Exception");
		}
	}

}
