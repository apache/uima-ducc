package org.apache.uima.ducc.ps.service;

import org.apache.uima.ducc.ps.service.errors.builtin.WindowBasedErrorHandler;
import org.junit.Test;

import junit.framework.Assert;

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

public class JunitErrorHandlingTestSuite {

	public JunitErrorHandlingTestSuite() {
		
		
	}

	@Test
	public void testNoWindow() throws Exception {
		// fails on 3rd error 
		WindowBasedErrorHandler errorHandler = 
				new WindowBasedErrorHandler();
		errorHandler.withMaxFrameworkErrors(3).build();
		int taskCount = 10;
		int failures=0;
		for( int i = 0; i < taskCount; i++) {
			failures++;
			if ( errorHandler.exceededErrorWindow(i) ) {
				break;
			}
		}
		// there should be 3 failures
		Assert.assertEquals(3, failures);
	}
	@Test
	public void testDefaults() throws Exception {
		// fails on 1st error 
		WindowBasedErrorHandler errorHandler = 
				new WindowBasedErrorHandler();
		errorHandler.build();
		int taskCount = 10;
		int failures=0;
		for( int i = 0; i < taskCount; i++) {
			failures++;
			if ( errorHandler.exceededErrorWindow(i) ) {
				break;
			}
		}
		// there should be 1 failure
		Assert.assertEquals(1, failures);
	}
	@Test
	public void testErrorFirstTask() throws Exception {
		// fails on 3rd error 
		WindowBasedErrorHandler errorHandler = 
				new WindowBasedErrorHandler();
		errorHandler.withMaxFrameworkErrors(1).withProcessErrorWindow(1).build();
		int taskCount = 10;
		int failures=0;
		for( int i = 0; i < taskCount; i++) {
			failures++;
			if ( errorHandler.exceededErrorWindow(i) ) {
				break;
			}
		}
		// there should be 1 failure
		Assert.assertEquals(1, failures);
	}
	@Test
	public void testZeroThresholdWindow() throws Exception {
		WindowBasedErrorHandler errorHandler = 
				new WindowBasedErrorHandler();
		errorHandler.withMaxFrameworkErrors(0).
		             withProcessErrorWindow(5).build();
		int taskCount = 10;
		for( int i = 0; i < taskCount; i++) {
			// simulate failure on task3. The test should not fail since max errors is 0, meaning
			// don't ever fail.
			if (  i == 3 ) {
				if ( errorHandler.exceededErrorWindow(i) ) {
					Assert.fail("Unexpected test failure - window=5 maxErrors=0 taskCount:"+i);
				}
			}
		}
	}
	@Test
	public void testErrorsLessWindow() throws Exception {
		WindowBasedErrorHandler errorHandler = 
				new WindowBasedErrorHandler();
		errorHandler.withMaxFrameworkErrors(3).
		             withProcessErrorWindow(5).build();
		int taskCount = 10;
		for( int i = 0; i < taskCount; i++) {
			// simulate failure on task1, task3, and task9. Setup calls for failure
			// if 3 errors occur within 5 tasks window. In this case, task1 and task3
			// should not cause a failure. Also task9 should not cause error since
			// its a new window.
			if ( i == 1 || i == 3 || i == 9) {
				if ( errorHandler.exceededErrorWindow(i) ) {
					Assert.fail("Unexpected test failure - window=5 maxErrors=3 taskCount:"+i);
				}
			}
		}
	}
	@Test
	public void testErrorsExceedingWindow() throws Exception {
		WindowBasedErrorHandler errorHandler = 
				new WindowBasedErrorHandler();
		errorHandler.withMaxFrameworkErrors(3).
		             withProcessErrorWindow(8).build();
		int taskCount = 10;
		for( int i = 0; i < taskCount; i++) {
			// simulate failure on task1, task3, and task7. Setup calls for failure
			// if 3 errors occur within 8 tasks window. In this case, task1, task3, and task7
			// should cause failure. 
			if ( i == 1 || i == 3 || i == 7) {
				if ( errorHandler.exceededErrorWindow(i) ) {
					return;
				}
			}
		}
		Assert.fail("Unexpected test success - Should have failed - window=8 maxErrors=3 taskCount:"+taskCount);
	}
}
