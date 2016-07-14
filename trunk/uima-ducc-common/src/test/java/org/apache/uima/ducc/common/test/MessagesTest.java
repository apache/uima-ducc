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

import org.apache.uima.ducc.common.internationalization.Messages;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class MessagesTest {

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
	
	private static String[] keys = {
		"and away we go",
		"CAS.id",
		"CAS.size",
		"changing core pool size to",
		"client initialization complete",
		"client initialization failed",
		"client initialization in progress",
		"client terminated",
		"client termination failed",
		"client terminating",
		"collection reader initialization failed",
		"creating driver thread",
		"default",
		"driver begin",
		"driver end",
		"driver init",
		"enter",
		"exit",
		"file",
		"from",
		"host",
		"invalid",
		"job.broker",
		"job.queue",
		"kill thread for",
		"no",
		"not found",
		"log directory",
		"log directory (default)",
		"pending job termination",
		"pending processes termination",
		"permits to force thread terminations",
		"plist",
		"process count",
		"publishing state",
		"received",
		"releasing",
		"removed queue",
		"retry",
		"retry attempts exhausted",
		"running",
		"seqNo",
		"shares",
		"size zero request ignored",
		"terminate",
		"terminate driver thread",
		"terminating thread",
		"thread",
		"threads-per-share",
		"UIMA-AS",
		"unable to create user log appender",
		"user log",
		"work item monitor class",
	};

	public static void showMessages(Messages messages) {
		for(int i=0; i <keys.length; i++) {
			String key = keys[i];
			System.out.println(messages.fetch(key));
			messages.fetch_exact(key);
		}
		System.out.println(messages.fetch("foobar"));
	}

	@Test
	public void test() {
		try {
			showMessages(Messages.getInstance());
			showMessages(Messages.getInstance("foo","bar"));
			showMessages(Messages.getInstance("","bar"));
			showMessages(Messages.getInstance(null,"bar"));
		}
		catch(Exception e) {
			e.printStackTrace();
			fail("Exception");
		}
		
	}

}
