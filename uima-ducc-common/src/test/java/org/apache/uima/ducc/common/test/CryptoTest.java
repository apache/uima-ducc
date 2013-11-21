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

import org.apache.uima.ducc.common.crypto.Crypto;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class CryptoTest {

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
			String user = System.getProperty("user.name");
			String home = System.getProperty("user.home");
			Crypto crypto = new Crypto(user,home);
			String message = "Hello DUCC!";
			byte[] cypheredMessage = crypto.encrypt(message);
			Properties properties = new Properties();
			String key_signature = "signature";
			properties.put(key_signature, cypheredMessage);
			cypheredMessage = (byte[]) properties.get(key_signature);
			Object decypheredMessage = crypto.decrypt(cypheredMessage);
			if(!message.equals(decypheredMessage)) {
				System.out.println("user="+user);
				System.out.println("home="+home);
				System.out.println("message="+(String)message);
				System.out.println("decypher="+(String)decypheredMessage);
				fail("decypher mismatch?!");
			}
		}
		catch(Exception e) {
			e.printStackTrace();
			fail("Exception");
		}
	}

}
