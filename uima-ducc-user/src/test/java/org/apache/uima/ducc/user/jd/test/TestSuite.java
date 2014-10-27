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
package org.apache.uima.ducc.user.jd.test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.net.URL;

import org.apache.uima.ducc.user.jd.iface.JdUserCollectionReader;
import org.apache.uima.ducc.user.jd.iface.JdUserException;
import org.apache.uima.ducc.user.jd.iface.JdUserMetaCas;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestSuite {

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
	
	private boolean debug = true;
	
	private void out_println(String message) {
		System.out.println(message);
	}
	
	private void debug(String message) {
		if(debug) {
			out_println(message);
		}
	}
	
	@Test
	public void test01() {
		try {
			int seqNo = 1;
			String serializedCas = "ABC";
			String documentText = "123";
			JdUserMetaCas jdUserMetaCas = new JdUserMetaCas(seqNo, serializedCas, documentText);
			assertTrue(seqNo == jdUserMetaCas.getSeqNo());
			assertTrue(serializedCas.equals(jdUserMetaCas.getSerializedCas()));
			assertTrue(documentText.equals(jdUserMetaCas.getDocumentText()));
		}
		catch(Exception e) {
			e.printStackTrace();
			fail("Exception");
		}
	}
	
	@Test
	public void test02() {
		try {
			String message = "XYZ";
			JdUserException jdUserException = new JdUserException(message);
			assertTrue(message.equals(jdUserException.getMessage()));
		}
		catch(Exception e) {
			e.printStackTrace();
			fail("Exception");
		}
	}
	
	@Test
	public void test03() {
		try {
			URL url = this.getClass().getResource("/CR100.xml");
			File file = new File(url.getFile());
			String crXml = file.getAbsolutePath();
			debug(crXml);
			String crCfg = null;
			JdUserCollectionReader jdUserCollectionReader = new JdUserCollectionReader(crXml, crCfg);
			int total = jdUserCollectionReader.getTotal();
			assertTrue(total == 100);
		}
		catch(Exception e) {
			e.printStackTrace();
			fail("Exception");
		}
	}
	
	@Test
	public void test04() {
		try {
			URL url = this.getClass().getResource("/CR100.xml");
			File file = new File(url.getFile());
			String crXml = file.getAbsolutePath();
			debug(crXml);
			String crCfg = null;
			JdUserCollectionReader jdUserCollectionReader = new JdUserCollectionReader(crXml, crCfg);
			int total = jdUserCollectionReader.getTotal();
			assertTrue(total == 100);
			int counter = 0;
			JdUserMetaCas jdUserMetaCas = jdUserCollectionReader.getJdUserMetaCas();
			while(jdUserMetaCas != null) {
				jdUserMetaCas.printMe();
				counter++;
				if(counter > 100) {
					fail("Too many CASes: "+counter);
				}
				jdUserMetaCas = jdUserCollectionReader.getJdUserMetaCas();
			}
			if(counter < 100) {
				fail("Not enough CASes: "+counter);
			}
		}
		catch(Exception e) {
			e.printStackTrace();
			fail("Exception");
		}
	}
	
	@Test
	public void test05() {
		try {
			URL url = this.getClass().getResource("/CR100.xml");
			File file = new File(url.getFile());
			String crXml = file.getAbsolutePath();
			debug(crXml);
			String crCfg = null;
			JdUserCollectionReader jdUserCollectionReader = new JdUserCollectionReader(crXml, crCfg);
			int total = jdUserCollectionReader.getTotal();
			assertTrue(total == 100);
			JdUserMetaCas jdUserMetaCas = null;
			int counter = 0;
			for(int i=0; i<total; i++) {
				jdUserMetaCas = jdUserCollectionReader.getJdUserMetaCas();
				assertTrue(jdUserMetaCas != null);
				counter++;
			}
			jdUserMetaCas = jdUserCollectionReader.getJdUserMetaCas();
			assertTrue(jdUserMetaCas == null);
			assertTrue(counter == 100);
		}
		catch(Exception e) {
			e.printStackTrace();
			fail("Exception");
		}
	}
	
}
