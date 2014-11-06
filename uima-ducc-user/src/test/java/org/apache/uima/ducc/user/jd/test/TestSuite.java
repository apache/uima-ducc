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
import java.util.Properties;

import org.apache.uima.cas.CAS;
import org.apache.uima.ducc.user.jd.JdUserCollectionReader;
import org.apache.uima.ducc.user.jd.JdUserException;
import org.apache.uima.ducc.user.jd.JdUserMetaCas;
import org.apache.uima.ducc.user.jd.iface.IJdUserErrorHandler;
import org.apache.uima.ducc.user.jd.iface.IJdUserErrorHandler.HandleKey;
import org.apache.uima.ducc.user.jd.iface.JdUserErrorHandler;
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
			JdUserCollectionReader jdcr = new JdUserCollectionReader(crXml, crCfg);
			int total = jdcr.getTotal();
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
			JdUserCollectionReader jdcr = new JdUserCollectionReader(crXml, crCfg);
			int total = jdcr.getTotal();
			assertTrue(total == 100);
			int counter = 0;
			JdUserMetaCas jdUserMetaCas = jdcr.getJdUserMetaCas();
			while(jdUserMetaCas != null) {
				jdUserMetaCas.printMe();
				counter++;
				if(counter > 100) {
					fail("Too many CASes: "+counter);
				}
				jdUserMetaCas = jdcr.getJdUserMetaCas();
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
			JdUserCollectionReader jdcr = new JdUserCollectionReader(crXml, crCfg);
			int total = jdcr.getTotal();
			assertTrue(total == 100);
			JdUserMetaCas jdUserMetaCas = null;
			int counter = 0;
			for(int i=0; i<total; i++) {
				jdUserMetaCas = jdcr.getJdUserMetaCas();
				assertTrue(jdUserMetaCas != null);
				counter++;
			}
			jdUserMetaCas = jdcr.getJdUserMetaCas();
			assertTrue(jdUserMetaCas == null);
			assertTrue(counter == 100);
		}
		catch(Exception e) {
			e.printStackTrace();
			fail("Exception");
		}
	}
	
	@Test
	public void test06() {
		try {
			IJdUserErrorHandler eh = new JdUserErrorHandler();
			CAS cas = null;
			Exception e = null;
			Properties properties = eh.handle(cas, e);
			String key;
			String value;
			key = HandleKey.killJobFlag.name();
			value = properties.getProperty(key);
			assertTrue(value.equalsIgnoreCase("true"));
		}
		catch(Exception e) {
			e.printStackTrace();
			fail("Exception");
		}
	}
	
	@Test
	public void test07() {
		try {
			URL url = this.getClass().getResource("/CR100.xml");
			File file = new File(url.getFile());
			String crXml = file.getAbsolutePath();
			debug(crXml);
			String crCfg = null;
			JdUserCollectionReader jdcr = new JdUserCollectionReader(crXml, crCfg);
			int total = jdcr.getTotal();
			assertTrue(total == 100);
			JdUserMetaCas jdUserMetaCas = null;
			jdUserMetaCas = jdcr.getJdUserMetaCas();
			assertTrue(jdUserMetaCas != null);
			String serializedCas = jdUserMetaCas.getSerializedCas();
			assertTrue(serializedCas != null);
			CAS cas = jdcr.deserialize(serializedCas);
			JdUserErrorHandler eh = new JdUserErrorHandler();
			Exception exception = null;
			Properties properties = eh.handle(cas, exception);
			String key;
			String value;
			key = HandleKey.killJobFlag.name();
			value = properties.getProperty(key);
			assertTrue(value.equalsIgnoreCase("true"));
			key = HandleKey.killJobReason.name();
			value = properties.getProperty(key);
			debug(key+": "+value);
			key = HandleKey.killProcessFlag.name();
			value = properties.getProperty(key);
			assertTrue(value.equalsIgnoreCase("true"));
			key = HandleKey.killProcessReason.name();
			value = properties.getProperty(key);
			debug(key+": "+value);
			jdcr.recycle(cas);
		}
		catch(Exception e) {
			e.printStackTrace();
			fail("Exception");
		}
	}
	
	@Test
	public void test08() {
		try {
			URL url = this.getClass().getResource("/CR100.xml");
			File file = new File(url.getFile());
			String crXml = file.getAbsolutePath();
			debug(crXml);
			String crCfg = null;
			JdUserCollectionReader jdcr = new JdUserCollectionReader(crXml, crCfg);
			int total = jdcr.getTotal();
			assertTrue(total == 100);
			JdUserMetaCas jdUserMetaCas = null;
			jdUserMetaCas = jdcr.getJdUserMetaCas();
			assertTrue(jdUserMetaCas != null);
			String serializedCas = jdUserMetaCas.getSerializedCas();
			assertTrue(serializedCas != null);
			CAS cas = jdcr.deserialize(serializedCas);
			JdUserErrorHandler eh = new JdUserErrorHandler();
			Exception exception = new RuntimeException();
			Properties properties = eh.handle(cas, exception);
			String key;
			String value;
			key = HandleKey.killWorkItemFlag.name();
			value = properties.getProperty(key);
			assertTrue(value.equalsIgnoreCase("true"));
			key = HandleKey.killWorkItemReason.name();
			value = properties.getProperty(key);
			debug(key+": "+value);
			key = HandleKey.killProcessFlag.name();
			value = properties.getProperty(key);
			assertTrue(value.equalsIgnoreCase("true"));
			key = HandleKey.killProcessReason.name();
			value = properties.getProperty(key);
			debug(key+": "+value);
			assertTrue(!properties.containsKey(HandleKey.killJobFlag));
			jdcr.recycle(cas);
		}
		catch(Exception e) {
			e.printStackTrace();
			fail("Exception");
		}
	}
	
	@Test
	public void test09() {
		try {
			int seqNo = 1;
			String serializedCas = "ABC";
			String documentText = "123";
			Exception exception = new RuntimeException("exception text");
			JdUserMetaCas jdUserMetaCas = new JdUserMetaCas(seqNo, serializedCas, documentText, exception);
			assertTrue(seqNo == jdUserMetaCas.getSeqNo());
			assertTrue(serializedCas.equals(jdUserMetaCas.getSerializedCas()));
			assertTrue(documentText.equals(jdUserMetaCas.getDocumentText()));
			assertTrue(exception.equals(jdUserMetaCas.getException()));
			jdUserMetaCas.printMe();
		}
		catch(Exception e) {
			e.printStackTrace();
			fail("Exception");
		}
	}
}
