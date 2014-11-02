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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.net.URL;

import org.apache.uima.ducc.container.jd.CasManagerStats.RetryReason;
import org.apache.uima.ducc.container.jd.JobDriverCasManager;
import org.apache.uima.ducc.container.jd.JobDriverException;
import org.apache.uima.ducc.container.jd.classload.JobDriverCollectionReader;
import org.apache.uima.ducc.container.jd.dispatch.RemoteWorkerIdentity;
import org.apache.uima.ducc.container.jd.mh.impl.OperatingInfo;
import org.apache.uima.ducc.container.net.iface.IMetaCas;
import org.apache.uima.ducc.container.net.impl.MetaCas;
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
	private boolean asExpected = true;
	
	private void out_println(String message) {
		System.out.println(message);
	}
	
	private void debug(String message) {
		if(debug) {
			out_println(message);
		}
	}
	
	private void asExpected(String text) {
		if(asExpected) {
			String message = "as expected: "+text;
			out_println(message);
		}
	}
	
	private void asExpected(Exception e) {
		if(asExpected) {
			String message = "as expected: "+e.getMessage();
			out_println(message);
		}
	}
	
	private String[] jarList242 = { 
			"/ducc-test.jar",
			"/ducc-user.jar",
			"/uimaj-as-core-2.4.2.jar",
			"/uimaj-core-2.4.2.jar",
			"/xstream-1.3.1.jar"
	};
	
	private String[] jarList260 = { 
			"/ducc-test.jar",
			"/ducc-user.jar",
			"/uimaj-as-core-2.6.0.jar",
			"/uimaj-core-2.6.0.jar",
			"/xstream-1.3.1.jar"
	};

	private void testIncludeAll(String[] jarList, String crXml, String crCfg) {
		try {
			URL[] classLoaderUrls = new URL[jarList.length];
			int i = 0;
			for(String jar : jarList) {
				classLoaderUrls[i] = this.getClass().getResource(jar);
				i++;
			}
			new JobDriverCollectionReader(classLoaderUrls, crXml, crCfg);
		}
		catch(Exception e) {
			e.printStackTrace();
			fail("Exception");
		}
	}
	
	@Test
	public void test_01() {
		URL urlXml = this.getClass().getResource("/CR100.xml");
		File file = new File(urlXml.getFile());
		String crXml = file.getAbsolutePath();
		String crCfg = null;
		testIncludeAll(jarList242, crXml, crCfg);
	}
	
	@Test
	public void test_02() {
		URL urlXml = this.getClass().getResource("/CR100.xml");
		File file = new File(urlXml.getFile());
		String crXml = file.getAbsolutePath();
		String crCfg = null;
		testIncludeAll(jarList260, crXml, crCfg);
	}
	
	private void testExcludeOne(String[] jarList, String crXml, String crCfg, int skip) {
		try {
			URL[] classLoaderUrls = new URL[jarList.length-1];
			int index = 0;
			for(int i=0; i<jarList.length; i++) {
				String jar = jarList[i];
				if(i == skip) {
					debug(i+" skip: "+jar);
				}
				else {
					debug(i+" use: "+jar);
					classLoaderUrls[index] = this.getClass().getResource(jar);
					index++;
				}
			}
			try {
				new JobDriverCollectionReader(classLoaderUrls, crXml, crCfg);
				fail("Exception missing...?");
			}
			catch(JobDriverException e) {
				asExpected(e);
			}
		}
		catch(Exception e) {
			e.printStackTrace();
			fail("Exception");
		}
	}
	
	@Test
	public void test_03() {
		URL urlXml = this.getClass().getResource("/CR100.xml");
		File file = new File(urlXml.getFile());
		String crXml = file.getAbsolutePath();
		String crCfg = null;
		testExcludeOne(jarList242, crXml, crCfg, 2);
		for(int i=0; i<jarList242.length; i++) {
			testExcludeOne(jarList242, crXml, crCfg, i);
		}
	}
	
	@Test
	public void test_04() {
		URL urlXml = this.getClass().getResource("/CR100.xml");
		File file = new File(urlXml.getFile());
		String crXml = file.getAbsolutePath();
		String crCfg = null;
		for(int i=0; i<jarList260.length; i++) {
			testExcludeOne(jarList260, crXml, crCfg, i);
		}
	}
	
	private void testNoXml(String[] jarList, String crXml, String crCfg) {
		try {
			URL[] classLoaderUrls = new URL[jarList.length];
			int i = 0;
			for(String jar : jarList) {
				classLoaderUrls[i] = this.getClass().getResource(jar);
				i++;
			}
			new JobDriverCollectionReader(classLoaderUrls, crXml, crCfg);
			fail("Exception missing...?");
		}
		catch(JobDriverException e) {
			asExpected(e);
		}
		catch(Exception e) {
			e.printStackTrace();
			fail("Exception");
		}
	}
	
	@Test
	public void test_05() {
		String crXml = null;
		String crCfg = null;
		testNoXml(jarList242, crXml, crCfg);
		testNoXml(jarList260, crXml, crCfg);
	}
	
	private void getTotal(String[] jarList, String crXml, String crCfg) {
		try {
			URL[] classLoaderUrls = new URL[jarList.length];
			int i = 0;
			for(String jar : jarList) {
				classLoaderUrls[i] = this.getClass().getResource(jar);
				i++;
			}
			JobDriverCollectionReader jdui = new JobDriverCollectionReader(classLoaderUrls, crXml, crCfg);
			int total = jdui.getTotal();
			assertTrue(total == 100);
			debug("total: "+total);
		}
		catch(Exception e) {
			e.printStackTrace();
			fail("Exception");
		}
	}
	
	@Test
	public void test_06() {
		URL urlXml = this.getClass().getResource("/CR100.xml");
		File file = new File(urlXml.getFile());
		String crXml = file.getAbsolutePath();
		String crCfg = null;
		getTotal(jarList242, crXml, crCfg);
		getTotal(jarList260, crXml, crCfg);
	}
	
	private void getMetaCas(String[] jarList, String crXml, String crCfg) {
		try {
			URL[] classLoaderUrls = new URL[jarList.length];
			int i = 0;
			for(String jar : jarList) {
				classLoaderUrls[i] = this.getClass().getResource(jar);
				i++;
			}
			JobDriverCollectionReader jdui = new JobDriverCollectionReader(classLoaderUrls, crXml, crCfg);
			MetaCas mc = jdui.getMetaCas();
			int seqNo = mc.getSeqNo();
			asExpected("seqNo = "+seqNo);
			assertTrue(seqNo == 1);
			String serializedCas = (String)mc.getSerializedCas();
			asExpected("cas = "+serializedCas);
			String prefix = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><xmi:XMI xmlns:cas=\"http:///uima/cas.ecore\"";
			assertTrue(serializedCas.startsWith(prefix));
			String documentText = mc.getDocumentText();
			asExpected("documentText = "+documentText);
			assertTrue(documentText.equals("1"));
		}
		catch(Exception e) {
			e.printStackTrace();
			fail("Exception");
		}
	}
	
	@Test
	public void test_07() {
		URL urlXml = this.getClass().getResource("/CR100.xml");
		File file = new File(urlXml.getFile());
		String crXml = file.getAbsolutePath();
		String crCfg = null;
		getMetaCas(jarList242, crXml, crCfg);
		getMetaCas(jarList260, crXml, crCfg);
	}
	
	private void getMetaCases(JobDriverCollectionReader jdui, int total) throws JobDriverException {
		for(int c=1; c <= total; c++) {
			MetaCas mc = jdui.getMetaCas();
			int seqNo = mc.getSeqNo();
			asExpected("seqNo = "+seqNo);
			assertTrue(seqNo == c);
			String serializedCas = (String)mc.getSerializedCas();
			asExpected("cas = "+serializedCas);
			String prefix = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><xmi:XMI xmlns:cas=\"http:///uima/cas.ecore\"";
			assertTrue(serializedCas.startsWith(prefix));
			String documentText = mc.getDocumentText();
			asExpected("documentText = "+documentText);
			assertTrue(documentText.equals(""+c));
		}
	}
	
	private void getMetaCases(String[] jarList, String crXml, String crCfg, int extra) {
		try {
			URL[] classLoaderUrls = new URL[jarList.length];
			int i = 0;
			for(String jar : jarList) {
				classLoaderUrls[i] = this.getClass().getResource(jar);
				i++;
			}
			JobDriverCollectionReader jdui = new JobDriverCollectionReader(classLoaderUrls, crXml, crCfg);
			int total = jdui.getTotal();
			getMetaCases(jdui, total);
			if(extra > 0) {
				for(int j=0; j<extra; j++) {
					MetaCas mc = jdui.getMetaCas();
					assertTrue(mc == null);
				}
			}
		}
		catch(Exception e) {
			e.printStackTrace();
			fail("Exception");
		}
	}	
	
	@Test
	public void test_08() {
		URL urlXml = this.getClass().getResource("/CR100.xml");
		File file = new File(urlXml.getFile());
		String crXml = file.getAbsolutePath();
		String crCfg = null;
		getMetaCases(jarList242, crXml, crCfg, 0);
		getMetaCases(jarList260, crXml, crCfg, 0);
	}
	
	@Test
	public void test_09() {
		URL urlXml = this.getClass().getResource("/CR100.xml");
		File file = new File(urlXml.getFile());
		String crXml = file.getAbsolutePath();
		String crCfg = null;
		getMetaCases(jarList242, crXml, crCfg, 10);
		getMetaCases(jarList260, crXml, crCfg, 10);
	}
	
	@Test
	public void test_10() {
		try {
			URL urlXml = this.getClass().getResource("/CR100.xml");
			File file = new File(urlXml.getFile());
			String crXml = file.getAbsolutePath();
			String crCfg = null;
			String[] jarList = jarList260;
			JobDriverCasManager jdcm = new JobDriverCasManager(jarList, crXml, crCfg);
			int total = jdcm.getCasManagerStats().getCrTotal();
			assertTrue(total == 100);
			IMetaCas metaCas = jdcm.getMetaCas();
			int retrys = 3;
			while(metaCas != null) {
				if(jdcm.getCasManagerStats().getRetryQueuePuts() < retrys) {
					jdcm.putMetaCas(metaCas, RetryReason.ProcessPreempt);
				}
				metaCas = jdcm.getMetaCas();
			}
			int crGets = jdcm.getCasManagerStats().getCrGets();
			debug("crGets:"+crGets);
			assertTrue(crGets == total);
			int rqPuts = jdcm.getCasManagerStats().getRetryQueuePuts();
			debug("rqPuts:"+rqPuts);
			int rqGets = jdcm.getCasManagerStats().getRetryQueueGets();
			debug("rqGets:"+rqGets);
			assertTrue(rqPuts == retrys);
			assertTrue(rqGets == rqPuts);
			asExpected("puts == "+rqPuts);
			asExpected("gets == "+rqGets);
		}
		catch(Exception e) {
			e.printStackTrace();
			fail("Exception");
		}
	}
	
	@Test
	public void test_20() {
		String n01 = "node01";
		int p10 = 10;
		int t20 = 20;
		RemoteWorkerIdentity rwi01A = new RemoteWorkerIdentity(n01,p10,t20);
		RemoteWorkerIdentity rwi01B = new RemoteWorkerIdentity(n01,p10,t20);
		assertTrue(rwi01A.equals(rwi01A));
		assertTrue(rwi01B.equals(rwi01B));
		assertTrue(rwi01A.equals(rwi01B));
		assertTrue(rwi01A.compareTo(rwi01A) == 0);
		assertTrue(rwi01B.compareTo(rwi01B) == 0);
		assertTrue(rwi01A.compareTo(rwi01B) == 0);
		String n02 = "node02";
		int p30 = 30;
		int t40 = 40;
		RemoteWorkerIdentity rwi02A = new RemoteWorkerIdentity(n02,p10,t20);
		RemoteWorkerIdentity rwi01C = new RemoteWorkerIdentity(n01,p30,t20);
		RemoteWorkerIdentity rwi01D = new RemoteWorkerIdentity(n01,p10,t40);
		assertTrue(!rwi01A.equals(rwi02A));
		assertTrue(!rwi01A.equals(rwi01C));
		assertTrue(!rwi01A.equals(rwi01D));
		assertTrue(rwi01A.compareTo(rwi02A) != 0);
		assertTrue(rwi01A.compareTo(rwi01C) != 0);
		assertTrue(rwi01A.compareTo(rwi01D) != 0);
		assertTrue(rwi01A.getNode().equals(n01));
		assertTrue(rwi01A.getPid() == p10);
		assertTrue(rwi01A.getTid() == t20);
	}
	
	@Test
	public void test_30() {
		OperatingInfo oi = new OperatingInfo();
		oi.setWorkItemCrTotal(100);
		assertTrue(oi.getWorkItemCrTotal() == 100);
		oi.setWorkItemCrFetches(50);
		assertTrue(oi.getWorkItemCrFetches() == 50);
		oi.setWorkItemEndFailures(55);
		assertTrue(oi.getWorkItemEndFailures() == 55);
		oi.setWorkItemEndSuccesses(60);
		assertTrue(oi.getWorkItemEndSuccesses() == 60);
		oi.setWorkItemJpAcks(65);
		assertTrue(oi.getWorkItemJpAcks() == 65);
		oi.setWorkItemJpSends(70);
		assertTrue(oi.getWorkItemJpSends() == 70);
		oi.setWorkItemUserProcessingErrorRetries(75);
		assertTrue(oi.getWorkItemUserProcessingErrorRetries() == 75);
		oi.setWorkItemUserProcessingTimeouts(80);
		assertTrue(oi.getWorkItemUserProcessingTimeouts() == 80);
	}

}
