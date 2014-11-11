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
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.uima.ducc.container.jd.JobDriverException;
import org.apache.uima.ducc.container.jd.cas.CasManager;
import org.apache.uima.ducc.container.jd.cas.CasManagerStats.RetryReason;
import org.apache.uima.ducc.container.jd.classload.ProxyJobDriverCollectionReader;
import org.apache.uima.ducc.container.jd.mh.RemoteWorkerIdentity;
import org.apache.uima.ducc.container.jd.mh.impl.OperatingInfo;
import org.apache.uima.ducc.container.jd.test.helper.Utilities;
import org.apache.uima.ducc.container.net.iface.IMetaCas;
import org.apache.uima.ducc.container.net.impl.MetaCas;
import org.junit.Test;

public class TestSuite extends ATest {
	
	String prefix1 = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><xmi:XMI xmlns:tcas=\"http:///uima/tcas.ecore\"";
	String prefix0 = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><xmi:XMI xmlns:cas=\"http:///uima/cas.ecore\"";
	 
	private void checkCas(String cas) {
		assertTrue(cas.startsWith(prefix0) || cas.startsWith(prefix1));
	}
	
	private void testIncludeAll(String[] userCP, String crXml, String crCfg) {
		try {
			URL[] classLoaderUrls = new URL[userCP.length];
			int i = 0;
			for(String jar : userCP) {
				classLoaderUrls[i] = this.getClass().getResource(jar);
				i++;
			}
			new ProxyJobDriverCollectionReader(classLoaderUrls, crXml, crCfg);
		}
		catch(Exception e) {
			e.printStackTrace();
			fail("Exception");
		}
	}
	
	@Test
	public void test_01() {
		if(isDisabled(this.getClass().getName())) {
			return;
		}
		URL urlXml = this.getClass().getResource("/CR100.xml");
		File file = new File(urlXml.getFile());
		String crXml = file.getAbsolutePath();
		String crCfg = null;
		testIncludeAll(Utilities.userCP, crXml, crCfg);
	}
	
	private void testExcludeOne(String[] userCP, String crXml, String crCfg, int skip) {
		try {
			URL[] classLoaderUrls = new URL[userCP.length-1];
			int index = 0;
			for(int i=0; i<userCP.length; i++) {
				String jar = userCP[i];
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
				new ProxyJobDriverCollectionReader(classLoaderUrls, crXml, crCfg);
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
	public void test_02() {
		if(isDisabled(this.getClass().getName())) {
			return;
		}
		URL urlXml = this.getClass().getResource("/CR100.xml");
		File file = new File(urlXml.getFile());
		String crXml = file.getAbsolutePath();
		String crCfg = null;
		testExcludeOne(Utilities.userCP, crXml, crCfg, 2);
		for(int i=0; i<Utilities.userCP.length; i++) {
			testExcludeOne(Utilities.userCP, crXml, crCfg, i);
		}
	}
	
	private void testNoXml(String[] userCP, String crXml, String crCfg) {
		try {
			URL[] classLoaderUrls = new URL[userCP.length];
			int i = 0;
			for(String jar : userCP) {
				classLoaderUrls[i] = this.getClass().getResource(jar);
				i++;
			}
			new ProxyJobDriverCollectionReader(classLoaderUrls, crXml, crCfg);
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
	public void test_03() {
		if(isDisabled(this.getClass().getName())) {
			return;
		}
		String crXml = null;
		String crCfg = null;
		testNoXml(Utilities.userCP, crXml, crCfg);
	}
	
	private void getTotal(String[] userCP, String crXml, String crCfg) {
		try {
			URL[] classLoaderUrls = new URL[userCP.length];
			int i = 0;
			for(String jar : userCP) {
				classLoaderUrls[i] = this.getClass().getResource(jar);
				i++;
			}
			ProxyJobDriverCollectionReader pjdcr = new ProxyJobDriverCollectionReader(classLoaderUrls, crXml, crCfg);
			int total = pjdcr.getTotal();
			assertTrue(total == 100);
			debug("total: "+total);
		}
		catch(Exception e) {
			e.printStackTrace();
			fail("Exception");
		}
	}
	
	@Test
	public void test_04() {
		if(isDisabled(this.getClass().getName())) {
			return;
		}
		URL urlXml = this.getClass().getResource("/CR100.xml");
		File file = new File(urlXml.getFile());
		String crXml = file.getAbsolutePath();
		String crCfg = null;
		getTotal(Utilities.userCP, crXml, crCfg);
	}
	
	private void getMetaCas(String[] userCP, String crXml, String crCfg) {
		try {
			URL[] classLoaderUrls = new URL[userCP.length];
			int i = 0;
			for(String jar : userCP) {
				classLoaderUrls[i] = this.getClass().getResource(jar);
				i++;
			}
			ProxyJobDriverCollectionReader pjdcr = new ProxyJobDriverCollectionReader(classLoaderUrls, crXml, crCfg);
			MetaCas mc = pjdcr.getMetaCas();
			int seqNo = mc.getSeqNo();
			asExpected("seqNo = "+seqNo);
			assertTrue(seqNo == 1);
			String serializedCas = (String)mc.getSerializedCas();
			checkCas(serializedCas);
			asExpected("cas = "+serializedCas);
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
	public void test_05() {
		if(isDisabled(this.getClass().getName())) {
			return;
		}
		URL urlXml = this.getClass().getResource("/CR100.xml");
		File file = new File(urlXml.getFile());
		String crXml = file.getAbsolutePath();
		String crCfg = null;
		getMetaCas(Utilities.userCP, crXml, crCfg);
	}
	
	private void getMetaCases(ProxyJobDriverCollectionReader pjdcr, int total) throws JobDriverException {
		for(int c=1; c <= total; c++) {
			MetaCas mc = pjdcr.getMetaCas();
			int seqNo = mc.getSeqNo();
			asExpected("seqNo = "+seqNo);
			assertTrue(seqNo == c);
			String serializedCas = (String)mc.getSerializedCas();
			checkCas(serializedCas);
			asExpected("cas = "+serializedCas);
			String documentText = mc.getDocumentText();
			asExpected("documentText = "+documentText);
			assertTrue(documentText.equals(""+c));
		}
	}
	
	private void getMetaCases(String[] userCP, String crXml, String crCfg, int extra) {
		try {
			URL[] classLoaderUrls = new URL[userCP.length];
			int i = 0;
			for(String jar : userCP) {
				classLoaderUrls[i] = this.getClass().getResource(jar);
				i++;
			}
			ProxyJobDriverCollectionReader pjdcr = new ProxyJobDriverCollectionReader(classLoaderUrls, crXml, crCfg);
			int total = pjdcr.getTotal();
			getMetaCases(pjdcr, total);
			if(extra > 0) {
				for(int j=0; j<extra; j++) {
					MetaCas mc = pjdcr.getMetaCas();
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
	public void test_06() {
		if(isDisabled(this.getClass().getName())) {
			return;
		}
		URL urlXml = this.getClass().getResource("/CR100.xml");
		File file = new File(urlXml.getFile());
		String crXml = file.getAbsolutePath();
		String crCfg = null;
		getMetaCases(Utilities.userCP, crXml, crCfg, 0);
	}
	
	@Test
	public void test_07() {
		if(isDisabled(this.getClass().getName())) {
			return;
		}
		URL urlXml = this.getClass().getResource("/CR100.xml");
		File file = new File(urlXml.getFile());
		String crXml = file.getAbsolutePath();
		String crCfg = null;
		getMetaCases(Utilities.userCP, crXml, crCfg, 10);
	}
	
	@Test
	public void test_10() {
		if(isDisabled(this.getClass().getName())) {
			return;
		}
		try {
			URL urlXml = this.getClass().getResource("/CR100.xml");
			File file = new File(urlXml.getFile());
			String crXml = file.getAbsolutePath();
			String crCfg = null;
			CasManager cm = new CasManager(Utilities.userCP, crXml, crCfg);
			int total = cm.getCasManagerStats().getCrTotal();
			assertTrue(total == 100);
			IMetaCas metaCas = cm.getMetaCas();
			int retrys = 3;
			while(metaCas != null) {
				if(cm.getCasManagerStats().getRetryQueuePuts() < retrys) {
					cm.putMetaCas(metaCas, RetryReason.ProcessPreempt);
				}
				metaCas = cm.getMetaCas();
			}
			int crGets = cm.getCasManagerStats().getCrGets();
			debug("crGets:"+crGets);
			assertTrue(crGets == total);
			int rqPuts = cm.getCasManagerStats().getRetryQueuePuts();
			debug("rqPuts:"+rqPuts);
			int rqGets = cm.getCasManagerStats().getRetryQueueGets();
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
		if(isDisabled(this.getClass().getName())) {
			return;
		}
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
		if(isDisabled(this.getClass().getName())) {
			return;
		}
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
		oi.setWorkItemFinishedMillisMin(1000);
		assertTrue(oi.getWorkItemFinishedMillisMin() == 1000);
		oi.setWorkItemFinishedMillisMax(2000);
		assertTrue(oi.getWorkItemFinishedMillisMax() == 2000);
		oi.setWorkItemFinishedMillisAvg(1500);
		assertTrue(oi.getWorkItemFinishedMillisAvg() == 1500);
		oi.setWorkItemRunningMillisMin(1001);
		assertTrue(oi.getWorkItemRunningMillisMin() == 1001);
		oi.setWorkItemRunningMillisMax(2001);
		assertTrue(oi.getWorkItemRunningMillisMax() == 2001);
		ArrayList<String> pids01 = new ArrayList<String>();
		pids01.add("011");
		pids01.add("012");
		HashMap<String,ArrayList<String>> map = new HashMap<String,ArrayList<String>>();
		String mapKey = "node01";
		map.put("node01", pids01);
		oi.setMapOperating(map);
		HashMap<String,ArrayList<String>> mapOperating = oi.getMapOperating();
		assertTrue(mapOperating.size() == 1);
		ArrayList<String> list = mapOperating.get(mapKey);
		assertTrue(list.size() == 2);
	}

}
