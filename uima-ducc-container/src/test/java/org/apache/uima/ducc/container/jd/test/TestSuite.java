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

import org.apache.uima.ducc.common.container.FlagsHelper;
import org.apache.uima.ducc.container.jd.JobDriverException;
import org.apache.uima.ducc.container.jd.cas.CasManager;
import org.apache.uima.ducc.container.jd.cas.CasManagerStats.RetryReason;
import org.apache.uima.ducc.container.jd.classload.ProxyJobDriverCollectionReader;
import org.apache.uima.ducc.container.jd.mh.RemoteWorkerIdentity;
import org.apache.uima.ducc.container.jd.mh.iface.IWorkItemInfo;
import org.apache.uima.ducc.container.jd.mh.impl.OperatingInfo;
import org.apache.uima.ducc.container.jd.mh.impl.WorkItemInfo;
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
	private void config() {
		URL urlXml = this.getClass().getResource("/CR100.xml");
		File file = new File(urlXml.getFile());
		String crXml = file.getAbsolutePath();
		System.setProperty(FlagsHelper.Name.CollectionReaderXml.pname(), crXml);
		String userClasspath = Utilities.getInstance().getUserCP();
		System.setProperty(FlagsHelper.Name.UserClasspath.pname(), userClasspath);
	}
	
	private void testIncludeAll() {
		try {
			config();
			new ProxyJobDriverCollectionReader();
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
		testIncludeAll();
	}
	
	private void testExcludeOne(int skip) {
		try {
			URL urlXml = this.getClass().getResource("/CR100.xml");
			File file = new File(urlXml.getFile());
			String crXml = file.getAbsolutePath();
			System.setProperty(FlagsHelper.Name.CollectionReaderXml.pname(), crXml);
			String userClasspath = Utilities.getInstance().getUserCP();
			String[] classpathParts = userClasspath.split(File.pathSeparator);
			StringBuffer sb = new StringBuffer();
			for(int i=0; i<classpathParts.length; i++) {
				String jar = classpathParts[i];
				if(i == skip) {
					debug(i+" skip: "+jar);
				}
				else {
					debug(i+" use: "+jar);
					sb.append(this.getClass().getResource(jar));
					sb.append(File.pathSeparator);
				}
			}
			try {
				String userPartialClasspath = sb.toString();
				System.setProperty(FlagsHelper.Name.UserClasspath.pname(), userPartialClasspath);
				new ProxyJobDriverCollectionReader();
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
		testExcludeOne(2);
		String userClasspath = Utilities.getInstance().getUserCP();
		String[] cpParts = userClasspath.split(File.pathSeparator);
		for(int i=0; i<cpParts.length; i++) {
			testExcludeOne(i);
		}
	}
	
	private void testNoXml() {
		try {
			String userClasspath = Utilities.getInstance().getUserCP();
			System.setProperty(FlagsHelper.Name.UserClasspath.pname(), userClasspath);
			new ProxyJobDriverCollectionReader();
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
		testNoXml();
	}
	
	private void getTotal() {
		try {
			config();
			ProxyJobDriverCollectionReader pjdcr = new ProxyJobDriverCollectionReader();
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
		getTotal();
	}


	private void getMetaCas() {
		try {
			config();
			ProxyJobDriverCollectionReader pjdcr = new ProxyJobDriverCollectionReader();
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
		getMetaCas();
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
	
	private void getMetaCases(int extra) {
		try {
			config();
			ProxyJobDriverCollectionReader pjdcr = new ProxyJobDriverCollectionReader();
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
		getMetaCases(0);
	}
	
	@Test
	public void test_07() {
		if(isDisabled(this.getClass().getName())) {
			return;
		}
		getMetaCases(10);
	}
	
	@Test
	public void test_10() {
		if(isDisabled(this.getClass().getName())) {
			return;
		}
		try {
			config();
			CasManager cm = new CasManager();
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
		RemoteWorkerIdentity rwi01A = new RemoteWorkerIdentity(n01,null,p10,t20);
		RemoteWorkerIdentity rwi01B = new RemoteWorkerIdentity(n01,null,p10,t20);
		assertTrue(rwi01A.equals(rwi01A));
		assertTrue(rwi01B.equals(rwi01B));
		assertTrue(rwi01A.equals(rwi01B));
		assertTrue(rwi01A.compareTo(rwi01A) == 0);
		assertTrue(rwi01B.compareTo(rwi01B) == 0);
		assertTrue(rwi01A.compareTo(rwi01B) == 0);
		String n02 = "node02";
		int p30 = 30;
		int t40 = 40;
		RemoteWorkerIdentity rwi02A = new RemoteWorkerIdentity(n02,null,p10,t20);
		RemoteWorkerIdentity rwi01C = new RemoteWorkerIdentity(n01,null,p30,t20);
		RemoteWorkerIdentity rwi01D = new RemoteWorkerIdentity(n01,null,p10,t40);
		assertTrue(!rwi01A.equals(rwi02A));
		assertTrue(!rwi01A.equals(rwi01C));
		assertTrue(!rwi01A.equals(rwi01D));
		assertTrue(rwi01A.compareTo(rwi02A) != 0);
		assertTrue(rwi01A.compareTo(rwi01C) != 0);
		assertTrue(rwi01A.compareTo(rwi01D) != 0);
		assertTrue(rwi01A.getNodeName().equals(n01));
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
		oi.setWorkItemJpGets(70);
		assertTrue(oi.getWorkItemJpGets() == 70);
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
		ArrayList<IWorkItemInfo> list = new ArrayList<IWorkItemInfo>();
		IWorkItemInfo wii = new WorkItemInfo();
		wii.setNodeName("node01");
		wii.setPid(1);
		wii.setTid(1);
		wii.setOperatingMillis(9991);
		list.add(wii);
		wii = new WorkItemInfo();
		wii.setNodeName("node02");
		wii.setPid(2);
		wii.setTid(2);
		wii.setOperatingMillis(9992);
		list.add(wii);
		oi.setActiveWorkItemInfo(list);
		list = oi.getActiveWorkItemInfo();
		assertTrue(list.size() == 2);
	}

}
