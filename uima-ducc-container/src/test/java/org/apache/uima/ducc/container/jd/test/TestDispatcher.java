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
import java.util.Random;

import org.apache.uima.ducc.container.jd.JobDriverCommon;
import org.apache.uima.ducc.container.jd.mh.Dispatcher;
import org.apache.uima.ducc.container.jd.mh.iface.IOperatingInfo;
import org.apache.uima.ducc.container.jd.mh.iface.IProcessInfo;
import org.apache.uima.ducc.container.jd.mh.impl.ProcessInfo;
import org.apache.uima.ducc.container.jd.test.helper.ThreadInfo;
import org.apache.uima.ducc.container.jd.test.helper.ThreadInfoFactory;
import org.apache.uima.ducc.container.net.iface.IMetaCas;
import org.apache.uima.ducc.container.net.iface.IMetaCasTransaction.Type;
import org.apache.uima.ducc.container.net.impl.MetaCasTransaction;
import org.apache.uima.ducc.container.net.impl.TransactionId;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestDispatcher {

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
	
	/*
	private void asExpected(Exception e) {
		if(asExpected) {
			String message = "as expected: "+e.getMessage();
			out_println(message);
		}
	}
	*/
	
	private MetaCasTransaction create(String node, int pid, int tid, Type type) {
		MetaCasTransaction mct = new MetaCasTransaction();
		mct.setRequesterName(node);
		mct.setRequesterProcessId(pid);
		mct.setRequesterThreadId(tid);
		mct.setType(type);
		return mct;
	}
	
	private IMetaCas transCommon(Dispatcher dispatcher, MetaCasTransaction trans, int reqNo) {
		dispatcher.handleMetaCasTransation(trans);
		IMetaCas metaCas = trans.getMetaCas();
		if(metaCas != null) {
			if(reqNo > 0) {
				String seqNo = ""+reqNo;
				debug("system key:"+metaCas.getSystemKey());
				assertTrue(metaCas.getSystemKey().equals(seqNo));
				asExpected("system key == "+seqNo);
				debug("user key:"+metaCas.getUserKey());
				assertTrue(metaCas.getUserKey().equals(seqNo));
				asExpected("user key == "+seqNo);
			}
		}
		else {
			debug("metaCas is null");
		}
		return metaCas;
	}
	
	private IMetaCas transGet(Dispatcher dispatcher, String node, int pid, int tid, int reqNo) {
		debug("Get");
		MetaCasTransaction trans = create(node, pid, tid, Type.Get);
		trans.setTransactionId(new TransactionId(reqNo,0));
		return transCommon(dispatcher, trans, reqNo);
	}
	
	private IMetaCas transAck(Dispatcher dispatcher, String node, int pid, int tid, int reqNo) {
		debug("Ack");
		MetaCasTransaction trans = create(node, pid, tid, Type.Ack);
		trans.setTransactionId(new TransactionId(reqNo,1));
		return transCommon(dispatcher, trans, reqNo);
	}
	
	private IMetaCas transEnd(Dispatcher dispatcher, String node, int pid, int tid, int reqNo) {
		debug("End");
		MetaCasTransaction trans = create(node, pid, tid, Type.End);
		trans.setTransactionId(new TransactionId(reqNo,2));
		return transCommon(dispatcher, trans, reqNo);
	}
	
	private String[] jarList260 = { 
			"/ducc-user.jar",
			"/ducc-test.jar",
			"/uimaj-as-core-2.6.0.jar",
			"/uimaj-core-2.6.0.jar",
			"/xstream-1.3.1.jar"
	};
	
	// single node:pid:tid
	
	@Test
	public void test_01() {
		try {
			URL urlXml = this.getClass().getResource("/CR100.xml");
			File file = new File(urlXml.getFile());
			String crXml = file.getAbsolutePath();
			String crCfg = null;
			JobDriverCommon.setInstance(jarList260, crXml, crCfg);
			int size = JobDriverCommon.getInstance().getMap().size();
			debug("map size:"+size);
			Dispatcher dispatcher = new Dispatcher();
			ThreadInfoFactory tif = new ThreadInfoFactory(1,1,1);
			ThreadInfo ti = tif.getRandom();
			String node = ti.getNode();
			int pid = ti.getPid();
			int tid = ti.getTid();
			int casNo = 1;
			IMetaCas metaCasPrevious = null;
			IMetaCas metaCas = transGet(dispatcher,node,pid,tid,casNo);
			while(metaCas != null) {
				transAck(dispatcher,node,pid,tid,casNo);
				transEnd(dispatcher,node,pid,tid,casNo);
				casNo++;
				metaCasPrevious = metaCas;
				metaCas = transGet(dispatcher,node,pid,tid,casNo);
			}
			assertTrue(metaCasPrevious.getSystemKey().equals("100"));
			asExpected("CASes processed count == 100");
			IOperatingInfo oi = dispatcher.handleGetOperatingInfo();
			assertTrue(oi.getWorkItemCrFetches() == 100);
			asExpected("CASes fetched count == 100");
		}
		catch(Exception e) {
			e.printStackTrace();
			fail("Exception");
		}
	}
	
	// multiple node:pid:tid
	
	@Test
	public void test_02() {
		try {
			URL urlXml = this.getClass().getResource("/CR100.xml");
			File file = new File(urlXml.getFile());
			String crXml = file.getAbsolutePath();
			String crCfg = null;
			JobDriverCommon.setInstance(jarList260, crXml, crCfg);
			int size = JobDriverCommon.getInstance().getMap().size();
			debug("map size:"+size);
			Dispatcher dispatcher = new Dispatcher();
			ThreadInfoFactory tif = new ThreadInfoFactory(2,2,2);
			ThreadInfo ti = tif.getRandom();
			debug("random:"+ti.toKey());
			int casNo = 1;
			IMetaCas metaCasPrevious = null;
			IMetaCas metaCas = transGet(dispatcher,ti.getNode(),ti.getPid(),ti.getTid(),casNo);
			assertTrue(metaCas != null);
			while(metaCas != null) {
				transAck(dispatcher,ti.getNode(),ti.getPid(),ti.getTid(),casNo);
				transEnd(dispatcher,ti.getNode(),ti.getPid(),ti.getTid(),casNo);
				casNo++;
				metaCasPrevious = metaCas;
				assertTrue(metaCasPrevious != null);
				ti = tif.getRandom();
				debug("random:"+ti.toKey());
				metaCas = transGet(dispatcher,ti.getNode(),ti.getPid(),ti.getTid(),casNo);
			}
			assertTrue(metaCasPrevious.getSystemKey().equals("100"));
			asExpected("CASes processed count == 100");
			IOperatingInfo oi = dispatcher.handleGetOperatingInfo();
			assertTrue(oi.getWorkItemCrFetches() == 100);
			asExpected("CASes fetched count == 100");
		}
		catch(Exception e) {
			e.printStackTrace();
			fail("Exception");
		}
	}
	
	// multiple node:pid:tid with preemptions
	
	@Test
	public void test_03() {
		try {
			URL urlXml = this.getClass().getResource("/CR100.xml");
			File file = new File(urlXml.getFile());
			String crXml = file.getAbsolutePath();
			String crCfg = null;
			JobDriverCommon.setInstance(jarList260, crXml, crCfg);
			int size = JobDriverCommon.getInstance().getMap().size();
			debug("map size:"+size);
			Dispatcher dispatcher = new Dispatcher();
			ThreadInfoFactory tif = new ThreadInfoFactory(2,2,2);
			ThreadInfo ti = tif.getRandom();
			debug("random:"+ti.toKey());
			int casNo = -1;
			IMetaCas metaCasPrevious = null;
			IMetaCas metaCas = transGet(dispatcher,ti.getNode(),ti.getPid(),ti.getTid(),casNo);
			assertTrue(metaCas != null);
			while(metaCas != null) {
				randomPreempt(dispatcher,ti);
				transAck(dispatcher,ti.getNode(),ti.getPid(),ti.getTid(),casNo);
				randomPreempt(dispatcher,ti);
				transEnd(dispatcher,ti.getNode(),ti.getPid(),ti.getTid(),casNo);
				randomPreempt(dispatcher,ti);
				casNo--;
				metaCasPrevious = metaCas;
				assertTrue(metaCasPrevious != null);
				ti = tif.getRandom();
				debug("random:"+ti.toKey());
				metaCas = transGet(dispatcher,ti.getNode(),ti.getPid(),ti.getTid(),casNo);
			}
			assertTrue(metaCasPrevious.getSystemKey().equals("100"));
			asExpected("CASes processed count == 100");
			IOperatingInfo oi = dispatcher.handleGetOperatingInfo();
			assertTrue(oi.getWorkItemCrFetches() == 100);
			asExpected("CASes fetched count == 100");
			assertTrue(oi.getWorkItemPreemptions() == expectedPremptionsTest03);
			asExpected("CASes preempted count == "+expectedPremptionsTest03);
		}
		catch(Exception e) {
			e.printStackTrace();
			fail("Exception");
		}
	}
	
	private long seedTest03 = 1;
	private Random randomTest03 = new Random(seedTest03);
	private long pctTest03 = 15;
	
	private long expectedPremptionsTest03 = 52;
	
	private void randomPreempt(Dispatcher dispatcher, ThreadInfo ti) {
		int n = randomTest03.nextInt(100);
		if(n < pctTest03) {
			IProcessInfo processInfo = new ProcessInfo(ti.getNode(),ti.getPid());
			dispatcher.handlePreemptProcess(processInfo);
		}
		
	}
}
