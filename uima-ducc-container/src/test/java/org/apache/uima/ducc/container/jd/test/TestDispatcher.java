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
import java.util.Random;

import org.apache.uima.ducc.container.jd.JobDriver;
import org.apache.uima.ducc.container.jd.config.IJobDriverConfig;
import org.apache.uima.ducc.container.jd.config.JobDriverConfig;
import org.apache.uima.ducc.container.jd.mh.Dispatcher;
import org.apache.uima.ducc.container.jd.mh.iface.IOperatingInfo;
import org.apache.uima.ducc.container.jd.mh.iface.IProcessInfo;
import org.apache.uima.ducc.container.jd.mh.iface.IWorkItemInfo;
import org.apache.uima.ducc.container.jd.mh.impl.ProcessInfo;
import org.apache.uima.ducc.container.jd.test.helper.ThreadInfo;
import org.apache.uima.ducc.container.jd.test.helper.ThreadInfoFactory;
import org.apache.uima.ducc.container.jd.test.helper.Utilities;
import org.apache.uima.ducc.container.net.iface.IMetaCas;
import org.apache.uima.ducc.container.net.iface.IMetaCasTransaction.Type;
import org.apache.uima.ducc.container.net.impl.MetaCasTransaction;
import org.apache.uima.ducc.container.net.impl.TransactionId;
import org.junit.Test;

public class TestDispatcher extends ATest {


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
	
	// single node:pid:tid
	
	@Test
	public void test_01() {
		if(isDisabled(this.getClass().getName())) {
			return;
		}
		try {
			URL urlXml = this.getClass().getResource("/CR100.xml");
			File file = new File(urlXml.getFile());
			String crXml = file.getAbsolutePath();
			String crCfg = null;
			IJobDriverConfig jdCfg = new JobDriverConfig();
			jdCfg.setUserClasspath(Utilities.userCP);
			jdCfg.setCrXml(crXml);
			jdCfg.setCrCfg(crCfg);
			JobDriver.setInstance(jdCfg);
			int size = JobDriver.getInstance().getMap().size();
			debug("map size:"+size);
			Dispatcher dispatcher = new Dispatcher();
			ThreadInfoFactory tif = new ThreadInfoFactory(1,1,1);
			ThreadInfo ti = tif.getRandom();
			String node = ti.getNodeName();
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
	public void test_02a() {
		if(isDisabled(this.getClass().getName())) {
			return;
		}
		try {
			URL urlXml = this.getClass().getResource("/CR100.xml");
			File file = new File(urlXml.getFile());
			String crXml = file.getAbsolutePath();
			String crCfg = null;
			IJobDriverConfig jdCfg = new JobDriverConfig();
			jdCfg.setUserClasspath(Utilities.userCP);
			jdCfg.setCrXml(crXml);
			jdCfg.setCrCfg(crCfg);
			JobDriver.setInstance(jdCfg);
			int size = JobDriver.getInstance().getMap().size();
			debug("map size:"+size);
			Dispatcher dispatcher = new Dispatcher();
			ThreadInfoFactory tif = new ThreadInfoFactory(2,2,2);
			ThreadInfo ti = tif.getRandom();
			debug("random:"+ti.toKey());
			int casNo = 1;
			IMetaCas metaCasPrevious = null;
			IMetaCas metaCas = transGet(dispatcher,ti.getNodeName(),ti.getPid(),ti.getTid(),casNo);
			assertTrue(metaCas != null);
			while(metaCas != null) {
				IOperatingInfo oi = dispatcher.handleGetOperatingInfo();
				if(metaCas.getSystemKey().equals("100")) {
					assertTrue(!oi.isWorkItemCrPending());
				}
				else {
					assertTrue(oi.isWorkItemCrPending());
				}
				transAck(dispatcher,ti.getNodeName(),ti.getPid(),ti.getTid(),casNo);
				transEnd(dispatcher,ti.getNodeName(),ti.getPid(),ti.getTid(),casNo);
				casNo++;
				metaCasPrevious = metaCas;
				assertTrue(metaCasPrevious != null);
				ti = tif.getRandom();
				debug("random:"+ti.toKey());
				metaCas = transGet(dispatcher,ti.getNodeName(),ti.getPid(),ti.getTid(),casNo);
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
	public void test_02b() {
		if(isDisabled(this.getClass().getName())) {
			return;
		}
		try {
			URL urlXml = this.getClass().getResource("/CR100.xml");
			File file = new File(urlXml.getFile());
			String crXml = file.getAbsolutePath();
			String crCfg = null;
			IJobDriverConfig jdCfg = new JobDriverConfig();
			jdCfg.setUserClasspath(Utilities.userCP);
			jdCfg.setCrXml(crXml);
			jdCfg.setCrCfg(crCfg);
			JobDriver.setInstance(jdCfg);
			int size = JobDriver.getInstance().getMap().size();
			debug("map size:"+size);
			Dispatcher dispatcher = new Dispatcher();
			ThreadInfoFactory tif = new ThreadInfoFactory(200,10,1);
			ThreadInfo ti = tif.getRandom();
			debug("random:"+ti.toKey());
			int casNo = 1;
			IMetaCas metaCasPrevious = null;
			IMetaCas metaCas = transGet(dispatcher,ti.getNodeName(),ti.getPid(),ti.getTid(),casNo);
			assertTrue(metaCas != null);
			IOperatingInfo oi = dispatcher.handleGetOperatingInfo();
			while(oi.getWorkItemCrFetches() < 100) {
				if(metaCas != null) {
					transAck(dispatcher,ti.getNodeName(),ti.getPid(),ti.getTid(),casNo);
					//transEnd(dispatcher,ti.getNodeName(),ti.getPid(),ti.getTid(),casNo);
					casNo++;
					metaCasPrevious = metaCas;
					assertTrue(metaCasPrevious != null);
				}
				ti = tif.getRandom();
				debug("random:"+ti.toKey());
				metaCas = transGet(dispatcher,ti.getNodeName(),ti.getPid(),ti.getTid(),casNo);
				oi = dispatcher.handleGetOperatingInfo();
			}
			assertTrue(oi.getWorkItemCrFetches() == 100);
			asExpected("CASes fetched count == 100");
			ArrayList<IWorkItemInfo> list = oi.getActiveWorkItemInfo();
			assertTrue(list.size() == 100);
			asExpected("Operating count == 100");
		}
		catch(Exception e) {
			e.printStackTrace();
			fail("Exception");
		}
	}
	
	// multiple node:pid:tid with preemptions
	
	@Test
	public void test_03() {
		if(isDisabled(this.getClass().getName())) {
			return;
		}
		try {
			URL urlXml = this.getClass().getResource("/CR100.xml");
			File file = new File(urlXml.getFile());
			String crXml = file.getAbsolutePath();
			String crCfg = null;
			IJobDriverConfig jdCfg = new JobDriverConfig();
			jdCfg.setUserClasspath(Utilities.userCP);
			jdCfg.setCrXml(crXml);
			jdCfg.setCrCfg(crCfg);
			JobDriver.setInstance(jdCfg);
			int size = JobDriver.getInstance().getMap().size();
			debug("map size:"+size);
			Dispatcher dispatcher = new Dispatcher();
			ThreadInfoFactory tif = new ThreadInfoFactory(2,2,2);
			ThreadInfo ti = tif.getRandom();
			debug("random:"+ti.toKey());
			int casNo = -1;
			IMetaCas metaCasPrevious = null;
			IMetaCas metaCas = transGet(dispatcher,ti.getNodeName(),ti.getPid(),ti.getTid(),casNo);
			assertTrue(metaCas != null);
			while(metaCas != null) {
				randomPreemptTest03(dispatcher,ti);
				transAck(dispatcher,ti.getNodeName(),ti.getPid(),ti.getTid(),casNo);
				randomPreemptTest03(dispatcher,ti);
				try {
					Thread.sleep(20);
				}
				catch(Exception e) {
				}
				dispatcher.handleGetOperatingInfo();
				transEnd(dispatcher,ti.getNodeName(),ti.getPid(),ti.getTid(),casNo);
				randomPreemptTest03(dispatcher,ti);
				casNo--;
				metaCasPrevious = metaCas;
				assertTrue(metaCasPrevious != null);
				ti = tif.getRandom();
				debug("random:"+ti.toKey());
				metaCas = transGet(dispatcher,ti.getNodeName(),ti.getPid(),ti.getTid(),casNo);
			}
			assertTrue(metaCasPrevious.getSystemKey().equals("100"));
			asExpected("CASes processed count == 100");
			IOperatingInfo oi = dispatcher.handleGetOperatingInfo();
			assertTrue(oi.getWorkItemCrFetches() == 100);
			asExpected("CASes fetched count == 100");
			assertTrue(oi.getWorkItemPreemptions() == expectedPremptionsTest03);
			asExpected("CASes preempted count == "+expectedPremptionsTest03);
			try {
				Thread.sleep(50);
			}
			catch(Exception e) {
			}
			dispatcher.handleGetOperatingInfo();
			long tod = oi.getWorkItemTodMostRecentStart();
			assertTrue(tod >= 0);
			asExpected("Most Recent Start > 0, value="+tod);
		}
		catch(Exception e) {
			e.printStackTrace();
			fail("Exception");
		}
	}
	
	private long seedTest03 = 3;
	private Random randomTest03 = new Random(seedTest03);
	private long pctTest03 = 15;
	
	private long expectedPremptionsTest03 = 32;
	
	private void randomPreemptTest03(Dispatcher dispatcher, ThreadInfo ti) {
		int n = randomTest03.nextInt(100);
		if(n < pctTest03) {
			IProcessInfo processInfo = new ProcessInfo(ti.getNodeName(),null,ti.getPid());
			dispatcher.handlePreemptProcess(processInfo);
		}
	}
	
	// multiple node:pid:tid with errors
	
	@Test
	public void test_04() {
		if(isDisabled(this.getClass().getName())) {
			return;
		}
		try {
			URL urlXml = this.getClass().getResource("/CR100.xml");
			File file = new File(urlXml.getFile());
			String crXml = file.getAbsolutePath();
			String crCfg = null;
			IJobDriverConfig jdCfg = new JobDriverConfig();
			jdCfg.setUserClasspath(Utilities.userCP);
			jdCfg.setCrXml(crXml);
			jdCfg.setCrCfg(crCfg);
			JobDriver.setInstance(jdCfg);
			int size = JobDriver.getInstance().getMap().size();
			debug("map size:"+size);
			Dispatcher dispatcher = new Dispatcher();
			ThreadInfoFactory tif = new ThreadInfoFactory(2,2,2);
			ThreadInfo ti = tif.getRandom();
			debug("random:"+ti.toKey());
			int casNo = -1;
			IMetaCas metaCasPrevious = null;
			IMetaCas metaCas = transGet(dispatcher,ti.getNodeName(),ti.getPid(),ti.getTid(),casNo);
			assertTrue(metaCas != null);
			int inject = 0;
			while(metaCas != null) {
				transAck(dispatcher,ti.getNodeName(),ti.getPid(),ti.getTid(),casNo);
				if(randomErrorTest04()) {
					Exception exception = new RuntimeException("injected error test #04");
					metaCas.setUserSpaceException(exception);
					inject++;
				}
				transEnd(dispatcher,ti.getNodeName(),ti.getPid(),ti.getTid(),casNo);
				casNo--;
				metaCasPrevious = metaCas;
				assertTrue(metaCasPrevious != null);
				ti = tif.getRandom();
				debug("random:"+ti.toKey());
				metaCas = transGet(dispatcher,ti.getNodeName(),ti.getPid(),ti.getTid(),casNo);
			}
			assertTrue(metaCasPrevious.getSystemKey().equals("100"));
			asExpected("CASes processed count == 100");
			IOperatingInfo oi = dispatcher.handleGetOperatingInfo();
			assertTrue(oi.getWorkItemCrFetches() == 100);
			asExpected("CASes fetched count == 100");
			long endSuccess = oi.getWorkItemEndSuccesses();
			long endFailure = oi.getWorkItemEndFailures();
			debug("injected errors: "+inject);
			debug("end success: "+endSuccess);
			debug("end failure: "+endFailure);
			assertTrue(endFailure == expectedErrorsTest04);
			asExpected("CASes error count == "+expectedErrorsTest04);
			assertTrue(endSuccess+endFailure == 100);
			asExpected("CASes failure+success count == 100");
			boolean killJob = oi.isKillJob();
			if(endFailure >= 15) {
				assertTrue(killJob == true);
			}
			else {
				assertTrue(killJob == false);
			}
		}
		catch(Exception e) {
			e.printStackTrace();
			fail("Exception");
		}
	}
	
	private long seedTest04 = 4;
	private Random randomTest04 = new Random(seedTest04);
	private long pctTest04 = 15;
	
	private long expectedErrorsTest04 = 17;
	
	private boolean randomErrorTest04() {
		boolean retVal = false;
		int n = randomTest04.nextInt(100);
		if(n < pctTest04) {
			retVal = true;
		}
		return retVal;
	}
	
	
	// multiple node:pid:tid with errors
	
	@Test
	public void test_05a() {
		if(isDisabled(this.getClass().getName())) {
			return;
		}
		try {
			URL urlXml = this.getClass().getResource("/CR100.xml");
			File file = new File(urlXml.getFile());
			String crXml = file.getAbsolutePath();
			String crCfg = null;
			IJobDriverConfig jdCfg = new JobDriverConfig();
			jdCfg.setUserClasspath(Utilities.userCP);
			jdCfg.setCrXml(crXml);
			jdCfg.setCrCfg(crCfg);
			JobDriver.setInstance(jdCfg);
			int size = JobDriver.getInstance().getMap().size();
			debug("map size:"+size);
			Dispatcher dispatcher = new Dispatcher();
			ThreadInfoFactory tif = new ThreadInfoFactory(2,2,2);
			ThreadInfo ti = tif.getRandom();
			debug("random:"+ti.toKey());
			int casNo = -1;
			IMetaCas metaCasPrevious = null;
			IMetaCas metaCas = transGet(dispatcher,ti.getNodeName(),ti.getPid(),ti.getTid(),casNo);
			assertTrue(metaCas != null);
			int inject = 0;
			while(metaCas != null) {
				transAck(dispatcher,ti.getNodeName(),ti.getPid(),ti.getTid(),casNo);
				if(randomErrorTest05()) {
					Exception exception = new RuntimeException("injected error test #05");
					metaCas.setUserSpaceException(exception);
					inject++;
				}
				transEnd(dispatcher,ti.getNodeName(),ti.getPid(),ti.getTid(),casNo);
				casNo--;
				metaCasPrevious = metaCas;
				assertTrue(metaCasPrevious != null);
				ti = tif.getRandom();
				debug("random:"+ti.toKey());
				metaCas = transGet(dispatcher,ti.getNodeName(),ti.getPid(),ti.getTid(),casNo);
			}
			assertTrue(metaCasPrevious.getSystemKey().equals("100"));
			asExpected("CASes processed count == 100");
			IOperatingInfo oi = dispatcher.handleGetOperatingInfo();
			assertTrue(oi.getWorkItemCrFetches() == 100);
			asExpected("CASes fetched count == 100");
			long endSuccess = oi.getWorkItemEndSuccesses();
			long endFailure = oi.getWorkItemEndFailures();
			debug("injected errors: "+inject);
			debug("end success: "+endSuccess);
			debug("end failure: "+endFailure);
			assertTrue(endFailure == expectedErrorsTest05);
			asExpected("CASes error count == "+expectedErrorsTest05);
			assertTrue(endSuccess+endFailure == 100);
			asExpected("CASes failure+success count == 100");
			boolean killJob = oi.isKillJob();
			assertTrue(killJob == false);
		}
		catch(Exception e) {
			e.printStackTrace();
			fail("Exception");
		}
	}
	
	@Test
	public void test_05b() {
		if(isDisabled(this.getClass().getName())) {
			return;
		}
		try {
			URL urlXml = this.getClass().getResource("/CR100.xml");
			File file = new File(urlXml.getFile());
			String crXml = file.getAbsolutePath();
			String crCfg = null;
			IJobDriverConfig jdCfg = new JobDriverConfig();
			jdCfg.setUserClasspath(Utilities.userCP);
			jdCfg.setCrXml(crXml);
			jdCfg.setCrCfg(crCfg);
			//
			String ehcp = "KillJobLimit="+2;
			jdCfg.setErrorHandlerConfigurationParameters(ehcp);
			//
			JobDriver.setInstance(jdCfg);
			int size = JobDriver.getInstance().getMap().size();
			debug("map size:"+size);
			Dispatcher dispatcher = new Dispatcher();
			ThreadInfoFactory tif = new ThreadInfoFactory(2,2,2);
			ThreadInfo ti = tif.getRandom();
			debug("random:"+ti.toKey());
			int casNo = -1;
			IMetaCas metaCasPrevious = null;
			IMetaCas metaCas = transGet(dispatcher,ti.getNodeName(),ti.getPid(),ti.getTid(),casNo);
			assertTrue(metaCas != null);
			int inject = 0;
			while(metaCas != null) {
				transAck(dispatcher,ti.getNodeName(),ti.getPid(),ti.getTid(),casNo);
				if(randomErrorTest05()) {
					Exception exception = new RuntimeException("injected error test #05");
					metaCas.setUserSpaceException(exception);
					inject++;
				}
				transEnd(dispatcher,ti.getNodeName(),ti.getPid(),ti.getTid(),casNo);
				casNo--;
				metaCasPrevious = metaCas;
				assertTrue(metaCasPrevious != null);
				ti = tif.getRandom();
				debug("random:"+ti.toKey());
				metaCas = transGet(dispatcher,ti.getNodeName(),ti.getPid(),ti.getTid(),casNo);
			}
			assertTrue(metaCasPrevious.getSystemKey().equals("100"));
			asExpected("CASes processed count == 100");
			IOperatingInfo oi = dispatcher.handleGetOperatingInfo();
			assertTrue(oi.getWorkItemCrFetches() == 100);
			asExpected("CASes fetched count == 100");
			long endSuccess = oi.getWorkItemEndSuccesses();
			long endFailure = oi.getWorkItemEndFailures();
			debug("injected errors: "+inject);
			debug("end success: "+endSuccess);
			debug("end failure: "+endFailure);
			assertTrue(endFailure == expectedErrorsTest05);
			asExpected("CASes error count == "+expectedErrorsTest05);
			assertTrue(endSuccess+endFailure == 100);
			asExpected("CASes failure+success count == 100");
			boolean killJob = oi.isKillJob();
			assertTrue(killJob == true);
		}
		catch(Exception e) {
			e.printStackTrace();
			fail("Exception");
		}
	}
	
	private long seedTest05 = 5;
	private Random randomTest05 = new Random(seedTest05);
	private long pctTest05 = 5;
	
	private long expectedErrorsTest05 = 7;
	
	private boolean randomErrorTest05() {
		boolean retVal = false;
		int n = randomTest05.nextInt(100);
		if(n < pctTest05) {
			retVal = true;
		}
		return retVal;
	}
	
	// multiple node:pid:tid with errors & retrys
	
	@Test
	public void test_06() {
		if(isDisabled(this.getClass().getName())) {
			return;
		}
		try {
			URL urlXml = this.getClass().getResource("/CR100.xml");
			File file = new File(urlXml.getFile());
			String crXml = file.getAbsolutePath();
			String crCfg = null;
			IJobDriverConfig jdCfg = new JobDriverConfig();
			jdCfg.setUserClasspath(Utilities.userCP);
			jdCfg.setCrXml(crXml);
			jdCfg.setCrCfg(crCfg);
			String eh = "org.apache.uima.ducc.user.jd.test.helper.TestJdContainerErrorHandlerRandomRetry";
			jdCfg.setErrorHandlerClassName(eh);
			JobDriver.setInstance(jdCfg);
			int size = JobDriver.getInstance().getMap().size();
			debug("map size:"+size);
			Dispatcher dispatcher = new Dispatcher();
			ThreadInfoFactory tif = new ThreadInfoFactory(2,2,2);
			ThreadInfo ti = tif.getRandom();
			debug("random:"+ti.toKey());
			int casNo = -1;
			IMetaCas metaCasPrevious = null;
			IMetaCas metaCas = transGet(dispatcher,ti.getNodeName(),ti.getPid(),ti.getTid(),casNo);
			assertTrue(metaCas != null);
			int inject = 0;
			while(metaCas != null) {
				transAck(dispatcher,ti.getNodeName(),ti.getPid(),ti.getTid(),casNo);
				if(randomErrorTest06()) {
					Exception exception = new RuntimeException("injected error test #06");
					metaCas.setUserSpaceException(exception);
					inject++;
				}
				transEnd(dispatcher,ti.getNodeName(),ti.getPid(),ti.getTid(),casNo);
				casNo--;
				metaCasPrevious = metaCas;
				assertTrue(metaCasPrevious != null);
				ti = tif.getRandom();
				debug("random:"+ti.toKey());
				metaCas = transGet(dispatcher,ti.getNodeName(),ti.getPid(),ti.getTid(),casNo);
			}
			assertTrue(metaCasPrevious.getSystemKey().equals("100"));
			asExpected("CASes processed count == 100");
			IOperatingInfo oi = dispatcher.handleGetOperatingInfo();
			assertTrue(oi.getWorkItemCrFetches() == 100);
			asExpected("CASes fetched count == 100");
			long endSuccess = oi.getWorkItemEndSuccesses();
			long endFailure = oi.getWorkItemEndFailures();
			long endRetry = oi.getWorkItemEndRetrys();
			debug("injected errors: "+inject);
			debug("end success: "+endSuccess);
			debug("end failure: "+endFailure);
			debug("end retry: "+endRetry);
			assertTrue(endSuccess+endFailure == 100);
			asExpected("CASes failure+success count == 100");
			assertTrue(endRetry > 0);
			asExpected("CASes retry count == "+endRetry);
		}
		catch(Exception e) {
			e.printStackTrace();
			fail("Exception");
		}
	}
	
	private long seedTest06 = 6;
	private Random randomTest06 = new Random(seedTest06);
	private long pctTest06 = 15;
	
	private boolean randomErrorTest06() {
		boolean retVal = false;
		int n = randomTest06.nextInt(100);
		if(n < pctTest06) {
			retVal = true;
		}
		return retVal;
	}
}
