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

import org.apache.uima.ducc.common.container.FlagsHelper;
import org.apache.uima.ducc.container.jd.JobDriver;
import org.apache.uima.ducc.container.jd.mh.MessageHandler;
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

public class TestMessageHandler extends ATest {

	private MetaCasTransaction create(String node, int pid, int tid, Type type) {
		MetaCasTransaction mct = new MetaCasTransaction();
		mct.setRequesterNodeName(node);
		mct.setRequesterProcessName(pid+"");
		mct.setRequesterProcessId(pid);
		mct.setRequesterThreadId(tid);
		mct.setType(type);
		return mct;
	}
	
	private IMetaCas transCommon(MessageHandler messageHandler, MetaCasTransaction trans, int reqNo) {
		messageHandler.handleMetaCasTransation(trans);
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
	
	private IMetaCas transGet(MessageHandler messageHandler, String node, int pid, int tid, int reqNo) {
		debug("Get");
		MetaCasTransaction trans = create(node, pid, tid, Type.Get);
		trans.setTransactionId(new TransactionId(reqNo,0));
		return transCommon(messageHandler, trans, reqNo);
	}
	
	private IMetaCas transAck(MessageHandler messageHandler, String node, int pid, int tid, int reqNo) {
		debug("Ack");
		MetaCasTransaction trans = create(node, pid, tid, Type.Ack);
		trans.setTransactionId(new TransactionId(reqNo,1));
		return transCommon(messageHandler, trans, reqNo);
	}
	
	private IMetaCas transEnd(MessageHandler messageHandler, String node, int pid, int tid, int reqNo) {
		debug("End");
		MetaCasTransaction trans = create(node, pid, tid, Type.End);
		trans.setTransactionId(new TransactionId(reqNo,2));
		return transCommon(messageHandler, trans, reqNo);
	}
	
	// single node:pid:tid
	
	@Test
	public void test_01() {
		if(isDisabled(this.getClass().getName())) {
			return;
		}
		announce("test_01");
		try {
			URL urlXml = null;
			File file = null;
			String path = null;
			//
			urlXml = this.getClass().getResource("/CR100.xml");
			file = new File(urlXml.getFile());
			path = file.getAbsolutePath();
			System.setProperty(FlagsHelper.Name.CollectionReaderXml.pname(), path);
			//
			urlXml = this.getClass().getResource("/DDSleepDescriptor.xml");
			file = new File(urlXml.getFile());
			path = file.getAbsolutePath();
			System.setProperty(FlagsHelper.Name.JpDd.pname(), path);
			//
			String userClasspath = Utilities.getInstance().getUserCP();
			System.setProperty(FlagsHelper.Name.UserClasspath.pname(), userClasspath);
			JobDriver.createInstance();
			int size = JobDriver.getInstance().getRemoteThreadMap().size();
			debug("map size:"+size);
			MessageHandler messageHandler = new MessageHandler();
			ThreadInfoFactory tif = new ThreadInfoFactory(1,1,1);
			ThreadInfo ti = tif.getRandom();
			String node = ti.getNodeName();
			int pid = ti.getPid();
			int tid = ti.getTid();
			int casNo = 1;
			IMetaCas metaCasPrevious = null;
			IMetaCas metaCas = transGet(messageHandler,node,pid,tid,casNo);
			while(metaCas != null) {
				transAck(messageHandler,node,pid,tid,casNo);
				transEnd(messageHandler,node,pid,tid,casNo);
				casNo++;
				metaCasPrevious = metaCas;
				metaCas = transGet(messageHandler,node,pid,tid,casNo);
			}
			assertTrue(metaCasPrevious.getSystemKey().equals("100"));
			asExpected("CASes processed count == 100");
			IOperatingInfo oi = messageHandler.handleGetOperatingInfo();
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
		announce("test_02a");
		try {
			URL urlXml = null;
			File file = null;
			String path = null;
			//
			urlXml = this.getClass().getResource("/CR100.xml");
			file = new File(urlXml.getFile());
			path = file.getAbsolutePath();
			System.setProperty(FlagsHelper.Name.CollectionReaderXml.pname(), path);
			//
			urlXml = this.getClass().getResource("/DDSleepDescriptor.xml");
			file = new File(urlXml.getFile());
			path = file.getAbsolutePath();
			System.setProperty(FlagsHelper.Name.JpDd.pname(), path);
			//
			String userClasspath = Utilities.getInstance().getUserCP();
			System.setProperty(FlagsHelper.Name.UserClasspath.pname(), userClasspath);
			JobDriver.createInstance();
			int size = JobDriver.getInstance().getRemoteThreadMap().size();
			debug("map size:"+size);
			MessageHandler messageHandler = new MessageHandler();
			ThreadInfoFactory tif = new ThreadInfoFactory(2,2,2);
			ThreadInfo ti = tif.getRandom();
			debug("random:"+ti.toKey());
			int casNo = 1;
			IMetaCas metaCasPrevious = null;
			IMetaCas metaCas = transGet(messageHandler,ti.getNodeName(),ti.getPid(),ti.getTid(),casNo);
			assertTrue(metaCas != null);
			while(metaCas != null) {
				IOperatingInfo oi = messageHandler.handleGetOperatingInfo();
				if(metaCas.getSystemKey().equals("100")) {
					assertTrue(!oi.isWorkItemCrPending());
				}
				else {
					assertTrue(oi.isWorkItemCrPending());
				}
				transAck(messageHandler,ti.getNodeName(),ti.getPid(),ti.getTid(),casNo);
				transEnd(messageHandler,ti.getNodeName(),ti.getPid(),ti.getTid(),casNo);
				casNo++;
				metaCasPrevious = metaCas;
				assertTrue(metaCasPrevious != null);
				ti = tif.getRandom();
				debug("random:"+ti.toKey());
				metaCas = transGet(messageHandler,ti.getNodeName(),ti.getPid(),ti.getTid(),casNo);
			}
			assertTrue(metaCasPrevious.getSystemKey().equals("100"));
			asExpected("CASes processed count == 100");
			IOperatingInfo oi = messageHandler.handleGetOperatingInfo();
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
		announce("test_2b");
		try {
			URL urlXml = null;
			File file = null;
			String path = null;
			//
			urlXml = this.getClass().getResource("/CR100.xml");
			file = new File(urlXml.getFile());
			path = file.getAbsolutePath();
			System.setProperty(FlagsHelper.Name.CollectionReaderXml.pname(), path);
			//
			urlXml = this.getClass().getResource("/DDSleepDescriptor.xml");
			file = new File(urlXml.getFile());
			path = file.getAbsolutePath();
			System.setProperty(FlagsHelper.Name.JpDd.pname(), path);
			//
			String userClasspath = Utilities.getInstance().getUserCP();
			System.setProperty(FlagsHelper.Name.UserClasspath.pname(), userClasspath);
			JobDriver.createInstance();
			int size = JobDriver.getInstance().getRemoteThreadMap().size();
			debug("map size:"+size);
			MessageHandler messageHandler = new MessageHandler();
			ThreadInfoFactory tif = new ThreadInfoFactory(200,10,1);
			ThreadInfo ti = tif.getRandom();
			debug("random:"+ti.toKey());
			int casNo = 1;
			IMetaCas metaCasPrevious = null;
			IMetaCas metaCas = transGet(messageHandler,ti.getNodeName(),ti.getPid(),ti.getTid(),casNo);
			assertTrue(metaCas != null);
			IOperatingInfo oi = messageHandler.handleGetOperatingInfo();
			while(oi.getWorkItemCrFetches() < 100) {
				if(metaCas != null) {
					transAck(messageHandler,ti.getNodeName(),ti.getPid(),ti.getTid(),casNo);
					//transEnd(messageHandler,ti.getNodeName(),ti.getPid(),ti.getTid(),casNo);
					casNo++;
					metaCasPrevious = metaCas;
					assertTrue(metaCasPrevious != null);
				}
				ti = tif.getRandom();
				debug("random:"+ti.toKey());
				metaCas = transGet(messageHandler,ti.getNodeName(),ti.getPid(),ti.getTid(),casNo);
				oi = messageHandler.handleGetOperatingInfo();
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
		announce("test_03");
		try {
			URL urlXml = null;
			File file = null;
			String path = null;
			//
			urlXml = this.getClass().getResource("/CR100.xml");
			file = new File(urlXml.getFile());
			path = file.getAbsolutePath();
			System.setProperty(FlagsHelper.Name.CollectionReaderXml.pname(), path);
			//
			urlXml = this.getClass().getResource("/DDSleepDescriptor.xml");
			file = new File(urlXml.getFile());
			path = file.getAbsolutePath();
			System.setProperty(FlagsHelper.Name.JpDd.pname(), path);
			//
			String userClasspath = Utilities.getInstance().getUserCP();
			System.setProperty(FlagsHelper.Name.UserClasspath.pname(), userClasspath);
			JobDriver.createInstance();
			int size = JobDriver.getInstance().getRemoteThreadMap().size();
			debug("map size:"+size);
			MessageHandler messageHandler = new MessageHandler();
			ThreadInfoFactory tif = new ThreadInfoFactory(2,2,2);
			ThreadInfo ti = tif.getRandom();
			debug("random:"+ti.toKey());
			int casNo = -1;
			IMetaCas metaCasPrevious = null;
			IMetaCas metaCas = transGet(messageHandler,ti.getNodeName(),ti.getPid(),ti.getTid(),casNo);
			assertTrue(metaCas != null);
			while(metaCas != null) {
				randomPreemptTest03(messageHandler,ti);
				transAck(messageHandler,ti.getNodeName(),ti.getPid(),ti.getTid(),casNo);
				randomPreemptTest03(messageHandler,ti);
				try {
					Thread.sleep(20);
				}
				catch(Exception e) {
				}
				messageHandler.handleGetOperatingInfo();
				transEnd(messageHandler,ti.getNodeName(),ti.getPid(),ti.getTid(),casNo);
				randomPreemptTest03(messageHandler,ti);
				casNo--;
				metaCasPrevious = metaCas;
				assertTrue(metaCasPrevious != null);
				ti = tif.getRandom();
				debug("random:"+ti.toKey());
				metaCas = transGet(messageHandler,ti.getNodeName(),ti.getPid(),ti.getTid(),casNo);
			}
			assertTrue(metaCasPrevious.getSystemKey().equals("100"));
			asExpected("CASes processed count == 100");
			IOperatingInfo oi = messageHandler.handleGetOperatingInfo();
			assertTrue(oi.getWorkItemCrFetches() == 100);
			asExpected("CASes fetched count == 100");
			assertTrue(oi.getWorkItemPreemptions() == expectedPremptionsTest03);
			asExpected("CASes preempted count == "+expectedPremptionsTest03);
			try {
				Thread.sleep(50);
			}
			catch(Exception e) {
			}
			messageHandler.handleGetOperatingInfo();
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
	
	private void randomPreemptTest03(MessageHandler messageHandler, ThreadInfo ti) {
		int n = randomTest03.nextInt(100);
		if(n < pctTest03) {
			IProcessInfo processInfo = new ProcessInfo(ti.getNodeName(),null,""+ti.getPid(),ti.getPid());
			messageHandler.handlePreemptProcess(processInfo);
		}
	}
	
	// multiple node:pid:tid with errors
	
	@Test
	public void test_04() {
		if(isDisabled(this.getClass().getName())) {
			return;
		}
		announce("test_04");
		try {
			URL urlXml = null;
			File file = null;
			String path = null;
			//
			urlXml = this.getClass().getResource("/CR100.xml");
			file = new File(urlXml.getFile());
			path = file.getAbsolutePath();
			System.setProperty(FlagsHelper.Name.CollectionReaderXml.pname(), path);
			//
			urlXml = this.getClass().getResource("/DDSleepDescriptor.xml");
			file = new File(urlXml.getFile());
			path = file.getAbsolutePath();
			System.setProperty(FlagsHelper.Name.JpDd.pname(), path);
			//
			String userClasspath = Utilities.getInstance().getUserCP();
			System.setProperty(FlagsHelper.Name.UserClasspath.pname(), userClasspath);
			JobDriver.createInstance();
			int size = JobDriver.getInstance().getRemoteThreadMap().size();
			debug("map size:"+size);
			MessageHandler messageHandler = new MessageHandler();
			ThreadInfoFactory tif = new ThreadInfoFactory(2,2,2);
			ThreadInfo ti = tif.getRandom();
			debug("random:"+ti.toKey());
			int casNo = -1;
			IMetaCas metaCasPrevious = null;
			IMetaCas metaCas = transGet(messageHandler,ti.getNodeName(),ti.getPid(),ti.getTid(),casNo);
			assertTrue(metaCas != null);
			int inject = 0;
			while(metaCas != null) {
				transAck(messageHandler,ti.getNodeName(),ti.getPid(),ti.getTid(),casNo);
				if(randomErrorTest04()) {
					String serializedException = new RuntimeException("injected error test #04").toString();
					metaCas.setUserSpaceException(serializedException);
					inject++;
				}
				transEnd(messageHandler,ti.getNodeName(),ti.getPid(),ti.getTid(),casNo);
				casNo--;
				metaCasPrevious = metaCas;
				assertTrue(metaCasPrevious != null);
				ti = tif.getRandom();
				debug("random:"+ti.toKey());
				metaCas = transGet(messageHandler,ti.getNodeName(),ti.getPid(),ti.getTid(),casNo);
			}
			//assertTrue(metaCasPrevious.getSystemKey().equals("100"));
			//asExpected("CASes processed count == 100");
			IOperatingInfo oi = messageHandler.handleGetOperatingInfo();
			//assertTrue(oi.getWorkItemCrFetches() == 100);
			//asExpected("CASes fetched count == 100");
			long endSuccess = oi.getWorkItemEndSuccesses();
			long endFailure = oi.getWorkItemEndFailures();
			debug("injected errors: "+inject);
			debug("end success: "+endSuccess);
			debug("end failure: "+endFailure);
			assertTrue(endFailure == expectedErrorsTest04);
			asExpected("CASes error count == "+expectedErrorsTest04);
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
	
	private long expectedErrorsTest04 = 16;
	
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
		announce("test_05a");
		try {
			URL urlXml = null;
			File file = null;
			String path = null;
			//
			urlXml = this.getClass().getResource("/CR100.xml");
			file = new File(urlXml.getFile());
			path = file.getAbsolutePath();
			System.setProperty(FlagsHelper.Name.CollectionReaderXml.pname(), path);
			//
			urlXml = this.getClass().getResource("/DDSleepDescriptor.xml");
			file = new File(urlXml.getFile());
			path = file.getAbsolutePath();
			System.setProperty(FlagsHelper.Name.JpDd.pname(), path);
			//
			String userClasspath = Utilities.getInstance().getUserCP();
			System.setProperty(FlagsHelper.Name.UserClasspath.pname(), userClasspath);
			JobDriver.createInstance();
			int size = JobDriver.getInstance().getRemoteThreadMap().size();
			debug("map size:"+size);
			MessageHandler messageHandler = new MessageHandler();
			ThreadInfoFactory tif = new ThreadInfoFactory(2,2,2);
			ThreadInfo ti = tif.getRandom();
			debug("random:"+ti.toKey());
			int casNo = -1;
			IMetaCas metaCasPrevious = null;
			IMetaCas metaCas = transGet(messageHandler,ti.getNodeName(),ti.getPid(),ti.getTid(),casNo);
			assertTrue(metaCas != null);
			int inject = 0;
			while(metaCas != null) {
				transAck(messageHandler,ti.getNodeName(),ti.getPid(),ti.getTid(),casNo);
				if(randomErrorTest05()) {
					String serializedException = new RuntimeException("injected error test #05").toString();
					metaCas.setUserSpaceException(serializedException);
					inject++;
				}
				transEnd(messageHandler,ti.getNodeName(),ti.getPid(),ti.getTid(),casNo);
				casNo--;
				metaCasPrevious = metaCas;
				assertTrue(metaCasPrevious != null);
				ti = tif.getRandom();
				debug("random:"+ti.toKey());
				metaCas = transGet(messageHandler,ti.getNodeName(),ti.getPid(),ti.getTid(),casNo);
			}
			assertTrue(metaCasPrevious.getSystemKey().equals("100"));
			asExpected("CASes processed count == 100");
			IOperatingInfo oi = messageHandler.handleGetOperatingInfo();
			assertTrue(oi.getWorkItemCrFetches() == 100);
			asExpected("CASes fetched count == 100");
			long endSuccess = oi.getWorkItemEndSuccesses();
			long endFailure = oi.getWorkItemEndFailures();
			debug("injected errors: "+inject);
			debug("end success: "+endSuccess);
			debug("end failure: "+endFailure);
			System.out.println(endFailure);
			assertTrue(endFailure == expectedErrorsTest05a);
			asExpected("CASes error count == "+expectedErrorsTest05a);
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
		announce("test_05b");
		try {
			URL urlXml = null;
			File file = null;
			String path = null;
			//
			urlXml = this.getClass().getResource("/CR100.xml");
			file = new File(urlXml.getFile());
			path = file.getAbsolutePath();
			System.setProperty(FlagsHelper.Name.CollectionReaderXml.pname(), path);
			//
			urlXml = this.getClass().getResource("/DDSleepDescriptor.xml");
			file = new File(urlXml.getFile());
			path = file.getAbsolutePath();
			System.setProperty(FlagsHelper.Name.JpDd.pname(), path);
			//
			String userClasspath = Utilities.getInstance().getUserCP();
			System.setProperty(FlagsHelper.Name.UserClasspath.pname(), userClasspath);
			//
			String ehcp = "KillJobLimit="+2;
			System.setProperty(FlagsHelper.Name.UserErrorHandlerCfg.pname(), ehcp);
			//
			JobDriver.createInstance();
			int size = JobDriver.getInstance().getRemoteThreadMap().size();
			debug("map size:"+size);
			MessageHandler messageHandler = new MessageHandler();
			ThreadInfoFactory tif = new ThreadInfoFactory(2,2,2);
			ThreadInfo ti = tif.getRandom();
			debug("random:"+ti.toKey());
			int casNo = -1;
			IMetaCas metaCasPrevious = null;
			IMetaCas metaCas = transGet(messageHandler,ti.getNodeName(),ti.getPid(),ti.getTid(),casNo);
			assertTrue(metaCas != null);
			int inject = 0;
			while(metaCas != null) {
				transAck(messageHandler,ti.getNodeName(),ti.getPid(),ti.getTid(),casNo);
				if(randomErrorTest05()) {
					String serializedException = new RuntimeException("injected error test #05").toString();
					metaCas.setUserSpaceException(serializedException);
					inject++;
				}
				transEnd(messageHandler,ti.getNodeName(),ti.getPid(),ti.getTid(),casNo);
				casNo--;
				metaCasPrevious = metaCas;
				assertTrue(metaCasPrevious != null);
				ti = tif.getRandom();
				debug("random:"+ti.toKey());
				metaCas = transGet(messageHandler,ti.getNodeName(),ti.getPid(),ti.getTid(),casNo);
			}
			IOperatingInfo oi = messageHandler.handleGetOperatingInfo();
			long endSuccess = oi.getWorkItemEndSuccesses();
			long endFailure = oi.getWorkItemEndFailures();
			debug("injected errors: "+inject);
			debug("end success: "+endSuccess);
			debug("end failure: "+endFailure);
			assertTrue(endFailure == expectedErrorsTest05b);
			asExpected("CASes error count == "+expectedErrorsTest05b);
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
	
	private long expectedErrorsTest05a = 7;
	private long expectedErrorsTest05b = 3;
	
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
		announce("test_06");
		try {
			URL urlXml = null;
			File file = null;
			String path = null;
			//
			urlXml = this.getClass().getResource("/CR100.xml");
			file = new File(urlXml.getFile());
			path = file.getAbsolutePath();
			System.setProperty(FlagsHelper.Name.CollectionReaderXml.pname(), path);
			//
			urlXml = this.getClass().getResource("/DDSleepDescriptor.xml");
			file = new File(urlXml.getFile());
			path = file.getAbsolutePath();
			System.setProperty(FlagsHelper.Name.JpDd.pname(), path);
			//
			String userClasspath = Utilities.getInstance().getUserCP();
			System.setProperty(FlagsHelper.Name.UserClasspath.pname(), userClasspath);
			//
			String eh = "org.apache.uima.ducc.user.jd.test.helper.TestJdContainerErrorHandlerRandomRetry";
			System.setProperty(FlagsHelper.Name.UserErrorHandlerClassname.pname(), eh);
			//
			JobDriver.createInstance();
			int size = JobDriver.getInstance().getRemoteThreadMap().size();
			debug("map size:"+size);
			MessageHandler messageHandler = new MessageHandler();
			ThreadInfoFactory tif = new ThreadInfoFactory(2,2,2);
			ThreadInfo ti = tif.getRandom();
			debug("random:"+ti.toKey());
			int casNo = -1;
			IMetaCas metaCasPrevious = null;
			IMetaCas metaCas = transGet(messageHandler,ti.getNodeName(),ti.getPid(),ti.getTid(),casNo);
			assertTrue(metaCas != null);
			int inject = 0;
			while(metaCas != null) {
				transAck(messageHandler,ti.getNodeName(),ti.getPid(),ti.getTid(),casNo);
				if(randomErrorTest06()) {
					String serializedException = new RuntimeException("injected error test #06").toString();
					metaCas.setUserSpaceException(serializedException);
					inject++;
				}
				transEnd(messageHandler,ti.getNodeName(),ti.getPid(),ti.getTid(),casNo);
				casNo--;
				metaCasPrevious = metaCas;
				assertTrue(metaCasPrevious != null);
				ti = tif.getRandom();
				debug("random:"+ti.toKey());
				metaCas = transGet(messageHandler,ti.getNodeName(),ti.getPid(),ti.getTid(),casNo);
			}
			assertTrue(metaCasPrevious.getSystemKey().equals("100"));
			asExpected("CASes processed count == 100");
			IOperatingInfo oi = messageHandler.handleGetOperatingInfo();
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
