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
package org.apache.uima.ducc.container.jd.test.messagehandler;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.net.URL;
import java.util.Random;

import org.apache.uima.ducc.common.container.FlagsHelper;
import org.apache.uima.ducc.container.jd.JobDriver;
import org.apache.uima.ducc.container.jd.JobDriverException;
import org.apache.uima.ducc.container.jd.blacklist.JobProcessBlacklist;
import org.apache.uima.ducc.container.jd.mh.MessageHandler;
import org.apache.uima.ducc.container.jd.mh.iface.IOperatingInfo;
import org.apache.uima.ducc.container.jd.mh.iface.IProcessInfo;
import org.apache.uima.ducc.container.jd.mh.impl.ProcessInfo;
import org.apache.uima.ducc.container.jd.test.helper.ThreadInfo;
import org.apache.uima.ducc.container.jd.test.helper.ThreadInfoFactory;
import org.apache.uima.ducc.container.jd.test.helper.Utilities;
import org.apache.uima.ducc.container.net.iface.IMetaCas;
import org.apache.uima.ducc.container.net.impl.MetaCasTransaction;
import org.junit.Test;

public class TestMessageHandler03 extends TestMessageHandler {
	
	protected JobDriver jd;
	
    public void setUp() throws JobDriverException {
        initialize();
        jd = JobDriver.getNewInstance();
        JobProcessBlacklist.getInstance().disable();
    }
    
	// multiple node:pid:tid with preemptions
	
	@Test
	public void test_03() {
		try {
			if(skipAll) {
				return;
			}
			//
			setUp();
			//
			File working = mkWorkingDir();
			String directory = working.getAbsolutePath();
			System.setProperty(FlagsHelper.Name.JobDirectory.pname(), directory);
			//
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
			int size = jd.getRemoteWorkerThreadMap().size();
			debug("map size:"+size);
			MessageHandler messageHandler = new MessageHandler();
			ThreadInfoFactory tif = new ThreadInfoFactory(2,2,2);
			ThreadInfo ti = tif.getRandom();
			debug("random:"+ti.toKey());
			int casNo = -1;
			IMetaCas metaCasPrevious = null;
			MetaCasTransaction trans = transGet(messageHandler,ti.getNodeName(),ti.getPid(),ti.getTid(),casNo);
			IMetaCas metaCas = trans.getMetaCas();
			assertTrue(metaCas != null);
			while(metaCas != null) {
				randomPreemptTest03(messageHandler,ti);
				transAck(messageHandler,trans,casNo);
				randomPreemptTest03(messageHandler,ti);
				try {
					Thread.sleep(20);
				}
				catch(Exception e) {
				}
				messageHandler.handleGetOperatingInfo();
				transEnd(messageHandler,trans,casNo);
				randomPreemptTest03(messageHandler,ti);
				casNo--;
				metaCasPrevious = metaCas;
				assertTrue(metaCasPrevious != null);
				ti = tif.getRandom();
				debug("random:"+ti.toKey());
				trans = transGet(messageHandler,ti.getNodeName(),ti.getPid(),ti.getTid(),casNo);
				metaCas = trans.getMetaCas();
			}
			assertTrue(metaCasPrevious.getSystemKey().equals("100"));
			asExpected("CASes processed count == 100");
			IOperatingInfo oi = messageHandler.handleGetOperatingInfo();
			assertTrue(oi.getWorkItemCrFetches() == 100);
			asExpected("CASes fetched count == 100");
			assertTrue(oi.getWorkItemPreemptions() > 0);
			asExpected("CASes preempted count == "+oi.getWorkItemPreemptions());
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
	
	private void randomPreemptTest03(MessageHandler messageHandler, ThreadInfo ti) {
		int n = randomTest03.nextInt(100);
		if(n < pctTest03) {
			IProcessInfo processInfo = new ProcessInfo(ti.getNodeName(),null,""+ti.getPid(),ti.getPid());
			messageHandler.handleProcessPreempt(processInfo);
		}
	}

}
