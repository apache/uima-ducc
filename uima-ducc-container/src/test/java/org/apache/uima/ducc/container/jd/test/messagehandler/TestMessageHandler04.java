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
import org.apache.uima.ducc.container.jd.test.helper.ThreadInfo;
import org.apache.uima.ducc.container.jd.test.helper.ThreadInfoFactory;
import org.apache.uima.ducc.container.jd.test.helper.Utilities;
import org.apache.uima.ducc.ps.net.iface.IMetaTask;
import org.apache.uima.ducc.ps.net.impl.MetaTaskTransaction;
import org.apache.uima.ducc.user.error.iface.Transformer;
import org.junit.Test;

public class TestMessageHandler04 extends TestMessageHandler {
	
	protected JobDriver jd;
	
    public void setUp() throws JobDriverException {
        initialize();
        jd = JobDriver.getNewInstance();
        JobProcessBlacklist.getInstance().disable();
    }
    
	// multiple node:pid:tid with errors
	
	@Test
	public void test_04() {
		try {
			if(skipAll) {
				return;
			}
			//
			setUp();
			//
			System.setProperty(FlagsHelper.Name.JobId.pname(), "40");
			//
			File working = mkWorkingDir();
			String directory = working.getAbsolutePath();
			System.setProperty(FlagsHelper.Name.JobDirectory.pname(), directory);
			//
			String key = "ducc.process.log.dir";
			String value = directory;
			System.setProperty(key, value);
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
			IMetaTask metaCasPrevious = null;
			MetaTaskTransaction trans = transGet(messageHandler,ti.getNodeName(),ti.getPid(),ti.getTid(),casNo);
			IMetaTask metaCas = trans.getMetaTask();
			assertTrue(metaCas != null);
			int inject = 0;
			int index = 0;
			
			while(metaCas != null) {
				index++;
				System.out.println("index:"+index);
				IOperatingInfo oi = messageHandler.handleGetOperatingInfo();
				System.out.println("kill:"+oi.isKillJob());
				transAck(messageHandler,trans,casNo);
				if(randomErrorTest04()) {
					Exception e = new RuntimeException("injected error test #04");
					Object serializedException = Transformer.serialize(e);
					metaCas.setUserSpaceException(serializedException);
					inject++;
				}
				transEnd(messageHandler,trans,casNo);
				IOperatingInfo oi2 = messageHandler.handleGetOperatingInfo();
				System.out.println("kill2:"+oi2.isKillJob());
				casNo--;
				metaCasPrevious = metaCas;
				assertTrue(metaCasPrevious != null);
				ti = tif.getRandom();
				debug("random:"+ti.toKey());
				trans = transGet(messageHandler,ti.getNodeName(),ti.getPid(),ti.getTid(),casNo);
				metaCas = trans.getMetaTask();
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
	
}
