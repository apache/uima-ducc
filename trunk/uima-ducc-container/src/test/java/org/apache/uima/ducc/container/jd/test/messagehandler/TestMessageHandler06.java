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
import org.apache.uima.ducc.container.net.iface.IMetaCas;
import org.apache.uima.ducc.container.net.impl.MetaCasTransaction;
import org.apache.uima.ducc.user.error.iface.Transformer;
import org.junit.Test;

public class TestMessageHandler06 extends TestMessageHandler {
	
	protected JobDriver jd;
	
    public void setUp() throws JobDriverException {
        initialize();
        jd = JobDriver.getNewInstance();
        JobProcessBlacklist.getInstance().disable();
    }
    
	// multiple node:pid:tid with errors & retrys
	
	@Test
	public void test_06() {
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
			//
			String eh = "org.apache.uima.ducc.user.jd.test.helper.TestJdContainerErrorHandlerRandomRetry";
			System.setProperty(FlagsHelper.Name.UserErrorHandlerClassname.pname(), eh);
			//
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
			int inject = 0;
			int index = 0;
			while(metaCas != null) {
				index++;
				
				transAck(messageHandler,trans,casNo);
				if(randomErrorTest06()) {
					Exception e = new RuntimeException("injected error test #06");
					Object serializedException = Transformer.serialize(e);
					metaCas.setUserSpaceException(serializedException);
					inject++;
				}
				System.out.println("index:"+index+" "+"error:"+inject);
				transEnd(messageHandler,trans,casNo);
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
	private long pctTest06 = 10;
	
	private boolean randomErrorTest06() {
		boolean retVal = false;
		int n = randomTest06.nextInt(100);
		if(n < pctTest06) {
			retVal = true;
		}
		return retVal;
	}

}
