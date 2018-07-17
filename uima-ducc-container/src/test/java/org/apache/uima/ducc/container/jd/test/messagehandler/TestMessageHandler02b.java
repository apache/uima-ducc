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
import java.util.ArrayList;

import org.apache.uima.ducc.common.container.FlagsHelper;
import org.apache.uima.ducc.container.jd.JobDriver;
import org.apache.uima.ducc.container.jd.JobDriverException;
import org.apache.uima.ducc.container.jd.blacklist.JobProcessBlacklist;
import org.apache.uima.ducc.container.jd.mh.MessageHandler;
import org.apache.uima.ducc.container.jd.mh.iface.IOperatingInfo;
import org.apache.uima.ducc.container.jd.mh.iface.IWorkItemInfo;
import org.apache.uima.ducc.container.jd.test.helper.ThreadInfo;
import org.apache.uima.ducc.container.jd.test.helper.ThreadInfoFactory;
import org.apache.uima.ducc.container.jd.test.helper.Utilities;
import org.apache.uima.ducc.ps.net.iface.IMetaTask;
import org.apache.uima.ducc.ps.net.impl.MetaTaskTransaction;
import org.junit.Test;

public class TestMessageHandler02b extends TestMessageHandler {
	
	protected JobDriver jd;
	
    public void setUp() throws JobDriverException {
        initialize();
        jd = JobDriver.getNewInstance();
        JobProcessBlacklist.getInstance().disable();
    }
    
	// multiple node:pid:tid
	
	@Test
	public void test_02b() {
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
			enforce = false;
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
			ThreadInfoFactory tif = new ThreadInfoFactory(200,10,1);
			ThreadInfo ti = tif.getUnique();
			debug("random:"+ti.toKey());
			int casNo = 1;
			IMetaTask metaCasPrevious = null;
			MetaTaskTransaction trans = transGet(messageHandler,ti.getNodeName(),ti.getPid(),ti.getTid(),casNo);
			IMetaTask metaCas = trans.getMetaTask();
			assertTrue(metaCas != null);
			IOperatingInfo oi = messageHandler.handleGetOperatingInfo();
			while(oi.getWorkItemCrFetches() < 100) {
				if(metaCas != null) {
					transAck(messageHandler,trans,casNo);
					//transEnd(messageHandler,ti.getNodeName(),ti.getPid(),ti.getTid(),casNo);
					casNo++;
					metaCasPrevious = metaCas;
					assertTrue(metaCasPrevious != null);
				}
				ti = tif.getUnique();
				debug("random:"+ti.toKey());
				trans = transGet(messageHandler,ti.getNodeName(),ti.getPid(),ti.getTid(),casNo);
				metaCas = trans.getMetaTask();
				oi = messageHandler.handleGetOperatingInfo();
				ArrayList<IWorkItemInfo> list = oi.getActiveWorkItemInfo();
				debug("list:"+list.size());
				debug("worker-threads:"+jd.getRemoteWorkerThreadMap().size());
			}
			assertTrue(oi.getWorkItemCrFetches() == 100);
			asExpected("CASes fetched count == 100");
			ArrayList<IWorkItemInfo> list = oi.getActiveWorkItemInfo();
			assertTrue(list.size() == 100);
			asExpected("Operating count == 100");
			enforce = true;
		}
		catch(Exception e) {
			enforce = true;
			e.printStackTrace();
			fail("Exception");
		}
	}
	
}
