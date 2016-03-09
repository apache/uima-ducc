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

import java.util.ArrayList;

import org.apache.uima.ducc.container.jd.JobDriver;
import org.apache.uima.ducc.container.jd.JobDriverException;
import org.apache.uima.ducc.container.jd.cas.CasManager;
import org.apache.uima.ducc.container.jd.cas.CasManagerStats.RetryReason;
import org.apache.uima.ducc.container.jd.mh.RemoteWorkerThread;
import org.apache.uima.ducc.container.jd.mh.iface.IWorkItemInfo;
import org.apache.uima.ducc.container.jd.mh.impl.OperatingInfo;
import org.apache.uima.ducc.container.jd.mh.impl.WorkItemInfo;
import org.apache.uima.ducc.container.net.iface.IMetaCas;
import org.junit.Before;
import org.junit.Test;

public class Test2 extends TestSuite {
	
	protected JobDriver jd;
	
	@Before
    public void setUp() throws JobDriverException {
        initialize();
        jd = JobDriver.getNewInstance();
    }
	
	@Test
	public void test_20() {
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
	public void test_21() {
		String n01 = "node01";
		int p10 = 10;
		int t20 = 20;
		RemoteWorkerThread rwt01A = new RemoteWorkerThread(n01,null,p10+"",p10,t20);
		RemoteWorkerThread rwt01B = new RemoteWorkerThread(n01,null,p10+"",p10,t20);
		assertTrue(rwt01A.equals(rwt01A));
		assertTrue(rwt01B.equals(rwt01B));
		assertTrue(rwt01A.equals(rwt01B));
		assertTrue(rwt01A.compareTo(rwt01A) == 0);
		assertTrue(rwt01B.compareTo(rwt01B) == 0);
		assertTrue(rwt01A.compareTo(rwt01B) == 0);
		String n02 = "node02";
		int p30 = 30;
		int t40 = 40;
		RemoteWorkerThread rwt02A = new RemoteWorkerThread(n02,null,p10+"",p10,t20);
		RemoteWorkerThread rwt01C = new RemoteWorkerThread(n01,null,p30+"",p30,t20);
		RemoteWorkerThread rwt01D = new RemoteWorkerThread(n01,null,p10+"",p10,t40);
		assertTrue(!rwt01A.equals(rwt02A));
		assertTrue(!rwt01A.equals(rwt01C));
		assertTrue(!rwt01A.equals(rwt01D));
		assertTrue(rwt01A.compareTo(rwt02A) != 0);
		assertTrue(rwt01A.compareTo(rwt01C) != 0);
		assertTrue(rwt01A.compareTo(rwt01D) != 0);
		assertTrue(rwt01A.getNodeName().equals(n01));
		assertTrue(rwt01A.getPid() == p10);
		assertTrue(rwt01A.getTid() == t20);
	}
	
	@Test
	public void test_22() {
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
