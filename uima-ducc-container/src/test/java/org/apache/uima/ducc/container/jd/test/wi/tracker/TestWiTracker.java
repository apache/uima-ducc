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
package org.apache.uima.ducc.container.jd.test.wi.tracker;

import static org.junit.Assert.assertTrue;

import java.util.Random;

import org.apache.uima.ducc.container.common.fsm.iface.IFsm;
import org.apache.uima.ducc.container.jd.JobDriver;
import org.apache.uima.ducc.container.jd.JobDriverException;
import org.apache.uima.ducc.container.jd.mh.RemoteWorkerThread;
import org.apache.uima.ducc.container.jd.mh.iface.remote.IRemoteWorkerThread;
import org.apache.uima.ducc.container.jd.test.TestBase;
import org.apache.uima.ducc.container.jd.wi.IWorkItem;
import org.apache.uima.ducc.container.jd.wi.WiTracker;
import org.apache.uima.ducc.container.jd.wi.WorkItem;
import org.apache.uima.ducc.container.net.iface.IMetaCas;
import org.apache.uima.ducc.container.net.impl.MetaCas;
import org.junit.Before;
import org.junit.Test;

public class TestWiTracker extends TestBase  {

	private static Random random = new Random();
	
	protected JobDriver jd;
	
	@Before
    public void setUp() throws JobDriverException {
        initialize();
        jd = JobDriver.getNewInstance();
    }
	
	private IFsm getFsm() {
		IFsm fsm = null;
		return fsm;
	}
	
	private IMetaCas getMetaCas(int seqNo) {
		IMetaCas metaCas = null;
		String text = ""+seqNo;
		Object cas = null;
		metaCas = new MetaCas(seqNo, text, cas);
		return metaCas;
	}
	
	private IWorkItem getWi(int seqNo) {
		IWorkItem wi = null;
		IMetaCas metaCas = getMetaCas(seqNo);
		IFsm fsm = getFsm();
		wi = new WorkItem(metaCas, fsm);
		return wi;
	}
	
	private IRemoteWorkerThread getRemoteWorkerThread() {
		IRemoteWorkerThread rwt = null;
		int node = random.nextInt(900)+100;
		String nodeName = "node:"+node;
		String nodeAddress = "address:"+node;
		int pid = random.nextInt(30000)+10000;
		String pidName = "pid:"+pid;
		int tid = random.nextInt(90)+10;
		rwt = new RemoteWorkerThread(nodeName, nodeAddress, pidName, pid, tid);
		return rwt;
	}
	
	@Test
	public void test() {
		
		WiTracker tracker = WiTracker.getInstance();
		int seqNo = 1;
		//
		IRemoteWorkerThread rwt01A = getRemoteWorkerThread();
		IWorkItem wi01A = tracker.assign(rwt01A);
		IMetaCas metaCas = getMetaCas(1);
		wi01A.setMetaCas(metaCas);
		//
		assertTrue(tracker.getSize() == 1);
		//
		IRemoteWorkerThread rwt01B = rwt01A;
		tracker.assign(rwt01B);
		//
		assertTrue(tracker.getSize() == 1);
		//
		seqNo = 2;
		IWorkItem wi02A = getWi(seqNo);
		IRemoteWorkerThread rwt02A = getRemoteWorkerThread();
		tracker.assign(rwt02A);
		wi02A.setMetaCas(metaCas);
		//
		assertTrue(tracker.getSize() == 2);
		//
		IRemoteWorkerThread rwt02B = rwt02A;
		tracker.assign(rwt02B);
		//
		assertTrue(tracker.getSize() == 2);
	}

}
