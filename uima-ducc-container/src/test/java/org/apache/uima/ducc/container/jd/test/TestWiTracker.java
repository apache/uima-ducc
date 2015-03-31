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

import java.util.Random;

import org.apache.uima.ducc.container.common.fsm.iface.IFsm;
import org.apache.uima.ducc.container.jd.mh.RemoteWorkerThread;
import org.apache.uima.ducc.container.jd.mh.iface.remote.IRemoteWorkerThread;
import org.apache.uima.ducc.container.jd.wi.IWorkItem;
import org.apache.uima.ducc.container.jd.wi.WiTracker;
import org.apache.uima.ducc.container.jd.wi.WorkItem;
import org.apache.uima.ducc.container.net.iface.IMetaCas;
import org.apache.uima.ducc.container.net.impl.MetaCas;
import org.junit.Test;

public class TestWiTracker {

	private static Random random = new Random();
	
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
		int seqNo = 0;
		//
		seqNo = 1;
		IWorkItem wi01A = getWi(seqNo);
		IRemoteWorkerThread rwt01A = getRemoteWorkerThread();
		tracker.assign(wi01A,rwt01A);
		//
		assertTrue(tracker.getSize() == 1);
		//
		IWorkItem wi01B = getWi(seqNo);
		IRemoteWorkerThread rwt01B = getRemoteWorkerThread();
		tracker.assign(wi01B,rwt01B);
		//
		assertTrue(tracker.getSize() == 1);
		//
		seqNo = 2;
		IWorkItem wi02A = getWi(seqNo);
		IRemoteWorkerThread rwt02A = getRemoteWorkerThread();
		tracker.assign(wi02A,rwt02A);
		//
		assertTrue(tracker.getSize() == 2);
		//
		IWorkItem wi02B = getWi(seqNo);
		IRemoteWorkerThread rwt02B = getRemoteWorkerThread();
		tracker.assign(wi02B,rwt02B);
		//
		assertTrue(tracker.getSize() == 2);
	}

}
