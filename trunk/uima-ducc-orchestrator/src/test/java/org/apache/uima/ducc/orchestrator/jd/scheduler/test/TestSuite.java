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
package org.apache.uima.ducc.orchestrator.jd.scheduler.test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.net.URL;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.uima.ducc.common.NodeIdentity;
import org.apache.uima.ducc.common.SizeBytes;
import org.apache.uima.ducc.common.head.IDuccHead;
import org.apache.uima.ducc.common.main.DuccService;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.orchestrator.DuccHead;
import org.apache.uima.ducc.orchestrator.ckpt.OrchestratorCheckpoint;
import org.apache.uima.ducc.orchestrator.jd.scheduler.JdHostProperties;
import org.apache.uima.ducc.orchestrator.jd.scheduler.JdReservation;
import org.apache.uima.ducc.orchestrator.jd.scheduler.JdScheduler;
import org.apache.uima.ducc.transport.event.common.DuccSchedulingInfo;
import org.apache.uima.ducc.transport.event.common.DuccWorkMap;
import org.apache.uima.ducc.transport.event.common.DuccWorkReservation;
import org.apache.uima.ducc.transport.event.common.IDuccSchedulingInfo;
import org.apache.uima.ducc.transport.event.common.IDuccState.ReservationState;
import org.apache.uima.ducc.transport.event.common.IDuccWork;
import org.apache.uima.ducc.transport.event.common.IDuccWorkMap;
import org.apache.uima.ducc.transport.event.common.IDuccWorkReservation;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestSuite {
	
	private static DuccLogger logger = null;
	
	@Before
	public void before() {
		ducc_home();
		ducc_logger();
		ducc_config();
	}
	
	@After
	public void after() {
	}
	
	private void ducc_logger() {
		logger = new DuccLogger(TestSuite.class);
		DuccService.setDuccLogger(logger);
	}
	
	private void ducc_config() {
		try {
			DuccService.setDuccLogger(logger);
		}
		catch(Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	private String getResource(String name) {
		String retVal = name;
		try {
			URL urlXml = null;
			File file = null;
			String path = null;
			//
			urlXml = this.getClass().getResource(name);
			file = new File(urlXml.getFile());
			path = file.getAbsolutePath();
			retVal = path;
		}
		catch(Exception e) {
		}
		return retVal;
	}
	
	private void ducc_home() {
		String folder = "/ducc_runtime/resources";
		String value = getResource(folder);
		String key = "DUCC_HOME";
		System.setProperty(key, value);
	}

	private int seed = 0;
	private Random random = new Random(seed);
	
	private long KB = 1024;
	private long MB = 1024*KB;
	private long GB = 1024*MB;
	
	private void rmAssign(IDuccWorkReservation dwr) {
		IDuccSchedulingInfo si = dwr.getSchedulingInfo();
		long size = 15 *GB;
		si.setMemorySizeAllocatedInBytes(size);
	}
	
	private void assign(IDuccWorkMap dwm) {
		for(DuccId reservationId : dwm.getReservationKeySet()) {
			IDuccWork dw = dwm.findDuccWork(reservationId);
			IDuccWorkReservation dwr = (IDuccWorkReservation) dw;
			switch(dwr.getReservationState()) {
			case WaitingForResources:
				TestHelper.assign(dwr);
				// state: WaitingForResources
				rmAssign(dwr);
				dwr.stateChange(ReservationState.Assigned);
				OrchestratorCheckpoint.getInstance().saveState();
				break;
			default:
				break;
			}
		}
	}
	
	private void publication(JdScheduler jdScheduler, IDuccWorkMap dwm) {
		assign(dwm);
		jdScheduler.handle(dwm);
	}
	
	private void randomAssign(IDuccWorkMap dwm) {
		if(random.nextBoolean()) {
			assign(dwm);
		}
	}
	
	private void randomPublication(JdScheduler jdScheduler, IDuccWorkMap dwm) {
		if(random.nextInt(10) < 4) {
			randomAssign(dwm);
			jdScheduler.handle(dwm);
		}
	}
	
	@Test
	public void testJdReservation() {
		try {
			IDuccWorkReservation dwr = new DuccWorkReservation();
			DuccId duccId = new DuccId(1);
			dwr.setDuccId(duccId);
			dwr.setReservationState(ReservationState.Assigned);
			IDuccSchedulingInfo si = new DuccSchedulingInfo();
			dwr.setSchedulingInfo(si);
			si.setMemorySizeAllocatedInBytes(15*SizeBytes.GB);
			//
			SizeBytes reservationSize = new SizeBytes(SizeBytes.Type.GBytes,15);
			SizeBytes sliceSize = new SizeBytes(SizeBytes.Type.MBytes,300);
			JdReservation jdReservation = new JdReservation(dwr,reservationSize,sliceSize);
			assertTrue(jdReservation.getSlicesAvailable() > 0);
		}
		catch(Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	//Test
	public void testJdSchedulerStatic() {
		try {
			IDuccHead dh = DuccHead.getInstance();
			System.out.print(dh.is_ducc_head_virtual_master());
			//
			JdHostProperties jdHostProperties = new JdHostProperties();
			//
			JdScheduler jdScheduler = new JdScheduler();
			jdScheduler.resetAutomanage();
			//
			jdScheduler.handle(null);
			IDuccWorkMap dwm = new DuccWorkMap();
			jdScheduler.handle(dwm);
			assertTrue(jdScheduler.countReservationsTotal() == 0);
			assertTrue(jdScheduler.countReservationsUp() == 0);
			assertTrue(jdScheduler.countSlicesTotal() == 0);
			//
			IDuccWorkReservation dwr = null;
			//
			dwr = TestHelper.getDWR(jdHostProperties);
			dwm.addDuccWork(dwr);
			jdScheduler.handle(dwm);
			assertTrue(jdScheduler.countReservationsTotal() == 1);
			assertTrue(jdScheduler.countReservationsUp() == 0);
			assertTrue(jdScheduler.countSlicesTotal() == 0);
			//
			dwr = TestHelper.getDWR(jdHostProperties);
			rmAssign(dwr);
			dwr.setReservationState(ReservationState.Assigned);
			dwm.addDuccWork(dwr);
			jdScheduler.handle(dwm);
			assertTrue(jdScheduler.countReservationsTotal() == 2);
			assertTrue(jdScheduler.countReservationsUp() == 1);
			assertTrue(jdScheduler.countSlicesTotal() > 0);
			//
			dwr = TestHelper.getDWR(jdHostProperties);
			rmAssign(dwr);
			dwr.setReservationState(ReservationState.Assigned);
			dwm.addDuccWork(dwr);
			jdScheduler.handle(dwm);
			assertTrue(jdScheduler.countReservationsTotal() == 3);
			assertTrue(jdScheduler.countReservationsUp() == 2);
			//
			dwm.removeDuccWork(dwr.getDuccId());
			jdScheduler.handle(dwm);
			assertTrue(jdScheduler.countReservationsTotal() == 2);
			assertTrue(jdScheduler.countReservationsUp() == 1);
			//
			assertTrue(jdScheduler.countSlicesTotal() > 0);
			assertTrue(jdScheduler.countSlicesInuse() == 0);
		}
		catch(Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	//Test
	public void testJdSchedulerDynamic() {
		try {
			DuccId jobId = new DuccId(999);
			//
			JdScheduler jdScheduler = new JdScheduler();
			//
			jdScheduler.handle(null);
			assertTrue(jdScheduler.countReservationsTotal() == 0);
			assertTrue(jdScheduler.countReservationsUp() == 0);
			
			IDuccWorkMap dwm = new DuccWorkMap();
			jdScheduler.handle(dwm);
			//
			publication(jdScheduler, dwm);
			//
			HashMap<DuccId,DuccId> map = new HashMap<DuccId,DuccId>();
			int expectedInuse = 5;
			for(int i=0; i < expectedInuse; i++) {
				DuccId jobIdentity = TestHelper.getJobIdentity();
				DuccId processIdentity = TestHelper.getProcessIdentity();
				logger.info("jdId", jobIdentity, processIdentity);
				map.put(jobIdentity, processIdentity);
				NodeIdentity nodeIdentity = jdScheduler.allocate(jobIdentity, processIdentity);
				assertTrue(nodeIdentity != null);
				randomPublication(jdScheduler, dwm);
			}
			assertTrue(jdScheduler.countSlicesInuse() == expectedInuse);
			assertTrue(jdScheduler.countSlicesTotal() > 0);
			for(Entry<DuccId, DuccId> entry : map.entrySet()) {
				DuccId jobIdentity = entry.getKey();
				DuccId processIdentity = entry.getValue();
				jdScheduler.deallocate(jobIdentity, processIdentity);
				randomPublication(jdScheduler, dwm);
			}
			publication(jdScheduler, dwm);
			assertTrue(jdScheduler.countSlicesInuse() == 0);
			assertTrue(jdScheduler.countSlicesTotal() > 0);
			//
			int allocations = 0;
			while(allocations < 100) {
				DuccId jobIdentity = TestHelper.getJobIdentity();
				DuccId processIdentity = TestHelper.getProcessIdentity();
				map.put(jobIdentity, processIdentity);
				NodeIdentity nodeIdentity = jdScheduler.allocate(jobIdentity, processIdentity);
				if(nodeIdentity == null) {
					publication(jdScheduler, dwm);
					nodeIdentity = jdScheduler.allocate(jobIdentity, processIdentity);
				}
				//assertTrue(nodeIdentity != null);
				randomPublication(jdScheduler, dwm);
				if(nodeIdentity != null) {
					allocations += 1;
					long slicesTotal = jdScheduler.countSlicesTotal();
					long slicesAvailable = jdScheduler.countSlicesAvailable();
					long slicesInuse = jdScheduler.countSlicesInuse();
					if(slicesTotal != (slicesAvailable+slicesInuse)) {
						StringBuffer sb = new StringBuffer();
						sb.append("slicesTotal="+slicesTotal);
						sb.append(" ");
						sb.append("slicesAvailable="+slicesAvailable);
						sb.append(" ");
						sb.append("slicesInuse="+slicesInuse);
						fail(sb.toString());
					}
				}
			}
			for(Entry<DuccId, DuccId> entry : map.entrySet()) {
				DuccId jdId = entry.getKey();
				DuccId jdProcessDuccId = (DuccId) jdId;
				jdScheduler.deallocate(jdProcessDuccId, jobId);
				randomPublication(jdScheduler, dwm);
				long slicesTotal = jdScheduler.countSlicesTotal();
				long slicesAvailable = jdScheduler.countSlicesAvailable();
				long slicesInuse = jdScheduler.countSlicesInuse();
				assertTrue(slicesTotal == (slicesAvailable+slicesInuse));
			}
			publication(jdScheduler, dwm);
			assertTrue(jdScheduler.countSlicesInuse() == 0);
			assertTrue(jdScheduler.countSlicesTotal() > 0);
			assertTrue(jdScheduler.countSlicesAvailable() > 0);
			assertTrue(jdScheduler.countReservationsTotal() == 1);
		}
		catch(Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

}
