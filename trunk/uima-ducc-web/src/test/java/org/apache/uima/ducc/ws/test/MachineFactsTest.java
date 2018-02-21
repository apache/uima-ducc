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
package org.apache.uima.ducc.ws.test;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.uima.ducc.ws.cli.json.MachineFacts;
import org.apache.uima.ducc.ws.cli.json.MachineFactsList;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.gson.Gson;

public class MachineFactsTest {
	
	private Random random = new Random();
	
	private String[] statusSet = { "defined", "down", "up" };
	private String[] memorySet = { "", "24", "31", "39", "43", "47", "79" };
	
	private String createStatus() {
		int index = random.nextInt(statusSet.length);
		String status = statusSet[index];
		return status;
	}
	
	private String createIP() {
		String ip = "";
		int type = random.nextInt(10);
		if(type > 2) {
			int index = random.nextInt(256);
			ip = "192.168.5."+index;
		}
		return ip;
	}
	
	private String createName() {
		String name = "node1"+random.nextInt(801);
		return name;
	}
	
	private String createMemory() {
		int index = random.nextInt(memorySet.length);
		String memory = memorySet[index];
		return memory;
	}
	
	private String createSwap() {
		String swap = "";
		int type = random.nextInt(10);
		if(type > 2) {
			int index = random.nextInt(10);
			swap = ""+index;
		}
		return swap;
	}
	
	private String createFree() {
		String free = "";
		int type = random.nextInt(10);
		if(type > 2) {
			int index = random.nextInt(10);
			free = ""+index;
		}
		return free;
	}
	
	private double createCPU() {
		return random.nextDouble();
	}
	
	private boolean createCgroupsEnabled() {
		return random.nextBoolean();
	}
	
	private boolean createCgroupsCpuReportingEnabled() {
		return random.nextBoolean();
	}
	
	private List<String> createAliens() {
		List<String> aliens = new ArrayList<String>();
		int count = random.nextInt(10);
		for(int i=0;i<count;i++) {
			aliens.add(""+random.nextInt(99999));
		}
		return aliens;
	}
	
	private String createHeartbeat() {
		String heartbeat = "";
		int type = random.nextInt(100);
		int beat = random.nextInt(60);
		if(type > 5) {
			if(type > 15) {
				heartbeat = ""+beat;
			}
			else {
				heartbeat = ""+random.nextInt(600)+60;
			}
		}
		return heartbeat;
	}
	
	private MachineFacts createMachineFacts() {
		String status = createStatus();
		String ip = createIP();
		String name = createName();
		String memTotal = createMemory();
		String memFree = createMemory();
		String swap = createSwap();
		String delta = createSwap();
		String free = createFree();
		double cpu = createCPU();
		boolean cgroupsEnabled = createCgroupsEnabled();
		boolean cgroupsCpuReportingEnabled = createCgroupsCpuReportingEnabled();
		List<String> aliens = createAliens();
		String heartbeat = createHeartbeat();
		MachineFacts machineFacts = new MachineFacts(status,"",ip,name, memTotal, memFree, swap, delta, free, cpu, cgroupsEnabled, cgroupsCpuReportingEnabled, aliens, heartbeat);
		return machineFacts;
	}
	
	private MachineFactsList createMachineFactsList() {
		MachineFactsList machineFactsList = new MachineFactsList();
		for(int i=0; i<10; i++) {
			machineFactsList.add(createMachineFacts());
		} 
		return machineFactsList;
	}
	
	private boolean compare(MachineFacts m1, MachineFacts m2) {
		boolean retVal = false;
		try {
			if(true
			&& m1.heartbeat.equals(m2.heartbeat) 
			&& m1.ip.equals(m2.ip) 
			&& m1.memTotal.equals(m2.memTotal) 
			&& m1.memFree.equals(m2.memFree) 
			&& m1.name.equals(m2.name) 
			&& m1.status.equals(m2.status) 
			&& m1.swapInuse.equals(m2.swapInuse) 
			&& m1.swapDelta.equals(m2.swapDelta) 
			&& m1.swapFree.equals(m2.swapFree) 
			) {
				retVal = true;
			}
		}
		catch(Exception e) {
		}
		return retVal;
	}
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testMachineFacts() {
		MachineFactsTest machineFactsTest = new MachineFactsTest();
		MachineFactsList machineFactsList = machineFactsTest.createMachineFactsList();
		Gson gson = new Gson();
		String jSon = gson.toJson(machineFactsList);
		MachineFactsList reconstituted = gson.fromJson(jSon, MachineFactsList.class);
		for(int i=0; i<machineFactsList.size(); i++) {
			MachineFacts m1 = machineFactsList.get(i);
			MachineFacts m2 = reconstituted.get(i);
			if(!compare(m1,m2)) {
				fail("missing "+"name="+m1.name);
			}
		}
	}

}
