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
package org.apache.uima.ducc.cli.ws.json;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.gson.Gson;

public class MachineFactsTest {
	
	private long shareSize = 15;
	
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
		String name = "bluej"+random.nextInt(801);
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
	
	private List<String> createAliens() {
		List<String> aliens = new ArrayList<String>();
		int count = random.nextInt(10);
		for(int i=0;i<count;i++) {
			aliens.add(""+random.nextInt(99999));
		}
		return aliens;
	}
	
	private String createSharesTotal(String memory) {
		String sharesTotal = "";
		try {
			Integer integer = new Integer(memory);
			long shares = integer / shareSize;
			sharesTotal = ""+shares;
		}
		catch(Exception e) {
		}
		return sharesTotal;
	}
	
	private String createSharesInuse(String total) {
		String sharesInuse = "";
		try {
			Integer integer = new Integer(total);
			long shares = random.nextInt(integer+1);
			sharesInuse = ""+shares;
		}
		catch(Exception e) {
		}
		return sharesInuse;
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
		String reserve = createMemory();
		String memory = createMemory();
		String swap = createSwap();
		List<String> aliens = createAliens();
		String sharesTotal = createSharesTotal(memory);
		String sharesInuse = createSharesInuse(sharesTotal);
		String heartbeat = createHeartbeat();
		MachineFacts machineFacts = new MachineFacts(status,ip,name, reserve, memory, swap, aliens, sharesTotal, sharesInuse, heartbeat);
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
			&& m1.memory.equals(m2.memory) 
			&& m1.name.equals(m2.name) 
			&& m1.reserve.equals(m2.reserve) 
			&& m1.sharesInuse.equals(m2.sharesInuse) 
			&& m1.sharesTotal.equals(m2.sharesTotal)
			&& m1.status.equals(m2.status) 
			&& m1.swap.equals(m2.swap) 
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
