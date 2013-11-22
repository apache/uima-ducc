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
package org.apache.uima.ducc.test;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.uima.ducc.jd.client.Plist;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class PlistTest {

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
	
	private static void validate(HashMap<String,String> hMap, Plist plist) {
		TreeMap<String,String> pMap = plist.getParameterMap();
		Iterator<String> iterator = pMap.keySet().iterator();
		while(iterator.hasNext()) {
			String key = iterator.next();
			String v1 = pMap.get(key);
			String v2 = hMap.get(key);
			if(!v1.equals(v2)) {
				fail("Plist corrupted?");
			}
		}
	}
	
	private static void build(StringBuffer sb, HashMap<String,String> map, String key, String value) {
		sb.append(key);
		sb.append(":");
		sb.append(value);
		map.put(key, value);
	}
	
	@Test
	public void test() {
		try {
			Plist plist;
			StringBuffer sb;
			HashMap<String,String> map;
			//
			map = new HashMap<String,String>();
			sb = new StringBuffer();
			build(sb,map,"WorkItems","0-59");
			sb.append(",");
			build(sb,map,"FailItems","17-18");
			plist = new Plist(sb.toString());
			validate(map,plist);
			//
			map = new HashMap<String,String>();
			sb = new StringBuffer();
			build(sb,map,"x","1");
			sb.append(",");
			build(sb,map,"y","2");
			sb.append(",");
			build(sb,map,"z","http://3");
			plist = new Plist(sb.toString());
			validate(map,plist);
			//
			map = new HashMap<String,String>();
			plist = new Plist(null);
			validate(map,plist);
			plist = new Plist("x");
			validate(map,plist);
			plist = new Plist("x:");
			validate(map,plist);
			plist = new Plist(":1");
			validate(map,plist);
			plist = new Plist(",");
			validate(map,plist);
			plist = new Plist(":,");
			validate(map,plist);
		}
		catch(Exception e) {
			e.printStackTrace();
			fail("Exception");
		}
	}

}
