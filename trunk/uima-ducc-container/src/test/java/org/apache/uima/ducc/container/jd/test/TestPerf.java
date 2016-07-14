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

import java.util.TreeMap;

import org.apache.uima.ducc.container.jd.wi.perf.PerfKey;
import org.junit.Before;
import org.junit.Test;

public class TestPerf {
	
	@Before
    public void setUp() {
    }
	
	@Before
    public void tearDown() {
    }
	
	@Test
	public void test_perf_00() {
		try {
			new PerfKey(null,null);
			fail("Expceted NPE");
		}
		catch(NullPointerException e) {
		}
		try {
			new PerfKey("",null);
			fail("Expceted NPE");
		}
		catch(NullPointerException e) {
		}
		try {
			new PerfKey(null,"");
			fail("Expceted NPE");
		}
		catch(NullPointerException e) {
		}
		try {
			new PerfKey("","");
		}
		catch(NullPointerException e) {
			fail("Unexpceted NPE");
		}
		TreeMap<PerfKey,String> map = new TreeMap<PerfKey,String>();
		PerfKey pk1 = new PerfKey("s1","u1");
		PerfKey pk2 = new PerfKey("s2","u2");
		PerfKey pk3a = new PerfKey("s3","u3a");
		PerfKey pk3b = new PerfKey("s3","u3b");
		map.put(pk1, "v1");
		map.put(pk2, "v2");
		map.put(pk3a, "v3a");
		map.put(pk3b, "v3b");
		assertTrue(map.size() == 4);
		int index = 0;
		String name = null;
		String uniqueName = null;
		for(PerfKey pk : map.keySet()) {
			index += 1;
			switch(index) {
			case 1:
				name = "s"+index;
				assertTrue(name.equals(pk.getName()));
				uniqueName = "u"+index;
				assertTrue(uniqueName.equals(pk.getUniqueName()));
				break;
			case 2:
				name = "s"+index;
				assertTrue(name.equals(pk.getName()));
				uniqueName = "u"+index;
				assertTrue(uniqueName.equals(pk.getUniqueName()));
				break;
			case 3:
				name = "s"+index;
				assertTrue(name.equals(pk.getName()));
				uniqueName = "u"+index+"a";
				assertTrue(uniqueName.equals(pk.getUniqueName()));
				break;
			case 4:
				name = "s"+(index-1);
				assertTrue(name.equals(pk.getName()));
				uniqueName = "u"+(index-1)+"b";
				assertTrue(uniqueName.equals(pk.getUniqueName()));
				break;
			default:
				fail("Unexpected index");
				break;
			}
		}
	}

}
