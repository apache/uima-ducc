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

import org.apache.uima.ducc.ws.server.DuccHandlerUtils;
import org.junit.Test;


public class TestSuite {

	private void test(double v, String f) {
		String r = DuccHandlerUtils.getSwapSizeHover(v);
		//System.out.println(r);
		assertTrue(f.equals(r));
	}
	
	@Test
	public void getDisplayableSize() {
		test(Math.pow(10,12),"1000.0 GB");
		test(Math.pow(10,11),"100.0 GB");
		test(Math.pow(10,10),"10.0 GB");
		test(Math.pow(10,9),"1.0 GB");
		test(Math.pow(10,6),"1.0 MB");
		test(Math.pow(10,3),"1.0 KB");
		test(Math.pow(10,0),"1.0 Bytes");
		test(Math.pow(10,9)/2,"0.5 GB");
		test(Math.pow(10,6)/2,"0.5 MB");
		test(Math.pow(10,3)/2,"0.5 KB");
		test(Math.pow(10,9)/10,"0.1 GB");
		test(Math.pow(10,6)/10,"0.1 MB");
		test(Math.pow(10,3)/10,"0.1 KB");
		test(Math.pow(10,9)/10-1,"100.0 MB");
		test(Math.pow(10,6)/10-1,"100.0 KB");
		test(Math.pow(10,3)/10-1,"99.0 Bytes");
		test(0,"0.0 GB");
	}

}
