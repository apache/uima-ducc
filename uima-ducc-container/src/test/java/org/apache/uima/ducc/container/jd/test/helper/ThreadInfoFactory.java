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
package org.apache.uima.ducc.container.jd.test.helper;

import java.util.ArrayList;
import java.util.Random;

public class ThreadInfoFactory {

	private ArrayList<ThreadInfo> list = new ArrayList<ThreadInfo>();
	
	public ThreadInfoFactory(int nodes, int pids, int tids) {
		for(int i=0; i<nodes; i++) {
			for(int j=0; j<pids; j++) {
				for(int k=0; k<tids; k++) {
					int node = (i+1);
					int pid = (node*100)+(j+1);
					int tid = (k+1);
					ThreadInfo ti = new ThreadInfo(node, pid, tid);
					list.add(ti);
				}
			}
		}
	}
	
	public ThreadInfo getRandom() {
		ThreadInfo ti = null;
		if(list.size() > 0) {
			Random random = new Random();
			int n = list.size();
			int index = random.nextInt(n);
			ti = list.get(index);
		}
		return ti;
	}
	
	public ThreadInfo getUnique() {
		ThreadInfo ti = null;
		if(list.size() > 0) {
			ti = list.get(0);
			list.remove(0);
		}
		return ti;
	}
}
