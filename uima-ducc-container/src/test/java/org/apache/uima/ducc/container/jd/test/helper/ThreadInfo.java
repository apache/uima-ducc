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

public class ThreadInfo {

	private int node = 0;
	private int pid = 0;
	private int tid = 0;
	
	public ThreadInfo(int node, int pid, int tid) {
		this.node = node;
		this.pid = pid;
		this.tid = tid;
	}
	
	public String getNode() {
		return "node"+node;
	}
	
	public int getPid() {
		return pid;
	}
	
	public int getTid() {
		return tid;
	}
	
	public String toKey() {
		return getNode()+":"+getPid()+":"+getTid();
	}
}
