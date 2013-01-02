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
package org.apache.uima.ducc.agent.metrics.collectors;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.uima.ducc.common.agent.metrics.memory.NodeMemory;
import org.apache.uima.ducc.common.node.metrics.NodeMemoryInfo;


public class NodeMemInfoCollector implements CallableMemoryCollector {
	private long fakeMemorySize = -1;
	private String[] targetFields;

	public NodeMemInfoCollector(String[] targetFields) {
		this.targetFields = targetFields;
		String tmp;
		if ((tmp = System
				.getProperty("ducc.agent.node.metrics.fake.memory.size")) != null) {
			try {
				fakeMemorySize = Long.parseLong(tmp);
			} catch (NumberFormatException e) {
				e.printStackTrace();
			}
		}
	}

	public NodeMemory call() throws Exception {

		BufferedReader fileReader = new BufferedReader(new FileReader(
				"/proc/meminfo"));
		// the order of fields corresponds to the field label position
		long memInfoValues[] = new long[targetFields.length];
		try {
			String line;
			// Read each line from meminfo file
			while ((line = fileReader.readLine()) != null) {
				// parse line and remove spaces
				String[] parts = line.trim().split("\\s+");

				// ignore lines that contain fields we dont need. The
				// targetFields array
				// contains labels of fields we are interested in. For each line
				// read
				// from file try to find a match.
				for (int i = 0; i < targetFields.length; i++) {
					if (parts[0].equals(targetFields[i])) {
						// got a field we need
						try {
							memInfoValues[i] = Long.parseLong(parts[1]);
						} catch (NumberFormatException e) {
							throw e;
						}
						break; // get the next field
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
		return new NodeMemoryInfo(memInfoValues, fakeMemorySize);
	}
	public static void main(String[] args) {
	    String[] meminfoTargetFields = new String[] {"MemTotal:","MemFree:","SwapTotal:","SwapFree:"};

		try {
			NodeMemInfoCollector nmi = new NodeMemInfoCollector(meminfoTargetFields);
			ExecutorService pool = Executors.newFixedThreadPool(1);
			while( true ) {
				Future<NodeMemory> nmiFuture = pool.submit(nmi);
				NodeMemory memInfo = nmiFuture.get();
				System.out.println("... Memonfo Data:"
						+memInfo.getMemTotal()+
						" Memory Free:"+memInfo.getMemFree()+
						" Swap Total:"+memInfo.getSwapTotal()+
						" Swap Free:"+memInfo.getSwapFree());
				synchronized(nmi) {
					nmi.wait(4000);
				}
			}
			
		} catch( Exception e) {
			e.printStackTrace();
		}
	}
}
