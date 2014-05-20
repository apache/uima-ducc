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
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.uima.ducc.common.agent.metrics.memory.NodeMemory;
import org.apache.uima.ducc.common.node.metrics.NodeMemoryInfo;
import org.apache.uima.ducc.common.utils.Utils;


public class NodeMemInfoCollector implements CallableMemoryCollector {
	private long fakeMemorySize = -1;
	private String[] targetFields;
    private int gidMax = 500;   // default. Used to sum up memory of processes owned by uids < gidMax
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
		// SYSTEM_GID_MAX
		if ((tmp = System
				.getProperty("ducc.agent.node.metrics.sys.gid.max")) != null) {
			try {
				gidMax = Integer.valueOf(tmp);
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
		} finally {
			fileReader.close();
		}
		long memUsed = 0;
		// if running ducc in simulation mode skip memory adjustment. Report free memory = fakeMemorySize
		if ( fakeMemorySize > -1 ) {
			// sum up memory of all processes owned by UIDs < gidMax 
			memUsed = collectRSSFromPSCommand();
			//System.out.println("Total:"+memInfoValues[0] + " Available:"+memInfoValues[1] +" Calculated:"+(memInfoValues[0] - memUsed)+" Priviledged Memory:"+memUsed);
		}
		
		memInfoValues[1] = memInfoValues[0] - memUsed;
		return new NodeMemoryInfo(memInfoValues, fakeMemorySize);
	}
	private long collectRSSFromPSCommand() throws Exception {
		InputStream stream = null;
	    BufferedReader reader = null;
	  
	      ProcessBuilder pb;
	      if ( Utils.isMac() ) {
	        pb = new ProcessBuilder("ps","-Ao","user=,pid=,uid=,rss=");
	      } else {
	        pb = new ProcessBuilder("ps","-Ao","user:12,pid,uid,rss", "--no-heading");
	      }
	      pb.redirectErrorStream(true);
	      Process proc = pb.start();
	      //  spawn ps command and scrape the output
	      stream = proc.getInputStream();
	      reader = new BufferedReader(new InputStreamReader(stream));
	      String line;
	      String regex = "\\s+";
	      long memoryUsed = 0;
	      // read the next line from ps output
	      while ((line = reader.readLine()) != null) {
	          String tokens[] = line.split(regex);
	          if ( tokens.length > 0 ) {
	        	  try {
	        		  int uid = Integer.valueOf(tokens[2]);
	        		  if ( uid < gidMax ) {
	        			  memoryUsed += Long.valueOf(tokens[3]);
	        		  }
	        	  } catch( NumberFormatException nfe) {
	        		  
	        	  }
	          }
	      }	
	      stream.close();
	      return memoryUsed;
	}
	public static void main(String[] args) {
	    String[] meminfoTargetFields = new String[] {"MemTotal:","MemFree:","SwapTotal:","SwapFree:"};

		try {
			NodeMemInfoCollector nmi = new NodeMemInfoCollector(meminfoTargetFields);
			ExecutorService pool = Executors.newFixedThreadPool(1);
			while( true ) {
				Future<NodeMemory> nmiFuture = pool.submit(nmi);
				NodeMemory memInfo = nmiFuture.get();
				System.out.println("... Meminfo Data -"+
						" Memory Total:"+memInfo.getMemTotal()+
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
