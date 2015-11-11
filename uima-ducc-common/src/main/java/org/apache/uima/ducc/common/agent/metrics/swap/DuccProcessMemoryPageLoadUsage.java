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
package org.apache.uima.ducc.common.agent.metrics.swap;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

public class DuccProcessMemoryPageLoadUsage implements
		ProcessMemoryPageLoadUsage {
	String pid;
	
	public DuccProcessMemoryPageLoadUsage(String pid) {
		this.pid = pid;
	}	
	public long getMajorFaults() throws Exception {
		return collectProcessMajorFaults();
	}
	private long collectProcessMajorFaults() throws Exception {
		String[] command = new String[] {"/bin/ps","-o","maj_flt",pid};

		ProcessBuilder builder = new ProcessBuilder(command);
		builder.redirectErrorStream(true);
		Process process = builder.start();
		InputStream is = process.getInputStream();
		InputStreamReader isr = new InputStreamReader(is);
		BufferedReader br = new BufferedReader(isr);
		String line;
		int count = 0;
		String faults = null;
		try {
			while ((line = br.readLine()) != null) {
				// skip the header line
				if (count == 1) {
					faults = line.trim();
					break;
				}
				count++;
			}
		} finally {
			if (is != null) {
				is.close();
			}
			process.waitFor();
			process.destroy();
		}
		
		if ( faults != null) {
			return Long.parseLong(faults.trim());
		} else {
			return 0;
		}
	}

}
