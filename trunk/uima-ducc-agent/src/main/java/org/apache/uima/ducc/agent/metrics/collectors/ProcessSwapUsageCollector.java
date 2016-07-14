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

import java.io.RandomAccessFile;
import java.util.concurrent.Callable;

import org.apache.uima.ducc.common.agent.metrics.swap.DuccProcessMemoryPageLoadUsage;
import org.apache.uima.ducc.common.agent.metrics.swap.ProcessMemoryPageLoadUsage;
import org.apache.uima.ducc.common.utils.DuccLogger;

public class ProcessSwapUsageCollector implements
		Callable<ProcessMemoryPageLoadUsage> {
	String pid;
	
	public ProcessSwapUsageCollector(DuccLogger logger, String pid,
			RandomAccessFile fileHandle, int howMany, int offset) {
		this.pid = pid;
	}

	public ProcessMemoryPageLoadUsage call() throws Exception {
		try {
			return new DuccProcessMemoryPageLoadUsage(pid);
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
	}
/*
	private String execTopShell() throws Exception {
		List<String> command = new ArrayList<String>();
		command.add("top");
		command.add("-b");
		command.add("-n");
		command.add("1");
		command.add("-p");
		command.add(pid);

		ProcessBuilder builder = new ProcessBuilder(command);
		Process process = builder.start();
		InputStream is = process.getInputStream();
		InputStreamReader isr = new InputStreamReader(is);
		BufferedReader br = new BufferedReader(isr);
		String line;
		int count = 0;
		String cpu = "";
		try {
			while ((line = br.readLine()) != null) {
				if (count == 7) {
					String[] values = line.trim().split("\\s+");
					cpu = values[9];
					process.destroy();
					break;
				}
				count++;
			}
		} finally {
			if (is != null) {
				is.close();
			}
		}
		process.waitFor();
		return cpu;
	}
	*/
}
