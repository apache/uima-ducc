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
package org.apache.uima.ducc.common.node.metrics;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Serializable;

public class NodeCpuInfo implements Serializable {
	private static final long serialVersionUID = -4770810550678067034L;
	private String availableProcessors = "0";
    private String currentLoad = "0.0";
    
	public NodeCpuInfo(int availableProcessors) {
		setAvailableProcessors(String.valueOf(availableProcessors));
		try {
			currentLoad = String.valueOf(getCPULoad());
		} catch( Exception e) {
			e.printStackTrace();
		}
	}
    public String getCurrentLoad() {
    	return currentLoad;
    }
	public String getAvailableProcessors() {
		return availableProcessors;
	}

	private void setAvailableProcessors(String availableProcessors) {
		this.availableProcessors = availableProcessors;
	}

	private double getCPULoad() throws Exception {
		double cpu = 0.0;
		InputStreamReader in = null;
		String[] command = {
				"/bin/sh",
				"-c",
				"/bin/grep 'cpu' /proc/stat | /bin/awk '{usage=($2+$4)*100/($2+$4+$5)} END {print usage}'" };
		try {
			ProcessBuilder pb = new ProcessBuilder();
			pb.command(command);

			pb.redirectErrorStream(true);
			Process swapCollectorProcess = pb.start();
			in = new InputStreamReader(swapCollectorProcess.getInputStream());
			BufferedReader reader = new BufferedReader(in);
			String line = null;
			while ((line = reader.readLine()) != null) {
				System.out.println(line);
				try {
					cpu = Double.parseDouble(line.trim());
				} catch (NumberFormatException e) {
					cpu = 0;
					e.printStackTrace();
				}
			}
		} catch (Exception e) {
			throw e;
		} finally {
			if (in != null) {
				try {
					in.close();
				} catch (Exception e) {
				}

			}
		}

		return cpu;
	}
}
