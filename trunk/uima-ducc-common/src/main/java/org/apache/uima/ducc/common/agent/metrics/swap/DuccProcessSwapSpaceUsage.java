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
import java.io.InputStreamReader;


import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.Utils;

public class DuccProcessSwapSpaceUsage implements ProcessSwapSpaceUsage {
	String pid=null;
	String execScript=null;
	DuccLogger logger=null;
	String[] command;
	
	public DuccProcessSwapSpaceUsage( String pid, String owner, String execScript, DuccLogger logger) {
		this.pid = pid;
		this.execScript = execScript;
		this.logger = logger;
	    String c_launcher_path = 
	            Utils.resolvePlaceholderIfExists(
	                    System.getProperty("ducc.agent.launcher.ducc_spawn_path"),System.getProperties());
	    command = new String[] { c_launcher_path,
	              "-u", owner, "--", execScript, pid }; 
	}
	public long getSwapUsage() {
		long swapusage=0;
		if ( pid != null && execScript != null ) {
			InputStreamReader in = null;
			try {
				ProcessBuilder pb = new ProcessBuilder();
				//String[] command = {execScript,pid};
				pb.command(command); //command);

				//logger.info("------------ getSwapUsage-", null, cmd);
				pb.redirectErrorStream(true);
				Process swapCollectorProcess = pb.start();
				in = new InputStreamReader(swapCollectorProcess.getInputStream());
				BufferedReader reader = new BufferedReader(in);
				String line=null;
				boolean skip = true;
				while ((line = reader.readLine()) != null) {
					try {
						if ( line.startsWith("1001")) {
							skip = false;
							continue;
						}
						if (!skip) {
							swapusage = Long.parseLong(line.trim());
							logger.info("getSwapUsage-",null, "PID:"+pid+" Swap Usage:"+line);
						}
					} catch( NumberFormatException e) {
						logger.error("getSwapUsage", null, line);
					}
				}
			} catch( Exception e) {
				logger.error("getSwapUsage", null, e);
			} finally {
				if ( in != null ) {
					try {
						in.close();	
					} catch( Exception e) {
						logger.error("getSwapUsage", null, e);
					}
					
				}
			}
		}
		return swapusage;
	}
	
}
