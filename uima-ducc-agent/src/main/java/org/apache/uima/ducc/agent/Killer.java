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
package org.apache.uima.ducc.agent;

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccPropertiesResolver;
import org.apache.uima.ducc.common.utils.id.DuccId;

public class Killer {

	private static DuccLogger logger = new DuccLogger(Killer.class);
	private static DuccId jobid = null;

	private static Killer instance = new Killer();

	public static Killer getInstance() {
		return instance;
	}

	private String ducc_spawn_path = "ducc.agent.launcher.ducc_spawn_path";

	private String killCmd = "/bin/kill";
	private String killSignal = "-SIGKILL";
	private String dashU = "-u";
	private String dashDash = "--";
	
	private String psCmd = "/bin/ps";
	private String dashDashNoHeading = "--no-heading";
	private String dashDashPid = "--pid";
	
	// ducc_ling -u <user> -- /bin/kill -SIGKILL <pid>

	private String getDucclingCmd() {
		String retVal = DuccPropertiesResolver.get(ducc_spawn_path);
		return retVal;
	}

	// -1 something went wrong?
	// 0 pid is gone
	// 1 pid still exists
	public int kill(final String user, final String pid) {
		String location = "kill";
		int retVal = -1;
		
		try {
			if(isKilled(user, pid)) {
				retVal = 0;
			}
			else {
				StringBuffer sb = new StringBuffer();
				sb.append(getDucclingCmd());
				sb.append(" ");
				sb.append(dashU);
				sb.append(" ");
				sb.append(user);
				sb.append(" ");
				sb.append(dashDash);
				sb.append(" ");
				sb.append(killCmd);
				sb.append(" ");
				sb.append(killSignal);
				sb.append(" ");
				sb.append(pid);
				String cmdLine = sb.toString();
				logger.info(location, jobid, cmdLine);
				Process process = Runtime.getRuntime().exec(cmdLine);
				process.waitFor();
				logger.info(location, jobid, "rc=" + process.exitValue());
				int exitValue = process.exitValue();
				retVal = exitValue;
			}
		} catch (Exception e) {
			logger.error(location, jobid, e);
		}
		return retVal;
	}

	public boolean isKilled(final String user, final String pid) {
		String location = "isKilled";
		boolean retVal = false;
		try {
			StringBuffer sb = new StringBuffer();
			sb.append(psCmd);
			sb.append(" ");
			sb.append(dashDashNoHeading);
			sb.append(" ");
			sb.append(dashDashPid);
			sb.append(" ");
			sb.append(pid);
			String cmdLine = sb.toString();
			logger.info(location, jobid, cmdLine);
			Process process = Runtime.getRuntime().exec(cmdLine);
			process.waitFor();
			logger.info(location, jobid, "rc=" + process.exitValue());
			int exitValue = process.exitValue();
			if(exitValue == 1) {
				retVal = true;
			}
		} catch (Exception e) {
			logger.error(location, jobid, e);
		}
		return retVal;
	}
	
}
