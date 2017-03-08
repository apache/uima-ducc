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
package org.apache.uima.ducc.orchestrator.user;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Map;

import org.apache.uima.ducc.common.IDuccUser;
import org.apache.uima.ducc.common.internationalization.Messages;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.Utils;


public class DuccAsUser {

	private static DuccLogger duccLogger = DuccLoggerComponents.getOrLogger(DuccAsUser.class.getName());
	private static Messages messages = Messages.getInstance();

	public static String magicString = "1001 Command launching...";

	public static String identity = "orchestrator";

	private static File devNull = new File("/dev/null");

	public static String duckling(String user, String umask, String file, String text) {

		String methodName = "duckling";

		StringBuffer retVal = new StringBuffer();

		String c_launcher_path =
			Utils.resolvePlaceholderIfExists(
					System.getProperty("ducc.agent.launcher.ducc_spawn_path"),System.getProperties());

		duccLogger.trace(methodName, null, messages.fetchLabel("the duckling launcher")+c_launcher_path);

		ArrayList<String> cmd = new ArrayList<String>();

		cmd.add(c_launcher_path);

		cmd.add("-u");
		cmd.add(user);
		cmd.add("-a");
		cmd.add("-f");
		cmd.add(file);
		cmd.add(text);

		StringBuffer sbDebug = new StringBuffer();

		duccLogger.trace(methodName, null, "plist: "+sbDebug.toString().trim());
		duccLogger.trace(methodName, null, "cmd: "+cmd);

		ProcessBuilder pb = new ProcessBuilder(cmd);

		Map<String, String> env = pb.environment();

		env.clear();
		if (umask != null) {
		    env.put("DUCC_UMASK",  umask);
		}
		//env.put(IDuccUser.EnvironmentVariable.DUCC_ID_JOB.value(), identity);

		try {
			pb = pb.redirectError(devNull);
			Process process = pb.start();
			String line;
			BufferedReader bri = new BufferedReader(new InputStreamReader(process.getInputStream()));
			boolean trigger = false;
			duccLogger.trace(methodName, null, "read stdout: start");
			while ((line = bri.readLine()) != null) {
				duccLogger.trace(methodName, null, "stdout: "+line);
				if(trigger) {
					retVal.append(line+"\n");
				}
				if(line.startsWith(magicString)) {
					duccLogger.trace(methodName, null, "magic!");
					trigger = true;
				}
			}
			bri.close();
			duccLogger.trace(methodName, null, "read stdout: end");
			duccLogger.trace(methodName, null, "process waitfor: start");
			process.waitFor();
			duccLogger.trace(methodName, null, "process waitfor: end");
		}
		catch(Exception e) {
			duccLogger.warn(methodName, null, e);
		}

		return retVal.toString();
	}
}
