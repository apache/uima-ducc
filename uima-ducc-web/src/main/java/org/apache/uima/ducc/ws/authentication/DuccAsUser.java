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
package org.apache.uima.ducc.ws.authentication;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Map;

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.DuccPropertiesResolver;
import org.apache.uima.ducc.common.utils.Utils;

public class DuccAsUser {
	
	private static DuccLogger duccLogger = DuccLoggerComponents.getWsLogger(DuccAsUser.class.getName());
	
	public static String magicString = "1001 Command launching...";
	
	private static File devNull = new File("/dev/null");
	
	public static String duckling(String user, String[] args) {
		
		String methodName = "duckling";
		
		StringBuffer retVal = new StringBuffer();
		
		String c_launcher_path = 
			Utils.resolvePlaceholderIfExists(
					System.getProperty("ducc.agent.launcher.ducc_spawn_path"),System.getProperties());
		
		duccLogger.debug(methodName, null, "the duckling launcher "+c_launcher_path);
		
		ArrayList<String> cmd = new ArrayList<String>();
		
		cmd.add(c_launcher_path);
		
		StringBuffer sbInfo  = new StringBuffer();
		StringBuffer sbDebug = new StringBuffer();
		String prev = "";
		
		for( String arg : args ) {
			cmd.add(arg);
			if(!arg.equals("-cp")) {
				if(!prev.equals("-cp")) {
					sbInfo.append(arg+" ");
				}
			}
			sbDebug.append(arg+" ");
			prev = arg;
		}

		duccLogger.info(methodName, null, "plist: "+sbInfo.toString().trim());
		duccLogger.debug(methodName, null, "plist: "+sbDebug.toString().trim());
		
		duccLogger.info(methodName, null, "cmd: "+cmd);
		duccLogger.trace(methodName, null, "cmd: "+cmd);
		
		ProcessBuilder pb = new ProcessBuilder(cmd);
		
		Map<String, String> env = pb.environment();
		
		env.put("JobId", "webserver");
		
		String runmode = DuccPropertiesResolver.getInstance().getProperty(DuccPropertiesResolver.ducc_runmode);
		if(runmode != null) {
			if(runmode.equals("Test")) {
				env.put("USER", user);
			}
		}
		
		try {
			pb = pb.redirectError(devNull);
			Process process = pb.start();
			String line;
			BufferedReader bri = new BufferedReader(new InputStreamReader(process.getInputStream()));
			boolean trigger = false;
			duccLogger.trace(methodName, null, "read stdout: start");
			while ((line = bri.readLine()) != null) {
				duccLogger.info(methodName, null, "stdout: "+line);
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
			duccLogger.info(methodName, null, e);
		}
		
		return retVal.toString();
	}
	
	public static String ducklingQuiet(String user, String[] args, String[] argsMasked) {

		StringBuffer retVal = new StringBuffer();
		
		String c_launcher_path = 
			Utils.resolvePlaceholderIfExists(
					System.getProperty("ducc.agent.launcher.ducc_spawn_path"),System.getProperties());

		ArrayList<String> cmd = new ArrayList<String>();
		
		cmd.add(c_launcher_path);
		
		StringBuffer sbInfo  = new StringBuffer();
		StringBuffer sbDebug = new StringBuffer();
		String prev = "";
		
		for(int i=0; i<args.length; i++) {
			String arg = args[i];
			cmd.add(arg);
			if(!arg.equals("-cp")) {
				if(!prev.equals("-cp")) {
					sbInfo.append(argsMasked[i]+" ");
				}
			}
			sbDebug.append(argsMasked[i]+" ");
			prev = arg;
		}

		ProcessBuilder pb = new ProcessBuilder(cmd);
		
		Map<String, String> env = pb.environment();
		
		env.put("JobId", "webserver");
		
		String runmode = DuccPropertiesResolver.getInstance().getProperty(DuccPropertiesResolver.ducc_runmode);
		if(runmode != null) {
			if(runmode.equals("Test")) {
				env.put("USER", user);
			}
		}
		
		try {
			pb = pb.redirectError(devNull);
			Process process = pb.start();
			String line;
			BufferedReader bri = new BufferedReader(new InputStreamReader(process.getInputStream()));
			boolean trigger = true;
			while ((line = bri.readLine()) != null) {
				if(trigger) {
					retVal.append(line+"\n");
				}
				if(line.startsWith(magicString)) {
					trigger = true;
				}
			}
			bri.close();
			process.waitFor();
		}
		catch(Exception e) {
		}
		
		return retVal.toString();
	}
}
