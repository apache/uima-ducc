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
package org.apache.uima.ducc.common.authentication;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Map;

import org.apache.uima.ducc.common.utils.DuccPropertiesResolver;
import org.apache.uima.ducc.common.utils.Utils;


public class DuccAsUser {

	public static String magicString = "1001 Command launching...";
	
	public static String duckling(String user, String[] args, String[] argsMasked) {

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
			Process process = pb.start();
			String line;
			BufferedReader bri = new BufferedReader(new InputStreamReader(process.getInputStream()));
			BufferedReader bre = new BufferedReader(new InputStreamReader(process.getErrorStream()));
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
			while ((line = bre.readLine()) != null) {
				retVal.append(line);
			}
			bre.close();
			process.waitFor();
		}
		catch(Exception e) {
		}
		
		return retVal.toString();
	}
}
