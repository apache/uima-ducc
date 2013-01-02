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
package org.apache.uima.ducc.ws.server;

import java.util.ArrayList;
import java.util.Map;

import org.apache.uima.ducc.common.internationalization.Messages;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.Utils;


public class DuccAsUser {

	private static DuccLogger duccLogger = DuccLoggerComponents.getWsLogger(DuccAsUser.class.getName());
	private static Messages messages = Messages.getInstance();
	
	public static boolean duckling(String[] args) {
		
		String methodName = "duckling";
		
		String c_launcher_path = 
			Utils.resolvePlaceholderIfExists(
					System.getProperty("ducc.agent.launcher.ducc_spawn_path"),System.getProperties());
		
		duccLogger.debug(methodName, null, messages.fetchLabel("the duckling launcher")+c_launcher_path);
		
		ArrayList<String> cmd = new ArrayList<String>();
		
		cmd.add(c_launcher_path);
		
		for( String arg : args ) {
			cmd.add(arg);
		}
		
		StringBuffer sb = new StringBuffer();
		
		for( String c : cmd ) {
			sb.append(c);
			sb.append(" ");
		}
		
		duccLogger.info(methodName, null, sb.toString().trim());
		
		ProcessBuilder pb = new ProcessBuilder(cmd);
		
		Map<String, String> env = pb.environment();
		
		env.put("JobId", "webserver");
		
		try {
			Process process = pb.start();
			process.waitFor();
		}
		catch(Exception e) {
			duccLogger.info(methodName, null, e);
		}
		
		return true;
	}
}
