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

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.DuccPropertiesResolver;

public class UserAuthenticate {
	
	private static DuccLogger duccLogger = DuccLoggerComponents.getWsLogger(UserAuthenticate.class.getName());
	
	private String failure = "failure";
	
	public String launch(String[] args) {
		String methodName = "launch";
		String result = null;
		try {
			if(args == null) {
				result = failure + " args==null";
			}
			else if(args.length != 2) {
				result = failure + " args.length!=2";
			}
			else if(args[0] == null) {
				result = failure + " args[0]==null";
			}
			else if(args[1] == null) {
				result = failure + " args[1]==null";
			}
			else {
				String userId = args[0];
				String cp = System.getProperty("java.class.path");
				String jclass = "org.apache.uima.ducc.ws.authentication.PamAuthenticate";
				String jhome = System.getProperty("java.home");
				String java = "/bin/java";
				StringBuffer mask = new StringBuffer();
				for(int i=0; i<args[1].length(); i++) {
					mask.append("x");
				}
				String[] arglist = { "-u", userId, "-q", "--", jhome+java, "-cp", cp, jclass, args[0], args[1] };
				String[] masklist = { "-u", userId, "-q", "--", jhome+java, "-cp", cp, jclass, args[0], mask.toString() };
				duccLogger.debug(methodName, null, masklist[0]);
				duccLogger.debug(methodName, null, masklist[1]);
				duccLogger.debug(methodName, null, masklist[2]);
				duccLogger.debug(methodName, null, masklist[3]);
				duccLogger.debug(methodName, null, masklist[4]);
				duccLogger.debug(methodName, null, masklist[5]);
				//duccLogger.debug(methodName, null, masklist[6]);
				String[] cplist = cp.split(":");
				if(cplist != null) {
					for(String item : cplist) {
						duccLogger.debug(methodName, null, item);
					}
				}
				duccLogger.debug(methodName, null, masklist[7]);
				duccLogger.debug(methodName, null, masklist[8]);
				duccLogger.debug(methodName, null, masklist[9]);
				result = DuccAsUser.ducklingQuiet(userId, arglist, masklist);
			}
		}
		catch(Throwable t) {
			result = failure+" "+t.getMessage();
		}
		return result;
	}
	
	public static void main(String[] args) {
		String key = "DUCC_HOME";
		String value = System.getenv(key);
		if(value != null) {
			System.setProperty(key, value);
		}
		DuccPropertiesResolver dpr = DuccPropertiesResolver.getInstance();
		key = "ducc.agent.launcher.ducc_spawn_path";
		value = dpr.getFileProperty("ducc.agent.launcher.ducc_spawn_path");
		if(value != null) {
			System.setProperty(key, value);
		}
		UserAuthenticate instance = new UserAuthenticate();
		String result = instance.launch(args);
		System.out.println(result);
	}

}
