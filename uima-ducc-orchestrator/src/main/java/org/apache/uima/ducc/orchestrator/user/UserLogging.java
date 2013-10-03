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

import java.io.File;

import org.apache.uima.ducc.common.utils.TimeStamp;

public class UserLogging {

	private static String ducc_log = "ducc.log";
	
	private String userName = null;
	private String userLogDir = null;
	private String userDuccLog = null;
	
	
	public static void record(String userName, String userLogDir, String text) {
		 UserLogging userLogging = new UserLogging(userName, userLogDir);
		 userLogging.toUserDuccLog(text);
	}
	
	public UserLogging(String userName, String userLogDir) {
		setUserName(userName);
		setUserLogDir(userLogDir);
		setUserDuccLog(fpJoin(getUserLogDir(),ducc_log));
	}
	
	private void setUserName(String value) {
		userName = value;
	}
	
	private String getUserName() {
		return userName;
	}
	
	private void setUserLogDir(String value) {
		userLogDir = value;
	}
	
	private String getUserLogDir() {
		return userLogDir;
	}
	
	private void setUserDuccLog(String value) {
		userDuccLog = value;
	}
	
	private String getUserDuccLog() {
		return userDuccLog;
	}
	
	private String fpJoin(String dir, String fn) {
		StringBuffer sb = new StringBuffer();
		if(dir != null) {
			sb.append(dir);
			if(dir.endsWith(File.separator)) {
			}
			else {
				sb.append(File.separator);
			}
		}
		if(fn != null) {
			sb.append(fn);
		}
		String retVal = sb.toString();
		return retVal;
	}
	
	public void toUserDuccLog(String text) {
		if(text != null) {
			String millis = ""+System.currentTimeMillis();
			String ts = TimeStamp.simpleFormat(millis);
			String user = getUserName();
			String file = getUserDuccLog();
			DuccAsUser.duckling(user, file, ts+" "+text);
		}
	}
}
