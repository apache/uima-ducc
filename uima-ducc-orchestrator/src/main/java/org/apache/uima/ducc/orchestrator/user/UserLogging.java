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
	private static String ducc_error = "ducc.error";
	
	public static void record(String user, String userLogDir, String text) {
		 String file = fpJoin(userLogDir, ducc_log);
		 UserLogging.write(user, file, text);
	}
	
	public static void error(String user, String userLogDir, String text) {
		String file = fpJoin(userLogDir, ducc_error);
		 UserLogging.write(user, file, text);
	}
	
	private static String fpJoin(String dir, String fn) {
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
	
	static void write(String user, String file, String text) {
		if(text != null) {
			String millis = ""+System.currentTimeMillis();
			String ts = TimeStamp.simpleFormat(millis);
			DuccAsUser.duckling(user, file, ts+" "+text);
		}
	}
}
