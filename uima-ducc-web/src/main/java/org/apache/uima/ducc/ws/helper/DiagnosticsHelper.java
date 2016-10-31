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
package org.apache.uima.ducc.ws.helper;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.id.DuccId;

public class DiagnosticsHelper {
	
	private static DuccLogger duccLogger = DuccLogger.getLogger(DiagnosticsHelper .class.getName(), null);
	private static DuccId duccId = null;
	
	private static String disk_info = null;
	
	private static File devNull = new File("/dev/null");
	
	private static void refresh_ducc_disk_info() {
		String location = "refresh_ducc_disk_info";
		if(disk_info == null) {
			disk_info = "";
			StringBuffer sb = new StringBuffer();
			try {
				String path = System.getProperty("DUCC_HOME")
							+File.separator
							+"admin"
							+File.separator
							+"tools"
							+File.separator
							+"ducc_disk_info"
							;
				String[] command = { path };
				ProcessBuilder pb = new ProcessBuilder( command );
				pb = pb.redirectError(devNull);
				Process process = pb.start();
				InputStream is = process.getInputStream();
		        InputStreamReader isr = new InputStreamReader(is);
		        BufferedReader br = new BufferedReader(isr);
		        String line;
		        while ((line = br.readLine()) != null) {
		           sb.append(line);
		           sb.append("\n");
		        }
		        disk_info = sb.toString();
		        duccLogger.debug(location, duccId, disk_info);
		        int exitValue = process.waitFor();
		        duccLogger.debug(location, duccId, exitValue);
			}
			catch(Exception e) {
				duccLogger.error(location, duccId, e);
			}
		}
	}
	
	public static void reset_ducc_disk_info() {
		synchronized(DiagnosticsHelper.class) {
			disk_info = null;
		}
	}
	
	public static String get_ducc_disk_info() {
		synchronized(DiagnosticsHelper.class) {
			refresh_ducc_disk_info();
			return disk_info;
		}
	}
}
