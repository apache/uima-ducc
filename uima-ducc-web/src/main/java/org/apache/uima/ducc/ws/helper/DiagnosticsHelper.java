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
import java.util.concurrent.atomic.AtomicReference;

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.id.DuccId;

public class DiagnosticsHelper extends Thread {
	
	private static DuccLogger duccLogger = DuccLogger.getLogger(DiagnosticsHelper .class.getName(), null);
	private static DuccId jobid = null;
	
	private static File devNull = new File("/dev/null");
	
	private static AtomicReference<DiagnosticsHelper> instance = new AtomicReference<DiagnosticsHelper>();
	private static AtomicReference<String> disk_info = new AtomicReference<String>();
	
	private static long interval = 1000*60*60; // 60 minutes between disk info calculations
	
	static {
		DiagnosticsHelper expect = null;
		DiagnosticsHelper update = new DiagnosticsHelper();
		instance.compareAndSet(expect,update);
		instance.get().run();
	}
	
	private static void refresh_ducc_disk_info() {
		String location = "refresh_ducc_disk_info";
		StringBuffer sb = new StringBuffer();
		duccLogger.debug(location, jobid, "time start");
		try {
			String path = System.getProperty("DUCC_HOME")
						+File.separator
						+"admin"
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
		       disk_info.set(sb.toString());
		       duccLogger.info(location, jobid, disk_info);
		       int exitValue = process.waitFor();
		       duccLogger.debug(location, jobid, "rc="+exitValue);
		}
		catch(Exception e) {
			duccLogger.error(location, jobid, e);
		}
		duccLogger.debug(location, jobid, "time end");
	}
	
	public static String get_ducc_disk_info() {
		return disk_info.get();
	}
	
	public static boolean isNotKilled() {
		return true;
	}
	
	@Override 
	public void run() {
		String location = "run";
		while (isNotKilled()) {
			try {
				refresh_ducc_disk_info();
				duccLogger.debug(location, jobid, "sleep "+interval+" begin");
				Thread.sleep(interval);
				duccLogger.debug(location, jobid, "sleep "+interval+" end");
			}
			catch(Exception e) {
				duccLogger.error(location, jobid, e);
			}
		}
	}
}
