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
package org.apache.uima.ducc.ws;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.id.DuccId;

public class DuccMonitor extends Thread {

	private static DuccLogger logger = DuccLogger.getLogger(DuccMonitor.class);
	private static DuccId jobid = null;
	
	private static long millis_per_second = 1000;
	
	private static DuccMonitor instance = null;
	
	private AtomicBoolean no_kill_i = new AtomicBoolean(true);
	private AtomicLong sleep_time = new AtomicLong(30);
	
	/*
	 * start thread that monitors for obsolete machines
	 */
	public static void turn_on() {
		String location = "turn_on";
		try {
			if(instance == null) {
				instance = new DuccMonitor();
				instance.no_kill_i.set(true);
				instance.start();
				logger.info(location, jobid, "");
			}
			logger.debug(location, jobid, "");
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
		}
	}
	
	/*
	 * stop thread that monitors for obsolete machines
	 */
	public static void turn_off() {
		String location = "turn_off";
		try {
			if(instance != null) {
				instance.no_kill_i.set(false);
				instance = null;
				logger.info(location, jobid, "");
			}
			logger.debug(location, jobid, "");
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
		}
	}
	
	private DuccMonitor() {	
	}
	
	/*
	 * thread to monitor for obsolete machines
	 */
	public void run() {
		String location = "run";
		logger.info(location, jobid, "begin");
		while(no_kill_i.get()) {
			cleanup_MachinesData();
			pause();
		}
		logger.info(location, jobid, "end");
	}
	
	/*
	 * sleep for designated amount of time between checks
	 */
	private void pause() {
		String location = "pause";
		try {
			Thread.sleep(millis_per_second*sleep_time.get());
		}
		catch(Exception e) {
			logger.debug(location, jobid, e);
		}	
	}
	/*
	 * perform cleanup, as necessary
	 */
	private void cleanup_MachinesData() {
		DuccMachinesData md = DuccMachinesData.getInstance();
		md.cleanup();
	}
}
