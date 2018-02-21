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
import java.util.List;

import com.google.gson.Gson;

public class JsonHelper {

	private static Gson gson = new Gson();
	
	/**
	 * This class is used to generate Json data via Gson representing
	 * one JobProcess in a DUCC Job
	 */
	public class JobProcess {
		String process_number;
		String log_file;
		String log_size;
		String host_name;
		String pid;
		String scheduler_state;
		String scheduler_state_reason;
		String agent_state;
		String agent_state_reason;
		String exit;
		String time_init;
		String time_run;
		String time_gc;
		String pgin;
		String swap;
		String swap_max;
		String pct_cpu_overall;
		String pct_cpu_current;
		String rss;
		String rss_max;
		String wi_time_avg;
		String wi_time_max;
		String wi_time_min;
		String wi_done;
		String wi_error;
		String wi_dispatch;
		String wi_retry;
		String wi_preempt;
		String jconsole_url;
	}
	
	/**
	 * 
	 * This class is used to generate Json data via Gson representing
	 * one Job's collection of JobProcesses.
	 */
	public class JobProcessList {
		String job_number;
		String log_directory;
		List<JobProcess> processes = new ArrayList<JobProcess>();
		void set_job_number(String job_number) {
			this.job_number = job_number;
		}
		void addJobProcess(JobProcess p) {
			processes.add(p);
		}
		String toJson() {
			return gson.toJson(this);
		}
	}

}
