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
package org.apache.uima.ducc.common.system;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.Properties;

import org.apache.uima.ducc.common.IDuccEnv;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.id.DuccId;


public class SystemState {
	
	private static final DuccLogger logger = DuccLoggerComponents.getOrLogger(SystemState.class.getName());
	
	private static SystemState instance = new SystemState();
	
	public static SystemState getInstance() {
		return instance;
	}
	
	private static String jobsStop = IDuccEnv.DUCC_STATE_DIR+"ducc.jobs.stop";
	
	private static DuccId jobid = null;
	
	public boolean isAcceptJobs() {
		String location = "isAcceptJobs";
		boolean retVal = true;
		try {
			File file = new File(jobsStop);
			retVal = !file.exists();
		}
		catch(Throwable t) {
			logger.error(location, jobid, t);
		}
		return retVal;
	}
	
	public void resetAcceptJobs(String userid) {
		String location = "resetAcceptJobs";
		try {
			Properties properties = new Properties();
			properties.put("userid", userid);
			String comments = null;
			FileOutputStream fos = null;
			OutputStreamWriter out = null;
			fos = new FileOutputStream(jobsStop);
			out = new OutputStreamWriter(fos);
			properties.store(out, comments);
			out.close();
			fos.close();
		}
		catch(Throwable t) {
			logger.error(location, jobid, t);
		}
	}
	
	public void setAcceptJobs() {
		String location = "setAcceptJobs";
		try {
			File file = new File(jobsStop);
			if(file.exists()) {
				file.delete();
			}
		}
		catch(Throwable t) {
			logger.error(location, jobid, t);
		}
	}
}
