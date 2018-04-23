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
package org.apache.uima.ducc.common.head;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.uima.ducc.common.IDuccEnv;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.id.DuccId;

public abstract class ADuccHead implements IDuccHead {

	private static DuccLogger logger = null;
	private static DuccId jobid = null;
	
	private AtomicReference<String> state = new AtomicReference<String>(initial);;
	
	/*
	 * create instance of this class
	 */
	public ADuccHead(DuccLogger logger) {
		setLogger(logger);
		init();
	}
	
	/*
	 * initialize
	 */
	private void init() {
		String location = "init";
		String prev = state.get();
		query_ducc_head_mode();
		String curr = state.get();
		logger.info(location, jobid, prev, "->", curr);
	}
	
	/*
	 * set logger
	 */
	private void setLogger(DuccLogger value) {
		logger = value;
	}
	
	/*
	 * join lhs + rhs with single separator
	 */
	private String joiner(String lhs,String rhs) {
		String retVal = lhs;
		if(lhs != null) {
			if(rhs != null) {
				retVal = lhs+File.separator+rhs;
				retVal = retVal.replaceAll("/+","/");
			}
		}
		return retVal;
	}
	
	/*
	 * produce command line string
	 */
	private String getCommand() {
		String ducc_home = IDuccEnv.DUCC_HOME_DIR;
		String admin_ducc_master_py = "admin"+File.separator+"ducc_head_mode.py";
		String retVal = joiner(ducc_home,admin_ducc_master_py);
		return retVal;
	}
	
	/*
	 * run /<ducc_home>/admin/ducc_head_mode.py
	 */
	private String runCmd() {
		String location = "runCmd";
		String retVal = null;
		try {
			String command = getCommand();
			logger.info(location, jobid, command);
			String[] pb_command = { command };
			ProcessBuilder pb = new ProcessBuilder( pb_command );
			Process p = pb.start();
			InputStream pOut = p.getInputStream();
			InputStreamReader isr = new InputStreamReader(pOut);
			BufferedReader br = new BufferedReader(isr);
	        String line;
	        StringBuffer sb = new StringBuffer();
	        while ((line = br.readLine()) != null) {
	        	sb.append(line);
	        	logger.info(location, jobid, line);
	        }
	        retVal = sb.toString();
	        int rc = p.waitFor();
	        logger.info(location, jobid, rc);
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
		}
		return retVal;
	}
	
	
	/*
	 * get ducc head mode { result expected is "master" or "backup" }
	 */
	@Override
	public String get_ducc_head_mode() {
		String curr = state.get();
		return curr;
	}
	
	/*
	 * query ducc head mode { result expected is "master" or "backup" }
	 */
	private void query_ducc_head_mode() {
		String location = "query_ducc_head_mode";
		String prev = state.get();
		String curr = runCmd();
		if(curr == null) {
			curr = master;
		}
		state.set(curr);
		logger.debug(location, jobid, prev, "->", curr);
	}
	
	/*
	 * true if a == b and a != null and b != null
	 * false otherwise
	 */
	private boolean isEqual(String a, String b) {
		boolean retVal = false;
		if(a != null) {
			if(b != null) {
				retVal = a.equals(b);
			}
		}
		return retVal;
	}
	
	/*
	 * true if "unspecified"
	 */
	@Override
	public boolean is_ducc_head_unspecified() {
		boolean retVal = false;
		String curr = state.get();
		retVal = isEqual(curr, unspecified);
		return retVal;
	}
	
	/*
	 * true if "master" or "backup"
	 */
	@Override
	public boolean is_ducc_head_reliable() {
		boolean retVal = false;
		String curr = state.get();
		retVal = isEqual(curr, master) || isEqual(curr, backup);
		return retVal;
	}
	
	/*
	 * true if "master"
	 */
	@Override
	public boolean is_ducc_head_master() {
		boolean retVal = false;
		String curr = state.get();
		retVal = isEqual(curr, master);
		return retVal;
	}
	
	/*
	 * true if "master" or "unspecified"
	 */
	@Override
	public boolean is_ducc_head_virtual_master() {
		boolean retVal = false;
		String curr = state.get();
		retVal = isEqual(curr, master) || isEqual(curr, unspecified);
		return retVal;
	}
	
	/*
	 * true if "backup"
	 */
	@Override
	public boolean is_ducc_head_backup() {
		boolean retVal = false;
		String curr = state.get();
		retVal = isEqual(curr, backup);
		return retVal;
	}
	
	/*
	 * do a transition
	 */
	@Override
	public DuccHeadTransition transition() {
		String location = "transition";
		DuccHeadTransition retVal = DuccHeadTransition.unspecified;
		try {
			synchronized(ADuccHead.class) {
				String prev = state.get();
				query_ducc_head_mode();
				String curr = state.get();
				
				if(isEqual(prev,master)) {
					if(isEqual(curr,master)) {
						retVal = DuccHeadTransition.master_to_master;
					}
					else {
						retVal = DuccHeadTransition.master_to_backup;
						logger.warn(location, jobid, retVal);
					}
				}
				else if(isEqual(prev,backup)) {
					if(isEqual(curr,master)) {
						retVal = DuccHeadTransition.backup_to_master;
						logger.warn(location, jobid, retVal);
					}
					else {
						retVal = DuccHeadTransition.backup_to_backup;
					}
				}
				else if(isEqual(prev,initial)) {
					if(isEqual(curr,master)) {
						retVal = DuccHeadTransition.master_to_master;
					}
					else if(isEqual(curr,backup)) {
						retVal = DuccHeadTransition.backup_to_backup;
					}
					else {
						retVal = DuccHeadTransition.unspecified;
					}
					logger.info(location, jobid, retVal);
				}
				else {
					retVal = DuccHeadTransition.unspecified;
				}
				logger.debug(location, jobid, retVal, prev, curr);
			}
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
		}
		return retVal;
	}
}
