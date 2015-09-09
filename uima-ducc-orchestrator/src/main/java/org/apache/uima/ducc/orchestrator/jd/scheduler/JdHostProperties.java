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
package org.apache.uima.ducc.orchestrator.jd.scheduler;

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.DuccPropertiesResolver;
import org.apache.uima.ducc.common.utils.id.DuccId;

public class JdHostProperties {

	private static final DuccLogger logger = DuccLoggerComponents.getOrLogger(JdHostProperties.class.getName());
	private static final DuccId jobid = null;

	private static DuccPropertiesResolver dpr = DuccPropertiesResolver.getInstance();
	
	private String hostClass = null;				// name, nominally "JobDriver"
	private String hostDescription = null;			// text, nominally "Job Driver"
	private String hostMemorySize = null;			// size, with postfix { KB, MB, GB, TB }
	private String hostNumberOfMachines = null;
	private String hostUser = null;					// name, nominally "System"
	private String jdShareQuantum = null;			// size, in MB
	private String slicesReserve = null;			// count, trigger for more
	
	public JdHostProperties() {
		update();
	}
	
	public String getHostClass() {
		return hostClass;
	}
	
	public String getHostDescription() {
		return hostDescription;
	}
	
	public String getHostMemorySize() {
		return hostMemorySize;
	}
	
	public String getNumberOfMachines() {
		return hostNumberOfMachines;
	}
	
	public String getHostUser() {
		return hostUser;
	}
	
	public String getJdShareQuantum() {
		return jdShareQuantum;
	}
	
	public String getSlicesReserve() {
		return slicesReserve;
	}
	
	private void update() {
		String methodName = "update";
		String key = null;
		String value = null;
		//
		key = DuccPropertiesResolver.ducc_jd_host_class;
		value = dpr.getFileProperty(key);
		if(value != null) {
			if(hostClass == null) {
				hostClass = value;
				logger.trace(methodName, jobid, key+"="+value);
			}
			else if(!hostClass.equals(value)) {
				hostClass = value;
				logger.trace(methodName, jobid, key+"="+value);
			}
		}
		//
		key = DuccPropertiesResolver.ducc_jd_host_description;
		value = dpr.getFileProperty(key);
		if(value != null) {
			if(hostDescription == null) {
				hostDescription = value;
				logger.trace(methodName, jobid, key+"="+value);
			}
			else if(!hostDescription.equals(value)) {
				hostDescription = value;
				logger.trace(methodName, jobid, key+"="+value);
			}
		}
		//
		key = DuccPropertiesResolver.ducc_jd_host_memory_size;
		value = dpr.getFileProperty(key);
		if(value != null) {
			if(hostMemorySize == null) {
				hostMemorySize = value;
				logger.trace(methodName, jobid, key+"="+value);
			}
			else if(!hostMemorySize.equals(value)) {
				hostMemorySize = value;
				logger.trace(methodName, jobid, key+"="+value);
			}
		}
		//
		key = DuccPropertiesResolver.ducc_jd_host_number_of_machines;
		value = dpr.getFileProperty(key);
		if(value != null) {
			if(hostNumberOfMachines == null) {
				hostNumberOfMachines = value;
				logger.trace(methodName, jobid, key+"="+value);
			}
			else if(!hostNumberOfMachines.equals(value)) {
				hostNumberOfMachines = value;
				logger.trace(methodName, jobid, key+"="+value);
			}
		}
		//
		key = DuccPropertiesResolver.ducc_jd_host_user;
		value = dpr.getFileProperty(key);
		if(value != null) {
			if(hostUser == null) {
				hostUser = value;
				logger.trace(methodName, jobid, key+"="+value);
			}
			else if(!hostUser.equals(value)) {
				hostUser = value;
				logger.trace(methodName, jobid, key+"="+value);
			}
		}
		//
		key = DuccPropertiesResolver.ducc_jd_share_quantum;
		value = dpr.getFileProperty(key);
		if(value != null) {
			if(jdShareQuantum == null) {
				jdShareQuantum = value;
				logger.trace(methodName, jobid, key+"="+value);
			}
			else if(!jdShareQuantum.equals(value)) {
				jdShareQuantum = value;
				logger.trace(methodName, jobid, key+"="+value);
			}
		}
		//
		key = DuccPropertiesResolver.ducc_jd_share_quantum_reserve_count;
		value = dpr.getFileProperty(key);
		if(value != null) {
			if(slicesReserve == null) {
				slicesReserve = value;
				logger.trace(methodName, jobid, key+"="+value);
			}
			else if(!slicesReserve.equals(value)) {
				slicesReserve = value;
				logger.trace(methodName, jobid, key+"="+value);
			}
		}
	}
}
