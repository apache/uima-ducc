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
package org.apache.uima.ducc.orchestrator.factory;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.DuccPropertiesResolver;
import org.apache.uima.ducc.common.utils.id.DuccId;

public class JobFactoryHelper {
	
	private static final DuccLogger logger = DuccLoggerComponents.getOrLogger(JobFactoryHelper.class.getName());
	private static final DuccId jobid = null;
	
	public static long KB = 1024;
	public static long MB = 1024*KB;
	public static long GB = 1024*MB;
	
	public static long defaultBytesSizeJobProcess = 1*GB;
	public static long defaultBytesSizeJobDriver = 300*MB;
	
	public static long getDefaultByteSizeJobProcess() {
		return defaultBytesSizeJobProcess;
	}
	
	public static long getByteSizeJobProcess(String process_memory_size) {
		String location = "getByteSizeJobProcess";
		long retVal = getDefaultByteSizeJobProcess();
		try {
			retVal = Long.parseLong(process_memory_size);
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
		}
		logger.debug(location, jobid, retVal);
		return retVal;
	}
	
	public static long getDefaultByteSizeJobDriver() {
		return defaultBytesSizeJobDriver;
	}
	
	private static AtomicLong jdSize = new AtomicLong(0);
	
	public static long getByteSizeJobDriver() {
		String location = "getByteSizeJobDriver";
		long retVal = getDefaultByteSizeJobDriver();
		try {
			String ducc_jd_share_quantum = DuccPropertiesResolver.getInstance().getFileProperty(DuccPropertiesResolver.ducc_jd_share_quantum);
			long sizeNew = Long.parseLong(ducc_jd_share_quantum) * MB;
			if(sizeNew > 0) {
				long sizeOld = jdSize.get();
				if(sizeNew != sizeOld) {
					jdSize.set(sizeNew);
					logger.info(location, jobid, "old: "+sizeOld+" "+"new: "+sizeNew);
				}
				retVal = sizeNew;
			}
			else {
				logger.warn(location, jobid, DuccPropertiesResolver.ducc_jd_share_quantum+"="+ducc_jd_share_quantum+" "+"invalid");
			}
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
		}
		logger.debug(location, jobid, retVal);
		return retVal;
	}
}
