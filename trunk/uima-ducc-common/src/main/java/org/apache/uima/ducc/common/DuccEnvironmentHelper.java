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
package org.apache.uima.ducc.common;

import java.util.Map;

import org.apache.uima.ducc.common.main.DuccService;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.id.DuccId;

public class DuccEnvironmentHelper {
	
	private static DuccLogger logger = DuccService.getDuccLogger(DuccEnvironmentHelper.class.getName());
	private static DuccId jobId = null;
	
	// Boolean
	
	public static String DUCC_TOLERATE_SERIAL_VERSION_UID_MISMATCH = "DUCC_TOLERATE_SERIAL_VERSION_UID_MISMATCH";
	
	private static Boolean tolerate_serial_version_uid_mismatch = null;
	
	public static boolean isTolerateSerialVersionUidMismatch() {
		String location = "isTolerateSerialVersionUidMismatch";
		boolean retVal = false;
		if(tolerate_serial_version_uid_mismatch == null) {
			try {
				Map<String, String> env = System.getenv();
				if(env != null) {
					String key = DUCC_TOLERATE_SERIAL_VERSION_UID_MISMATCH;
					if(env.containsKey(key)) {
						String value = env.get(key);
						logger.info(location, jobId, key+"="+value);
						if(value != null) {
							tolerate_serial_version_uid_mismatch = Boolean.valueOf(value);
							retVal = tolerate_serial_version_uid_mismatch.booleanValue();
						}
					}
					else {
						logger.debug(location, jobId, key+" "+"not found in environment");
					}
				}
			}
			catch(Exception e) {
				logger.error(location, jobId, e);
			}
		}
		else {
			retVal = tolerate_serial_version_uid_mismatch.booleanValue();
		}
		return retVal;
	}
	
}
