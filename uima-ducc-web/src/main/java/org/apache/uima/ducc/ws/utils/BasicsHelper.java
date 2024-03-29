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
package org.apache.uima.ducc.ws.utils;

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.id.DuccId;

public class BasicsHelper {

	private static DuccLogger logger = DuccLogger.getLogger(BasicsHelper.class);
	private static DuccId jobid = null;
	
	public static int safeString2Int(String value) {
		return safeString2Int(value,0);
	}
	
	public static int safeString2Int(String value, int defaultValue) {
		String location = "safeString2Int";
		int retVal = defaultValue;
		try {
			retVal = Integer.parseInt(value);
		}
		catch(Exception e) {
			logger.debug(location, jobid, e);
		}
		return retVal;
	}
	
	public static boolean isSim() {
		String location = "isSim";
		DuccId jobid = null;
		boolean retVal = false;
		String ducc_sim_env = System.getenv("DUCC_SIM");
		if(ducc_sim_env != null) {
			logger.debug(location, jobid, ducc_sim_env);
			Boolean bool = new Boolean(ducc_sim_env);
			retVal = bool.booleanValue();
		}
		return retVal;
	}
}
