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
package org.apache.uima.ducc.common.utils;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.id.DuccId;

public class InetHelper {
	
	private static DuccLogger duccLogger = DuccLogger.getLogger(InetHelper.class);
	private static DuccId jobid = null;
	
	public static String getCanonicalHostName() {
		String methodName = "getCanonicalHostName";
		String hostname = "localhost";
		try {
			InetAddress inetAddress = InetAddress.getLocalHost();
			hostname = inetAddress.getCanonicalHostName();
		}
		catch(Exception e) {
			duccLogger.error(methodName, jobid, e);
		}
		return hostname;
	}
	
	public static String getShortHostName() {
		String methodName = "getShortHostName";
		String hostname = "localhost";
		try {
			String canonicalHostName = getCanonicalHostName();
			if(canonicalHostName != null) {
				hostname = canonicalHostName;
				if(canonicalHostName.contains(".")) {
					hostname = canonicalHostName.split("\\.")[0];
				}
			}
		}
		catch(Exception e) {
			duccLogger.error(methodName, jobid, e);
		}
		return hostname;
	}
	
	public static String getHostName() {
		return getShortHostName();
	}
	
	/*
	 * get list of corresponding IPs for given list of host names
	 */
	public static String[] getIP(String[] hostList) {
		List<String> ipList = new ArrayList<String>();
		try {
			if(hostList != null) {
				for(String hostName : hostList) {
					InetAddress inetAddress = InetAddress.getByName(hostName);
					StringBuffer sb = new StringBuffer();
					byte[] byteAddress = inetAddress.getAddress();
					if(byteAddress != null) {
						for(int i=0; i<byteAddress.length; i++) {
							if(i > 0) {
								sb.append(".");
							}
							sb.append(byteAddress[i] & 0xFF);
						}
						ipList.add(sb.toString());
					}
				}
			}
		}
		catch(Exception e) {
			// Oh well...
		}
		return ipList.toArray(new String[0]);
	}
	

}
