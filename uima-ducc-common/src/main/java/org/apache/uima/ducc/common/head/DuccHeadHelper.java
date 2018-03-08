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

import org.apache.uima.ducc.common.utils.DuccPropertiesHelper;

public class DuccHeadHelper {
	
	public static boolean isVirtualIpAddress() {
		boolean retVal = false;
		String vip = getVirtualIpAddress();
		if(vip != null) {
			retVal = true;
		}
		return retVal;
	}
	
	public static String getVirtualIpAddress() {
		String retVal = null;
		String ipDevice = DuccPropertiesHelper.getDuccHeadVirtualIpDevice();
		String ipAddress = DuccPropertiesHelper.getDuccHeadVirtualIpAddress();
		if(ipDevice.length() > 0) {
			if(ipAddress.length() > 0) {
				if(!ipAddress.equals("0.0.0.0")) {
					retVal = ipAddress;
				}
			}
		}
		return retVal;
	}
	
	public static String getVirtualHost(String defaultHost) {
		String retVal = defaultHost;
		String virtualHost = getVirtualIpAddress();
		if(virtualHost != null) {
			retVal = virtualHost;
		}
		return retVal;
	}
	
}
