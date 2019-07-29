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

import java.net.InetAddress;

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.ws.types.NodeId;

public class DnsHelper {
	
	private static DuccLogger duccLogger = DuccLogger.getLogger(DnsHelper.class);
	
	public static boolean isKnowable() {
		String location = "isKnowable";
		DuccId jobid = null;
		boolean retVal = false;
		try {
			String result = InetAddress.getLocalHost().getHostName();
			if(result != null) {
				if(!result.isEmpty()) {
					retVal = true;
				}
			}
		}
		catch(Exception e) {
			duccLogger.debug(location, jobid, e);
		}
		return retVal;
	}
	
	// -1 = unknowable
	//  0 = unknown
	//  1 = known
	
	public static int isKnownHost(String hostname) {
		String location = "isKnownHost";
		DuccId jobid = null;
		int retVal = -1;
		if(hostname != null) {
			if(isKnowable()) {
				retVal = 0;
				try {
					InetAddress inetAddress = InetAddress.getByName(hostname);
					if(inetAddress != null) {
						retVal = 1;
					}
				}
				catch(Exception e) {
					duccLogger.debug(location, jobid, e);
				}
			}
			String text = hostname+":"+retVal;
			duccLogger.debug(location, jobid, text);
		}
		return retVal;
	}
	
	public static boolean isKnownHost(NodeId nodeId) {
		boolean retVal = true;
		if(nodeId != null) {
			if(isKnownHost(nodeId.getLongName()) == 0) {
				retVal = false;
			}
		}
		return retVal;
	}
	
	public static void main(String[] args) {
		for(String arg : args) {
			System.out.println(arg+" "+isKnownHost(arg));
		}
	}
}
