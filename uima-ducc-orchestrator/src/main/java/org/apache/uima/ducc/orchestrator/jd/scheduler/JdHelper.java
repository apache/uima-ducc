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

import java.util.Map.Entry;

import org.apache.uima.ducc.common.NodeIdentity;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.common.IDuccReservation;
import org.apache.uima.ducc.transport.event.common.IDuccReservationMap;
import org.apache.uima.ducc.transport.event.common.IDuccWorkReservation;

public class JdHelper {

	public static NodeIdentity getNodeIdentity(IDuccWorkReservation dwr) {
		NodeIdentity retVal = null;
		if(dwr != null) {
			IDuccReservationMap rm = dwr.getReservationMap();
			if(rm != null) {
				for(Entry<DuccId, IDuccReservation> entry : rm.entrySet()) {
					retVal = entry.getValue().getNodeIdentity();
					break;
				}
			}
		}
		return retVal;
	}
	
	public static long KB = 1024;
	public static long MB = 1024*KB;
	public static long GB = 1024*MB;
	public static long TB = 1024*GB;
	
	public static long parseSize(String value) {
		long size = 0;
		try {
			String tValue = value.trim();
			if(tValue.endsWith("KB")) {
				tValue = tValue.substring(0, tValue.length() - 2);
				size = new Long(tValue) * KB;
			}
			else if(tValue.endsWith("MB")) {
				tValue = tValue.substring(0, tValue.length() - 2);
				size = new Long(tValue) * MB;
			}
			else if(tValue.endsWith("GB")) {
				tValue = tValue.substring(0, tValue.length() - 2);
				size = new Long(tValue) * GB;
			}
			else if(tValue.endsWith("TB")) {
				tValue = tValue.substring(0, tValue.length() - 2);
				size = new Long(tValue) * TB;
			}
			else {
				size = new Long(tValue);
			}
		}
		catch(Exception e) {
		}
		return size;
	}
	
	public static long parseCount(String value) {
		long count = 0;
		try {
			String tValue = value.trim();
			count = new Long(tValue);
		}
		catch(Exception e) {
		}
		return count;	
	}
	
	public static long getShareSize(JdHostProperties jdHostProperties) {
		long retVal = 0;
		try {
			if(jdHostProperties != null) {
				String value = jdHostProperties.getHostMemorySize();
				retVal = parseSize(value);
			}
		}
		catch(Exception e) {
		}
		return retVal;
	}
	
	public static long getSliceSize(JdHostProperties jdHostProperties) {
		long retVal = 0;
		if(jdHostProperties != null) {
			String value = jdHostProperties.getShareQuantum();
			retVal = parseSize(value+"MB");
		}
		return retVal;
	}
	
	public static long SlicesReserveDefault = 2;
	
	public static long getSlicesReserve(JdHostProperties jdHostProperties) {
		long retVal = 0;
		if(jdHostProperties != null) {
			String value = jdHostProperties.getSlicesReserve();
			retVal = parseCount(value);
		}
		if(retVal < 1) {
			retVal = SlicesReserveDefault;
		}
		return retVal;
	}

}
