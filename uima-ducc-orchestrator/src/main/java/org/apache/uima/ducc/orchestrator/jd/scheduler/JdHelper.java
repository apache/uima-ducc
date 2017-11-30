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
import org.apache.uima.ducc.common.SizeBytes;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.common.IDuccReservation;
import org.apache.uima.ducc.transport.event.common.IDuccReservationMap;
import org.apache.uima.ducc.transport.event.common.IDuccWorkReservation;

public class JdHelper {
	
	private static DuccLogger logger = new DuccLogger(JdHelper.class);
	private static DuccId jobid = null;
	
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
	
	private static long parseCount(String value) {
		long count = -1;
		try {
			String tValue = value.trim();
			count = new Long(tValue);
		}
		catch(Exception e) {
		}
		return count;	
	}
	
	public static SizeBytes getReservationSize(IDuccWorkReservation dwr) {
		String location = "getReservationSize";
		SizeBytes retVal = null;
		try {
			long sizeBytes = dwr.getSchedulingInfo().getMemorySizeAllocatedInBytes();
			retVal = new SizeBytes(SizeBytes.Type.Bytes, sizeBytes);
			logger.trace(location, dwr.getDuccId(), retVal.getGBytes()+" GB");
		}
		catch(Exception e) {
		}
		return retVal;
	}
	
	public static SizeBytes getSliceSize(JdHostProperties jdHostProperties) {
		String location = "getSliceSize";
		SizeBytes retVal = null;
		if(jdHostProperties != null) {
			String value = jdHostProperties.getJdShareQuantum();
			retVal = new SizeBytes(SizeBytes.Type.MBytes, Long.parseLong(value));
			logger.trace(location, jobid, retVal.getMBytes()+" MB");
		}
		return retVal;
	}
	
	public static long SlicesReserveDefault = 2;
	
	public static long getSlicesReserve(JdHostProperties jdHostProperties) {
		String location = "getSlicesReserve";
		long retVal = 0;
		if(jdHostProperties != null) {
			String value = jdHostProperties.getSlicesReserve();
			retVal = parseCount(value);
			logger.trace(location, jobid, retVal+" "+"specified");
		}
		if(retVal < 0) {
			retVal = SlicesReserveDefault;
			logger.trace(location, jobid, retVal+" "+"default");
		}
		return retVal;
	}

}
