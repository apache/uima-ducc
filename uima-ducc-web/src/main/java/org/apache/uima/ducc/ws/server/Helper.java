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
package org.apache.uima.ducc.ws.server;

import java.util.Map;
import java.util.Map.Entry;

import org.apache.uima.ducc.common.ConvertSafely;
import org.apache.uima.ducc.common.SizeBytes;
import org.apache.uima.ducc.common.SizeBytes.Type;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.ws.DuccMachinesData;
import org.apache.uima.ducc.ws.DuccMachinesDataHelper;
import org.apache.uima.ducc.ws.MachineInfo;
import org.apache.uima.ducc.ws.types.NodeId;

public class Helper {
	
	private static DuccLogger duccLogger = DuccLoggerComponents.getWsLogger(Helper.class.getName());
	private static DuccId jobid = null;
	
	public static SizeBytes getSummaryReserve() {
		String methodName = "getSummaryReserve";
		long sumReserve = 0;
		DuccMachinesData instance = DuccMachinesData.getInstance();
		Map<MachineInfo, NodeId> machines = instance.getMachines();
		if(!machines.isEmpty()) {
			for(Entry<MachineInfo, NodeId> entry : machines.entrySet()) {
				MachineInfo machineInfo = entry.getKey();
				if(DuccMachinesDataHelper.isUp(machineInfo)) {
					try {
						// Calculate total for Memory(GB):usable
						sumReserve += ConvertSafely.String2Long(machineInfo.getMemReserve());
					}
					catch(Exception e) {
						duccLogger.trace(methodName, jobid, e);
					}
				}
			}
		}
		SizeBytes retVal = new SizeBytes(Type.GBytes, sumReserve);
		return retVal;
	}
}
