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
package org.apache.uima.ducc.ws.registry.sort;

import java.util.ArrayList;
import java.util.List;

import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.common.DuccWorkJob;
import org.apache.uima.ducc.transport.event.common.IDuccProcess;
import org.apache.uima.ducc.transport.event.common.IDuccProcessMap;
import org.apache.uima.ducc.transport.event.common.IDuccWorkMap;
import org.apache.uima.ducc.ws.DuccData;

public class ServicesHelper {
	
	private static ServicesHelper instance = new ServicesHelper();
	
	public static ServicesHelper getInstance() {
		return instance;
	}
	
	public String getStateHover(IServiceAdapter serviceAdapter) {
		StringBuffer retVal = new StringBuffer();
		String pingerStatus = serviceAdapter.getPingerStatus();
		if(pingerStatus != null) {
			if(pingerStatus.length() > 0) {
				if(retVal.length() > 0) {
					retVal.append("\n");
				}
				retVal.append(pingerStatus);
			}
		}
		if(!serviceAdapter.isPingActive()) {
			if(serviceAdapter.isServiceIssue()) {
				if(serviceAdapter.isStateActive()) {
					if(retVal.length() > 0) {
						retVal.append("\n");
					}
					//retVal.append("Service not responding to pinger");  // UIMA-4177
				}
			}
		}
		String errorText = serviceAdapter.getErrorText();
		if(errorText != null) {
			if(errorText.length() > 0) {
				if(retVal.length() > 0) {
					retVal.append("\n");
				}
				retVal.append(errorText);
			}
		}
		String statistics = serviceAdapter.getStatistics();
		if(statistics != null) {
			if(!statistics.equals("N/A")) {
				if(statistics.length() > 0) {
					if(retVal.length() > 0) {
						retVal.append("\n");
					}
					//retVal.append("<pre>");
					retVal.append(statistics);
					//retVal.append("</pre>");
				}
			}
		}
		return retVal.toString();
	}
	
	public List<DuccWorkJob> getServicesList(IServiceAdapter serviceAdapter) {
		List<DuccWorkJob> retVal = new ArrayList<DuccWorkJob>();
		List<String> implementors = serviceAdapter.getImplementors();
		IDuccWorkMap duccWorkMap = DuccData.getInstance().get();
		retVal = duccWorkMap.getServicesList(implementors);
		return retVal;
	}
	
	/*
	 * return actual count if all non-negative (all have cgroups)
	 * return -1 if all -1 (none have cgroups)
	 * return -2 if mixed (some do and some don't have cgroups) but retVal would have been == 0
	 * return -3 if mixed (some do and some don't have cgroups) but retVal would have been > 0
	 */
	public long getPgIn(IServiceAdapter serviceAdapter) {
		long retVal = 0;
		boolean flagCgroup = false;
		boolean flagNoCgroup = false;
		List<DuccWorkJob> servicesList = getServicesList(serviceAdapter);
		for(DuccWorkJob service : servicesList) {
			IDuccProcessMap map = service.getProcessMap();
			for(DuccId key : map.keySet()) {
				IDuccProcess process = map.get(key);
				if(process.isActive()) {
					long value = process.getMajorFaults();
					if(value < 0) {
						flagNoCgroup = true;
					}
					else {
						flagCgroup = true;
						retVal += value;
					}
				}
			}
		}
		if(flagCgroup && flagNoCgroup) {
			if(retVal > 0){
				retVal = -3;
			}
			else {
				retVal = -2;
			}
		}
		else if(flagNoCgroup) {
			retVal = -1;
		}
		return retVal;
	}
	
	/*
	 * return actual count if all non-negative (all have cgroups)
	 * return -1 if all -1 (none have cgroups)
	 * return -2 if mixed (some do and some don't have cgroups) but retVal would have been == 0
	 * return -3 if mixed (some do and some don't have cgroups) but retVal would have been > 0
	 */
	public long getSwap(IServiceAdapter serviceAdapter) {
		long retVal = 0;
		boolean flagCgroup = false;
		boolean flagNoCgroup = false;
		List<DuccWorkJob> servicesList = getServicesList(serviceAdapter);
		for(DuccWorkJob service : servicesList) {
			IDuccProcessMap map = service.getProcessMap();
			for(DuccId key : map.keySet()) {
				IDuccProcess process = map.get(key);
				if(process.isActive()) {
					long value = process.getSwapUsage();
					if(value < 0) {
						flagNoCgroup = true;
					}
					else {
						flagCgroup = true;
						retVal += value;
					}
				}
			}
		}
		if(flagCgroup && flagNoCgroup) {
			if(retVal > 0){
				retVal = -3;
			}
			else {
				retVal = -2;
			}
		}
		else if(flagNoCgroup) {
			retVal = -1;
		}
		return retVal;
	}
	
	/*
	 * return actual count if all non-negative (all have cgroups)
	 * return -1 if all -1 (none have cgroups)
	 * return -2 if mixed (some do and some don't have cgroups) but retVal would have been == 0
	 * return -3 if mixed (some do and some don't have cgroups) but retVal would have been > 0
	 */
	public long getSwapMax(IServiceAdapter serviceAdapter) {
		long retVal = 0;
		boolean flagCgroup = false;
		boolean flagNoCgroup = false;
		List<DuccWorkJob> servicesList = getServicesList(serviceAdapter);
		for(DuccWorkJob service : servicesList) {
			IDuccProcessMap map = service.getProcessMap();
			for(DuccId key : map.keySet()) {
				IDuccProcess process = map.get(key);
				if(process.isActive()) {
					long value = process.getSwapUsageMax();
					if(value < 0) {
						flagNoCgroup = true;
					}
					else {
						flagCgroup = true;
						retVal += value;
					}
				}
			}
		}
		if(flagCgroup && flagNoCgroup) {
			if(retVal > 0){
				retVal = -3;
			}
			else {
				retVal = -2;
			}
		}
		else if(flagNoCgroup) {
			retVal = -1;
		}
		return retVal;
	}
}
