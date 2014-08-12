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

import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.common.DuccWorkJob;
import org.apache.uima.ducc.transport.event.common.DuccWorkMap;
import org.apache.uima.ducc.transport.event.common.IDuccProcess;
import org.apache.uima.ducc.transport.event.common.IDuccProcessMap;
import org.apache.uima.ducc.ws.DuccData;

public class ServicesHelper {
	
	private static ServicesHelper instance = new ServicesHelper();
	
	public static ServicesHelper getInstance() {
		return instance;
	}
	
	public String getStateHover(IServiceAdapter serviceAdapter) {
		StringBuffer retVal = new StringBuffer();
		String popUp = serviceAdapter.getPopup();
		if(popUp != null) {
			if(popUp.length() > 0) {
				if(retVal.length() > 0) {
					retVal.append("\n");
				}
				retVal.append(popUp);
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
			if(statistics.length() > 0) {
				if(retVal.length() > 0) {
					retVal.append("\n");
				}
				retVal.append(statistics);
			}
		}
		return retVal.toString();
	}
	
	public ArrayList<DuccWorkJob> getServicesList(IServiceAdapter serviceAdapter) {
		ArrayList<DuccWorkJob> retVal = new ArrayList<DuccWorkJob>();
		ArrayList<String> implementors = serviceAdapter.getImplementors();
		DuccWorkMap duccWorkMap = DuccData.getInstance().get();
		retVal = duccWorkMap.getServices(implementors);
		return retVal;
	}
	
	public long getPgIn(IServiceAdapter serviceAdapter) {
		long retVal = 0;
		ArrayList<DuccWorkJob> servicesList = getServicesList(serviceAdapter);
		for(DuccWorkJob service : servicesList) {
			IDuccProcessMap map = service.getProcessMap();
			for(DuccId key : map.keySet()) {
				IDuccProcess process = map.get(key);
				if(process.isActive()) {
					retVal += process.getMajorFaults();
				}
			}
		}
		return retVal;
	}
	
	public long getSwap(IServiceAdapter serviceAdapter) {
		long retVal = 0;
		ArrayList<DuccWorkJob> servicesList = getServicesList(serviceAdapter);
		for(DuccWorkJob service : servicesList) {
			IDuccProcessMap map = service.getProcessMap();
			for(DuccId key : map.keySet()) {
				IDuccProcess process = map.get(key);
				if(process.isActive()) {
					retVal += process.getSwapUsage();
				}
			}
		}
		return retVal;
	}
	
	public long getSwapMax(IServiceAdapter serviceAdapter) {
		long retVal = 0;
		ArrayList<DuccWorkJob> servicesList = getServicesList(serviceAdapter);
		for(DuccWorkJob service : servicesList) {
			IDuccProcessMap map = service.getProcessMap();
			for(DuccId key : map.keySet()) {
				IDuccProcess process = map.get(key);
				if(process.isActive()) {
					retVal += process.getSwapUsageMax();
				}
			}
		}
		return retVal;
	}
}
