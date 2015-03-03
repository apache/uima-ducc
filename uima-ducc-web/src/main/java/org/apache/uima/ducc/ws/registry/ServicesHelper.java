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
package org.apache.uima.ducc.ws.registry;

import java.util.ArrayList;
import java.util.Properties;

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.common.DuccWorkJob;
import org.apache.uima.ducc.transport.event.common.DuccWorkMap;
import org.apache.uima.ducc.transport.event.common.IDuccProcess;
import org.apache.uima.ducc.transport.event.common.IDuccProcessMap;
import org.apache.uima.ducc.ws.DuccData;
import org.apache.uima.ducc.ws.DuccDataHelper;

public class ServicesHelper {
	
	private static DuccLogger logger = DuccLoggerComponents.getWsLogger(ServicesHelper.class.getName());
	private static DuccId jobid = null;
	
	private static ServicesHelper instance = new ServicesHelper();
	
	public static ServicesHelper getInstance() {
		return instance;
	}
	
	public String getId(ServicesRegistry servicesRegistry, Properties propertiesMeta) {
		String retVal = "";
		if(propertiesMeta != null) {
			if(propertiesMeta.containsKey(IServicesRegistry.numeric_id)) {
				String value = propertiesMeta.getProperty(IServicesRegistry.numeric_id);
				if(value != null) {
					retVal = value;
				}
			}
		}
		return retVal;
	}
	
	public ArrayList<String> getImplementors(ServicesRegistry servicesRegistry, Properties propertiesMeta) {
		ArrayList<String> retVal = new ArrayList<String>();
		if(propertiesMeta != null) {
			if(propertiesMeta.containsKey(IServicesRegistry.implementors)) {
                // UIMA-4258, use common implementors parser
                String[] implementors = DuccDataHelper.parseServiceIds(propertiesMeta);
				for(String implementor : implementors) {
					retVal.add(implementor);
				}
			}
		}
		return retVal;
	}
	public long getDeployments(ServicesRegistry servicesRegistry, Properties propertiesMeta) {
		ArrayList<String> implementors = getImplementors(servicesRegistry, propertiesMeta);
		return implementors.size();
	}
	
	public ArrayList<DuccWorkJob> getServicesList(ServicesRegistry servicesRegistry, Properties propertiesMeta) {
		ArrayList<DuccWorkJob> retVal = new ArrayList<DuccWorkJob>();
		ArrayList<String> implementors = getImplementors(servicesRegistry, propertiesMeta);
		DuccWorkMap duccWorkMap = DuccData.getInstance().get();
		retVal = duccWorkMap.getServices(implementors);
		return retVal;
	}
	
	public long getPgin(ServicesRegistry servicesRegistry, Properties propertiesMeta) {
		String location = "getPgin";
		String id = getId(servicesRegistry, propertiesMeta);
		long retVal = 0;
		ArrayList<DuccWorkJob> servicesList = getServicesList(servicesRegistry, propertiesMeta);
		for(DuccWorkJob service : servicesList) {
			IDuccProcessMap map = service.getProcessMap();
			for(DuccId key : map.keySet()) {
				IDuccProcess process = map.get(key);
				if(process.isActive()) {
					retVal += process.getMajorFaults();
				}
			}
		}
		logger.debug(location, jobid, id, retVal);
		return retVal;
	}
	
	public long getSwap(ServicesRegistry servicesRegistry, Properties propertiesMeta) {
		String location = "getSwap";
		String id = getId(servicesRegistry, propertiesMeta);
		long retVal = 0;
		ArrayList<DuccWorkJob> servicesList = getServicesList(servicesRegistry, propertiesMeta);
		for(DuccWorkJob service : servicesList) {
			IDuccProcessMap map = service.getProcessMap();
			for(DuccId key : map.keySet()) {
				IDuccProcess process = map.get(key);
				if(process.isActive()) {
					retVal += process.getSwapUsage();
				}
			}
		}
		logger.debug(location, jobid, id, retVal);
		return retVal;
	}
	
	public long getSwapMax(ServicesRegistry servicesRegistry, Properties propertiesMeta) {
		String location = "getSwapMax";
		String id = getId(servicesRegistry, propertiesMeta);
		long retVal = 0;
		ArrayList<DuccWorkJob> servicesList = getServicesList(servicesRegistry, propertiesMeta);
		for(DuccWorkJob service : servicesList) {
			IDuccProcessMap map = service.getProcessMap();
			for(DuccId key : map.keySet()) {
				IDuccProcess process = map.get(key);
				if(process.isActive()) {
					retVal += process.getSwapUsageMax();
				}
			}
		}
		logger.debug(location, jobid, id, retVal);
		return retVal;
	}
}
