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
import java.util.Properties;
import java.util.TreeMap;

import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.sm.IService.ServiceState;
import org.apache.uima.ducc.ws.DuccDataHelper;
import org.apache.uima.ducc.ws.registry.ServiceInterpreter;
import org.apache.uima.ducc.ws.registry.ServiceInterpreter.StartMode;
import org.apache.uima.ducc.ws.registry.ServicesRegistry;

public class ServiceAdapter implements IServiceAdapter {

	private ServiceInterpreter si;
	
	private long pgIn = 0;
	private long swap = 0;
	private long swapMax = 0;
	
	private ArrayList<String> dependentJobs = new ArrayList<String>();
	private ArrayList<String> dependentServices = new ArrayList<String>();
	private ArrayList<String> dependentReservations = new ArrayList<String>();
	
	public ServiceAdapter(Properties svc, Properties meta) {
		si = new ServiceInterpreter(svc, meta);
		init();
	}

	private void init() {
		initPgIn();
		initSwap();
		initSwapMax();
		initDependentJobs();
		initDependentServices();
		initDependentReservations();
	}
	
	@Override
	public Properties getSvc() {
		return si.getSvc();
	}
	
	@Override 
	public Properties getMeta() {
		return si.getMeta();
	}
	
	@Override
	public int getId() {
		Integer value = si.getId();
		return value.intValue();
	}

	@Override
	public long getLastUse() {
		long value = si.getLastUse();
		return value;
	}

	@Override
	public long getInstances() {
		long value = si.getInstances();
		return value;
	}

	@Override
	public long getDeployments() {
		long value = si.getDeployments();
		return value;
	}

	private void initPgIn() {
		pgIn = ServicesHelper.getInstance().getPgIn(this);
	}
	
	@Override
	public long getPgIn() {
		return pgIn;
	}

	private void initSwap() {
		swap = ServicesHelper.getInstance().getSwap(this);
	}
	
	@Override
	public long getSwap() {
		return swap;
	}

	private void initSwapMax() {
		swapMax = ServicesHelper.getInstance().getSwapMax(this);
	}
	
	@Override
	public long getSwapMax() {
		return swapMax;
	}

	@Override
	public long getSize() {
		long value = si.getSize();
		return value;
	}
	
	private boolean isFaultError() {
		boolean retVal = false;
		String value = getErrorText();
		if(value != null) {
			if(value.trim().length() > 0) {
				retVal = true;
			}
		}
		return retVal;
	}
	
	private boolean isHealthRelevant() {
		boolean retVal = true;
		String value = getState();
		if(value != null) {
			if(value.equalsIgnoreCase(ServiceState.Starting.name())) {
				retVal = false;
			}
			else if(value.equalsIgnoreCase(ServiceState.Waiting.name())) {
				retVal = false;
			}
			else if(value.equalsIgnoreCase(ServiceState.Initializing.name())) {
				retVal = false;
			}
			else if(value.equalsIgnoreCase(ServiceState.Stopped.name())) {
				retVal = false;
			}
		}
		return retVal;
	}
	
	private boolean isFaultHealth() {
		boolean retVal = false;
		boolean value = getHealth();
		if(!value) {
			if(isHealthRelevant()) {
				retVal = true;
			}
		}
		return retVal;
	}
	
	@Override
	public boolean isPingOnly() {
		boolean retVal = false;
		Boolean value = si.getPingOnly();
		if(value != null) {
			if(value) {
				retVal = true;
			}
		}
		return retVal;
	}
	
	@Override
	public boolean isPingActive() {
		boolean retVal = false;
		Boolean value = si.getPingActive();
		if(value != null) {
			if(value) {
				retVal = true;
			}
		}
		return retVal;
	}
	
	@Override
	public boolean isRegistered() {
		boolean retVal = false;
		String value = getServiceClass();
		if(value != null) {
			if(value.trim().equalsIgnoreCase("Registered")) {
				retVal = true;
			}
		}
		return retVal;
	}
	
	@Override
	public boolean isViable() {
		boolean retVal = false;
		String value = getState();
		if(value != null) {
			if(value.equalsIgnoreCase(ServiceState.Available.name())) {
				retVal = true;
			}
		}
		return retVal;
	}
	
	@Override
	public boolean isAlert() {
		boolean retVal = false;
		if(isFaultError()) {
			retVal = true;
		}
		else if(isFaultHealth()) {
			retVal = true;
		}
		return retVal;
	}
	
	@Override
	public boolean isDisabled() {
		boolean retVal = si.isDisabled();
		return retVal;
	}
	
	@Override
	public boolean getHealth() {
		boolean value = si.getHealth();
		return value;
	}

	private String getServiceClass() {
		String value = si.getServiceClass();
		return value;
	}
	
	@Override
	public String getPopup() {
		String value = si.getPopup();
		return value;
	}
	
	@Override
	public String getErrorText() {
		String value = si.getErrorText();
		return value;
	}

	@Override
	public String getName() {
		String value = si.getName();
		return value;
	}

	@Override
	public String getState() {
		String value = si.getState();
		return value;
	}

	@Override
	public String getStatistics() {
		String value = si.getStatistics();
		return value;
	}
	
	@Override
	public String getUser() {
		String value = si.getUser();
		return value;
	}

	@Override
	public String getShareClass() {
		String value = si.getSchedulingClass();
		return value;
	}

	@Override
	public String getDescription() {
		String value = si.getDescription();
		return value;
	}

	@Override
	public String getDisableReason() {
		String value = si.getDisableReason();
		return value;
	}
	
	@Override
	public StartMode getStartMode() {
		StartMode value = si.getStartMode();
		return value;
	}
	
	@Override
	public ArrayList<String> getImplementors() {
		ArrayList<String> value = si.getImplementors();
		return value;
	}
	
	public void initDependentJobs() {
		DuccDataHelper duccDataHelper = DuccDataHelper.getInstance();
		TreeMap<String, ArrayList<DuccId>> serviceToJobsMap = duccDataHelper.getServiceToJobsUsageMap();
		String name = getName();
		if(serviceToJobsMap.containsKey(name)) {
			ArrayList<DuccId> duccIds = serviceToJobsMap.get(name);
			int size = duccIds.size();
			if(size > 0) {
				ArrayList<String> list = new ArrayList<String>();
				for(DuccId duccId : duccIds) {
					list.add(duccId.toString());
				}
				dependentJobs = list;
			}
		}
	}

	@Override
	public ArrayList<String> getDependentJobs() {
		return dependentJobs;
	}

	public void initDependentServices() {
		String name = getName();
		ServicesRegistry servicesRegistry = ServicesRegistry.getInstance();
		if(servicesRegistry != null) {
			ArrayList<String> list = servicesRegistry.getServiceDependencies(name);
			if(list.size() > 0) {
				dependentServices = list;
			}
		}
	}

	@Override
	public ArrayList<String> getDependentServices() {
		return dependentServices;
	}

	public void initDependentReservations() {
		DuccDataHelper duccDataHelper = DuccDataHelper.getInstance();
		TreeMap<String, ArrayList<DuccId>> serviceToReservationsMap = duccDataHelper.getServiceToReservationsUsageMap();
		String name = getName();
		if(serviceToReservationsMap.containsKey(name)) {
			ArrayList<DuccId> duccIds = serviceToReservationsMap.get(name);
			int size = duccIds.size();
			if(size > 0) {
				ArrayList<String> list = new ArrayList<String>();
				for(DuccId duccId : duccIds) {
					list.add(duccId.toString());
				}
				dependentReservations = list;
			}
		}
	}
	
	@Override
	public ArrayList<String> getDependentReservations() {
		return dependentReservations;
	}

}
