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
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.sm.IService.ServiceState;
import org.apache.uima.ducc.ws.DuccDataHelper;

public class ServiceInterpreter {
	
	private static DuccLogger duccLogger = DuccLogger.getLogger(ServiceInterpreter.class);
	private static DuccId jobid = null;
	
	protected Properties svc;
	protected Properties meta;
	
	public ServiceInterpreter(Properties propertiesSvc, Properties propertiesMeta) {
		setSvc(propertiesSvc);
		setMeta(propertiesMeta);
	}
	
	public void setSvc(Properties value) {
		svc = value;
	}
	
	public Properties getSvc() {
		return svc;
	}
	
	public void setMeta(Properties value) {
		meta = value;
	}
	
	public Properties getMeta() {
		return meta;
	}
	
	private static String getUninterpreted(Properties properties, String key) {
		String retVal = getValue(properties, key, "");
		return retVal;
	}
	
	private static String getValue(Properties properties, String key, String defaultValue) {
		String retVal = defaultValue;
		if(properties != null) {
			if(key != null) {
				retVal = properties.getProperty(key, defaultValue);
			}
		}
		return retVal.trim();
	}
		
	private String placeholderPingerStatus = "";
	
	public String getPingerStatus() {
		String location = "getPingerStatus";
		String retVal = placeholderPingerStatus;
		try {
			String state = getState();
			retVal = "The service is "+state;
			if(state.equalsIgnoreCase(ServiceState.Waiting.name())) {
				boolean pingActive = getPingActive();
				if(pingActive) {
					retVal = "Pinger is running"; // UIMA-4177
				}
				else {
					retVal = "Pinger is not reporting";  // UIMA-4829
				}
			}
			else if(state.equalsIgnoreCase(ServiceState.Available.name())) {
				Boolean value = getPingActive();
				if(!value) {
					retVal = "Pinger is not active";
				}
			}
		}
		catch(Exception e) {
			duccLogger.debug(location, jobid, e);
		}
		return retVal;
	}
	
	public Boolean getServiceAlive() {
		String location = "getServiceAlive";
		boolean retVal = true;
		try {
			String value = getValue(meta,IServicesRegistry.service_alive,Boolean.valueOf(retVal).toString());
			retVal = Boolean.valueOf(value);
		}
		catch(Exception e) {
			duccLogger.error(location, jobid, e);
		}
		return retVal;
	}
	
	public boolean isServiceAlive() {
		return getServiceAlive();
	}
	
	public Boolean getServiceHealthy() {
		String location = "getServiceHealthy";
		boolean retVal = true;
		try {
			String value = getValue(meta,IServicesRegistry.service_healthy,Boolean.valueOf(retVal).toString());
			retVal = Boolean.valueOf(value);
		}
		catch(Exception e) {
			duccLogger.error(location, jobid, e);
		}
		return retVal;
	}
	
	public boolean isServiceHealthy() {
		return getServiceHealthy();
	}
	
	private Integer placeholderId = new Integer(-1);
	
	public Integer getId() {
		String location = "getId";
		Integer retVal = placeholderId;
		try {
			String value = getValue(meta,IServicesRegistry.numeric_id,"");
			int id = Integer.valueOf(value);
			retVal = id;
		}
		catch(Exception e) {
			duccLogger.debug(location, jobid, e);
		}
		return retVal;
	}
	
	private String placeholderName = "";
	
	public String getName() {
		String location = "getName";
		String retVal = placeholderName;
		try {
			ServiceName serviceName = new ServiceName(getValue(meta,IServicesRegistry.endpoint,""));
			String name = serviceName.getNormalized();
			retVal = name;
		}
		catch(Exception e) {
			duccLogger.error(location, jobid, e);
		}
		return retVal;
	}
	
	public String getState() {
		String location = "getState";
		String retVal = "";
		try {
			String state = getValue(meta,IServicesRegistry.service_state,"");
			retVal = state;
		}
		catch(Exception e) {
			duccLogger.error(location, jobid, e);
		}
		return retVal;
	}
	
	public String getStatistics() {
		String location = "getStatistics";
		String retVal = "";
		try {
			String statistics = getValue(meta,IServicesRegistry.service_statistics,"");
			retVal = statistics;
		}
		catch(Exception e) {
			duccLogger.error(location, jobid, e);
		}
		return retVal;
	}
	
	public Boolean getPingOnly() {
		String location = "getPingOnly";
		boolean retVal = false;
		try {
			String value = getValue(meta,IServicesRegistry.ping_only,Boolean.valueOf(retVal).toString());
			retVal = Boolean.valueOf(value);
		}
		catch(Exception e) {
			duccLogger.error(location, jobid, e);
		}
		return retVal;
	}
	
	public Boolean getPingActive() {
		String location = "getPingActive";
		boolean retVal = false;
		try {
			String value = getValue(meta,IServicesRegistry.ping_active,Boolean.valueOf(retVal).toString());
			retVal = Boolean.valueOf(value);
		}
		catch(Exception e) {
			duccLogger.error(location, jobid, e);
		}
		return retVal;
	}
	
	public Boolean getPingerReportedServiceHealth() {
		String location = "getPingerReportedServiceHealth";
		boolean retVal = true;
		try {
			String value = getValue(meta,IServicesRegistry.service_healthy,Boolean.valueOf(retVal).toString());
			retVal = Boolean.valueOf(value);
		}
		catch(Exception e) {
			duccLogger.error(location, jobid, e);
		}
		return retVal;
	}
	
	public String getServiceClass() {
		String location	 = "getServiceClass";
		String retVal = "";
		try {
			String serviceClass = getValue(meta,IServicesRegistry.service_class,"");
			retVal = serviceClass;
		}
		catch(Exception e) {
			duccLogger.error(location, jobid, e);
		}
		return retVal;
	}
	
	public String getErrorText() {
		String location = "getErrorText";
		String retVal = "";
		try {
			String errorText = getValue(meta,IServicesRegistry.submit_error,"");
			retVal = errorText;
		}
		catch(Exception e) {
			duccLogger.error(location, jobid, e);
		}
		return retVal;
	}
	
	private Long placeholderLastUse = new Long(-1);
	
	public Long getLastUse() {
		String location = "getLastUse";
		Long retVal = placeholderLastUse;
		try {
			String value = getUninterpreted(meta, IServicesRegistry.last_use);
			retVal = Long.parseLong(value);
		}
		catch(Exception e) {
			duccLogger.error(location, jobid, e);
		}
		return retVal;
	}
	
	private Long placeholderInstances = new Long(-1);
	
	public Long getInstances() {
		String location = "getInstances";
		Long retVal = placeholderInstances;
		try {
			String value = getUninterpreted(meta, IServicesRegistry.instances);
			retVal = Long.parseLong(value);
		}
		catch(Exception e) {
			duccLogger.error(location, jobid, e);
		}
		return retVal;
	} 
	
	public ArrayList<String> getImplementors() {
		String location = "getImplementors";
		ArrayList<String> retVal = new ArrayList<String>();
		try {
            // UIMA-4258, use common implementors parser
            String[] implementors = DuccDataHelper.parseImplementors(meta);
			for(String implementor : implementors) {
				retVal.add(implementor);
			}
		}
		catch(Exception e) {
			duccLogger.error(location, jobid, e);
		}
		return retVal;
	}
	
	private Long placeholderDeployments = new Long(-1);
	
	public Long getDeployments() {
		String location = "getDeployments";
		Long retVal = placeholderDeployments;
		try {
			ArrayList<String> implementors = getImplementors();
			Long deployments = new Long(implementors.size());
			retVal = deployments;
		}
		catch(Exception e) {
			duccLogger.error(location, jobid, e);
		}
		return retVal;
	}
	
	private String placeholderUser = "";
	
	public String getUser() {
		String location = "getUser";
		String retVal = placeholderUser;
		try {
			String user = getValue(meta,IServicesRegistry.user,"");
			retVal = user;
		}
		catch(Exception e) {
			duccLogger.error(location, jobid, e);
		}
		return retVal;
	}
	
	private String placeholderSchedulingClass = "";
	
	public String getSchedulingClass() {
		String location = "getSchedulingClass";
		String retVal = placeholderSchedulingClass;
		try {
			String schedulingClass = getValue(svc,IServicesRegistry.scheduling_class,"");
			retVal = schedulingClass;
		}
		catch(Exception e) {
			duccLogger.error(location, jobid, e);
		}
		return retVal;
	}
	
	private Long placeholderSize = new Long(-1);
	
	public Long getSize() {
		String location = "getSize";
		Long retVal = placeholderSize;
		String value = "?";
		try {
			value = getUninterpreted(svc, IServicesRegistry.process_memory_size);
			retVal = Long.parseLong(value);
		}
		catch(Exception e) {
			duccLogger.debug(location, jobid, "id:"+getId()+" "+"size:"+value);
		}
		return retVal;
	} 
	
	private String placeholderDescription = "";
	
	public String getDescription() {
		String location = "getDescription";
		String retVal = placeholderDescription;
		try {
			String description = getValue(svc,IServicesRegistry.description,"");
			retVal = description;
		}
		catch(Exception e) {
			duccLogger.error(location, jobid, e);
		}
		return retVal;
	}

	private Boolean getAutostart() {
		String location = "getAutostart";
		Boolean retVal = new Boolean(false);
		try {
			String value = getValue(meta,IServicesRegistry.autostart,"false");
			retVal = Boolean.valueOf(value);
		}
		catch(Exception e) {
			duccLogger.error(location, jobid, e);
		}
		return retVal;
	}
	
	private boolean isAutostart() {
		return getAutostart();
	}
	
	private Boolean getReference() {
		String location = "getReference";
		Boolean retVal = new Boolean(true);
		try {
			String value = getValue(meta,IServicesRegistry.reference,"true");
			retVal = Boolean.valueOf(value);
			if(!retVal) {
				// Override "Manual" whenever implementors == 0
				Boolean implementors = isImplementers();
				if(!implementors) {
					retVal = new Boolean(true);
				}
			}
		}
		catch(Exception e) {
			duccLogger.error(location, jobid, e);
		}
		return retVal;
	}
	
	private boolean isReference() {
		return getReference();
	}
	
	private Boolean isImplementers() {
		String location = "isImplementers";
		Boolean retVal = new Boolean(false);
		try {
			String value = getValue(meta,IServicesRegistry.implementors,"");
			String implementors = value.trim();
			if(implementors.length() > 0) {
				retVal = new Boolean(true);
			}
		}
		catch(Exception e) {
			duccLogger.error(location, jobid, e);
		}
		return retVal;
	}
	
	private Boolean getEnabled() {
		String location = "getEnabled";
		Boolean retVal = new Boolean(true);
		try {
			String value = getValue(meta,IServicesRegistry.enabled,"true");
			retVal = Boolean.valueOf(value);
		}
		catch(Exception e) {
			duccLogger.error(location, jobid, e);
		}
		return retVal;
	}
	
	private String placeholderDisableReason = "";
	
	public String getDisableReason() {
		String location = "getDisableReason";
		String retVal = placeholderDisableReason;
		try {
			String value = getValue(meta,IServicesRegistry.disable_reason,"");
			retVal = value;
		}
		catch(Exception e) {
			duccLogger.error(location, jobid, e);
		}
		return retVal;
	}
	
	public enum StartState { 
		Autostart(), 
		Reference(), 
		Manual(),
		Stopped(),
		Unknown();
		
		private StartState() {
		}
	}
	
	private int getStateOrdinality() {
		String location = "getStateOrdinality";
		int retVal = 0;
		try {
			String state = getState();
			ServiceState serviceState = ServiceState.valueOf(state);
			retVal = serviceState.ordinality();
		}
		catch(Exception e) {
			duccLogger.error(location, jobid, e);
		}
		return retVal;
	}
	
	public StartState getStartState() {
		StartState retVal = StartState.Unknown;
		if(isAutostart()) {
			retVal = StartState.Autostart;
		}
		else {
			int ordinality = getStateOrdinality();
			if(ordinality > 3) {
				if(isReference()) {
					retVal = StartState.Reference;
				}
				else {
					retVal = StartState.Manual;
				}
			}
			else if(ordinality < 4) {
				retVal = StartState.Stopped;
			}
		}
		return retVal;
	}
	
	public boolean isDisabled() {
		Boolean enabled = getEnabled();
		boolean retVal = true;
		if(enabled) {
			retVal = false;
		}
		return retVal;
	}
}
