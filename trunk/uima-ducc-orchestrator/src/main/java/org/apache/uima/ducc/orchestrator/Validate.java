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
package org.apache.uima.ducc.orchestrator;

import java.util.ArrayList;
import java.util.Properties;

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccPropertiesResolver;
import org.apache.uima.ducc.orchestrator.authentication.DuccWebAdministrators;
import org.apache.uima.ducc.orchestrator.utilities.CliVersion;
import org.apache.uima.ducc.orchestrator.utilities.MemorySpecification;
import org.apache.uima.ducc.transport.event.AbstractDuccOrchestratorEvent;
import org.apache.uima.ducc.transport.event.CancelJobDuccEvent;
import org.apache.uima.ducc.transport.event.CancelReservationDuccEvent;
import org.apache.uima.ducc.transport.event.CancelServiceDuccEvent;
import org.apache.uima.ducc.transport.event.SubmitJobDuccEvent;
import org.apache.uima.ducc.transport.event.SubmitReservationDuccEvent;
import org.apache.uima.ducc.transport.event.SubmitServiceDuccEvent;
import org.apache.uima.ducc.transport.event.cli.JobRequestProperties;
import org.apache.uima.ducc.transport.event.cli.JobSpecificationProperties;
import org.apache.uima.ducc.transport.event.cli.ReservationRequestProperties;
import org.apache.uima.ducc.transport.event.cli.ServiceRequestProperties;
import org.apache.uima.ducc.transport.event.common.DuccWorkReservation;
import org.apache.uima.ducc.transport.event.common.IDuccSchedulingInfo;


public class Validate {
	private static final DuccLogger logger = DuccLogger.getLogger(Validate.class);
	
	private static DuccWebAdministrators duccWebAdministrators = new DuccWebAdministrators();
	
	@SuppressWarnings("unchecked")
	private static void addError(Properties properties, String reason) {
		String methodName = "addError";
		String key = JobSpecificationProperties.key_submit_errors;
		ArrayList<String> value = (ArrayList<String>) properties.get(key);
		if(value == null) {
			value = new ArrayList<String>();
			properties.put(key, value);
		}
		value.add(reason);
		logger.info(methodName, null, reason);
		return;
	}
	
	@SuppressWarnings("unchecked")
	private static void addWarning(Properties properties, String reason) {
		String methodName = "addWarning";
		String key = JobSpecificationProperties.key_submit_warnings;
		ArrayList<String> value = (ArrayList<String>) properties.get(key);
		if(value == null) {
			value = new ArrayList<String>();
			properties.put(key, value);
		}
		value.add(reason);
		logger.info(methodName, null, reason);
		return;
	}
	
	private static String createReason(String type, String key, String value) {
		String retVal = type+": "+key+"="+value;
		return retVal;
	}
	 
	public static boolean integer(boolean retVal, Properties properties, String key, String defaultValue, String minValue) {
		String value = (String)properties.get(key);
		if(value == null) {
			String reason = createReason("default",key,defaultValue);
			addWarning(properties,reason);
		}
		else {
			try {
				int specified_value = Integer.parseInt(value);
				int min_value = Integer.parseInt(minValue);
				if(specified_value < min_value) {
					String reason = createReason("invalid, under "+minValue,key,value);
					addError(properties,reason);
					retVal = false;
				}
			}
			catch(Exception e) {
				String reason = createReason("invalid, non-integer",key,value);
				addError(properties,reason);
				retVal = false;
			}
		}
		return retVal;
	}
	public static boolean request(SubmitJobDuccEvent duccEvent) {
		boolean retVal = true;
		if (!validate_cli_version(duccEvent)) {
			return false;
		}	
		JobRequestProperties properties = (JobRequestProperties) duccEvent.getProperties();
		String key;
		String value;
		//
		retVal = integer(retVal,
				properties,
				JobSpecificationProperties.key_process_pipeline_count,
				IDuccSchedulingInfo.defaultThreadsPerProcess,
				IDuccSchedulingInfo.minThreadsPerProcess);
		// scheduling class
		key = JobRequestProperties.key_scheduling_class;
		value = (String) properties.get(key);
		if(value == null) {
			String reason = createReason("invalid", key, value);
			addError(properties,reason);
			retVal = false;
		}	
		return retVal;
	}
	
	public static boolean request(CancelJobDuccEvent duccEvent) {
		if (!validate_cli_version(duccEvent)) {
			return false;
		}		
		boolean retVal = true;
		//TODO
		return retVal;
	}
	
	public static boolean accept(SubmitReservationDuccEvent duccEvent) {
		String value = DuccPropertiesResolver.getInstance().getProperty(DuccPropertiesResolver.ducc_orchestrator_unmanaged_reservations_accepted);
		Boolean result = new Boolean(value);
		return result;
	}
		
	public static boolean request(SubmitReservationDuccEvent duccEvent) {
		boolean retVal = true;
		if (!validate_cli_version(duccEvent)) {
			return false;
		}	
		ReservationRequestProperties properties = (ReservationRequestProperties) duccEvent.getProperties();
		String key;
		String value;
		// memory size
		key = ReservationRequestProperties.key_memory_size;
		String memorySize = (String) properties.get(key);
		MemorySpecification memorySpecification = new MemorySpecification(memorySize);
		value = memorySpecification.getSize();
		if(value == null) {
			String reason = createReason("invalid", key, value);
			addError(properties,reason);
			retVal = false;
		}
		// scheduling class
		key = ReservationRequestProperties.key_scheduling_class;
		value = (String) properties.get(key);
		if(value == null) {
			String reason = createReason("invalid", key, value);
			addError(properties,reason);
			retVal = false;
		}
		return retVal;
	}
	
	public static boolean request(CancelReservationDuccEvent duccEvent, DuccWorkReservation duccWorkReservation) {
		String location = "request";
		boolean retVal = false;
		if (!validate_cli_version(duccEvent)) {
			return false;
		}
		Properties properties = duccEvent.getProperties();
		String userid = properties.getProperty(JobSpecificationProperties.key_user);
		String ownerid = duccWorkReservation.getStandardInfo().getUser();
		if((userid != null) && (userid.equals(ownerid))) {
			retVal = true;
		}
		else if(duccWebAdministrators.isAdministrator(userid)) {
			retVal = true;
		}
		else {
			String reason;
			if(userid == null) {
				reason = createReason("reservation cancel invalid",JobSpecificationProperties.key_user,"unspecified");
			}
			else {
				reason = createReason("reservation cancel unauthorized",JobSpecificationProperties.key_user,userid);
			}
			addWarning(properties,reason);
			logger.warn(location, duccWorkReservation.getDuccId(), reason);
		}
		return retVal;
	}
	
	public static boolean request(SubmitServiceDuccEvent duccEvent) {
		boolean retVal = true;
		if (!validate_cli_version(duccEvent)) {
			return false;
		}		
		JobRequestProperties properties = (JobRequestProperties) duccEvent.getProperties();
		String key;
		String value;
		//
		retVal = integer(retVal,
				properties,
				JobSpecificationProperties.key_process_pipeline_count,
				IDuccSchedulingInfo.defaultThreadsPerProcess,
				IDuccSchedulingInfo.minThreadsPerProcess);
		// scheduling class
		key = ServiceRequestProperties.key_scheduling_class;
		value = (String) properties.get(key);
		if(value == null) {
			String reason = createReason("invalid", key, value);
			addError(properties,reason);
			retVal = false;
		}
		return retVal;
	}
	
	public static boolean request(CancelServiceDuccEvent duccEvent) {
		boolean retVal = true;
		if (!validate_cli_version(duccEvent)) {
			return false;
		}
		//TODO
		return retVal;
	}
	
	private static boolean validate_cli_version(AbstractDuccOrchestratorEvent ev) {
		if (ev.getCliVersion() == CliVersion.getVersion()) {
			return true;
		}
		String reason = ev.getEventType() + " rejected. Incompatible CLI request using version " + ev.getCliVersion() + " while DUCC expects version " + CliVersion.getVersion() ;
		addError(ev.getProperties(),reason);
		logger.warn("validate_cli_request", null, reason);
		return false;
	}
}
