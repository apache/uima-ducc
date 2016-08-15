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

import java.util.Map.Entry;
import java.util.Properties;

import org.apache.uima.ducc.common.NodeConfiguration;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.DuccProperties;
import org.apache.uima.ducc.common.utils.DuccPropertiesResolver;
import org.apache.uima.ducc.common.utils.SystemPropertyResolver;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.orchestrator.jd.scheduler.JdScheduler;
import org.apache.uima.ducc.transport.event.SubmitJobDuccEvent;
import org.apache.uima.ducc.transport.event.SubmitReservationDuccEvent;
import org.apache.uima.ducc.transport.event.SubmitServiceDuccEvent;
import org.apache.uima.ducc.transport.event.cli.JobRequestProperties;
import org.apache.uima.ducc.transport.event.cli.ReservationRequestProperties;
import org.apache.uima.ducc.transport.event.cli.ServiceRequestProperties;
import org.apache.uima.ducc.transport.event.common.DuccWorkPopDriver;
import org.apache.uima.ducc.transport.event.common.IDuccProcess;
import org.apache.uima.ducc.transport.event.common.IDuccProcessMap;
import org.apache.uima.ducc.transport.event.common.IDuccWorkJob;
import org.apache.uima.ducc.transport.event.common.IProcessState.ProcessState;

public class OrchestratorHelper {
	
	private static DuccLogger logger = DuccLoggerComponents.getOrLogger(OrchestratorHelper.class.getName());
	
	public static DuccId jobid = null;
	
	public static String DUCC_HOME = "DUCC_HOME";
	public static String resources = "resources";
	public static String scheduler = "scheduler";
	public static String classes = "classes";
	public static String name = "name";
	
	private static NodeConfiguration getNodeConfiguration() {
		String location = "getNodeConfiguration";
		NodeConfiguration nc = null;
		String class_definitions = null;
		try {
			class_definitions =  System.getProperty(DUCC_HOME) + "/"+resources+"/"+SystemPropertyResolver.getStringProperty(DuccPropertiesResolver.ducc_rm_class_definitions, scheduler+"."+classes);
			nc = new NodeConfiguration(class_definitions, null, null, logger);  // UIMA-4275 use single common constructor
			nc.readConfiguration();
		}
		catch(Throwable t) {
			logger.error(location, jobid, t);
			logger.error(location, jobid, class_definitions);
		}
		return nc;
	}
	
	private static String getDefaultFairShareClass() {
		String location = "getDefaultFairShareClass";
		String defaultReserveName = null;
		try {
			NodeConfiguration nc = getNodeConfiguration();
			DuccProperties rp = nc.getDefaultFairShareClass();
			defaultReserveName = rp.getProperty(name);
		}
		catch(Throwable t) {
			logger.error(location, jobid, t);
		}
		return defaultReserveName;
	}
	
	protected static void assignDefaultFairShareClass(Properties properties, String key) {
		String location = "assignDefaultFairShareClass";
		try {
			String value = (String) properties.getProperty(key);
			if(value == null) {
				value = getDefaultFairShareClass();
				properties.setProperty(key, value);
				logger.info(location, jobid, key+"="+value);
			}
		}
		catch(Throwable t) {
			logger.error(location, jobid, t);
		}
	}
	
	private static String getDefaultFixedClass() {
		String location = "getDefaultFixedClass";
		String defaultFixedName = null;
		try {
			NodeConfiguration nc = getNodeConfiguration();
			DuccProperties rp = nc.getDefaultFixedClass();
			defaultFixedName = rp.getProperty(name);
		}
		catch(Throwable t) {
			logger.error(location, jobid, t);
		}
		return defaultFixedName;
	}
	
	protected static void assignDefaultFixedClass(Properties properties, String key) {
		String location = "assignDefaultFixedClass";
		try {
			String value = (String) properties.getProperty(key);
			if(value == null) {
				value = getDefaultFixedClass();
				properties.setProperty(key, value);
				logger.info(location, jobid, key+"="+value);
			}
		}
		catch(Throwable t) {
			logger.error(location, jobid, t);
		}
	}
	
	private static String getDefaultReserveClass() {
		String location = "getDefaultReserveClass";
		String defaultReserveName = null;
		try {
			NodeConfiguration nc = getNodeConfiguration();
			DuccProperties rp = nc.getDefaultReserveClass();
			defaultReserveName = rp.getProperty(name);
		}
		catch(Throwable t) {
			logger.error(location, jobid, t);
		}
		return defaultReserveName;
	}
	
	protected static void assignDefaultReserveClass(Properties properties, String key) {
		String location = "assignDefaultReserveClass";
		try {
			String value = (String) properties.getProperty(key);
			if(value == null) {
				value = getDefaultReserveClass();
				properties.setProperty(key, value);
				logger.info(location, jobid, key+"="+value);
			}
		}
		catch(Throwable t) {
			logger.error(location, jobid, t);
		}
	}
	
	public static void assignDefaults(SubmitJobDuccEvent duccEvent) {
		Properties properties = duccEvent.getProperties();
		String key = JobRequestProperties.key_scheduling_class;
		assignDefaultFairShareClass(properties, key);
	}
	
	public static void assignDefaults(SubmitReservationDuccEvent duccEvent) {
		Properties properties = duccEvent.getProperties();
		String key = ReservationRequestProperties.key_scheduling_class;
		assignDefaultReserveClass(properties, key);
	}
	
	public static void assignDefaults(SubmitServiceDuccEvent duccEvent) {
		Properties properties = duccEvent.getProperties();
		String key = ServiceRequestProperties.key_scheduling_class;
		assignDefaultFixedClass(properties, key);
	}
	
	/*
	public static void assignDefaults(SubmitReservationDuccEvent duccEvent) {
		String location = "assignDefaults";
		String class_definitions =  null;
		NodeConfiguration nc = null;
		try {
			class_definitions =  System.getProperty(DUCC_HOME) + "/"+resources+"/"+SystemPropertyResolver.getStringProperty(DuccPropertiesResolver.ducc_rm_class_definitions, scheduler+"."+classes);
			nc = new NodeConfiguration(class_definitions, logger);
			nc.readConfiguration();
			String key = ReservationRequestProperties.key_scheduling_class;
			Properties ep = duccEvent.getProperties();
			String value = (String) ep.getProperty(key);
			if(value == null) {
				DuccProperties rp = nc.getDefaultReserveClass();
				String defaultReserveName = rp.getProperty(name);
				value = defaultReserveName;
				if(value != null) {
					ep.setProperty(key, value);
				}
				logger.info(location, jobid, key+"="+value);
			}
		}
		catch(Throwable t) {
			try {
				logger.error(location, jobid, t);
				logger.error(location, jobid, class_definitions);
				nc.printConfiguration(); 
			}
			catch(Throwable t2) {
				logger.error(location, jobid, t2);
			}
		}
	}
	*/
	
	public static void jdDeallocate(IDuccWorkJob job, IDuccProcess jdProcess) {
		String location = "jdDeallocate";
		if(job != null) {
			DuccId jobIdentity = job.getDuccId();
			if(jdProcess != null) {
				DuccId driverIdentity = (DuccId) jdProcess.getDuccId();
				JdScheduler jdScheduler = JdScheduler.getInstance();
				ProcessState processState = jdProcess.getProcessState();
				if(processState != null) {
					switch(processState) {
					case LaunchFailed:
					case Failed:
					case FailedInitialization:
					case Stopped:
					case Killed:
					case Abandoned:
						jdScheduler.deallocate(jobIdentity, driverIdentity);
						logger.debug(location, driverIdentity, "state: "+processState);
						break;
					default:
						logger.debug(location, jobIdentity, "state: "+processState);
						break; 
					}
				}
				else {
					logger.debug(location, jobIdentity, "state: "+processState);
				}
			}
			else {
				logger.debug(location, jobIdentity, "jdProcess: "+jdProcess);
			}
		}
		else {
			logger.debug(location, null, "job: "+job);
		}
	}
	
	public static void jdDeallocate(IDuccWorkJob job) {
		String location = "jdDeallocate";
		JdScheduler jdScheduler = JdScheduler.getInstance();
		if(job != null) {
			DuccId jobIdentity = job.getDuccId();
			DuccWorkPopDriver driver = job.getDriver();
			if(driver != null) {
				IDuccProcessMap processMap = job.getDriver().getProcessMap();
				if(processMap != null) {
					for(Entry<DuccId, IDuccProcess> entry : processMap.entrySet()) {
						IDuccProcess jd = entry.getValue();
						ProcessState processState = jd.getProcessState();
						if(processState != null) {
							switch(processState) {
							case LaunchFailed:
							case Failed:
							case FailedInitialization:
							case Stopped:
							case Killed:
							case Abandoned:
								DuccId driverIdentity = entry.getKey();
								jdScheduler.deallocate(jobIdentity, driverIdentity);
								break;
							default:
								logger.debug(location, jobIdentity, "state: "+processState);
								break;
							}
						}
						else {
							logger.debug(location, jobIdentity, "state: "+processState);
						}
					}
				}
				else {
					logger.debug(location, jobIdentity, "map: "+processMap);
				}
			}
			else {
				logger.debug(location, jobIdentity, "driver: "+driver);
			}
		}
		else {
			logger.debug(location, null, "job: "+job);
		}
	}
}
