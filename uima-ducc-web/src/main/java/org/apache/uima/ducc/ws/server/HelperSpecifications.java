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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.uima.ducc.cli.DuccUiConstants;
import org.apache.uima.ducc.common.persistence.or.IDbDuccWorks;
import org.apache.uima.ducc.common.persistence.or.ITypedProperties;
import org.apache.uima.ducc.common.persistence.or.TypedProperties;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.database.DbDuccWorks;
import org.apache.uima.ducc.transport.event.common.DuccWorkJob;
import org.apache.uima.ducc.ws.utils.alien.EffectiveUser;

public class HelperSpecifications {

	/*
	 * Specifications for Jobs, Managed Reservations are kept in DB as JSON data.
	 * See DbDuccWorks.upsertSpecification for data format.
	 * 
	 * Legacy DUCC system (2.2.1 and prior) kept specification data in two properties files,
	 * one for user specified and one for system (DUCC) specified.  The present code supports
	 * both storage methodologies for backwards compatibility.
	 */
	
	private static DuccLogger duccLogger = DuccLogger.getLogger(HelperSpecifications.class);
	private static DuccId jobid = null;

	public static enum PType { user, all };

	private static HelperSpecifications instance = new HelperSpecifications();
	
	public static HelperSpecifications getInstance() {
		return instance;
	}
	
	private IDbDuccWorks dbDuccWorks = null;

	public HelperSpecifications() {
		init();
	}

	private void init() {
		String location = "init";
		try {
			dbDuccWorks = new DbDuccWorks(duccLogger);
		} catch (Exception e) {
			duccLogger.error(location, jobid, e);
		}
	}

	/*
	 * Fetch contents of two separate files, one each for user and system
	 * properties
	 * 
	 * This method returns a map comprising two keyed entries, one for "user"
	 * properties and one for "all" properties (ie user + system)
	 */
	private HashMap<String, Properties> getSpecificationPropertiesFromFiles(
			DuccWorkJob dwj, EffectiveUser eu, String filename) {
		String location = "getJobSpecificationPropertiesFromFiles";
		DuccId duccid = jobid;
		HashMap<String, Properties> map = new HashMap<String, Properties>();
		try {
			duccid = dwj.getDuccId();
			if (dwj != null) {
				String path = dwj.getUserLogDir() + filename;
				String key;
				Properties properties;
				key = PType.user.name();
				properties = DuccFile.getUserSpecifiedProperties(eu, dwj);
				if (properties != null) {
					if (properties.size() > 0) {
						map.put(key, properties);
					}
				}
				key = PType.all.name();
				properties = DuccFile.getProperties(eu, path);
				if (properties != null) {
					if (properties.size() > 0) {
						map.put(key, properties);
					}
				}
			}
		} catch (Throwable t) {
			duccLogger.debug(location, duccid, t);
			// oh well...
		}
		return map;
	}

	/*
	 * DB comprises Map with two keyed entries, one for "user" properties and
	 * the other for "system" properties.
	 * 
	 * This method returns a map comprising two keyed entries, one for "user"
	 * properties and one for "all" properties (ie user + system)
	 */
	private Map<String, Properties> getSpecificationPropertiesFromDb(
			String specificationType, Long id) {
		String location = "getSpecificationPropertiesFromDb";
		DuccId duccid = jobid;
		Map<String, Properties> map = new HashMap<String, Properties>();
		try {
			ITypedProperties typedProperties = dbDuccWorks.fetchSpecification(
					specificationType, id);
			Map<String, Properties> dbmap = typedProperties.getMap();
			if (dbmap != null) {
				Properties user = dbmap.get(ITypedProperties.PropertyType.user
						.name());
				if (user != null) {
					map.put(PType.user.name(), user);
				}
				Properties all = dbmap.get(ITypedProperties.PropertyType.system
						.name());
				if (all == null) {
					all = new Properties();
				}
				all.putAll(user);
				map.put(PType.all.name(), all);
			}
			duccid = new DuccId(id);
			duccLogger.debug(location, duccid, "size=" + map.size());
		} catch (Throwable t) {
			duccLogger.debug(location, duccid, t);
			// oh well...
		}
		return map;
	}

	private HashMap<String, Properties> getJobSpecificationPropertiesFromFiles(
			DuccWorkJob dwj, EffectiveUser eu) {
		String location = "getJobSpecificationPropertiesFromFiles";
		DuccId duccid = jobid;
		HashMap<String, Properties> map = new HashMap<String, Properties>();
		try {
			if (dwj != null) {
				duccid = dwj.getDuccId();
				String filename = DuccUiConstants.job_specification_properties;
				map = getSpecificationPropertiesFromFiles(dwj, eu, filename);
			}
		} catch (Throwable t) {
			duccLogger.debug(location, duccid, t);
			// oh well...
		}
		return map;
	}

	private Map<String, Properties> getJobSpecificationPropertiesFromDb(
			DuccWorkJob dwj, EffectiveUser eu) {
		String location = "getJobSpecificationPropertiesFromDb";
		DuccId duccid = jobid;
		Map<String, Properties> map = new HashMap<String, Properties>();
		try {
			if (dwj != null) {
				String specificationType = TypedProperties.SpecificationType.Job
						.name();
				Long id = dwj.getDuccId().getFriendly();
				map = getSpecificationPropertiesFromDb(specificationType, id);
			}
		} catch (Throwable t) {
			duccLogger.debug(location, duccid, t);
			// oh well...
		}
		return map;
	}
	
    public Map<String,Properties> getJobSpecificationProperties(DuccWorkJob dwj, EffectiveUser eu) {
    	Map<String,Properties> map = new HashMap<String,Properties>();
    	if(map.size() == 0) {
    		map = getJobSpecificationPropertiesFromDb(dwj, eu);
    	}
    	if(map.size() == 0) {
    		map = getJobSpecificationPropertiesFromFiles(dwj, eu);
    	}
    	if(map.size() == 0) {
    		map = null;
    	}
    	return map;
    }
    
    private Map<String,Properties> getManagedReservationSpecificationPropertiesFromDb(DuccWorkJob dwj, EffectiveUser eu) {
    	String location = "getManagedReservationSpecificationPropertiesFromDb";
    	DuccId duccid = jobid;
    	Map<String,Properties> map = new HashMap<String,Properties>();
    	try {
    		if(dwj != null) {
    			String specificationType = TypedProperties.SpecificationType.ManagedReservation.name();
    			Long id = dwj.getDuccId().getFriendly();
    			map = getSpecificationPropertiesFromDb(specificationType, id);
    		}
    	}
    	catch (Throwable t) {
    		duccLogger.debug(location, duccid, t);
    		// oh well...
    	}
    	return map;
    }
    
    private HashMap<String,Properties> getManagedReservationSpecificationPropertiesFromFiles(DuccWorkJob dwj, EffectiveUser eu) {
    	String location = "getManagedReservationSpecificationPropertiesFromFiles";
    	DuccId duccid = jobid;
    	HashMap<String,Properties> map = new HashMap<String,Properties>();
    	try {
    		if(dwj != null) {
    			duccid = dwj.getDuccId();
    	        String filename =  DuccUiConstants.managed_reservation_properties;
    	        map = getSpecificationPropertiesFromFiles(dwj, eu, filename);
    		}
    	}
    	catch (Throwable t) {
    		duccLogger.debug(location, duccid, t);
    		// oh well...
    	}
    	return map;
    }
    
    public Map<String,Properties> getManagedReservationSpecificationProperties(DuccWorkJob dwj, EffectiveUser eu) {
    	Map<String,Properties> map = new HashMap<String,Properties>();
    	if(map.size() == 0) {
    		map = getManagedReservationSpecificationPropertiesFromDb(dwj, eu);
    	}
    	if(map.size() == 0) {
    		map = getManagedReservationSpecificationPropertiesFromFiles(dwj, eu);
    	}
    	if(map.size() == 0) {
    		map = null;
    	}
    	return map;
    }
    
    //=====
    
    public Map<String,Properties> convertAllToSystem(Map<String,Properties> mapAll) {
    	Map<String,Properties> mapSystem = null;
    	if(mapAll != null) {
    		mapSystem = new HashMap<String,Properties>();
    		Properties propsU = mapAll.get("user");
    		Properties propsA = mapAll.get("all");
    		if(propsU == null) {
    			if(propsA == null) {
    				// nada
    			}
    			else {
    				mapSystem.put("system", propsA);
    			}
    		}
    		else {
    			mapSystem.put("user", propsU);
    			if(propsA == null) {
    				// nada
    			}
    			else {
    				Properties propsS = new Properties();
    				for(Object key : propsA.keySet()) {
    					if(!propsU.containsKey(key)) {
    						propsS.put(key,propsA.get(key));
    					}
    				}
    				mapSystem.put("system", propsS);
    			}
    		}
    	}
    	return mapSystem;
    }
	
}
