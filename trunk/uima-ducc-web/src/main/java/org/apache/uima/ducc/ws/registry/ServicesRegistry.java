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
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.uima.ducc.common.persistence.services.IStateServices;
import org.apache.uima.ducc.common.persistence.services.StateServicesDirectory;
import org.apache.uima.ducc.common.persistence.services.StateServicesFactory;
import org.apache.uima.ducc.common.persistence.services.StateServicesSet;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.ws.DuccDataHelper;
import org.apache.uima.ducc.ws.registry.sort.ServicesSortCache;
import org.springframework.util.StringUtils;

public class ServicesRegistry {
	
	private static DuccLogger logger = DuccLogger.getLogger(ServicesRegistry.class);
	private static DuccId jobid = null;
	
	private static ServicesRegistry instance = new ServicesRegistry();

	private ServicesRegistryMap map = new ServicesRegistryMap();
	
	private AtomicBoolean inProgress = new AtomicBoolean(false);
	
	public static ServicesRegistry getInstance() {
		return instance;
	}
	
	private ServicesRegistry() {
		refreshCache();
	}
	
	public void update() {
		String location = "update";
		DuccId jobid = null;
		if(inProgress.compareAndSet(false, true)) {
			try {
				refreshCache();
				logger.debug(location, jobid, "size:"+map.size());
			}		
			catch(Exception e) {
				logger.error(location, jobid, e);
			}
		}
		else {
			logger.warn(location, jobid, "skipping: already in progress...");
		}
		inProgress.set(false);
	}
	
	public void refreshCache() {
		String location = "refreshCache";
		try {
			ServicesRegistryMap mapRevised = new ServicesRegistryMap();
			IStateServices iss = StateServicesFactory.getInstance(this.getClass().getName(), "WS");
			StateServicesDirectory ssd = iss.getStateServicesDirectory();
			if(!ssd.getDescendingKeySet().isEmpty()) {
				for(Long key : ssd.getDescendingKeySet()) {
					StateServicesSet entry = ssd.get(key);
					Properties propertiesSvc = entry.get(IServicesRegistry.svc);
					Properties propertiesMeta = entry.get(IServicesRegistry.meta);
					ServicesRegistryMapPayload value = new ServicesRegistryMapPayload(propertiesSvc, propertiesMeta);
					mapRevised.put(key, value);
					String endpoint = propertiesMeta.getProperty(IServicesRegistry.endpoint);
					logger.debug(location, jobid, "key: "+key+" "+"endpoint: "+endpoint);
				}
			}
			map = mapRevised;
			logger.debug(location, jobid, "size: "+map.size());
			ServicesSortCache.getInstance().update(map);
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
		}
	}
	
	public ServicesRegistryMap getMap() {
		return map;
	}
	
	public ServicesRegistryMap getCurrentMap() {
		refreshCache();
		return map;
	}
	
	public String[] getList(String string) {
		String[] retVal = new String[0];
		if(string != null) {
			string = string.trim();
			if(string.length() > 0) {
				retVal = StringUtils.delimitedListToStringArray(string, " ");
			}
		}
		return retVal;
	}
	
	public ArrayList<String> getArrayList(String list) {
		ArrayList<String> retVal = new ArrayList<String>();
		for(String string : getList(list)) {
			retVal.add(string);
		}
		return retVal;
	}
	
	private boolean compareEndpoints(String e0, String e1) {
		boolean retVal = false;
		if(e0 != null) {
			if(e1 != null) {
				String s0 = e0;
				String s1 = e1;
				if(s0.contains("?")) {
					s0 = s0.substring(0, s0.indexOf("?"));
				}
				if(s1.contains("?")) {
					s1 = s1.substring(0, s1.indexOf("?"));
				}
				retVal = s0.equals(s1);
			}
		}
		return retVal;
	}
	
	public ServicesRegistryMapPayload findServiceById(String id) {
		String location = "findServiceById";
		ServicesRegistryMapPayload retVal = null;
		try {
			logger.debug(location, jobid, "size: "+map.size());
			logger.debug(location, jobid, "search: "+id);
			for(Long key : map.keySet()) {
				ServicesRegistryMapPayload payload = map.get(key);
				Properties meta = payload.meta;
				if(meta != null) {
					if(meta.containsKey(IServicesRegistry.numeric_id)) {
						String sid = meta.getProperty(IServicesRegistry.numeric_id);
						if(sid.equals(id)) {
							retVal = payload;
							break;
						}
					}
				}
			}
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
		}
		if(retVal == null) {
			logger.warn(location, jobid, "not found: "+id);
		}
		return retVal;
	}
	
	public ServicesRegistryMapPayload findService(String name) {
		String location = "findService";
		ServicesRegistryMapPayload retVal = null;
		try {
			logger.debug(location, jobid, "size: "+map.size());
			logger.debug(location, jobid, "search: "+name);
			for(Long key : map.keySet()) {
				ServicesRegistryMapPayload payload = map.get(key);
				Properties meta = payload.meta;
				if(meta != null) {
					if(meta.containsKey(IServicesRegistry.endpoint)) {
						String endpoint = meta.getProperty(IServicesRegistry.endpoint);
						logger.trace(location, jobid, "key: "+key+" "+"compare: "+endpoint);
						if(compareEndpoints(name,endpoint)) {
							retVal = payload;
							break;
						}
					}
				}
			}
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
		}
		if(retVal == null) {
			logger.warn(location, jobid, "not found: "+name);
		}
		return retVal;
	}
	
	public String findServiceUser(String id) {
		String retVal = null;
		try {
			for(Long key : map.keySet()) {
				ServicesRegistryMapPayload payload = map.get(key);
				Properties meta = payload.meta;
				if(meta != null) {
					if(meta.containsKey(IServicesRegistry.numeric_id)) {
						String sid = meta.getProperty(IServicesRegistry.numeric_id);
						if(id.equals(sid)) {
							retVal = meta.getProperty(IServicesRegistry.user).trim();
							break;
						}
					}
				}
			}
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		return retVal;
	}
	
	public String findServiceName(DuccId duccId) {
		String retVal = null;
		try {
			long id = duccId.getFriendly();
			for(Long key : map.keySet()) {
				ServicesRegistryMapPayload payload = map.get(key);
				Properties meta = payload.meta;
				if(meta != null) {
                    // UIMA-4258, use common implementors parser
                    String[] list = DuccDataHelper.parseImplementors(meta);

					for( String member : list ) {
						if(member.equals(id+"")) {
							if(meta.containsKey(IServicesRegistry.endpoint)) {
								retVal = meta.getProperty(IServicesRegistry.endpoint);
							}
							break;
						}
					}
				}
			}
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		return retVal;
	}
	
	public String getServiceState(String name) {
		String retVal = IServicesRegistry.constant_NotKnown;
		try {
			ServicesRegistryMapPayload payload = findService(name);
			Properties properties = payload.meta;
			String service_state = properties.getProperty(IServicesRegistry.service_state).trim();
			if(service_state.equalsIgnoreCase(IServicesRegistry.constant_Available)) {
				String ping_active = properties.getProperty(IServicesRegistry.ping_active).trim();
				if(ping_active.equalsIgnoreCase(IServicesRegistry.constant_true)) {
					String service_healthy = properties.getProperty(IServicesRegistry.service_healthy).trim();
					if(service_healthy.equalsIgnoreCase(IServicesRegistry.constant_true)) {
						retVal = IServicesRegistry.constant_OK;
					}
					else {
						retVal = IServicesRegistry.constant_NotHealthy;
					}
				}
				else {
					retVal = IServicesRegistry.constant_NotPinging;
				}
			}
			else {
				retVal = IServicesRegistry.constant_NotAvailable;
			}
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		return retVal;
	}
	
	public ArrayList<String> getServiceDependencies(String name) {
		String location = "getServiceDependencies";
		ArrayList<String> retVal = new ArrayList<String>();
		try {
			if(name != null) {
				ServicesRegistryMapPayload payload = findService(name);
				if(payload != null) {
					Properties properties = payload.svc;
					if(properties != null) {
						String service_dependency = properties.getProperty(IServicesRegistry.service_dependency);
						logger.debug(location, jobid, "name: "+name+" "+"service_dependency: "+service_dependency);
						if(service_dependency != null) {
							String[] dependencies = service_dependency.split(" ");
							for(String dependency : dependencies) {
								String value = dependency.trim();
								if(value.length() > 0) {
									ServiceName serviceName = new ServiceName(dependency);
									retVal.add(serviceName.toString());
								}
							}
						}
					}
				}
			}
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		return retVal;
	}
	
	public ArrayList<String> getServiceInstancesHistory(String name) {
		String location = "getServiceInstancesHistory";
		ArrayList<String> retVal = new ArrayList<String>();
		try {
			if(name != null) {
				ServicesRegistryMapPayload payload = findService(name);
				if(payload != null) {
					Properties properties = payload.svc;
					if(properties != null) {
						String work_instances = properties.getProperty(IServicesRegistry.work_instances);
						logger.debug(location, jobid, "name: "+name+" "+"work_instances: "+work_instances);
						if(work_instances != null) {
							String[] dependencies = work_instances.split(" ");
							for(String dependency : dependencies) {
								String value = dependency.trim();
								if(value.length() > 0) {
									ServiceName serviceName = new ServiceName(dependency);
									retVal.add(serviceName.toString());
								}
							}
						}
					}
				}
			}
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		return retVal;
	}
}
