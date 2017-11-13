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

import java.util.Collection;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.TreeMap;

import org.apache.uima.ducc.common.persistence.services.IStateServices;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.ws.registry.ServicesRegistryMap;
import org.apache.uima.ducc.ws.registry.ServicesRegistryMapPayload;

public class ServicesSortCache {
	
	private static DuccLogger duccLogger = DuccLogger.getLogger(ServicesSortCache.class);
	private static DuccId jobid = null;
	
	private static ServicesSortCache instance = new ServicesSortCache();
	
	public static ServicesSortCache getInstance() {
		return instance;
	}
	
	private volatile TreeMap<SortableService,IServiceAdapter> map = new TreeMap<SortableService,IServiceAdapter>();
	
	public void update(ServicesRegistryMap registryMap) {
		TreeMap<SortableService,IServiceAdapter> mapRevised = new TreeMap<SortableService,IServiceAdapter>();
		for(Entry<Long, ServicesRegistryMapPayload> entry : registryMap.entrySet()) {
			ServicesRegistryMapPayload payload = entry.getValue();
			Properties meta = payload.meta;
			Properties svc = payload.svc;
			SortableService ss = new SortableService(svc,meta);
			mapRevised.put(ss,ss);
		}
		map = mapRevised;
	}
	
	private void enabled(int id, boolean bool) {
		String location = "enabled";
		try {
			for(Entry<SortableService, IServiceAdapter> entry : map.entrySet()) {
				IServiceAdapter payload = entry.getValue();
				Properties meta = payload.getMeta();
				String key = IStateServices.SvcMetaProps.numeric_id.pname();
				String value = meta.getProperty(key);
				int numeric_id = Integer.parseInt(value);
				if(numeric_id == id) {
					meta.setProperty(IStateServices.SvcMetaProps.enabled.pname(), Boolean.toString(bool));
					payload.setMeta(meta);
					break;
				}
			}
		}
		catch(Exception e) {
			duccLogger.error(location, jobid, e);
		}
	}
	
	public void setDisabled(int id) {
		enabled(id, false);
	}
	
	public void setEnabled(int id) {
		enabled(id, true);
	}
	
	public int size() {
		return map.size();
	}
	
	public Collection<IServiceAdapter> getSortedCollection() {
		return map.values();
	}
}
