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

import org.apache.uima.ducc.ws.registry.ServicesRegistryMap;
import org.apache.uima.ducc.ws.registry.ServicesRegistryMapPayload;

public class ServicesSortCache {
	
	private static ServicesSortCache instance = new ServicesSortCache();
	
	public static ServicesSortCache getInstance() {
		return instance;
	}
	
	private volatile TreeMap<SortableService,IServiceAdapter> map = new TreeMap<SortableService,IServiceAdapter>();
	
	public void update(ServicesRegistryMap registryMap) {
		TreeMap<SortableService,IServiceAdapter> mapRevised = new TreeMap<SortableService,IServiceAdapter>();
		for(Entry<Integer, ServicesRegistryMapPayload> entry : registryMap.entrySet()) {
			ServicesRegistryMapPayload payload = entry.getValue();
			Properties meta = payload.meta;
			Properties svc = payload.svc;
			SortableService ss = new SortableService(svc,meta);
			mapRevised.put(ss,ss);
		}
		map = mapRevised;
	}
	
	public int size() {
		return map.size();
	}
	
	public Collection<IServiceAdapter> getSortedCollection() {
		return map.values();
	}
}
