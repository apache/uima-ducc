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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.uima.ducc.common.persistence.services.IStateServices;
import org.apache.uima.ducc.common.persistence.services.StateServices;
import org.apache.uima.ducc.common.persistence.services.StateServicesDirectory;
import org.apache.uima.ducc.common.persistence.services.StateServicesSet;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.springframework.util.StringUtils;

public class ServicesRegistry {
	
	public ServicesRegistry() {
		refreshCache();
	}
	
	private ServicesRegistryMap map = new ServicesRegistryMap();
	
	public void refreshCache() {
		try {
			ServicesRegistryMap mapRevised = new ServicesRegistryMap();
			IStateServices iss = StateServices.getInstance();
			StateServicesDirectory ssd = iss.getStateServicesDirectory();
			if(!ssd.getDescendingKeySet().isEmpty()) {
				for(Integer key : ssd.getDescendingKeySet()) {
					StateServicesSet entry = ssd.get(key);
					Properties propertiesSvc = entry.get(IServicesRegistry.svc);
					Properties propertiesMeta = entry.get(IServicesRegistry.meta);
					ServicesRegistryMapPayload value = new ServicesRegistryMapPayload(propertiesSvc, propertiesMeta);
					mapRevised.put(key, value);
				}
			}
			map = mapRevised;
		}
		catch(IOException e) {
			e.printStackTrace();
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
	
	public ServicesRegistryMapPayload findService(String name) {
		ServicesRegistryMapPayload retVal = null;
		try {
			for(Integer key : map.keySet()) {
				ServicesRegistryMapPayload payload = map.get(key);
				Properties meta = payload.meta;
				if(meta != null) {
					if(meta.containsKey(IServicesRegistry.endpoint)) {
						String endpoint = meta.getProperty(IServicesRegistry.endpoint);
						if(name.equals(endpoint)) {
							retVal = payload;
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
			for(Integer key : map.keySet()) {
				ServicesRegistryMapPayload payload = map.get(key);
				Properties meta = payload.meta;
				if(meta != null) {
					String implementors = meta.getProperty(IServicesRegistry.implementors);
					String[] list = getList(implementors);
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
	
	/*
	
	public static ServicesRegistry sr = new ServicesRegistry();
	
	public static void test(long id) {
		DuccId duccId = new DuccId(id);
		String name = sr.findServiceName(duccId);
		if(name == null) {
			System.out.println(id+" not found");
		}
		else {
			System.out.println(id+" => "+name);
		}
	}
	
	public static void main(String[] args) {
		test(3);
		test(9);
	}
	*/
}
