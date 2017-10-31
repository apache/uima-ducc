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
package org.apache.uima.ducc.common.persistence.or;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

public class TypedProperties implements ITypedProperties {
	
	private HashMap<String,Properties> map = new HashMap<String,Properties>();
	
	@Override
	public void add(String type, Object name, Object value) {
		if((type != null) && (name != null) && (value != null)) {
			if(!map.containsKey(type)) {
				map.put(type, new Properties());
			}
			Properties properties = map.get(type);
			properties.put(name, value);
		}
	}
	
	@Override
	public void add(String type, Properties properties) {
		if((type != null) && (properties != null)) {
			for(Entry<Object, Object> property : properties.entrySet()) {
				add(type, property.getKey(), property.getValue());
			}
		}
	}
	
	@Override
	public Properties remove(String type) {
		Properties retVal = null;
		if(type != null) {
			if(!map.containsKey(type)) {
				retVal = map.remove(type);
			}
		}
		return retVal;
	}
	
	@Override
	public Object remove(String type, Object name) {
		Object retVal = null;
		if((type != null) && (name != null)) {
			if(!map.containsKey(type)) {
				Properties properties = map.get(type);
				if(properties.containsKey(name)) {
					retVal = properties.remove(name);
				}
			}
		}
		return retVal;
	}
	
	@Override
	public Properties queryType(String type) {
		Properties properties = null;
		if(type != null) {
			properties = map.get(type);
		}
		return properties;
	}

	@Override
	public Map<String, Properties> getMap() {
		return map;
	}
}
