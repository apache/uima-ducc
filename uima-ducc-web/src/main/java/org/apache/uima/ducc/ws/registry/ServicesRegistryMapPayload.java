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

import java.util.Properties;

import org.apache.uima.ducc.common.persistence.services.IStateServices;

public class ServicesRegistryMapPayload {
	
	public Properties svc;
	public Properties meta;
	
	public ServicesRegistryMapPayload(Properties svc, Properties meta) {
		this.svc = svc;
		this.meta = meta;
	}
	
	public Properties get(String key) {
		Properties retVal = null;
		if(key != null) {
			if(key.equals(IStateServices.svc)) {
				retVal = svc;
			}
			else if(key.equals(IStateServices.meta)) {
				retVal = meta;
			}
		}
		return retVal;
	}
}
