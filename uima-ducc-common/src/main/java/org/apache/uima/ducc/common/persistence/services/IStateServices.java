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
package org.apache.uima.ducc.common.persistence.services;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.uima.ducc.common.IDuccEnv;


public interface IStateServices {

	public static String directory_state_services = IDuccEnv.DUCC_STATE_SERVICES_DIR;

	public static final String svc = "svc";
	public static final String meta = "meta";
	
	public static final String endpoint = "endpoint";
	public static final String instances = "instances";
	public static final String autostart = "autostart";
	public static final String implementors = "implementors";
	public static final String numeric_id = "numeric_id";
	public static final String ping_active = "ping-active";
	public static final String service_class = "service-class";
	public static final String service_healthy = "service-healthy";
	public static final String service_state = "service-state";
	public static final String user = "user";
	public static final String scheduling_class = "scheduling_class";
	public static final String process_memory_size = "process_memory_size";
	public static final String description = "description";
	
	public ArrayList<String> getSvcList();
	public ArrayList<String> getMetaList();
	
	public StateServicesDirectory getStateServicesDirectory() throws IOException;
}
