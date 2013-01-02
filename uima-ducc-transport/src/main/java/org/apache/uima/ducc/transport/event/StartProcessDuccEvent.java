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
package org.apache.uima.ducc.transport.event;

import java.util.Map;
import java.util.Properties;

import org.apache.uima.ducc.common.utils.id.DuccId;


public class StartProcessDuccEvent extends AbstractDuccEvent {
	private static final long serialVersionUID = 682197216516923063L;
	private Map<DuccId,Process> processMap;
	private Properties processSpecification;
	
	public StartProcessDuccEvent(Map<DuccId,Process> processMap, Properties processSpecification) {
		super(EventType.START_PROCESS);
		this.processMap = processMap;
		this.processSpecification = processSpecification;
	}
	public Map<DuccId,Process> getProcessMap() {
		return processMap;
	}
	public Properties getProcessSpecification() {
		return processSpecification;
	}
}
