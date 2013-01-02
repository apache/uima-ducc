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
package org.apache.uima.ducc.transport.event.common;

import java.util.ArrayList;
import java.util.List;

public class DuccUimaAggregate implements IDuccUimaAggregate {

	/**
	 * please increment this sUID when removing or modifying a field 
	 */
	private static final long serialVersionUID = 1L;

	private List<IDuccUimaAggregateComponent> components = new ArrayList<IDuccUimaAggregateComponent>();
	
	private String name;
	private String description;
	private int threadCount = 1;
	private String brokerURL;
	private String endpoint;
	
	public DuccUimaAggregate(String name, String description, int threadCount, String brokerURL, String endpoint) {
		setName(name);
		setDescription(description);
		setThreadCount(threadCount);
		setBrokerURL(brokerURL);
		setEndpoint(endpoint);
	}
	
	public DuccUimaAggregate(String name, String description, int threadCount, String brokerURL, String endpoint,List<IDuccUimaAggregateComponent> components) {
		setName(name);
		setDescription(description);
		setThreadCount(threadCount);
		setBrokerURL(brokerURL);
		setEndpoint(endpoint);
		setComponents(components);
	}
	
	@Override
	public List<IDuccUimaAggregateComponent> getComponents() {
		return components;
	}
	
	@Override
	public void setComponents(List<IDuccUimaAggregateComponent> components) {
		this.components = components;
	}
	
	@Override
	public String getName() {
		return name;
	}

	@Override
	public void setName(String name) {
		this.name = name;
	}

	@Override
	public String getDescription() {
		return description;
	}

	@Override
	public void setDescription(String description) {
		this.description = description;
	}

	@Override
	public int getThreadCount() {
		return threadCount;
	}

	@Override
	public void setThreadCount(int threadCount) {
		this.threadCount = threadCount;
	}

	@Override
	public String getBrokerURL() {
		return brokerURL;
	}

	@Override
	public void setBrokerURL(String brokerURL) {
		this.brokerURL = brokerURL;
	}

	@Override
	public String getEndpoint() {
		return endpoint;
	}

	@Override
	public void setEndpoint(String endpoint) {
		this.endpoint = endpoint;
	}
}
