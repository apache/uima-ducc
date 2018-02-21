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
package org.apache.uima.ducc.common.node.metrics;

import java.io.Serializable;

public class NodeCpuInfo implements Serializable {
	private static final long serialVersionUID = -4770810550678067034L;
	private String availableProcessors = "0";
    private String currentLoad = "0.0";
    
	public NodeCpuInfo(int availableProcessors, String cpu) {
		setAvailableProcessors(String.valueOf(availableProcessors));
		currentLoad = cpu;

	}
    public String getCurrentLoad() {
    	return currentLoad;
    }
	public String getAvailableProcessors() {
		return availableProcessors;
	}

	private void setAvailableProcessors(String availableProcessors) {
		this.availableProcessors = availableProcessors;
	}


}
