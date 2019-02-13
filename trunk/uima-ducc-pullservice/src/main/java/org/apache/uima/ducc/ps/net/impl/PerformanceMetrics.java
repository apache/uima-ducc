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

package org.apache.uima.ducc.ps.net.impl;

import java.util.Enumeration;
import java.util.List;
import java.util.Properties;

import org.apache.uima.ducc.ps.net.iface.IPerformanceMetrics;

public class PerformanceMetrics implements IPerformanceMetrics {
	private static final long serialVersionUID = 1L;
	private List<Properties> perfMetrics;
	public List<Properties> get() {
		return perfMetrics;
	}
	public void set(List<Properties> metrics) {
		perfMetrics = metrics;
	}
    public String toString() {
    	StringBuilder sb = new StringBuilder();
    	if ( perfMetrics != null && !perfMetrics.isEmpty()) {
    		for( Properties p : perfMetrics ) {
    			Enumeration<Object> keys = p.keys();
    			while( keys.hasMoreElements() ) {
    				String key = (String)keys.nextElement();
    				sb.append(key).append(":").append(p.getProperty(key)).append("\t");
    			}
    		}
    	} 
    	return sb.toString();
    }
}
