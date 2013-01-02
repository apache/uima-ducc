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

public class DefaultNodeLoadAverageInfo implements NodeLoadAverage, Serializable {
	
	private static final long serialVersionUID = 1L;
	
	double loadAvg1;
	double loadAvg5;
	double loadAvg15;
	
	public DefaultNodeLoadAverageInfo(double[] loadAvg ) {
		loadAvg1 = loadAvg[0];
		loadAvg5 = loadAvg[1];
		loadAvg15 = loadAvg[2];
	}
	
	public String getLoadAvg1() {
		return String.valueOf(loadAvg1);
	}

	public String getLoadAvg5() {
		return String.valueOf(loadAvg5);
	}

	public String getLoadAvg15() {
		return String.valueOf(loadAvg15);
	}

	public String getCurrentRunnableProcessCount() {
		return null;
	}

	public String getTotalProcessCount() {
		return null;
	}

}
