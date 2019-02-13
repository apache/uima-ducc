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
package org.apache.uima.ducc.container.jd.wi.perf.files;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

public class PerformanceMetricsIndividualItem implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	private String name;
	private String uniqueName;
	
	private AtomicLong analysisTime = new AtomicLong(0);
	
	public PerformanceMetricsIndividualItem(String name, String uniqueName) {
		this.name = name;
		this.uniqueName = uniqueName;
	}
	
	public PerformanceMetricsIndividualItem(String name, 
										 String uniqueName,
										 long analysisTime
										 ) 
	{
		this.name = name;
		this.uniqueName = uniqueName;
		this.analysisTime.set(analysisTime);
	}
	
	public String getName() {
		return name;
	}
	
	public String getUniqueName() {
		return uniqueName;
	}
	
	public long getAnalysisTime() {
		return analysisTime.get();
	}
	
	//
	
	public long addAndGetAnalysisTime(long delta) {
		return analysisTime.addAndGet(delta);
	}
	
}
