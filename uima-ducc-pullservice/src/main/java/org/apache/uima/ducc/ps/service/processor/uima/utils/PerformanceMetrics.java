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

package org.apache.uima.ducc.ps.service.processor.uima.utils;

public class PerformanceMetrics {

	private String name;
	private String uniqueName;
	private long analysisTime;
	private long numberOfTasksProcessed;;
	
	/**
	 * Creates a performance metrics instance
	 * 
	 */
	public PerformanceMetrics(String name, String uimaContextPath, long analysisTime, long taskCount) {
		this.name = name;
		this.uniqueName = uimaContextPath;
		this.analysisTime = analysisTime;
		this.numberOfTasksProcessed = taskCount;
	}

	/**
	 * Gets the local name of the component as specified in the aggregate
	 * 
	 * @return the name
	 */
	public String getName() {
		return name;
	}

	/**
	 * Gets the unique name of the component reflecting its location in the
	 * aggregate hierarchy
	 * 
	 * @return the unique name
	 */
	public String getUniqueName() {
		if (uniqueName != null && uniqueName.trim().length() > 0 && !uniqueName.trim().equals("Components")) {
			// if ( !uimaContextPath.endsWith(getName())) {
			// return uimaContextPath+"/"+getName();
			// }
			return uniqueName;
		} else {
			return getName();
		}
	}

	/**
	 * Gets the elapsed time the CAS spent analyzing this component
	 * 
	 * @return time in milliseconds
	 */
	public long getAnalysisTime() {
		return analysisTime;
	}

	/**
	 * Gets how many tasks have been processed so far
	 * 
	 * @return number of tasks processed so far
	 */
	public long getNumberOfTasksProcessed() {
		return numberOfTasksProcessed;
	}
}
