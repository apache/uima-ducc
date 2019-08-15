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
package org.apache.uima.ducc.ws.server;

public class DuccLocalConstants {
	
	public static final String duccContext = "/ducc-servlet";
	
	public static final String duccContextExperimentCancelRequest = duccContext+"/experiment-cancel-request";
	
	public static final String duccContextExperiments = duccContext+"/experiments-data";
	public static final String duccContextExperimentDetails = duccContext+"/experiment-details-data";
	public static final String duccContextExperimentDetailsDirectory = duccContext+"/experiment-details-directory";
	public static final String duccContextExperimentDetailsJobs = duccContext+"/experiment-details-jobs-data";
	
	public static final String duccContextJsonExperiments = duccContext+"/json-format-aaData-experiments";
	public static final String duccContextJsonExperimentDetails = duccContext+"/json-format-aaData-experiment-details";
	public static final String duccContextJsonExperimentDetailsJobs = duccContext+"/json-format-aaData-experiment-details-jobs";
	
	public static final int maximumRecordsExperiments = 4096;
	public static final int defaultRecordsExperiments = 16;
}
