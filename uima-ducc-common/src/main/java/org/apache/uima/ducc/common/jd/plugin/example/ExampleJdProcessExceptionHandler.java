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
package org.apache.uima.ducc.common.jd.plugin.example;

import java.util.HashMap;
import java.util.Properties;

import org.apache.uima.cas.CAS;
import org.apache.uima.ducc.common.jd.plugin.IJdProcessExceptionHandler;


public class ExampleJdProcessExceptionHandler implements IJdProcessExceptionHandler {

	private static final String className = ExampleJdProcessExceptionHandler.class.getName();

	private static final int maxJobErrors = 15;
	private static final int maxProcessErrors = 1;
	
	private Integer jobErrorCounter = new Integer(0);
	private HashMap<String,Integer> map = new HashMap<String,Integer>();
	
	/**
	 * processId 	is the process identifier
	 * cas 			is the CAS for which the exception occurred
	 * e 			is the Exception that occurred
	 */
	public synchronized Directive handle(String processId, CAS cas, Throwable t, Properties properties) {
		// default is to continue the process but not retry the CAS
    Directive directive = Directive.ProcessContinue_CasNoRetry;
		// insure non-null process identifier
		if(processId == null) {
			StringBuffer message = new StringBuffer();
			message.append(className);
			message.append(" "+"processId missing");
			System.err.println(message);
			processId = "unknown";
		}
		// if the maximum number of process errors is exceeded
		//   then cancel process and do not retry CAS
		Integer processErrorCounter;
		if(map.containsKey(processId)) {
			processErrorCounter = map.get(processId);
			processErrorCounter += 1;
		}
		else {
			processErrorCounter = new Integer(1);
			map.put(processId, processErrorCounter);
		}
		if(processErrorCounter >= maxProcessErrors) {
			directive = Directive.ProcessStop_CasNoRetry;
		}
		// if the maximum number of job errors is exceeded
		//   then cancel job
		jobErrorCounter++;
		if(jobErrorCounter >= maxJobErrors) {
			directive = Directive.JobStop;
		}
		// record to log
		StringBuffer message = new StringBuffer();
		message.append(className);
		message.append(" "+"directive="+directive);
		message.append(" "+"["+processId+"]="+processErrorCounter);
		message.append(" "+"[total]="+jobErrorCounter);
		System.out.println(message);
		return directive;
	}
}
