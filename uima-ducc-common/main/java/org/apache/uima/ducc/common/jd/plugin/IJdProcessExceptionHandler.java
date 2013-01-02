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
package org.apache.uima.ducc.common.jd.plugin;

import java.util.Properties;

import org.apache.uima.cas.CAS;

public interface IJdProcessExceptionHandler {
	
	public static Directive defaultDirective = Directive.ProcessStop_CasNoRetry;
	
	public enum Directive { ProcessContinue_CasNoRetry, 
							ProcessContinue_CasRetry,
							ProcessStop_CasNoRetry,
							ProcessStop_CasRetry,
							JobStop,
							Pass;
							
							private String reason = null;
							
							public boolean hasReason() {
								return reason != null;
							}
							
							public void setReason(String reason) {
								this.reason = reason;
							}
							
							public String getReason() {
								return reason;
							}
						  };
	
	/**
	 * processId 	is the process identifier
	 * cas 			is the CAS for which the exception occurred
	 * t 			is the Throwable that occurred
	 * properties	is a set of associated properties comprising:
	 * 					SequenceNumber - DUCC runtime supplied work item sequence number
	 */
						  
	public Directive handle(String processId, CAS cas, Throwable t, Properties properties);
	
	public static enum JdProperties {
		SequenceNumber,
	}
	
}
