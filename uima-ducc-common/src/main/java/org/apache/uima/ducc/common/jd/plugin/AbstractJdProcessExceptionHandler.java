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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.uima.cas.CAS;

public abstract class AbstractJdProcessExceptionHandler implements IJdProcessExceptionHandler {

	protected static final String className = AbstractJdProcessExceptionHandler.class.getName();

	protected static final int maxJobErrors = 15;
	protected static final int maxProcessErrors = 3;
	protected static final int maxCasTimeouts = 2;
	
	protected AtomicInteger jobErrorCounter = new AtomicInteger(0);
	protected ConcurrentHashMap<String,AtomicInteger> mapProcessErrorCounts = new ConcurrentHashMap<String,AtomicInteger>();
	protected ConcurrentHashMap<String,AtomicInteger> mapCasTimeoutCounts = new ConcurrentHashMap<String,AtomicInteger>();
	
	/**
	 * processId 	is the process identifier
	 * cas 			is the CAS for which the exception occurred
	 * e 			is the Exception that occurred
	 */
	public Directive handle(String processId, CAS cas, Throwable t, Properties properties) {
		Directive directive = null;
		try {
			directive = handleRetry(processId, cas, t, properties);
			if(directive != null) {
				return directive;
			}
			directive = handleError(processId, cas, t, properties);
			if(directive != null) {
				return directive;
			}
			directive = Directive.ProcessContinue_CasNoRetry;
			directive.setReason("default");
		}
		catch(Exception e) {
			directive = Directive.ProcessContinue_CasNoRetry;
			directive.setReason("exception during exception handler");
		}
		
		return directive;
	}
	
	protected void record(String message) {
		System.out.println(className+": "+message);
	}
	
	protected void record(StringBuffer message) {
		record(message.toString());
	}
	
	protected boolean isEqual(String s1, String s2) {
		if(s1 == null) {
			return false;
		}
		if(s2 == null) {
			return false;
		}
		return s1.equals(s2);
	}
	
	private enum AnalysisOfCause {
		RemoteTimeout,
		LocalTimeout,
		Other
	}
	
	protected AnalysisOfCause getAnalysisOfCause(Throwable t) {
		AnalysisOfCause retVal = AnalysisOfCause.Other;
		if(t != null) {
			Throwable nextCause = t.getCause();
			int level = 0;
			while( nextCause != null ) {
				String sCause = nextCause.toString();
				record(sCause);
				if(isEqual(sCause,"org.apache.uima.aae.error.UimaASProcessCasTimeout")) {
					if(level > 0) {
						retVal = AnalysisOfCause.RemoteTimeout;
					}
					else {
						retVal = AnalysisOfCause.LocalTimeout;
					}
					break;
        		}
        		nextCause = nextCause.getCause();
        		level++;
			}
		}
		return retVal;
	}
	
	protected boolean isLocalTimeout(AnalysisOfCause analysisOfCause) {
		boolean retVal = false;
		if(analysisOfCause != null) {
			switch(analysisOfCause) {
			case LocalTimeout:
				retVal = true;
				break;
			}
		}
		return retVal;
	}
	
	protected Directive handleRetry(String processId, CAS cas, Throwable t, Properties properties) {
		Directive directive = null;
		AnalysisOfCause analysisOfCause = getAnalysisOfCause(t);
		if(isLocalTimeout(analysisOfCause)) {
			// if the maximum number of CAS timeouts is exceeded
			//   then do not retry CAS
			AtomicInteger casTimeoutCounter;
			String casId = (String) properties.get(JdProperties.SequenceNumber);
			synchronized(mapCasTimeoutCounts) {
				if(!mapCasTimeoutCounts.containsKey(casId)) {
					casTimeoutCounter = new AtomicInteger(0);
					mapCasTimeoutCounts.put(casId, casTimeoutCounter);
				}
			}
			casTimeoutCounter = mapCasTimeoutCounts.get(casId);
			Integer casTimeouts = casTimeoutCounter.addAndGet(1);
			if(casTimeouts >= maxCasTimeouts) {
				directive = Directive.ProcessStop_CasNoRetry;
				directive.setReason("casTimeouts:"+casTimeouts+" >= "+"maxCasTimeouts:"+maxCasTimeouts);
			}
			else {
				directive = Directive.ProcessStop_CasRetry;
				directive.setReason("CasTimeout");
			}
			// record to log
			StringBuffer message = new StringBuffer();
			message.append("directive="+directive);
			message.append(", ");
			message.append("["+casId+"]="+casTimeouts+" "+"timeouts");
			record(message);
		}
		return directive;
	}
	
	protected Directive handleError(String processId, CAS cas, Throwable t, Properties properties) {
		// default is to continue the process and not retry the CAS
		Directive directive = Directive.ProcessContinue_CasNoRetry;
		directive.setReason("default");
		// insure non-null process identifier
		if(processId == null) {
			StringBuffer message = new StringBuffer();
			message.append("processId missing");
			record(message);
			processId = "unknown";
		}
		// if the maximum number of process errors is exceeded
		//   then cancel process and do not retry CAS
		AtomicInteger processErrorCounter;
		synchronized(mapProcessErrorCounts) {
			if(!mapProcessErrorCounts.containsKey(processId)) {
				processErrorCounter = new AtomicInteger(0);
				mapProcessErrorCounts.put(processId, processErrorCounter);
			}
		}
		processErrorCounter = mapProcessErrorCounts.get(processId);
		Integer processErrors = processErrorCounter.addAndGet(1);
		if(processErrors >= maxProcessErrors) {
			directive = Directive.ProcessStop_CasNoRetry;
			directive.setReason("processErrors:"+processErrors+" >= "+"maxProcessErrors:"+maxProcessErrors);
		}
		// if the maximum number of job errors is exceeded
		//   then cancel job
		Integer jobErrors = jobErrorCounter.addAndGet(1);
		if(jobErrors >= maxJobErrors) {
			directive = Directive.JobStop;
			directive.setReason("jobErrors:"+jobErrors+" >= "+"maxJobErrors:"+maxJobErrors);
		}
		// record to log
		StringBuffer message = new StringBuffer();
		message.append("directive="+directive);
		message.append(", ");
		message.append("["+processId+"]="+processErrors);
		message.append(", ");
		message.append("[total]="+jobErrors);
		record(message);
		return directive;
	}
}
