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
package org.apache.uima.ducc;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.uima.ducc.ErrorHandlerProgrammability.Key;
import org.apache.uima.ducc.logger.ToLog;
import org.apache.uima.ducc.user.error.iface.Transformer;

public class ErrorHandler implements IErrorHandler {

	private String initializationData = null;
	private ErrorHandlerProgrammability ehp = null;
	
	/**
	 * A map comprising an entry for each work item with a corresponding count
	 * of the number of times the work item has been retried
	 */
	private ConcurrentHashMap<String,AtomicLong> retryMap = new ConcurrentHashMap<String,AtomicLong>();
	
	/**
	 * The number of work item errors encountered for the job
	 */
	private AtomicInteger jobErrorCount = new AtomicInteger(0);
	
	public ErrorHandler() {
	}
	
	public ErrorHandler(String initializationData) {
		setInitializationData(initializationData);
	}
	
	private void setInitializationData(String value) {
		initializationData = value;
	}
	
	private String getInitializationData() {
		return initializationData;
	}
	
	@Override
	public void initialize(String initializationData) {
		setInitializationData(initializationData);
	}
	
	private void wiError(ErrorHandlerDirective jdUserDirective) {
		int count = jobErrorCount.incrementAndGet();
		Integer max_job_errors = ehp.getInteger(Key.max_job_errors);
		// kill job if max errors limit is surpassed
		if(count > max_job_errors) {
			jdUserDirective.setKillJob();
		}
		StringBuffer sb = new StringBuffer();
		sb.append("work item error count:"+count);
		ToLog.info(ErrorHandler.class, sb.toString());
	}
	
	@Override
	public IErrorHandlerDirective handle(String serializedCAS, Object object) {
		// Do not actually initialize until the first handle situation
		// arises so as to not create the file ErrorHandler.log unless 
		// a work item error or timeout occurs.
		synchronized(ErrorHandler.class) {
			if(ehp == null) {
				ehp = new ErrorHandlerProgrammability(getInitializationData());
			}
		}
		// Make ready a default directive for return
		ErrorHandlerDirective jdUserDirective = new ErrorHandlerDirective();
		try {
			Throwable userThrowable = null;
			if(serializedCAS != null) {
				// CAS is provided
			}
			if(object != null) {
				// JD exception (e.g. timeout)
				if(object instanceof Exception) {
					userThrowable = (Throwable) object;
					userThrowable.getClass();
					ToLog.info(ErrorHandler.class, serializedCAS);
					ToLog.warning(ErrorHandler.class, userThrowable);
					if(serializedCAS != null) {
						retryMap.putIfAbsent(serializedCAS, new AtomicLong(0));
						AtomicLong retryCount = retryMap.get(serializedCAS);
						long count = retryCount.incrementAndGet();
						Integer max_timeout_retrys_per_workitem = ehp.getInteger(Key.max_timeout_retrys_per_workitem);
						// don't kill work item if still eligible for timeout retry
						if(count <= max_timeout_retrys_per_workitem) {
							jdUserDirective.resetKillWorkItem();
							String text = "retry # "+count+" of "+max_timeout_retrys_per_workitem+" for: "+serializedCAS;
							ToLog.info(ErrorHandler.class,text);
						}
						else {
							wiError(jdUserDirective);
						}
					}
					else {
						wiError(jdUserDirective);
					}
				}
				// User code exception
				else {
					Object byteArray = object;
					userThrowable = Transformer.deserialize(byteArray);
					userThrowable.getClass();
					wiError(jdUserDirective);
				}
			}
			else {
				wiError(jdUserDirective);
			}
		}
		catch(Exception e) {
			wiError(jdUserDirective);
			e.printStackTrace();
			ToLog.warning(ErrorHandler.class,e);
		}
		// record results in ErrorHandler.log
		StringBuffer sb = new StringBuffer();
		sb.append("KillJob: ");
		sb.append(jdUserDirective.isKillJob());
		sb.append(" ");
		sb.append("KillProcess: ");
		sb.append(jdUserDirective.isKillProcess());
		sb.append(" ");
		sb.append("KillKillWorkItem: ");
		sb.append(jdUserDirective.isKillWorkItem());
		ToLog.info(ErrorHandler.class, sb.toString());
		return jdUserDirective;
	}

}
