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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.uima.ducc.logger.ToLog;
import org.apache.uima.ducc.user.common.QuotedOptions;
import org.apache.uima.ducc.user.error.iface.Transformer;
import org.apache.uima.ducc.user.jd.JdUser;

public class ErrorHandler implements IErrorHandler {

	/**
	 * These are the System Properties nominally specified in the
	 * user's job submission (as -D's for driver_jvm_args) which will 
	 * be considered for adjusting runtime operational characteristics.
	 */
	public static enum SystemPropertyNames {
		JobDriverErrorHandlerMaximumNumberOfTimeoutRetrysPerWorkItem,
	}
	
	/**
	 * Return a directive with isKillWorkItem() == false unless and until 
	 * the maximumNumberOfTimeoutRetrysPerWorkItem is exceeded.
	 */
	private AtomicInteger maximumNumberOfTimeoutRetrysPerWorkItem = new AtomicInteger(0);
	
	/**
	 * Flag to insure initialization occurs exactly once.
	 */
	private AtomicBoolean alreadyInitialized = new AtomicBoolean(false);
	
	/**
	 * A map comprising an entry for each work item with a corresponding count
	 * of the number of times the work item has been retried
	 */
	private ConcurrentHashMap<String,AtomicLong> retryMap = new ConcurrentHashMap<String,AtomicLong>();
	
	public enum InitializationDataKey { 
		KillJobLimit("max_job_errors"), 
		;
		
		private String altname = null;
		
		private InitializationDataKey() {
			altname = name();
		}
		
		private InitializationDataKey(String value) {
			altname = value;
		}
		
		public String altname() {
			return altname;
		}
	};
	
	private static int DefaultJobErrorLimit = JdUser.DefaultJobErrorLimit;
	
	private AtomicInteger jobErrorLimit = new AtomicInteger(DefaultJobErrorLimit);
	
	private AtomicInteger jobErrorCount = new AtomicInteger(0);
	
	public ErrorHandler() {
	}
	
	public ErrorHandler(String initializationData) {
		initialize(initializationData);
	}
	
	/**
	 * Insure we initialize exactly once
	 */
	private void initializeOnce() {
		synchronized(ErrorHandler.class) {
			if(!alreadyInitialized.get()) {
				Properties systemProperties = System.getProperties();
				initTimeoutRetrys(systemProperties);
				alreadyInitialized.set(true);
			}
		}
	}
	
	/**
	 * Use the user specified -DJobDriverErrorHandlerMaximumNumberOfTimeoutRetrysPerWorkItem to set
	 * max timeouts per work item, if specified otherwise 0 meaning no retrys
	 */
	private void initTimeoutRetrys(Properties systemProperties) {
		String key = SystemPropertyNames.JobDriverErrorHandlerMaximumNumberOfTimeoutRetrysPerWorkItem.name();
		if(systemProperties != null) {
			if(systemProperties.containsKey(key)) {
				String value = systemProperties.getProperty(key);
				try {
					int integer = Integer.parseInt(value);
					if(integer < 0) {
						String text = "Invalid: "+key+"="+value;
						ToLog.info(ErrorHandler.class,text);
					}
					else {
						maximumNumberOfTimeoutRetrysPerWorkItem.set(integer);;
						String text = "Override: "+key+"="+maximumNumberOfTimeoutRetrysPerWorkItem.get();
						ToLog.info(ErrorHandler.class,text);
					}
				}
				catch(Exception e) {
					String text = "Invalid: "+key+"="+value;
					ToLog.info(ErrorHandler.class,text);
				}
			}
			else {
				String text = "Default: "+key+"="+maximumNumberOfTimeoutRetrysPerWorkItem.get();
				ToLog.info(ErrorHandler.class,text);
			}
		}
		else {
			String text = "Default: "+key+"="+maximumNumberOfTimeoutRetrysPerWorkItem.get();
			ToLog.info(ErrorHandler.class,text);
		}
	}
	
	private Map<String, String> parse(String initializationData) {
		Map<String, String> map = new HashMap<String, String>();
		try {
			if(initializationData != null) {
				ArrayList<String> toks = QuotedOptions.tokenizeList(initializationData, true);
				if(toks != null) {
					for(String tok : toks) {
						String[] split = tok.split("=");
						String key = split[0].trim().toLowerCase();
						String value = split[1].trim();
						map.put(key, value);
					}
				}
			} 
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		return map;
	}
	
	@Override
	public void initialize(String initializationData) {
		if(initializationData != null) {
			Map<String, String> map = parse(initializationData);
			String key;
			key = InitializationDataKey.KillJobLimit.name().toLowerCase();
			if(map.containsKey(key)) {
				String value = map.get(key);
				initKillJob(value);
			}
			else {
				String altkey = InitializationDataKey.KillJobLimit.altname();
				if(map.containsKey(altkey)) {
					String value = map.get(altkey);
					initKillJob(value);
				}
			}
		}
	}

	private void initKillJob(String value) {
		try {
			int expect = DefaultJobErrorLimit;
			int update = Integer.parseInt(value);
			jobErrorLimit.compareAndSet(expect, update);
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public IErrorHandlerDirective handle(String serializedCAS, Object object) {
		initializeOnce();
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
					ToLog.info(ErrorHandler.class, userThrowable);
					if(serializedCAS != null) {
						retryMap.putIfAbsent(serializedCAS, new AtomicLong(0));
						AtomicLong retryCount = retryMap.get(serializedCAS);
						long count = retryCount.incrementAndGet();
						if(count <= maximumNumberOfTimeoutRetrysPerWorkItem.get()) {
							jdUserDirective.resetKillWorkItem();
							String text = "retry # "+count+" of "+maximumNumberOfTimeoutRetrysPerWorkItem.get()+" for: "+serializedCAS;
							ToLog.info(ErrorHandler.class,text);
						}
						else {
							jobErrorCount.incrementAndGet();
						}
					}
					else {
						jobErrorCount.incrementAndGet();
					}
				}
				// User code exception
				else {
					Object byteArray = object;
					userThrowable = Transformer.deserialize(byteArray);
					userThrowable.getClass();
					jobErrorCount.incrementAndGet();
				}
			}
			else {
				jobErrorCount.incrementAndGet();
			}
			if(jobErrorCount.get() > jobErrorLimit.get()) {
				jdUserDirective.setKillJob();
			}
		}
		catch(Exception e) {
			e.printStackTrace();
		}
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
