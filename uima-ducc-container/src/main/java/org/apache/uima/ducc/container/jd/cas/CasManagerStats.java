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
package org.apache.uima.ducc.container.jd.cas;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.uima.ducc.container.common.MessageBuffer;
import org.apache.uima.ducc.container.common.logger.IComponent;
import org.apache.uima.ducc.container.common.logger.ILogger;
import org.apache.uima.ducc.container.common.logger.Logger;

public class CasManagerStats {

	private static Logger logger = Logger.getLogger(CasManagerStats.class, IComponent.Id.JD.name());
	
	public enum RetryReason { ProcessPreempt, ProcessVolunteered, ProcessDown, NodeDown, UserErrorRetry, TimeoutRetry };
	
	private AtomicInteger crTotal = new AtomicInteger(0);
	private AtomicInteger crGets = new AtomicInteger(0);
	
	private AtomicInteger retryQueuePuts = new AtomicInteger(0);
	private AtomicInteger retryQueueGets = new AtomicInteger(0);
	
	private AtomicInteger endSuccess = new AtomicInteger(0);
	private AtomicInteger endFailure = new AtomicInteger(0);
	private AtomicInteger endRetry = new AtomicInteger(0);
	
	private AtomicBoolean seenAll = new AtomicBoolean(false);
	private AtomicBoolean killJob = new AtomicBoolean(false);
	
	private ConcurrentHashMap<String,AtomicInteger> retryReasonsMap = new ConcurrentHashMap<String,AtomicInteger>();
	
	public boolean isExhausted() {
		boolean retVal = false;
		if(getCrTotal() == getEnded()) {
			retVal = true;
		}
		return retVal;
	}
	
	// 1. CR has no more work items &&
	// 2. CR provided work items have all been processed &&
	// 3. number of processed work items < total number specified by user?
	public boolean isPremature() {
		String location = "isPremature";
		boolean retVal = false;
		if(isSeenAll()) {	// no more work items in CR
			if(crGets.get() > 0) {	// at least one work item fetched from CR
				if(crGets.get() == getEnded()) {	// all work items fetched from CR have been processed
					if(crGets.get() < crTotal.get()) {	// work items processed less than total specified by user
						retVal = true;
					}
				}
			}
		}
		if(retVal) {
			MessageBuffer mb = new MessageBuffer();
			mb.append("seenAll:"+seenAll.get());
			mb.append("crGets:"+crGets.get());
			mb.append("crTotal:"+crTotal.get());
			mb.append("endSuccess:"+endSuccess.get());
			mb.append("endFailure:"+endFailure.get());
			logger.debug(location, ILogger.null_id, mb.toString());
		}
		return retVal;
	}
	
	// CR has no more work items available?
	public boolean isSeenAll() {
		boolean retVal = seenAll.get();
		return retVal;
	}
	
	// CR has no more work items available
	public void setSeenAll() {
		seenAll.set(true);
	}
	
	public int getUnfinishedWorkCount() {
		return crTotal.get() - getEnded();
	}
	
	public int getPendingRetry() {
		return retryQueuePuts.get() - retryQueueGets.get();
	}
	
	public int getEnded() {
		return endSuccess.get() + endFailure.get();
	}
	
	public int getDispatched() {
		return (crGets.get() - getEnded()) - getPendingRetry();
	}
	
	public void setCrTotal(int value) {
		crTotal.set(value);
	}
	
	public int getCrTotal() {
		return crTotal.get();
	}
	
	public void incCrGets() {
		crGets.incrementAndGet();
	}
	
	public int getCrGets() {
		return crGets.get();
	}
	
	public void incRetryQueuePuts() {
		retryQueuePuts.incrementAndGet();
	}
	
	public int getRetryQueuePuts() {
		return retryQueuePuts.get();
	}
	
	public void incRetryQueueGets() {
		retryQueueGets.incrementAndGet();
	}
	
	public int getRetryQueueGets() {
		return retryQueueGets.get();
	}
	
	public void incRetryReasons(RetryReason retryReason) {
		if(retryReason != null) {
			String key = retryReason.name();
			retryReasonsMap.putIfAbsent(key, new AtomicInteger(0));
			AtomicInteger value = retryReasonsMap.get(key);
			value.incrementAndGet();
		}
	}
	
	public ConcurrentHashMap<String,AtomicInteger> getRetryReasons() {
		return retryReasonsMap;
	}
	
	public int getNumberOfPreemptions() {
		int retVal = 0;
		String key = RetryReason.ProcessPreempt.name();
		if(retryReasonsMap.containsKey(key)) {
			AtomicInteger value = retryReasonsMap.get(key);
			retVal = value.get();
		}
		return retVal;
	}
	
	public int getNumberOfRetrys() {
		int retVal = getEndRetry() - getNumberOfPreemptions();
		return retVal;
	}
	
	public void incEndSuccess() {
		endSuccess.incrementAndGet();
	}
	
	public int getEndSuccess() {
		return endSuccess.get();
	}
	
	public void incEndFailure() {
		endFailure.incrementAndGet();
	}
	
	public int getEndFailure() {
		return endFailure.get();
	}
	
	public void incEndRetry() {
		endRetry.incrementAndGet();
	}
	
	public int getEndRetry() {
		return endRetry.get();
	}
	
	public void setKillJob() {
		killJob.set(true);
	}
	
	public boolean isKillJob() {
		return killJob.get();
	}
}
