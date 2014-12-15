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
package org.apache.uima.ducc.container.jd.wi;

import java.util.concurrent.atomic.AtomicLong;

public class ProcessStatistics implements IProcessStatistics {

	private AtomicLong dispatch = new AtomicLong(0);
	private AtomicLong done = new AtomicLong(0);
	private AtomicLong error = new AtomicLong(0);
	private AtomicLong preempt = new AtomicLong(0);
	private AtomicLong retry = new AtomicLong(0);
	
	private IWorkItemStatistics wis = new WorkItemStatistics();
	
	@Override
	public void dispatch(IWorkItem wi) {
		dispatch.incrementAndGet();
	}

	@Override
	public void done(IWorkItem wi) {
		dispatch.decrementAndGet();
		done.incrementAndGet();
		wis.ended(wi);
	}

	@Override
	public void error(IWorkItem wi) {
		dispatch.decrementAndGet();
		error.incrementAndGet();
	}

	@Override
	public void preempt(IWorkItem wi) {
		dispatch.decrementAndGet();
		preempt.incrementAndGet();
	}

	@Override
	public void retry(IWorkItem wi) {
		dispatch.decrementAndGet();
		retry.incrementAndGet();
	}

	@Override
	public long getCountDispatch() {
		return dispatch.get();
	}

	@Override
	public long getCountDone() {
		return done.get();
	}

	@Override
	public long getCountError() {
		return error.get();
	}

	@Override
	public long getCountPreempt() {
		return preempt.get();
	}

	@Override
	public long getCountRetry() {
		return retry.get();
	}

	@Override
	public long getMillisAvg() {
		return wis.getMillisAvg();
	}

	@Override
	public long getMillisMax() {
		return wis.getMillisMax();
	}

	@Override
	public long getMillisMin() {
		return wis.getMillisMin();
	}

	@Override
	public long getMillisStdDev() {
		return wis.getMillisStdDev();
	}

}
