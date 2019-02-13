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

import org.apache.uima.ducc.container.common.MessageBuffer;
import org.apache.uima.ducc.container.common.Standardize;
import org.apache.uima.ducc.container.common.logger.IComponent;
import org.apache.uima.ducc.container.common.logger.ILogger;
import org.apache.uima.ducc.container.common.logger.Logger;

public class ProcessStatistics implements IProcessStatistics {

	private static Logger logger = Logger.getLogger(ProcessStatistics.class, IComponent.Id.JD.name());
	
	private AtomicLong dispatch = new AtomicLong(0);
	private AtomicLong done = new AtomicLong(0);
	private AtomicLong error = new AtomicLong(0);
	private AtomicLong preempt = new AtomicLong(0);
	private AtomicLong retry = new AtomicLong(0);
	
	private IWorkItemStatistics wis = new WorkItemStatistics();
	
	private void loggit(String location, IWorkItem wi) {
		MessageBuffer mb = new MessageBuffer();
		mb.append(Standardize.Label.seqNo.get()+wi.getMetaCas().getSystemKey());
		mb.append(Standardize.Label.dispatch.get()+dispatch.get());
		mb.append(Standardize.Label.done.get()+done.get());
		mb.append(Standardize.Label.error.get()+error.get());
		mb.append(Standardize.Label.preempt.get()+preempt.get());
		mb.append(Standardize.Label.retry.get()+retry.get());
		logger.debug(location, ILogger.null_id, mb.toString());
	}
	
	@Override
	public void dispatch(IWorkItem wi) {
		String location = "dispatch";
		dispatch.incrementAndGet();
		loggit(location, wi);
	}

	@Override
	public void done(IWorkItem wi) {
		String location = "done";
		dispatch.decrementAndGet();
		done.incrementAndGet();
		wis.ended(wi);
		loggit(location, wi);
	}

	@Override
	public void error(IWorkItem wi) {
		String location = "error";
		dispatch.decrementAndGet();
		error.incrementAndGet();
		loggit(location, wi);
	}

	@Override
	public void preempt(IWorkItem wi) {
		String location = "preempt";
		dispatch.decrementAndGet();
		preempt.incrementAndGet();
		loggit(location, wi);
	}

	@Override
	public void retry(IWorkItem wi) {
		String location = "retry";
		dispatch.decrementAndGet();
		retry.incrementAndGet();
		loggit(location, wi);
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
