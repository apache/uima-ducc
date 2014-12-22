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
package org.apache.uima.ducc.transport.event.common;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.uima.ducc.container.jd.mh.iface.IProcessInfo;

public class DuccProcessWorkItems implements IDuccProcessWorkItems {

	private static final long serialVersionUID = 1L;
	
	private AtomicLong dispatch = new AtomicLong(0);
	private AtomicLong done = new AtomicLong(0);
	private AtomicLong error = new AtomicLong(0);
	private AtomicLong retry = new AtomicLong(0);
	private AtomicLong preempt = new AtomicLong(0);
	
	private AtomicLong doneMillisAvg = new AtomicLong(0);
	private AtomicLong doneMillisMax = new AtomicLong(0);
	private AtomicLong doneMillisMin = new AtomicLong(0);

	public DuccProcessWorkItems() {	
	}
	
	public DuccProcessWorkItems(IProcessInfo pi) {	
		dispatch.set(pi.getDispatch());
		done.set(pi.getDone());
		error.set(pi.getError());
		retry.set(pi.getRetry());
		preempt.set(pi.getPreempt());
		doneMillisAvg.set(pi.getAvg());
		doneMillisMax.set(pi.getMax());
		doneMillisMin.set(pi.getMin());
	}
	
	public boolean isAssignedWork() {
		boolean retVal = true;
		if((getCountDispatch() == 0) 
		&& (getCountDone() == 0 )
		&& (getCountError() == 0) 
		&& (getCountPreempt() == 0) 
		&& (getCountRetry() == 0)
		) {
			retVal = false;
		}
		return retVal;
	}

	@Override
	public void setCountDispatch(long value) {
		dispatch.set(value);
	}

	@Override
	public void setCountDone(long value) {
		done.set(value);
	}

	@Override
	public void setCountError(long value) {
		error.set(value);
	}

	@Override
	public void setCountRetry(long value) {
		retry.set(value);
	}

	@Override
	public void setCountPreempt(long value) {
		preempt.set(value);
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
	public long getCountRetry() {
		return retry.get();
	}

	@Override
	public long getCountPreempt() {
		return preempt.get();
	}

	@Override
	public void setMillisAvg(long value) {
		doneMillisAvg.set(value);
	}

	@Override
	public void setMillisMax(long value) {
		doneMillisMax.set(value);
	}

	@Override
	public void setMillisMin(long value) {
		doneMillisMin.set(value);
	}
	@Override
	public long getMillisAvg() {
		return doneMillisAvg.get();
	}

	@Override
	public long getMillisMax() {
		return doneMillisMax.get();
	}

	@Override
	public long getMillisMin() {
		return doneMillisMin.get();
	}

	@Override
	public long getSecsAvg() {
		double value = doneMillisAvg.get()/1000.0;
		return (long) value;
	}

	@Override
	public long getSecsMax() {
		double value = doneMillisMax.get()/1000.0;
		return (long) value;
	}

	@Override
	public long getSecsMin() {
		double value = doneMillisMin.get()/1000.0;
		return (long) value;
	}

}
