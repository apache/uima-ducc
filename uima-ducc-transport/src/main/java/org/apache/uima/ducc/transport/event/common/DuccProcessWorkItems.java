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

	private long unassigned = 0;
	
	private AtomicLong dispatch = new AtomicLong(0);
	private AtomicLong done = new AtomicLong(0);
	private AtomicLong error = new AtomicLong(0);
	private AtomicLong retry = new AtomicLong(0);
	private AtomicLong lost = new AtomicLong(0);
	private AtomicLong preempt = new AtomicLong(0);
	private AtomicLong completedMillisTotal = new AtomicLong(0);
	private AtomicLong completedMillisAvg = new AtomicLong(0);
	private AtomicLong completedMillisMax = new AtomicLong(0);
	private AtomicLong completedMillisMin = new AtomicLong(0);

	public DuccProcessWorkItems() {	
	}
	
	public DuccProcessWorkItems(IProcessInfo pi) {	
		dispatch.set(pi.getDispatch());
		done.set(pi.getDone());
		error.set(pi.getError());
		retry.set(pi.getRetry());
		lost.set(0);
		preempt.set(pi.getPreempt());
		completedMillisAvg.set(pi.getAvg());
		completedMillisMax.set(pi.getMax());
		completedMillisMin.set(pi.getMin());
	}
	
	public long getCountUnassigned() {
		return unassigned;
	}

	public void setCountUnassigned(long count) {
		unassigned = count;
	}
	
	private void setMin(long update) {
		completedMillisMin.compareAndSet(0, update);
		while(true) {
			long min = completedMillisMin.get();
			if(update < min) {
				completedMillisMin.compareAndSet(min, update);
			}
			else {
				break;
			}
		}
	}
	
	private void setMax(long update) {
		completedMillisMax.compareAndSet(0, update);
		while(true) {
			long max = completedMillisMax.get();
			if(update > max) {
				completedMillisMax.compareAndSet(max, update);
			}
			else {
				break;
			}
		}
	}
	
	public void done(long delta) {
		done.incrementAndGet();
		completedMillisTotal.addAndGet(delta);
		setMin(delta);
		setMax(delta);
		undispatch();
	}
	
	public void error() {
		error.incrementAndGet();
		undispatch();
	}
	
	public void retry() {
		retry.incrementAndGet();
		undispatch();
	}
	
	public void lost() {
		lost.incrementAndGet();
	}
	
	public void preempt() {
		preempt.incrementAndGet();
		undispatch();
	}
	
	public void dispatch() {
		dispatch.incrementAndGet();
	}
	
	private void undispatch() {
		dispatch.decrementAndGet();
	}

	
	public long getCountDispatch() {
		long retVal = 0;
		try {
			retVal = dispatch.get();
		}
		catch(Throwable t) {
		}
		return retVal;
	}

	
	public long getCountDone() {
		long retVal = 0;
		try {
			retVal = done.get();
		}
		catch(Throwable t) {
		}
		return retVal;
	}

	
	public long getCountError() {
		long retVal = 0;
		try {
			retVal = error.get();
		}
		catch(Throwable t) {
		}
		return retVal;
	}

	
	public long getCountRetry() {
		long retVal = 0;
		try {
			retVal = retry.get();
		}
		catch(Throwable t) {
		}
		return retVal;
	}
	
	
	public long getCountLost() {
		long retVal = 0;
		try {
			retVal = lost.get();
		}
		catch(Throwable t) {
		}
		return retVal;
	}

	
	public long getCountPreempt() {
		long retVal = 0;
		try {
			retVal = preempt.get();
		}
		catch(Throwable t) {
		}
		return retVal;
	}
	
	
	public long getSecsAvgV1() {
		long retVal = 0;
		try {
			long count = done.get();
			if(count > 0) {
				double msecs = (double)completedMillisTotal.get() / (double)count;
				retVal = (long)(msecs/1000);
			}
		}
		catch(Throwable t) {
		}
		return retVal;
	}
	
	public long getSecsAvg() {
		long retVal = 0;
		try {
			double msecs = (double)completedMillisAvg.get();
			retVal = (long)(msecs/1000);
		}
		catch(Throwable t) {
		}
		if(retVal == 0) {
			retVal = getSecsAvgV1();
		}
		return retVal;
	}
	
	public long getSecsMax() {
		long retVal = 0;
		try {
			double msecs = (double)completedMillisMax.get();
			retVal = (long)(msecs/1000);
		}
		catch(Throwable t) {
		}
		return retVal;
	}

	
	public long getSecsMin() {
		long retVal = 0;
		try {
			double msecs = (double)completedMillisMin.get();
			retVal = (long)(msecs/1000);
		}
		catch(Throwable t) {
		}
		return retVal;
	}
	
}
