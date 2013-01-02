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

public class DuccProcessWorkItems implements IDuccProcessWorkItems {

	private static final long serialVersionUID = 1L;

	private AtomicLong dispatch = new AtomicLong(0);
	private AtomicLong done = new AtomicLong(0);
	private AtomicLong error = new AtomicLong(0);
	private AtomicLong retry = new AtomicLong(0);
	private AtomicLong preempt = new AtomicLong(0);
	private AtomicLong completedMillisTotal = new AtomicLong(0);
	private AtomicLong completedMillisMax = new AtomicLong(0);
	private AtomicLong completedMillisMin = new AtomicLong(0);
	
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

	@Override
	public long getCountDispatch() {
		long retVal = 0;
		try {
			retVal = dispatch.get();
		}
		catch(Throwable t) {
		}
		return retVal;
	}

	@Override
	public long getCountDone() {
		long retVal = 0;
		try {
			retVal = done.get();
		}
		catch(Throwable t) {
		}
		return retVal;
	}

	@Override
	public long getCountError() {
		long retVal = 0;
		try {
			retVal = error.get();
		}
		catch(Throwable t) {
		}
		return retVal;
	}

	@Override
	public long getCountRetry() {
		long retVal = 0;
		try {
			retVal = retry.get();
		}
		catch(Throwable t) {
		}
		return retVal;
	}

	@Override
	public long getCountPreempt() {
		long retVal = 0;
		try {
			retVal = preempt.get();
		}
		catch(Throwable t) {
		}
		return retVal;
	}
	
	@Override
	public long getSecsAvg() {
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

	@Override
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

	@Override
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
	
	// <test>
	
	private static void stats(DuccProcessWorkItems pwi) {
		System.out.println("     done: "+pwi.getCountDone());
		System.out.println("    error: "+pwi.getCountError());
		System.out.println("    retry: "+pwi.getCountRetry());
		System.out.println(" dispatch: "+pwi.getCountDispatch());
		System.out.println("avg(secs): "+pwi.getSecsAvg());
		System.out.println("min(secs): "+pwi.getSecsMin());
		System.out.println("max(secs): "+pwi.getSecsMax());
	}
	
	private static void done(DuccProcessWorkItems pwi, int msecs) {
		pwi.dispatch();
		pwi.done(msecs);
	}
	
	private static void error(DuccProcessWorkItems pwi) {
		pwi.dispatch();
		pwi.error();
	}
	
	private static void retry(DuccProcessWorkItems pwi) {
		pwi.dispatch();
		pwi.retry();
	}
	
	public static void main(String[] args) {
		DuccProcessWorkItems pwi = new DuccProcessWorkItems();
		pwi.dispatch();
		done(pwi,30000);
		done(pwi,40000);
		for(int i=0; i<9; i++) {
			error(pwi);
		}
		for(int i=0; i<4; i++) {
			retry(pwi);
		}
		stats(pwi);
	}
	
	// </test>
}
