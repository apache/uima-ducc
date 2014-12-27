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
package org.apache.uima.ducc.transport.event.jd;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.container.common.logger.IComponent;
import org.apache.uima.ducc.container.common.logger.Logger;
import org.apache.uima.ducc.transport.event.common.DuccProcessWorkItems;
import org.apache.uima.ducc.transport.event.common.IDuccProcessWorkItems;

public class DuccProcessWorkItemsReport implements IDuccProcessWorkItemsReport {
	
	private static final long serialVersionUID = 1L;

	private static Logger logger = Logger.getLogger(DuccProcessWorkItemsReport.class, IComponent.Id.JD.name());
	
	private ConcurrentHashMap<DuccId, IDuccProcessWorkItems> map = new ConcurrentHashMap<DuccId, IDuccProcessWorkItems>();
	private IDuccProcessWorkItems totals = new DuccProcessWorkItems();
	
	@Override
	public ConcurrentHashMap<DuccId, IDuccProcessWorkItems> getMap() {
		return map;
	}
	@Override
	public IDuccProcessWorkItems getTotals() {
		return totals;
	}
	
	@Override
	public void accum(DuccId key, IDuccProcessWorkItems value) {
		String location = "accum";
		long dispatch = totals.getCountDispatch();
		long done = totals.getCountDone();
		long error = totals.getCountError();
		long preempt = totals.getCountPreempt();
		long retry = totals.getCountRetry();
		// dispatch
		long newDispatch = dispatch+value.getCountDispatch();
		totals.setCountDispatch(newDispatch);
		// done
		long newDone = done+value.getCountDone();
		totals.setCountDone(newDone);
		// error
		long newError = error+value.getCountError();
		totals.setCountError(newError);
		// preempt
		long newPreempt = preempt+value.getCountPreempt();
		totals.setCountPreempt(newPreempt);
		// retry
		long newRetry = retry+value.getCountRetry();
		totals.setCountRetry(newRetry);
		// update avg, max, min
		if(value.getCountDone() > 0) {
			// avg
			long cnt1 = totals.getCountDone();
			long avg1 = totals.getMillisAvg();
			long cnt2 = value.getCountDone();
			long avg2 = value.getMillisAvg();
			double top = (avg1*cnt1)+(avg2*cnt2);
			double bot = (cnt1+cnt2);
			long avg = (long)(top/bot);
			totals.setMillisAvg(avg);
			// max
			long max = totals.getMillisMax();
			long maxCandidate = value.getMillisMax();
			logger.trace(location, null, "max="+max+" "+"maxCandidate="+maxCandidate);
			if(max > 0) {
				if(maxCandidate > 0) {
					if(maxCandidate > max) {
						max = maxCandidate;
					}
				}
			}
			else {
				max = maxCandidate;
			}
			totals.setMillisMax(max);
			// min
			long min = totals.getMillisMin();
			long minCandidate = value.getMillisMin();
			logger.trace(location, null, "min="+min+" "+"minCandidate="+minCandidate);
			if(min > 0) {
				if(minCandidate > 0) {
					if(minCandidate < min) {
						min = minCandidate;
					}
				}
			}
			else {
				min = minCandidate;
			}
			totals.setMillisMin(min);
		}
		// process
		map.put(key, value);
	}



}
