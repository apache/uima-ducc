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
package org.apache.uima.ducc.transport.event.jd.v1;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.common.DuccProcessWorkItems;
import org.apache.uima.ducc.transport.event.common.IDuccProcessWorkItems;

public class DuccProcessWorkItemsReportV1 extends ConcurrentHashMap<DuccId, IDuccProcessWorkItems> {

	private static final long serialVersionUID = 1L;
	
	private IDuccProcessWorkItems totals = new DuccProcessWorkItems();
	
	public IDuccProcessWorkItems getTotals() {
		return totals;
	}
	
	private IDuccProcessWorkItems get(DuccId duccId) {
		IDuccProcessWorkItems retVal = super.get(duccId);
		if(retVal == null) {
			retVal = new DuccProcessWorkItems();
			super.put(duccId, retVal);
		}
		return retVal;
	}
	
	public void done(DuccId processId, long time) {
		getTotals().done(time);
		if(processId != null) {
			get(processId).done(time);
		}
	}

	public void dispatch(DuccId processId) {
		getTotals().dispatch();
		if(processId != null) {
			get(processId).dispatch();
		}
	}
	
	public void error(DuccId processId) {
		getTotals().error();
		if(processId != null) {
			get(processId).error();
		}
	}
	
	public void retry(DuccId processId) {
		getTotals().retry();
		if(processId != null) {
			get(processId).retry();
		}
	}
	
	public void lost(DuccId processId) {
		getTotals().lost();
		if(processId != null) {
			get(processId).lost();
		}
	}
	
	public void preempt(DuccId processId) {
		getTotals().preempt();
		if(processId != null) {
			get(processId).preempt();
		}
	}
}
