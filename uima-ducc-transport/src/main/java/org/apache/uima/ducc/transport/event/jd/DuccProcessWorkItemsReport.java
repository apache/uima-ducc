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
import org.apache.uima.ducc.transport.event.common.DuccProcessWorkItems;
import org.apache.uima.ducc.transport.event.common.IDuccProcessWorkItems;

public class DuccProcessWorkItemsReport implements IDuccProcessWorkItemsReport {
	
	private static final long serialVersionUID = 1L;
	
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
		//TODO
		map.put(key, value);
	}



}
