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
package org.apache.uima.ducc.ws;

import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.common.DuccWorkPopDriver;
import org.apache.uima.ducc.transport.event.common.IDuccProcess;
import org.apache.uima.ducc.transport.event.common.IDuccProcessMap;
import org.apache.uima.ducc.transport.event.common.IDuccWorkJob;

public class PagingInfo {
	
	public long total = 0;
	public long procs = 0;
	
	public long timestamp = System.currentTimeMillis();
	
	PagingInfo() {	
	}
	
	PagingInfo(long total, long procs) {	
		this.total = total;
		this.procs = procs;
	}
	
	PagingInfo(IDuccWorkJob job) {
		if(job != null) {
			count(this, job.getProcessMap());
			DuccWorkPopDriver driver = job.getDriver();
			if(driver != null) {
				count(this, driver.getProcessMap());
			}
		}
	}
	
	private void count(PagingInfo pagingInfo, IDuccProcessMap pMap) {
		if(pMap != null) {
			for(DuccId pId : pMap.keySet()) {
				IDuccProcess process = pMap.get(pId);
				total += process.getMajorFaults();
				procs += 1;
			}
		}
	}
	
	public void display() {
		System.out.println("total:"+total+" "+"procs:"+procs);
	}
}
