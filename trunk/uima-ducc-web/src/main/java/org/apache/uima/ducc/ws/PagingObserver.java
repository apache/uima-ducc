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

import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.common.DuccWorkJob;
import org.apache.uima.ducc.transport.event.common.IDuccWork;
import org.apache.uima.ducc.transport.event.common.IDuccWorkJob;

public class PagingObserver {

	private static PagingObserver instance = new PagingObserver();
	
	public static int intervalInSeconds = 60;
	
	public static PagingObserver getInstance() {
		return instance;
	}
	
	private ConcurrentSkipListMap<DuccId,PagingInfo> mapData = new ConcurrentSkipListMap<DuccId,PagingInfo>();
	private ConcurrentSkipListMap<DuccId,PagingInfo> mapDiff = new ConcurrentSkipListMap<DuccId,PagingInfo>();
	
	public PagingObserver() {
	}
	
	public void put(IDuccWork dw) {
		if(dw != null) {
			if(dw instanceof IDuccWorkJob) {
				IDuccWorkJob job = (IDuccWorkJob) dw;
				if(!job.isCompleted()) {
					put(job);
				}
			}
		}
	}
	
	private void update(DuccId id, PagingInfo pCurr, PagingInfo pPrev) {
		PagingInfo pDiff = new PagingInfo(pCurr.total-pPrev.total, pCurr.procs-pPrev.procs);
		mapData.put(id, pCurr);
		mapDiff.put(id, pDiff);
	}
	
	public void put(IDuccWorkJob job) {
		if(job != null) {
			DuccId id = job.getDuccId();
			if(id != null) {
				PagingInfo pCurr = new PagingInfo(job);
				PagingInfo pPrev = mapData.get(id);
				if(pPrev == null) {
					update(id, pCurr, pCurr);
				}
				else {
					long elapsedMillis = pCurr.timestamp - pPrev.timestamp;
					if(elapsedMillis > intervalInSeconds * 1000) {
						update(id, pCurr, pPrev);
					}
				}
			}
		}
	}
	
	public void remove(DuccId duccId) {
		if(duccId != null) {
			mapData.remove(duccId);
			mapDiff.remove(duccId);
		}
	}
	
	public PagingInfo getData(DuccId duccId) {
		PagingInfo retVal = null;
		if(duccId != null) {
			retVal = mapData.get(duccId);
		}
		if(retVal == null) {
			retVal = new PagingInfo();
		}
		return retVal;
	}
	
	public PagingInfo getDiff(DuccId duccId) {
		PagingInfo retVal = null;
		if(duccId != null) {
			retVal = mapDiff.get(duccId);
		}
		if(retVal == null) {
			retVal = new PagingInfo();
		}
		return retVal;
	}
	
	public long getDiffCount(DuccId duccId) {
		return getDiff(duccId).total;
	}
	
	public boolean isPaging(DuccId duccId) {
		return getDiffCount(duccId) > 0;
	}
	
	public boolean isPaging(IDuccWorkJob job) {
		boolean retVal = false;
		if(job != null) {
			DuccId duccId = job.getDuccId();
			retVal = isPaging(duccId);
		}
		return retVal;
	}
	
	public static void main(String[] args) {
		PagingObserver po = PagingObserver.getInstance();
		DuccId id = null;
		po.remove(id);
		id = new DuccId(0);
		po.remove(id);
		PagingInfo pi = new PagingInfo();
		pi.display();
		IDuccWorkJob job = new DuccWorkJob();
		pi = new PagingInfo(job);
		pi.display();
	}
}
