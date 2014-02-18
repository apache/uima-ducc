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
package org.apache.uima.ducc.jd.client;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.jd.IJobDriver;
import org.apache.uima.ducc.transport.event.common.IDuccProcess;
import org.apache.uima.ducc.transport.event.common.IDuccProcessMap;


public class CasLimbo {

	private static DuccLogger duccOut = DuccLoggerComponents.getJdOut(CasLimbo.class.getName());
	
	private ConcurrentLinkedQueue<CasTuple> tupleQueue = new ConcurrentLinkedQueue<CasTuple>();
	
	private IJobDriver jd = null;
	
	public CasLimbo(IJobDriver jd) {
		this.jd = jd;
	}
	
	private IDuccProcessMap getProcessMap() {
		return jd.getJob().getProcessMap();
	}
	
	private DuccId getJobId() {
		return jd.getJob().getDuccId();
	}
	
	public void put(CasTuple casTuple) {
		tupleQueue.add(casTuple);
		jd.getDriverStatusReportLive().limboAdd(casTuple.getSeqno(),casTuple.getDuccId());
	}
	
	public boolean isEmpty() {
		String location = "isEmpty";
		boolean retVal = (size() == 0);
		duccOut.trace(location, getJobId(), retVal);
		return retVal;
	}
	
	public boolean hasAvailable() {
		String location = "hasAvailable";
		boolean retVal = false;
		Iterator<CasTuple> iterator = tupleQueue.iterator();
		if(!iterator.hasNext()) {
			duccOut.trace(location, getJobId(), "empty");
		}
		else {
			while(iterator.hasNext()) {
				CasTuple casTuple = iterator.next();
				if(isAvailable(casTuple)) {
					retVal = true;
					duccOut.debug(location, getJobId(), casTuple.getDuccId(), "available"+" "+"seqNo:"+casTuple.getSeqno()+" "+"wiId:"+casTuple.getCasDocumentText());
					break;
				}
				else {
					duccOut.trace(location, getJobId(), casTuple.getDuccId(), "not available"+" "+"seqNo:"+casTuple.getSeqno()+" "+"wiId:"+casTuple.getCasDocumentText());
				}
			}
		}
		return retVal; 
	}
	
	public CasTuple get() {
		String location = "get";
		CasTuple retVal = null;
		Iterator<CasTuple> iterator = tupleQueue.iterator();
		if(!iterator.hasNext()) {
			duccOut.trace(location, getJobId(), "empty");
		}
		else {
			while(iterator.hasNext()) {
				CasTuple casTuple = iterator.next();
				if(isAvailable(casTuple)) {
					if(tupleQueue.remove(casTuple)) {
						jd.getDriverStatusReportLive().limboRemove(casTuple.getSeqno(),casTuple.getDuccId());
						use(casTuple);
						retVal = casTuple;
						duccOut.debug(location, getJobId(), casTuple.getDuccId(), "available"+" "+"seqNo:"+casTuple.getSeqno()+" "+"wiId:"+casTuple.getCasDocumentText());
						break;
					}
				}
				else {
					duccOut.trace(location, getJobId(), casTuple.getDuccId(), "not available"+" "+"seqNo:"+casTuple.getSeqno()+" "+"wiId:"+casTuple.getCasDocumentText());
				}
			}
		}
		return retVal; 
	}
	
	private void use(CasTuple casTuple) {
		String location = "use";
		DuccId pDuccId = casTuple.getDuccId();
		if(pDuccId == null) {
			duccOut.debug(location, getJobId(), casTuple.getDuccId(), "process ID null");
		}
		else {
			IDuccProcessMap processMap = getProcessMap();
			IDuccProcess process = processMap.getProcess(pDuccId);
			if(process == null) {
				duccOut.debug(location, getJobId(), casTuple.getDuccId(), "process is null");
			}
			else {
				if(process.isPreempted()) {
					jd.getDriverStatusReportLive().countWorkItemsPreempted();
					jd.accountingWorkItemIsPreempt(pDuccId);
					duccOut.debug(location, getJobId(), casTuple.getDuccId(), "preempted:t"+" "+"seqNo:"+casTuple.getSeqno()+" "+"wiId:"+casTuple.getCasDocumentText());
				}
				else {
					jd.getDriverStatusReportLive().countWorkItemsRetry();
					jd.accountingWorkItemIsRetry(pDuccId);
					duccOut.debug(location, getJobId(), casTuple.getDuccId(), "preempted:f"+" "+"seqNo:"+casTuple.getSeqno()+" "+"wiId:"+casTuple.getCasDocumentText());
				}
			}
		}
	}
	
	private boolean isAvailable(CasTuple casTuple) {
		String location = "isAvailable";
		boolean retVal = false;
		DuccId pDuccId = casTuple.getDuccId();
		if(casTuple.isDelayedRetry()) {
			//retVal = false;
		}
		else if(pDuccId == null) {
			retVal = true;
			duccOut.debug(location, getJobId(), casTuple.getDuccId(), "process ID is null");
		}
		else {
			IDuccProcessMap processMap = getProcessMap();
			IDuccProcess process = processMap.getProcess(pDuccId);
			if(process == null) {
				retVal = true;
				duccOut.debug(location, getJobId(), casTuple.getDuccId(), "process is null");
			}
			else {	
				if(process.isDefunct()) {
					retVal = true;		// process ended, no wait necessary	
					duccOut.debug(location, getJobId(), casTuple.getDuccId(), "process is defunct");
				}
				else if(process.isPreempted()) {	
					retVal = false;		// process scheduled to die (by OR), wait
					duccOut.debug(location, getJobId(), casTuple.getDuccId(), "process is preempted");
				}
				else if(jd.getDriverStatusReportLive().isKillProcess(pDuccId)) {
					retVal = false;		// process scheduled to die (by JD), wait
					duccOut.debug(location, getJobId(), casTuple.getDuccId(), "process is killed");
				}
				else {
					retVal = true;		// process is survivor
					duccOut.debug(location, getJobId(), casTuple.getDuccId(), "process is survivor");
				}
			}
		}
		return retVal;
	}
	
	public ArrayList<CasTuple> release() {
		String location = "release";
		ArrayList<CasTuple> retVal = new ArrayList<CasTuple>();
		Iterator<CasTuple> iterator = tupleQueue.iterator();
		while(iterator.hasNext()) {
			CasTuple casTuple = iterator.next();
			boolean released = casTuple.undelay();
			if(released) {
				duccOut.debug(location, getJobId(), "seqNo:"+casTuple.getSeqno());
				casTuple.setRetry();
				retVal.add(casTuple);
			}
		}
		return retVal;
	}
	
	public int delayedSize() {
		Iterator<CasTuple> iterator = tupleQueue.iterator();
		int count = 0;
		while(iterator.hasNext()) {
			CasTuple casTuple = iterator.next();
			if(casTuple.isDelayedRetry()) {
				count++;
			}
		}
		return count;
	}
	
	public int size() {
		String location = "size";
		int retVal = tupleQueue.size();
		duccOut.debug(location, getJobId(), retVal);
		return retVal;
	}
	
	public void logReport() {
		String location = "logReport";
		Iterator<CasTuple> iterator = tupleQueue.iterator();
		while(iterator.hasNext()) {
			CasTuple casTuple = iterator.next();
			if(isAvailable(casTuple)) {
				duccOut.info(location, getJobId(), casTuple.getDuccId(), "available"+" "+"seqNo:"+casTuple.getSeqno()+" "+"wiId:"+casTuple.getCasDocumentText());
			}
			else {
				duccOut.info(location, getJobId(), casTuple.getDuccId(), "not available"+" "+"seqNo:"+casTuple.getSeqno()+" "+"wiId:"+casTuple.getCasDocumentText());
			}
		}
	}
	
	public void rectifyStatus() {
		String location = "rectifyStatus";
		Iterator<CasTuple> iterator = tupleQueue.iterator();
		if(!iterator.hasNext()) {
			duccOut.trace(location, getJobId(), "empty");
		}
		else {
			while(iterator.hasNext()) {
				CasTuple casTuple = iterator.next();
				if(isAvailable(casTuple)) {
					jd.getDriverStatusReportLive().limboRemove(casTuple.getSeqno(),casTuple.getDuccId());
					duccOut.debug(location, getJobId(), casTuple.getDuccId(), "available"+" "+"seqNo:"+casTuple.getSeqno()+" "+"wiId:"+casTuple.getCasDocumentText());
				}
				else {
					duccOut.trace(location, getJobId(), casTuple.getDuccId(), "not available"+" "+"seqNo:"+casTuple.getSeqno()+" "+"wiId:"+casTuple.getCasDocumentText());
				}
			}
		}
	}
}
