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

import org.apache.uima.cas.CAS;
import org.apache.uima.ducc.common.utils.id.DuccId;


public class CasTuple {
	private CAS cas;
	private int seqno;
	private boolean retry = false;
	private boolean delayedRetry = false;
	private DuccId pDuccId= null;
	
	public CasTuple(CAS cas, int seqno) {
		init(cas, seqno);
	}
	
	private void init(CAS cas, int seqno) {
		this.cas = cas;
		this.seqno = seqno;
	}
	
	public void setCas(CAS value) {
		cas = value;
	}
	
	public CAS getCas() {
		return cas;
	}
	
	public String getCasDocumentText() {
		return ""+getCas().getDocumentText();
	}
	
	public int getSeqno() {
		return seqno;
	}
	
	public void setRetry() {
		retry = true;
	}
	
	public boolean isRetry() {
		return retry;
	}
	
	public void setDelayedRetry() {
		delayedRetry = true;
	}
	
	public boolean isDelayedRetry() {
		return delayedRetry;
	}
	
	public boolean undelay() {
		boolean retVal = delayedRetry;
		if(delayedRetry) {
			retry = true;
			delayedRetry = false;
		}
		return retVal;
	}
	
	public DuccId getDuccId() {
		return pDuccId;
	}
	
	public void setDuccId(DuccId value) {
		pDuccId = value;
	}
}
