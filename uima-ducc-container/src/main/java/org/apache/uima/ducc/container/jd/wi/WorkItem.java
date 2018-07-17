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
package org.apache.uima.ducc.container.jd.wi;

import org.apache.uima.ducc.container.common.Assertion;
import org.apache.uima.ducc.container.common.fsm.iface.IFsm;
import org.apache.uima.ducc.container.common.fsm.iface.IState;
import org.apache.uima.ducc.container.common.logger.IComponent;
import org.apache.uima.ducc.container.common.logger.ILogger;
import org.apache.uima.ducc.container.common.logger.Logger;
import org.apache.uima.ducc.container.jd.fsm.wi.WiFsm;
import org.apache.uima.ducc.ps.net.iface.IMetaTask;

public class WorkItem implements IWorkItem {

	private static Logger logger = Logger.getLogger(WorkItem.class, IComponent.Id.JD.name());
	
	private IMetaTask metaCas = null;
	private IFsm fsm = null;
	
	private Tod todGet = new Tod();
	private Tod todAck = new Tod();
	private Tod todEnd = new Tod();
	
	private Tod todInvestment = new Tod();
	
	public WorkItem(IMetaTask metaCas, IFsm fsm) {
		setMetaCas(metaCas);
		setFsm(fsm);
	}
	
	@Override
	public void setMetaCas(IMetaTask value) {
		metaCas = value;
	}
	
	@Override
	public IMetaTask getMetaCas() {
		return metaCas;
	}
	
	@Override
	public void setFsm(IFsm value) {
		fsm = value;
	}	
	
	@Override
	public IFsm getFsm() {
		return fsm;
	}

	@Override
	public void reset() {
		metaCas = null;
		fsm.reset();
		todGet.reset();
		todAck.reset();
		todEnd.reset();
	}
	
	/*
	@Override
	public void resetTods() {
		todGet.reset();
		todAck.reset();
		todEnd.reset();
	}
	*/
	
	@Override
	public void setTodGet() {
		todGet.set();
	}

	@Override
	public void resetTodGet() {
		todGet.reset();
	}
	
	@Override
	public long getTodGet() {
		return todGet.get();
	}

	@Override
	public void setTodAck() {
		todAck.set();
		setTodInvestment();
	}
	
	@Override
	public void resetTodAck() {
		todAck.reset();
		resetTodInvestment();
	}

	@Override
	public long getTodAck() {
		return todAck.get();
	}

	@Override
	public void setTodInvestment() {
		todInvestment.set();
	}

	@Override
	public void resetTodInvestment() {
		todInvestment.reset();
	}

	@Override
	public long getTodInvestment() {
		return todInvestment.get();
	}
	
	@Override
	public void setTodEnd() {
		todEnd.set();
	}

	@Override
	public void resetTodEnd() {
		todEnd.reset();
	}

	@Override
	public long getTodEnd() {
		return todEnd.get();
	}

	@Override
	public long getMillisOperating() {
		long retVal = 0;
		long start = getTodAck();
		long end = getTodEnd();
		if(start > 0) {
			if(end == 0) {
				IState state = fsm.getStateCurrent();
				if(state.getStateName().equals(WiFsm.CAS_Active.getStateName())) {
					end = System.currentTimeMillis();
				}
				else {
					end = start;
				}
			}
			retVal = end - start;
		}
		Assertion.nonNegative(retVal);
		return retVal;
	}

	@Override
	public long getMillisInvestment() {
		long retVal = 0;
		IState state = fsm.getStateCurrent();
		if(state.getStateName().equals(WiFsm.CAS_Active.getStateName())) {
			long now = System.currentTimeMillis();
			retVal = now - getTodInvestment();
		}
		Assertion.nonNegative(retVal);
		return retVal;
	}
	
	@Override
	public int getSeqNo() {
		int retVal = 0;
		try {
			retVal = Integer.parseInt(getMetaCas().getSystemKey());
		}
		catch(Exception e) {
			// oh well
		}
		return retVal;
	}

	// Comparable
	
	@Override
	public int compareTo(Object o) {
		String location = "compareTo";
		int retVal = 0;
		try {
			if(o != null) {
				if(o instanceof IWorkItem) {
					IWorkItem that = (IWorkItem) o;
					Integer iThis = new Integer(this.getSeqNo());
					Integer iThat = new Integer(that.getSeqNo());
					retVal = iThis.compareTo(iThat);
				}
			}
		}
		catch(Exception e) {
			logger.error(location, ILogger.null_id, e);
		}
		return retVal;
	}
	
	@Override
	public int hashCode() {
		return this.getSeqNo();
	}
	
	
	@Override
	public boolean equals(Object obj) {
		String location = "equals";
		boolean retVal = false;
		try {
			if(obj != null) {
				if(this == obj) {
					retVal = true;
				}
				else {
					IWorkItem that = (IWorkItem) obj;
					if(this.compareTo(that) == 0) {
						retVal = true;
					}
				}
			}
		}
		catch(Exception e) {
			logger.error(location, ILogger.null_id, e);
		}
		return retVal;
	}
}
