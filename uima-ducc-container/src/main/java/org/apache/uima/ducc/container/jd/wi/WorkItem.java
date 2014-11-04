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
import org.apache.uima.ducc.container.jd.fsm.wi.WiFsm;
import org.apache.uima.ducc.container.net.iface.IMetaCas;

public class WorkItem implements IWorkItem {

	private IMetaCas metaCas = null;
	private IFsm fsm = null;
	
	private Tod todGet = new Tod();
	private Tod todAck = new Tod();
	private Tod todEnd = new Tod();
	
	public WorkItem(IMetaCas metaCas, IFsm fsm) {
		setMetaCas(metaCas);
		setFsm(fsm);
	}
	
	@Override
	public void setMetaCas(IMetaCas value) {
		metaCas = value;
	}
	
	@Override
	public IMetaCas getMetaCas() {
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
	public void resetTods() {
		todGet.reset();
		todAck.reset();
		todEnd.reset();
	}
	
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
	}
	
	@Override
	public void resetTodAck() {
		todAck.reset();
	}

	@Override
	public long getTodAck() {
		return todAck.get();
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
				if(state.getName().equals(WiFsm.CAS_Active.getName())) {
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
}
