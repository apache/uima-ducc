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
package org.apache.uima.ducc.container.jd.fsm.wi;

import org.apache.uima.ducc.container.jd.dispatch.iface.IRemoteWorkerIdentity;
import org.apache.uima.ducc.container.jd.dispatch.iface.IWorkItem;
import org.apache.uima.ducc.container.net.iface.IMetaCasTransaction;

public class ActionData implements IActionData {
	
	private IWorkItem workItem = null;
	private IRemoteWorkerIdentity remoteWorkerIdentity = null;
	private IMetaCasTransaction metaCasTransaction = null;
	
	public ActionData(IWorkItem workItem, IRemoteWorkerIdentity remoteWorkerIdentity, IMetaCasTransaction metaCasTransaction) {
		setWorkItem(workItem);
		setRemoteWorkerIdentity(remoteWorkerIdentity);
		setMetaCasTransaction(metaCasTransaction);
	}
	
	@Override
	public IWorkItem getWorkItem() {
		return workItem;
	}
	
	private void setWorkItem(IWorkItem value) {
		workItem = value;
	}
	
	@Override
	public IRemoteWorkerIdentity getRemoteWorkerIdentity() {
		return remoteWorkerIdentity;
	}
	
	private void setRemoteWorkerIdentity(IRemoteWorkerIdentity value) {
		remoteWorkerIdentity = value;
	}
	
	@Override
	public IMetaCasTransaction getMetaCasTransaction() {
		return metaCasTransaction;
	}
	
	private void setMetaCasTransaction(IMetaCasTransaction value) {
		metaCasTransaction = value;
	}
}
