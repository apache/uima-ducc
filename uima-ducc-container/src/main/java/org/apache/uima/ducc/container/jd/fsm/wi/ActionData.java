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

import org.apache.uima.ducc.container.jd.mh.iface.remote.IRemoteWorkerThread;
import org.apache.uima.ducc.container.jd.wi.IWorkItem;
import org.apache.uima.ducc.ps.net.iface.IMetaTaskTransaction;

public class ActionData implements IActionData {
	
	private IWorkItem workItem = null;
	private IRemoteWorkerThread remoteWorkerThread = null;
	private IMetaTaskTransaction metaCasTransaction = null;
	
	public ActionData(IWorkItem workItem, IRemoteWorkerThread remoteWorkerThread, IMetaTaskTransaction metaCasTransaction) {
		setWorkItem(workItem);
		setRemoteWorkerThread(remoteWorkerThread);
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
	public IRemoteWorkerThread getRemoteWorkerThread() {
		return remoteWorkerThread;
	}
	
	private void setRemoteWorkerThread(IRemoteWorkerThread value) {
		remoteWorkerThread = value;
	}
	
	@Override
	public IMetaTaskTransaction getMetaCasTransaction() {
		return metaCasTransaction;
	}
	
	private void setMetaCasTransaction(IMetaTaskTransaction value) {
		metaCasTransaction = value;
	}
}
