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

import org.apache.uima.aae.client.UimaAsynchronousEngine;
import org.apache.uima.ducc.common.internationalization.Messages;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.jd.JobDriverContext;


public class WorkItemFactory {
	
	private static DuccLogger duccOut = DuccLoggerComponents.getJdOut(WorkItemFactory.class.getName());
	private static Messages duccMsg = JobDriverContext.getInstance().getSystemMessages();
	
	private UimaAsynchronousEngine client;
	private DuccId jobId = null;
	private IWorkItemMonitor workItemMonitor = null;
	
	public WorkItemFactory(UimaAsynchronousEngine client, DuccId jobId, IWorkItemMonitor workItemMonitor) {
		init(client,jobId,workItemMonitor);
	}
	
	private void init(UimaAsynchronousEngine client, DuccId jobId, IWorkItemMonitor workItemMonitor) {
		String methodName = "init";
		duccOut.trace(methodName, jobId, duccMsg.fetch("enter"));
		this.client = client;
		this.jobId = jobId;
		this.workItemMonitor = workItemMonitor;
		duccOut.debug(methodName, jobId, duccMsg.fetchLabel("work item monitor class")+workItemMonitor.getClass().getName());
		duccOut.trace(methodName, jobId, duccMsg.fetch("exit"));
		return;
	}
	
	public WorkItem create(CasTuple casTupple) {
		String methodName = "create";
		duccOut.trace(methodName, jobId, duccMsg.fetch("enter"));
		duccOut.debug(methodName, jobId, duccMsg.fetchLabel("seqNo")+casTupple.getSeqno());
		WorkItem workItem;
		workItem = new WorkItem(client,casTupple,jobId,workItemMonitor);
		duccOut.trace(methodName, jobId, duccMsg.fetch("exit"));
		return workItem;
	}
}
