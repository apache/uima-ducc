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
package org.apache.uima.ducc.jd;

import org.apache.uima.ducc.common.jd.files.perf.PerformanceSummaryWriter;
import org.apache.uima.ducc.common.jd.files.workitem.WorkItemStateKeeper;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.jd.client.CasDispatchMap;
import org.apache.uima.ducc.transport.event.jd.v1.DriverStatusReportV1;


public interface IJobDriverAccess {
	
	public CasDispatchMap getCasDispatchMap();
	public DriverStatusReportV1 getDriverStatusReportLive();
	public DriverStatusReportV1 getDriverStatusReportCopy();
	public WorkItemStateKeeper getWorkItemStateKeeper();
	public PerformanceSummaryWriter getPerformanceSummaryWriter();
	public void assignLocation(IJobDriver jobDriver, String casId, String nodeIP, String PID);
	public void accountingWorkItemIsDispatch(DuccId processId);
	public void accountingWorkItemIsPreempt(DuccId processId);
	public void accountingWorkItemIsRetry(DuccId processId);
	public void accountingWorkItemIsError(DuccId processId);
	public void accountingWorkItemIsLost(DuccId processId);
	public void accountingWorkItemIsDone(DuccId processId, long time);
	public void rectifyStatus();
}
