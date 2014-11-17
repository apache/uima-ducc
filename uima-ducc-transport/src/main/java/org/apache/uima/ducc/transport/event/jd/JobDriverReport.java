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
package org.apache.uima.ducc.transport.event.jd;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.uima.ducc.common.jd.files.workitem.RemoteLocation;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.container.common.Util;
import org.apache.uima.ducc.container.jd.mh.iface.IOperatingInfo;
import org.apache.uima.ducc.container.jd.mh.iface.IWorkItemInfo;
import org.apache.uima.ducc.transport.event.common.IDuccCompletionType.JobCompletionType;
import org.apache.uima.ducc.transport.event.common.IDuccPerWorkItemStatistics;
import org.apache.uima.ducc.transport.event.common.IDuccUimaDeploymentDescriptor;
import org.apache.uima.ducc.transport.event.common.IRationale;
import org.apache.uima.ducc.transport.event.jd.IDriverState.DriverState;

public class JobDriverReport implements Serializable, IDriverStatusReport {

	private static final long serialVersionUID = 200L;

	private DuccId duccId = null;
	private String jmxUrl = null;
	
	private long workItemsTotal = 0;
	private int workItemsProcessingCompleted = 0;
	private int workItemsProcessingError = 0;
	private int workItemsRetry = 0;
	private int workItemsDispatched = 0;
	private int workItemsPreempted = 0;
	
	private long wiMillisMin = 0;
	private long wiMillisMax = 0;
	private long wiMillisAvg = 0;
	private long wiMillisOperatingLeast = 0;
	private long wiMillisCompletedMost = 0;
	
	private long wiTodMostRecentStart = 0;
	
	private boolean wiPending = true;
	private boolean wiPendingProcessAssignment = false;
	
	private boolean killJob = false;
	
	private ArrayList<IWorkItemInfo> listActiveWorkItemInfo = null;
	
	private ConcurrentHashMap<RemoteLocation, Long> mapProcessOperatingMillis = null;
	
	private long max(long a, long b) {
		long retVal = a;
		if(b > a) {
			retVal = b;
		}
		return retVal;
	}
	
	private long min(long a, long b) {
		long retVal = a;
		if(b < a) {
			retVal = b;
		}
		return retVal;
	}
	
	public JobDriverReport(IOperatingInfo operatingInfo) {
		//setDuccId(driverContainer.getDuccId());
		//setJmxUrl(driverContainer.getJmxUrl());
		setWorkItemsTotal(operatingInfo.getWorkItemCrTotal());
		setWorkItemsProcessingCompleted(operatingInfo.getWorkItemEndSuccesses());
		setWorkItemsProcessingError(operatingInfo.getWorkItemEndFailures());
		setWorkItemsRetry(operatingInfo.getWorkItemUserProcessingErrorRetries());
		setWorkItemsDispatched(operatingInfo.getWorkItemJpSends()-(operatingInfo.getWorkItemEndSuccesses()+operatingInfo.getWorkItemEndFailures()));
		// min of finished & running
		long fMin = operatingInfo.getWorkItemFinishedMillisMin();
		long min = fMin;
		long rMin = operatingInfo.getWorkItemRunningMillisMin();
		if(rMin > 0) {
			min = min(fMin, rMin);
		}
		setWiMillisMin(min);
		// max of finished & running
		long fMax = operatingInfo.getWorkItemFinishedMillisMax();
		long max = fMax;
		long rMax = operatingInfo.getWorkItemRunningMillisMax();
		if(rMax > 0) {
			max = max(fMax, rMax);
		}
		setWiMillisMax(max);
		// avg of finished
		long avg = operatingInfo.getWorkItemFinishedMillisAvg();
		setWiMillisAvg(avg);
		// min of running
		setWiMillisOperatingLeast(rMin);
		// max of finished
		setWiMillisCompletedMost(fMax);
		// most recent start TOD
		setMostRecentStart(operatingInfo.getWorkItemTodMostRecentStart());
		// pending means CR fetches < crTotal
		setWiPending(operatingInfo.isWorkItemCrPending());
		// kill job?
		if(operatingInfo.isKillJob()) {
			setKillJob();
		}
		// operating map
		setActiveWorkItemInfo(operatingInfo.getActiveWorkItemInfo());
	}
	
	private void setDuccId(DuccId value) {
		duccId = value;
	}
	
	private void setJmxUrl(String value) {
		jmxUrl = value;
	}
	
	private void setWorkItemsTotal(long value) {
		workItemsTotal = value;
	}
	
	private void setWorkItemsProcessingCompleted(int value) {
		workItemsProcessingCompleted = value;
	}
	
	private void setWorkItemsProcessingError(int value) {
		workItemsProcessingError = value;
	}
	
	private void setWorkItemsRetry(int value) {
		workItemsRetry = value;
	}
	
	private void setWorkItemsDispatched(int value) {
		workItemsDispatched = value;
	}
	
	private void setWiMillisMin(long value) {
		wiMillisMin = value;
	}
	
	private void setWiMillisMax(long value) {
		wiMillisMax = value;
	}
	
	private void setWiMillisAvg(long value) {
		wiMillisAvg = value;
	}
	
	private void setWiMillisOperatingLeast(long value) {
		wiMillisOperatingLeast = value;
	}
	
	private void setWiMillisCompletedMost(long value) {
		wiMillisCompletedMost = value;
	}
	
	private void setMostRecentStart(long value) {
		wiTodMostRecentStart = value;
	}
	
	private void setWiPending(boolean value) {
		wiPending = value;
	}
	
	private void setKillJob() {
		killJob = true;
	}
	
	private void setActiveWorkItemInfo(ArrayList<IWorkItemInfo> value) {
		listActiveWorkItemInfo = value;
	}
	
	@Override
	public long getVersion() {
		return serialVersionUID;
	}
	
	@Override
	public DuccId getDuccId() {
		return duccId;
	}

	@Override
	public String getLogReport() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long getWorkItemsTotal() {
		return workItemsTotal;
	}

	@Override
	public int getWorkItemsProcessingCompleted() {
		return workItemsProcessingCompleted;
	}

	@Override
	public int getWorkItemsProcessingError() {
		return workItemsProcessingError;
	}

	@Override
	public int getWorkItemsRetry() {
		return workItemsRetry;
	}

	@Override
	public int getWorkItemsDispatched() {
		return workItemsDispatched;
	}

	@Override
	public int getWorkItemsPreempted() {
		return workItemsPreempted;
	}

	@Override
	public int getWorkItemsLost() {
		return 0;
	}

	@Override
	public int getWorkItemPendingProcessAssignmentCount() {
		return 0;
	}

	@Override
	public long getWiMillisMin() {
		return wiMillisMin;
	}

	@Override
	public long getWiMillisMax() {
		return wiMillisMax;
	}

	@Override
	public long getWiMillisAvg() {
		return wiMillisAvg;
	}

	@Override
	public long getWiMillisOperatingLeast() {
		return wiMillisOperatingLeast;
	}

	@Override
	public long getWiMillisCompletedMost() {
		return wiMillisCompletedMost;
	}

	@Override
	public long getMostRecentStart() {
		return wiTodMostRecentStart;
	}

	@Override
	public boolean isPending() {
		return wiPending;
	}

	@Override
	public boolean isWorkItemPendingProcessAssignment() {
		return wiPendingProcessAssignment;
	}

	@Override
	public boolean isKillJob() {
		return killJob;
	}

	@Override
	public boolean isOperating(String nodeIP, String PID) {
		boolean retVal = false;
		if(listActiveWorkItemInfo != null) {
			for(IWorkItemInfo wii : listActiveWorkItemInfo) {
				if(Util.compare(wii.getNodeAddress(), nodeIP)) {
					if(Util.compare(""+wii.getPid(), PID)) {
						retVal = true;
						break;
					}
				}
			}
		}
		return retVal;
	}

	@Override
	public String getJdJmxUrl() {
		return jmxUrl;
	}

	@Override
	public IDuccUimaDeploymentDescriptor getUimaDeploymentDescriptor() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Iterator<DuccId> getKillDuccIds() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DriverState getDriverState() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public JobCompletionType getJobCompletionType() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IRationale getJobCompletionRationale() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IDuccPerWorkItemStatistics getPerWorkItemStatistics() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DuccProcessWorkItemsMap getDuccProcessWorkItemsMap() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ConcurrentHashMap<RemoteLocation, Long> getOperatingMillisMap() {
		if(mapProcessOperatingMillis == null) {
			mapProcessOperatingMillis = new ConcurrentHashMap<RemoteLocation, Long>();
			if(listActiveWorkItemInfo != null) {
				for(IWorkItemInfo wii: listActiveWorkItemInfo) {
					String nodeIP = wii.getNodeAddress();
					String PID = ""+wii.getPid();
					RemoteLocation rl = new RemoteLocation(nodeIP, PID);
					if(!mapProcessOperatingMillis.containsKey(rl)) {
						mapProcessOperatingMillis.put(rl, new Long(0));
					}
					long millis = wii.getOperatingMillis() + mapProcessOperatingMillis.get(rl);
					mapProcessOperatingMillis.put(rl, new Long(millis));
				}
			}
		}
		return mapProcessOperatingMillis;
	}

	@Override
	public ConcurrentHashMap<Integer, DuccId> getLimboMap() {
		ConcurrentHashMap<Integer, DuccId> map = new ConcurrentHashMap<Integer, DuccId>();
		return map;
	}

	@Override
	public ConcurrentHashMap<String, DuccId> getCasQueuedMap() {
		ConcurrentHashMap<String, DuccId> map = new ConcurrentHashMap<String, DuccId>();
		return map;
	}

}
