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
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.uima.ducc.common.jd.files.workitem.RemoteLocation;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.container.common.Util;
import org.apache.uima.ducc.container.common.logger.IComponent;
import org.apache.uima.ducc.container.common.logger.Logger;
import org.apache.uima.ducc.container.jd.mh.iface.IOperatingInfo;
import org.apache.uima.ducc.container.jd.mh.iface.IOperatingInfo.CompletionType;
import org.apache.uima.ducc.container.jd.mh.iface.IProcessInfo;
import org.apache.uima.ducc.container.jd.mh.iface.IWorkItemInfo;
import org.apache.uima.ducc.container.net.iface.IMetaCasTransaction.JdState;
import org.apache.uima.ducc.transport.event.common.DuccPerWorkItemStatistics;
import org.apache.uima.ducc.transport.event.common.DuccProcessWorkItems;
import org.apache.uima.ducc.transport.event.common.IDuccCompletionType.JobCompletionType;
import org.apache.uima.ducc.transport.event.common.IDuccPerWorkItemStatistics;
import org.apache.uima.ducc.transport.event.common.IDuccProcess;
import org.apache.uima.ducc.transport.event.common.IDuccProcessMap;
import org.apache.uima.ducc.transport.event.common.IDuccProcessWorkItems;
import org.apache.uima.ducc.transport.event.common.IRationale;
import org.apache.uima.ducc.transport.event.common.Rationale;
import org.apache.uima.ducc.transport.event.jd.IDriverState.DriverState;

public class JobDriverReport implements Serializable, IDriverStatusReport {

	private static Logger logger = Logger.getLogger(JobDriverReport.class, IComponent.Id.JD.name());
	
	private static final long serialVersionUID = 200L;

	private DuccId duccId = null;
	private String node = null;
	private int port = 0;
	private String jdState = null;
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
	
	private boolean killJob = false;
	
	private ArrayList<IWorkItemInfo> listActiveWorkItemInfo = null;
	
	private ConcurrentHashMap<RemoteLocation, Long> mapProcessOperatingMillis = null;
	private ConcurrentHashMap<RemoteLocation, Long> mapProcessInvestmentMillis = null;
	
	private String jpDeployable = null;
	
	private JobCompletionType jobCompletionType = JobCompletionType.EndOfJob;
	private IRationale jobCompletionRationale = null;
	
	private IDuccPerWorkItemStatistics duccPerWorkItemStatistics = null;
	
	private DuccProcessWorkItemsReport duccProcessWorkItemsReport = null;
	
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
	
	private DuccId getDuccId(IOperatingInfo operatingInfo) {
		DuccId retVal = null;
		try {
			String jobId = operatingInfo.getJobId();
			long value = Long.parseLong(jobId);
			retVal = new DuccId(value);
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		return retVal;
	}
	
	public JobDriverReport(IOperatingInfo operatingInfo, IDuccProcessMap dpMap) {
		String location = "JobDriverReport";
		setDuccId(getDuccId(operatingInfo));
		setJdState(operatingInfo.getJdState());
		//setJmxUrl(driverContainer.getJmxUrl());
		setWorkItemsTotal(operatingInfo.getWorkItemCrTotal());
		setWorkItemsProcessingCompleted(operatingInfo.getWorkItemEndSuccesses());
		setWorkItemsProcessingError(operatingInfo.getWorkItemEndFailures());
		setWorkItemsDispatched(operatingInfo.getWorkItemDispatcheds());
		setWorkItemsRetry(operatingInfo.getWorkItemRetrys());
		setWorkItemsPreempt(operatingInfo.getWorkItemPreemptions());
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
			setCompletionType(operatingInfo.getCompletionType());
			String completionText = operatingInfo.getCompletionText();
			if(completionText != null) {
				IRationale completionRationale = new Rationale(completionText);
				setCompletionRationale(completionRationale);
			}
		}
		// operating map
		setActiveWorkItemInfo(operatingInfo.getActiveWorkItemInfo());
		// JpDeployable
		setJpDeployable(operatingInfo.getJpDeployable());
		// per work statistics
		DuccPerWorkItemStatistics perWorkItemStatistics = new DuccPerWorkItemStatistics(
			operatingInfo.getWorkItemFinishedMillisMax(),
			operatingInfo.getWorkItemFinishedMillisMin(),
			operatingInfo.getWorkItemFinishedMillisAvg(),
			operatingInfo.getWorkItemFinishedMillisStdDev()
			);
		setPerWorkItemStatistics(perWorkItemStatistics);
		// per process statistics
		ArrayList<IProcessInfo> list = operatingInfo.getProcessItemInfo();
		if(list != null) {
			if(!list.isEmpty()) {
				duccProcessWorkItemsReport = new DuccProcessWorkItemsReport();
				for(IProcessInfo pi : list) {
					String ip = pi.getNodeAddress();
					int pid = pi.getPid();
					IDuccProcess dp = dpMap.findProcess(ip, ""+pid);
					if(dp != null) {
						DuccId key = dp.getDuccId();
						IDuccProcessWorkItems value = new DuccProcessWorkItems(pi);
						duccProcessWorkItemsReport.accum(key, value);
					}
					else {
						logger.debug(location, null, "process not found: "+"ip="+ip+" "+"pid="+pid);
						int i = 0;
						for(Entry<DuccId, IDuccProcess> entry : dpMap.entrySet()) {
							IDuccProcess value = entry.getValue();
							logger.debug(location, null, "process["+i+"]: "+"ip="+value.getNodeIdentity().getIp()+" "+"pid="+value.getPID());
							i++;
						}
					}
				}
			}
			else {
				logger.debug(location, null, "list is empty");
			}
		}
		else {
			logger.debug(location, null, "list is null");
		}
	}
	
	private void setDuccId(DuccId value) {
		duccId = value;
	}
	
	public void setNode(String value) {
		node = value;
	}
	
	public void setPort(int value) {
		port = value;
	}
	
	public void setJdState(String value) {
		jdState = value;
	}
	
	public void setJmxUrl(String value) {
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
	
	private void setWorkItemsDispatched(int value) {
		workItemsDispatched = value;
	}
	
	private void setWorkItemsRetry(int value) {
		workItemsRetry = value;
	}
	
	private void setWorkItemsPreempt(int value) {
		workItemsPreempted = value;
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
	
	private void setJpDeployable(String value) {
		jpDeployable = value;
	}
	
	private void setPerWorkItemStatistics(IDuccPerWorkItemStatistics value) {
		duccPerWorkItemStatistics = value;
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
	public String getNode() {
		return node;
	}
	
	@Override
	public int getPort() {
		return port;
	}

	@Override
	public String getJdState() {
		return jdState;
	}
	
	@Override
	public String getLogReport() {
		StringBuffer sb = new StringBuffer();
		sb.append("state: "+getJdState()+" ");
		sb.append("total: "+getWorkItemsTotal()+" ");
		sb.append("done: "+getWorkItemsProcessingCompleted()+" ");
		sb.append("error: "+getWorkItemsProcessingError()+" ");
		sb.append("killJob: "+isKillJob()+" ");
		return sb.toString();
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
	public String getUimaDeploymentDescriptor() {
		return null;
	}

	@Override
	public String getUimaAnalysisEngine() {
		return jpDeployable;
	}
	
	@Override
	public Iterator<DuccId> getKillDuccIds() {
		// TODO Auto-generated method stub
		return null;
	}

	@Deprecated
	@Override
	public DriverState getDriverState() {
		DriverState retVal = DriverState.Undefined;
		String state = getJdState();
		if(state != null) {
			if(state.equals(JdState.Initializing.name())) {
				retVal = DriverState.Initializing;
			}
			else if(state.equals(JdState.Active.name())) {
				retVal = DriverState.Running;
			}
			else if(state.equals(JdState.Ended.name())) {
				retVal = DriverState.Completed;
			}
		}
		return retVal;
	}

	private void setCompletionType(CompletionType completionType) {
		switch(completionType) {
		case Normal:
			break;
		case Exception:
			jobCompletionType = JobCompletionType.CanceledByDriver;
			break;
		}
	}
	
	private void setCompletionRationale(IRationale value) {
		jobCompletionRationale = value;
	}
	
	@Override
	public JobCompletionType getJobCompletionType() {
		return jobCompletionType;
	}

	@Override
	public IRationale getJobCompletionRationale() {
		return jobCompletionRationale;
	}

	@Override
	public IDuccPerWorkItemStatistics getPerWorkItemStatistics() {
		return duccPerWorkItemStatistics;
	}

	@Override
	public DuccProcessWorkItemsReport getDuccProcessWorkItemsMap() {
		return duccProcessWorkItemsReport;
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
	public ConcurrentHashMap<RemoteLocation, Long> getInvestmentMillisMap() {
		mapProcessInvestmentMillis = new ConcurrentHashMap<RemoteLocation, Long>();
		if(listActiveWorkItemInfo != null) {
			for(IWorkItemInfo wii: listActiveWorkItemInfo) {
				String nodeIP = wii.getNodeAddress();
				String PID = ""+wii.getPid();
				RemoteLocation rl = new RemoteLocation(nodeIP, PID);
				mapProcessInvestmentMillis.put(rl, wii.getInvestmentMillis());
			}
		}
		return mapProcessInvestmentMillis;
	}
	
}
