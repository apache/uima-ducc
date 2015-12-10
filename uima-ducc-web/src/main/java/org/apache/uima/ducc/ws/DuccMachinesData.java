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
package org.apache.uima.ducc.ws;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.uima.ducc.cli.ws.json.MachineFacts;
import org.apache.uima.ducc.cli.ws.json.MachineFactsList;
import org.apache.uima.ducc.cli.ws.json.NodePidList;
import org.apache.uima.ducc.common.IDuccEnv;
import org.apache.uima.ducc.common.node.metrics.NodeUsersInfo;
import org.apache.uima.ducc.common.node.metrics.NodeUsersInfo.NodeProcess;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.TimeStamp;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.NodeMetricsUpdateDuccEvent;
import org.apache.uima.ducc.transport.event.ProcessInfo;
import org.apache.uima.ducc.ws.db.DbQuery;
import org.apache.uima.ducc.ws.db.IDbMachine;
import org.apache.uima.ducc.ws.types.Ip;
import org.apache.uima.ducc.ws.types.NodeId;
import org.apache.uima.ducc.ws.types.UserId;
import org.apache.uima.ducc.ws.utils.DatedNodeMetricsUpdateDuccEvent;

public class DuccMachinesData {

	private static DuccLogger logger = DuccLoggerComponents.getWsLogger(DuccMachinesData.class.getName());
	private static DuccId jobid = null;
	
	private static ConcurrentSkipListMap<MachineInfo,String> sortedMachines = new ConcurrentSkipListMap<MachineInfo,String>();
	private static ConcurrentSkipListMap<String,MachineInfo> unsortedMachines = new ConcurrentSkipListMap<String,MachineInfo>();
	private static ConcurrentSkipListMap<String,MachineSummaryInfo> summaryMachines = new ConcurrentSkipListMap<String,MachineSummaryInfo>();
	
	private static AtomicLong memTotal = new AtomicLong(0);
	private static AtomicLong memFree = new AtomicLong(0);
	private static AtomicLong swapInuse = new AtomicLong(0);
	private static AtomicLong swapFree = new AtomicLong(0);

	private String domain = "";
	
	private static DuccMachinesData duccMachinesData = new DuccMachinesData();
	
	private static ConcurrentSkipListMap<String,String> ipToNameMap = new ConcurrentSkipListMap<String,String>();
	private static ConcurrentSkipListMap<String,String> nameToIpMap = new ConcurrentSkipListMap<String,String>();
	private static ConcurrentSkipListMap<String,String> isSwapping = new ConcurrentSkipListMap<String,String>();
	
	private static ConcurrentSkipListMap<String,TreeMap<String,NodeUsersInfo>> ipToNodeUsersInfoMap = new ConcurrentSkipListMap<String,TreeMap<String,NodeUsersInfo>>();
	
	public static DuccMachinesData getInstance() {
		return duccMachinesData;
	}
	
	public boolean isMachineSwapping(String ip) {
		return isSwapping.containsKey(ip);
	}
	
	public ConcurrentSkipListMap<String,MachineInfo> getMachines() {
		return unsortedMachines;
	}
	
	public ConcurrentSkipListMap<MachineInfo,String> getSortedMachines() {
		ConcurrentSkipListMap<MachineInfo,String> retVal = sortedMachines;
		return retVal;
	}
	
	public void updateSortedMachines() {
		String location = "updateSortedMachines";
		logger.debug(location, jobid, "start");
		try {
			ConcurrentSkipListMap<MachineInfo,String> map = new ConcurrentSkipListMap<MachineInfo,String>();
			for(Entry<String, MachineInfo> entry : unsortedMachines.entrySet()) {
				String value = entry.getKey();
				MachineInfo key = entry.getValue();
				map.put(key, value);
				logger.debug(location, jobid, "put: "+value);
			}
			sortedMachines = map;
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
		}
		logger.debug(location, jobid, "end");
	} 
	
	private volatile String published = null;
	
	private DuccMachinesData() {
		initialize();
	}
	
	private void initialize() {
		String location = "initialize";
		try {
			String fileName = IDuccEnv.DUCC_NODES_FILE_NAME;
			String dirResources = IDuccEnv.DUCC_RESOURCES_DIR;
			ArrayList<String> nodes =  DuccNodes.getInstance().get(dirResources,fileName);
			Iterator<String> iterator = nodes.iterator();
			while(iterator.hasNext()) {
				String nodeName = (String) iterator.next();
				String memTotal = "";
				String memFree = "";
				String swapInuse = "";
				String swapFree = "";
				MachineInfo machineInfo = new MachineInfo(IDuccEnv.DUCC_NODES_FILE_PATH, "", nodeName, memTotal, memFree, swapInuse, swapFree, false, null, -1, 0);
				unsortedMachines.put(machineInfo.getName(),machineInfo);
			}
			updateSortedMachines();
		}
		catch(Throwable t) {
			logger.warn(location, jobid, t);
		}
		try {
			InetAddress ia = InetAddress.getLocalHost();
			String chn = ia.getCanonicalHostName();
			int index = chn.indexOf(".");
			if(index > 0) {
				domain = chn.substring(index);
			}
		}
		catch(Throwable t) {
			logger.warn(location, jobid, t);
		}
	}
	
	public boolean isPublished() {
		return published != null;
	}
	
	public void setPublished() {
		published = TimeStamp.getCurrentMillis();
	}
	
	public MachineSummaryInfo getTotals() {
		MachineSummaryInfo totals = new MachineSummaryInfo();
		totals.memTotal = memTotal.get();
		totals.memFree = memFree.get();
		totals.swapInuse = swapInuse.get();
		totals.swapFree = swapFree.get();
		return totals;
	}
	
	private void updateTotals(Ip ip, MachineSummaryInfo newInfo) {
		if(summaryMachines.containsKey(ip.toString())) {
			MachineSummaryInfo oldInfo = summaryMachines.get(ip.toString());
			summaryMachines.put(ip.toString(), newInfo);
			memTotal.addAndGet(newInfo.memTotal-oldInfo.memTotal);
			memFree.addAndGet(newInfo.memFree-oldInfo.memFree);
			swapInuse.addAndGet(newInfo.swapInuse-oldInfo.swapInuse);
			swapFree.addAndGet(newInfo.swapFree-oldInfo.swapFree);
		}
		else {
			summaryMachines.put(ip.toString(), newInfo);
			memTotal.addAndGet(newInfo.memTotal);
			memFree.addAndGet(newInfo.memFree);
			swapInuse.addAndGet(newInfo.swapInuse);
			swapFree.addAndGet(newInfo.swapFree);
		}
	}
	
	private String normalizeMachineName(String machineName) {
		String retVal = machineName;
		try {
			if(!unsortedMachines.containsKey(machineName)) {
				if(machineName.contains(".")) {
					int index = machineName.indexOf(".");
					String domainlessMachineName = machineName.substring(0,index);
					if(unsortedMachines.containsKey(domainlessMachineName)) {
						retVal = domainlessMachineName;
					}
				}
				else {
					String domainfullMachineName = machineName+domain;
					if(unsortedMachines.containsKey(domainfullMachineName)) {
						retVal = domainfullMachineName;
					}
				}
			}
		}
		catch(Throwable t) {
		}
		return retVal;
	}
	
	public void put(DatedNodeMetricsUpdateDuccEvent duccEvent) {
		String location = "put";
		MachineSummaryInfo msi = new MachineSummaryInfo();
		NodeMetricsUpdateDuccEvent nodeMetrics = duccEvent.getNodeMetricsUpdateDuccEvent();
		Ip ip = new Ip(nodeMetrics.getNodeIdentity().getIp().trim());
		TreeMap<String, NodeUsersInfo> map = nodeMetrics.getNodeUsersMap();
		if(map != null) {
			ipToNodeUsersInfoMap.put(ip.toString(), map);
		}
		String machineName = nodeMetrics.getNodeIdentity().getName().trim();
		ipToNameMap.put(ip.toString(),machineName);
		nameToIpMap.put(machineName,ip.toString());
		// mem: total
		long nodeMemTotal = nodeMetrics.getNodeMemory().getMemTotal();
		logger.debug(location, jobid, "node: "+machineName+" "+"memTotal: "+nodeMemTotal);
		long lvalMemTotal = (long) ((1.0*nodeMemTotal)/(1024*1024)+0.5);
		msi.memTotal = lvalMemTotal;
		String memTotal = ""+lvalMemTotal/*+memUnits*/;
		// mem: free
		long nodeMemFree = nodeMetrics.getNodeMemory().getMemFree();
		logger.debug(location, jobid, "node: "+machineName+" "+"memFree: "+nodeMemFree);
		long lvalMemFree = (long) ((1.0*nodeMemFree)/(1024*1024)+0.0);  // do NOT round up!
		msi.memFree = lvalMemFree;
		String memFree = ""+lvalMemFree/*+memUnits*/;
		// swap: in-usewell
		double dvalSwapTotal = nodeMetrics.getNodeMemory().getSwapTotal();
		long lvalSwapTotal = (long) (dvalSwapTotal/(1024*1024)+0.5);
		double dvalSwapFree = nodeMetrics.getNodeMemory().getSwapFree();
		long lvalSwapFree = (long) (dvalSwapFree/(1024*1024)+0.5);
		long lvalSwapInuse = lvalSwapTotal - lvalSwapFree;
		String swapInuse = ""+lvalSwapInuse/*+memUnits*/;
		msi.swapInuse = lvalSwapInuse;
		String swapKey = ip.toString();
		String swapVal = swapInuse;
		if(msi.swapInuse > 0) {
			isSwapping.put(swapKey, swapVal);
		}
		else {
			isSwapping.remove(swapKey);
		}
		//String swapFree = ""+lval/*+memUnits*/;
		msi.swapFree = lvalSwapFree;
		String swapFree = ""+lvalSwapFree/*+memUnits*/;
		List<ProcessInfo> alienPids = nodeMetrics.getRogueProcessInfoList();
		boolean cGroups = nodeMetrics.getCgroups();
		MachineInfo current = new MachineInfo("", ip.toString(), machineName, memTotal, memFree, ""+swapInuse, ""+swapFree, cGroups, alienPids, duccEvent.getMillis(), duccEvent.getEventSize());
		String key = normalizeMachineName(machineName);
		MachineInfo previous = unsortedMachines.get(key);
		if(previous != null) {
			try {
				long swapPrev = Long.parseLong(previous.getSwapInuse());
				long swapCurr = Long.parseLong(current.getSwapInuse());
				long swapDelta = swapCurr - swapPrev;
				current.setSwapDelta(swapDelta);;
			}
			catch(Exception e) {
			}
			long pHbMax = previous.getHeartbeatMax();
			long tod = previous.getHeartbeatMaxTOD();
			long pHbElapsed = previous.getElapsedSeconds();
			if(pHbElapsed > pHbMax) {
				pHbMax = pHbElapsed;
				tod = previous.getHeartbeat();
			}
			current.setHeartbeatMax(pHbMax);
			current.setHeartbeatMaxTOD(tod);
			long pubSizeMax = previous.getPubSizeMax();
			long pubSize = current.getPubSize();
			if(pubSize > pubSizeMax) {
				pubSizeMax = pubSize;
			}
			current.setPubSizeMax(pubSizeMax);
		}
		unsortedMachines.put(key,current);
		updateTotals(ip,msi);
		setPublished();
	}
	
	public List<String> getPids(Ip ip, UserId user) {
		String location = "getPids";
		List<String> retVal = new ArrayList<String>();
		if(ip == null) {
		}
		else if(ip.toString() == null) {
		}
		else if(user == null) {
		}
		else if(user.toString() == null) {
		}
		else {
			try {
				TreeMap<String, NodeUsersInfo> map = ipToNodeUsersInfoMap.get(ip.toString());
				if(map != null) {
					NodeUsersInfo nodeUsersInfo = map.get(user.toString());
					if(nodeUsersInfo != null) {
						for( NodeProcess process : nodeUsersInfo.getReserveProcesses() ) {
							retVal.add(process.getPid());
						}
//						retVal = nodeUsersInfo.getPids();
					}
				}
			}
			catch(Exception e) {
				logger.error(location, jobid, e);
			}
		}
		return retVal;
	}
	
	public List<String> getPids(NodeId nodeId, UserId user) {
		Ip ip = new Ip(getIpForName(nodeId.toString()));
		return getPids(ip, user);
	}
	
	public List<NodePidList> getUserProcesses(List<String> nodeList, String user) {
		List<NodePidList> nodePidListList = new ArrayList<NodePidList>();
		for(String node : nodeList) {
			List<String> pids = getPids(new NodeId(node), new UserId(user));
			NodePidList nodePidList = new NodePidList(node, pids);
			nodePidListList.add(nodePidList);
		}
		return nodePidListList;
	}
	
	public int getPidCount(Ip ip, UserId user) {
		int retVal = 0;
		try {
			List<String> pidList = getPids(ip, user);
			if(pidList != null) {
				return pidList.size();
			}
		}
		catch(Exception e) {
			retVal = -1;
		}
		return retVal;
	}
	
	public int getPidCount(NodeId nodeId, UserId user) {
		Ip ip = new Ip(getIpForName(nodeId.toString()));
		return getPidCount(ip, user);
	}
	
	public String getNameForIp(String ip) {
		String retVal = null;
		try {
			retVal = ipToNameMap.get(ip);
		}
		catch(Throwable t) {
		}
		return retVal;
	}
	
	public String getIpForName(String name) {
		String retVal = null;
		try {
			retVal = nameToIpMap.get(name);
		}
		catch(Throwable t) {
		}
		return retVal;
	}
	
	public void enhance(MachineFacts facts, Map<String, IDbMachine> dbMachineMap) {
		if(facts != null) {
			if(dbMachineMap != null) {
				String[] machineStatus = DuccMachinesDataHelper.getMachineStatus(facts, dbMachineMap);
				facts.status = machineStatus[0];
				facts.statusReason = machineStatus[1];
				String reserveSize = DuccMachinesDataHelper.getMachineReserveSize(facts, dbMachineMap);
				facts.memReserve = reserveSize;
				String quantum = DuccMachinesDataHelper.getMachineQuantum(facts, dbMachineMap);
				facts.quantum = quantum;
			}
		}
	}
	
	public MachineFactsList getMachineFactsList() {
		Map<String, IDbMachine> dbMachineMap = DbQuery.getInstance().getMapMachines();
		MachineFactsList factsList = new MachineFactsList();
		ConcurrentSkipListMap<MachineInfo,String> sortedMachines = getSortedMachines();
		Iterator<MachineInfo> iterator;
		iterator = sortedMachines.keySet().iterator();
		while(iterator.hasNext()) {
			MachineInfo machineInfo = iterator.next();
			String status = machineInfo.getStatus();
			String ip = machineInfo.getIp();
			String name = machineInfo.getName();
			String memTotal = machineInfo.getMemTotal();
			String memFree = machineInfo.getMemFree();
			String swapInuse = machineInfo.getSwapInuse();
			String swapDelta = ""+machineInfo.getSwapDelta();
			String swapFree = machineInfo.getSwapFree();
			boolean cGroups = machineInfo.getCgroups();
			List<String> aliens = machineInfo.getAliens();
			String heartbeat = ""+machineInfo.getElapsed();
			MachineFacts facts = new MachineFacts(status,ip,name,memTotal,memFree,swapInuse,swapDelta,swapFree,cGroups,aliens,heartbeat);
			enhance(facts,dbMachineMap);
			factsList.add(facts);
		}
		return factsList;
	}
	
}
