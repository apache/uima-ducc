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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.uima.ducc.cli.ws.json.MachineFacts;
import org.apache.uima.ducc.cli.ws.json.MachineFactsList;
import org.apache.uima.ducc.cli.ws.json.NodePidList;
import org.apache.uima.ducc.common.ConvertSafely;
import org.apache.uima.ducc.common.IDuccEnv;
import org.apache.uima.ducc.common.Node;
import org.apache.uima.ducc.common.node.metrics.NodeUsersInfo;
import org.apache.uima.ducc.common.node.metrics.NodeUsersInfo.NodeProcess;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.DuccPropertiesResolver;
import org.apache.uima.ducc.common.utils.TimeStamp;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.NodeMetricsUpdateDuccEvent;
import org.apache.uima.ducc.transport.event.ProcessInfo;
import org.apache.uima.ducc.ws.MachineInfo.MachineStatus;
import org.apache.uima.ducc.ws.db.DbQuery;
import org.apache.uima.ducc.ws.db.IDbMachine;
import org.apache.uima.ducc.ws.server.DuccWebProperties;
import org.apache.uima.ducc.ws.types.Ip;
import org.apache.uima.ducc.ws.types.NodeId;
import org.apache.uima.ducc.ws.types.UserId;
import org.apache.uima.ducc.ws.utils.DatedNodeMetricsUpdateDuccEvent;

/**
 * A class to manage information about machines comprising data
 * from Agents and Resource Manager (RM).  The former report
 * via node metrics publications the later reports via database.
 */
public class DuccMachinesData {

	private static DuccLogger logger = DuccLoggerComponents.getWsLogger(DuccMachinesData.class.getName());
	private static DuccId jobid = null;
	
	private static ConcurrentSkipListMap<MachineInfo,NodeId> sortedMachines = new ConcurrentSkipListMap<MachineInfo,NodeId>();
	private static ConcurrentSkipListMap<NodeId,MachineInfo> unsortedMachines = new ConcurrentSkipListMap<NodeId,MachineInfo>();
	private static ConcurrentSkipListMap<NodeId,MachineSummaryInfo> summaryMachines = new ConcurrentSkipListMap<NodeId,MachineSummaryInfo>();
	
	private static AtomicLong memTotal = new AtomicLong(0);
	private static AtomicLong memFree = new AtomicLong(0);
	private static AtomicLong swapInuse = new AtomicLong(0);
	private static AtomicLong swapFree = new AtomicLong(0);
	
	private static DuccMachinesData duccMachinesData = new DuccMachinesData();
	
	private static ConcurrentSkipListMap<Ip,NodeId> ipToNameMap = new ConcurrentSkipListMap<Ip,NodeId>();
	private static ConcurrentSkipListMap<NodeId,Ip> nameToIpMap = new ConcurrentSkipListMap<NodeId,Ip>();
	private static ConcurrentSkipListMap<String,String> isSwapping = new ConcurrentSkipListMap<String,String>();
	
	private static ConcurrentSkipListMap<String,TreeMap<String,NodeUsersInfo>> ipToNodeUsersInfoMap = new ConcurrentSkipListMap<String,TreeMap<String,NodeUsersInfo>>();
	
	private static MachineFactsList machineFactsList = new MachineFactsList();
	
	public static DuccMachinesData getInstance() {
		return duccMachinesData;
	}
	
	public boolean isMachineSwapping(String ip) {
		return isSwapping.containsKey(ip);
	}
	
	public ConcurrentSkipListMap<MachineInfo,NodeId> getMachines() {
		return getSortedMachines();
	}
	
	public ConcurrentSkipListMap<MachineInfo,NodeId> getSortedMachines() {
		ConcurrentSkipListMap<MachineInfo,NodeId> retVal = sortedMachines;
		return retVal;
	}
	
	private long down_fudge = 10;
	private long DOWN_AFTER_SECONDS = WebServerComponent.updateIntervalSeconds + down_fudge;
	private long SECONDS_PER_MILLI = 1000;
	
	private long getAgentMillisMIA() {
		String location = "getAgentMillisMIA";
		long millisMIA = DOWN_AFTER_SECONDS*SECONDS_PER_MILLI;
		Properties properties = DuccWebProperties.get();
		String s_tolerance = properties.getProperty("ducc.rm.node.stability");
		String s_rate = properties.getProperty("ducc.agent.node.metrics.publish.rate");
		try {
			long tolerance = Long.parseLong(s_tolerance.trim());
			long rate = Long.parseLong(s_rate.trim());
			long secondsRM = (tolerance * rate) / SECONDS_PER_MILLI;
			logger.trace(location, jobid, "default:"+DOWN_AFTER_SECONDS+" "+"secondsRM:"+secondsRM);
			if(DOWN_AFTER_SECONDS < secondsRM) {
				millisMIA = secondsRM * SECONDS_PER_MILLI;
			}
		}
		catch(Throwable t) {
			logger.warn(location, jobid, t);
		}
		return millisMIA;
	}
	
	private void determineStatus(MachineInfo mi, IDbMachine dbMachine) {
		String location = "determineStatus";
		if(dbMachine != null) {
			// determine defined/down/up based on DB
			Boolean responsive = dbMachine.getResponsive();
			Boolean online = dbMachine.getOnline();
			Boolean blacklisted = dbMachine.getBlacklisted();
			MachineStatus machineStatus = MachineStatus.Down;
			if(responsive) {
				if(online) {
					if(!blacklisted) {
						machineStatus = MachineStatus.Up;
					}
				}
			}
			mi.setMachineStatus(machineStatus);
			mi.setResponsive(responsive);
			mi.setOnline(online);
			mi.setBlacklisted(blacklisted);
			StringBuffer sb = new StringBuffer();
			sb.append(mi.getName());
			sb.append(" ");
			sb.append(mi.getMachineStatus());
			sb.append(" ");
			sb.append(mi.getMachineStatusReason());
			sb.append(" ");
			String text = sb.toString().trim();
			logger.trace(location, jobid, text);
			mi.setQuantum(dbMachine.getQuantum());
		}
		else {
			// determine defined/down/up based on Agent
			if(mi.getElapsedSeconds() < 0) {
				mi.setMachineStatus(MachineStatus.Defined);
			}
			else if(mi.isExpired(getAgentMillisMIA())) {
				mi.setMachineStatus(MachineStatus.Down);
			}
			else {
				mi.setMachineStatus(MachineStatus.Up);
			}
		}
	}
	
	private Map<String, IDbMachine> getDbMapMachines() {
		String location = "getDbMapMachines";
		Map<String, IDbMachine> retVal = null;
		try {
			DbQuery dbQuery = DbQuery.getInstance();
			//if(!dbQuery.isEnabled()) {
				retVal = dbQuery.getMapMachines();
			//}
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
		}
		return retVal;
	}
	
	private IDbMachine getDbMachine(Map<String, IDbMachine> dbMapMachines, NodeId nodeId) {
		String location = "getDbMachine";
		IDbMachine retVal = null;
		try {
			if(dbMapMachines != null) {
				if(nodeId != null) {
					String name = nodeId.getLongName();
					retVal = dbMapMachines.get(name);
				}
			}
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
		}
		return retVal;
	}
	
	public void updateSortedMachines() {
		String location = "updateSortedMachines";
		logger.debug(location, jobid, "start");
		try {
			ConcurrentSkipListMap<MachineInfo,NodeId> map = new ConcurrentSkipListMap<MachineInfo,NodeId>();
			Map<String, IDbMachine> dbMapMachines = getDbMapMachines();
			for(Entry<NodeId,MachineInfo> entry : unsortedMachines.entrySet()) {
				NodeId nodeId = entry.getKey();
				IDbMachine dbMachine = getDbMachine(dbMapMachines, nodeId);
				MachineInfo machineInfo = entry.getValue();
				determineStatus(machineInfo, dbMachine);
				map.put(machineInfo, nodeId);
				logger.debug(location, jobid, "put: "+nodeId);
			}
			sortedMachines = map;
			updateMachineFactsList();
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
				double cpu = 0;
				MachineInfo machineInfo = new MachineInfo(IDuccEnv.DUCC_NODES_FILE_PATH, "", nodeName, memTotal, memFree, swapInuse, swapFree, cpu, false, null, -1, 0);
				putMachine(machineInfo);
			}
			updateSortedMachines();
		}
		catch(Throwable t) {
			logger.warn(location, jobid, t);
		}
	}
	
	// add or update machine info; remove corresponding "defined" entry, if it exists
	
	private void putMachine(MachineInfo machineInfo) {
		String location = "putMachine";
		try {
			if(machineInfo != null) {
				String name = machineInfo.getName();
				if(name != null) {
					String longName = name.trim();
					if(longName.length() > 0) {
						NodeId nodeId = new NodeId(longName);
						unsortedMachines.put(nodeId,machineInfo);
						logger.trace(location, jobid, "add="+nodeId.toString()+","+machineInfo.getIp());
						String shortName = longName.split("\\.")[0];
						if(!shortName.equals(longName)) {
							NodeId shortId = new NodeId(shortName);
							if(unsortedMachines.containsKey(shortId)) {
								unsortedMachines.remove(shortId);
								logger.trace(location, jobid, "del="+shortId.toString());
							}
						}
					}
				}
			}
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
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
	
	private void updateTotals(NodeId nodeId, MachineSummaryInfo newInfo) {
		if(summaryMachines.containsKey(nodeId)) {
			MachineSummaryInfo oldInfo = summaryMachines.get(nodeId);
			summaryMachines.put(nodeId, newInfo);
			memTotal.addAndGet(newInfo.memTotal-oldInfo.memTotal);
			memFree.addAndGet(newInfo.memFree-oldInfo.memFree);
			swapInuse.addAndGet(newInfo.swapInuse-oldInfo.swapInuse);
			swapFree.addAndGet(newInfo.swapFree-oldInfo.swapFree);
		}
		else {
			summaryMachines.put(nodeId, newInfo);
			memTotal.addAndGet(newInfo.memTotal);
			memFree.addAndGet(newInfo.memFree);
			swapInuse.addAndGet(newInfo.swapInuse);
			swapFree.addAndGet(newInfo.swapFree);
		}
	}
	
	private double getCpuLoadAvg(Node node) {
		String location = "getCpuLoadAvg";
		double cpu = 0;
		try {
			if(node != null) {
				String load = node.getNodeMetrics().getNodeLoadAverage().getLoadAvg1();
				cpu = ConvertSafely.String2Double(load);
			}
		}
		catch(Exception e) {
			logger.debug(location, jobid, e);
		}
		return cpu;
	}
	
	/**
	 * 
	 * @param duccEvent
	 * @return true is new node or false if already known node
	 * 
	 * Put new or updated node metrics into map of Agent node metric reports
	 */
	public boolean put(DatedNodeMetricsUpdateDuccEvent duccEvent) {
		boolean retVal = true;
		String location = "put";
		MachineSummaryInfo msi = new MachineSummaryInfo();
		NodeMetricsUpdateDuccEvent nodeMetrics = duccEvent.getNodeMetricsUpdateDuccEvent();
		Ip ip = new Ip(nodeMetrics.getNodeIdentity().getIp().trim());
		TreeMap<String, NodeUsersInfo> map = nodeMetrics.getNodeUsersMap();
		if(map != null) {
			String ipString = ip.toString();
			ipToNodeUsersInfoMap.put(ipString, map);
		}
		String machineName = nodeMetrics.getNodeIdentity().getName().trim();
		NodeId nodeId = new NodeId(machineName);
		// determine if this is new machine (true) or already known machine (false)
		if(ipToNameMap.containsKey(ip)) {
			retVal = false;
		}
		ipToNameMap.put(ip,nodeId);
		nameToIpMap.put(nodeId,ip);
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
		Node node = nodeMetrics.getNode();
		double cpu = getCpuLoadAvg(node);
		boolean cGroups = nodeMetrics.getCgroups();
		MachineInfo current = new MachineInfo("", ip.toString(), machineName, memTotal, memFree, ""+swapInuse, ""+swapFree, cpu, cGroups, alienPids, duccEvent.getMillis(), duccEvent.getEventSize());
		
		NodeId key = nodeId;
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
		putMachine(current);
		updateTotals(nodeId,msi);
		setPublished();
		return retVal;
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
	
	public String getNameForIp(String ipString) {
		String retVal = null;
		try {
			Ip ip = new Ip(ipString);
			NodeId nodeId = ipToNameMap.get(ip);
			if(nodeId != null) {
				retVal = nodeId.toString();
			}
		}
		catch(Throwable t) {
		}
		return retVal;
	}
	
	public String getIpForName(String name) {
		String retVal = null;
		try {
			NodeId nodeId = new NodeId(name);
			Ip ip = nameToIpMap.get(nodeId);
			if(ip != null) {
				retVal = ip.toString();
			}
		}
		catch(Throwable t) {
		}
		return retVal;
	}
	
	/**
	 * Create a cached data set employed by the WS 
	 * to display the System -> Machines page.  The
	 * code is dual-pathed, depending on whether or
	 * not the system is configured to use database.
	 */
	private void updateMachineFactsList() {
		String location = "updateMachineFactsList";
		try {
			DbQuery dbQuery = DbQuery.getInstance();
			if(!dbQuery.isEnabled()) {
				updateMachineFactsListAgent();
			}
			else {
				updateMachineFactsListDb();
			}
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
			System.out.println(e.getMessage());
		}
	}
	
	
	
	/**
	 * Adjust memory value by rounding down 
	 * to multiple of rm.share.quantum GB
	 */
	private String calculateMem(long quantum, String memParm) {
		String location = "calculateMem";
		String retVal = "0";
		if(memParm!= null) {
			String memString = memParm.trim();
			if(memString.length() > 0) {
				try {
					long memRaw = Long.parseLong(memString);
					long memAdj = (memRaw / quantum) * quantum;
					retVal = ""+memAdj;
				}
				catch(Exception e) {
					logger.error(location, jobid, e);
				}
			}
		}
		return retVal;
	}
	
	/**
	 * Fetch quantum from ducc.properties, default is 1.
	 */
	private long getQuantum() {
		String location = "getQuantum";
		long retVal = 1;
		try {
			DuccPropertiesResolver dpr = DuccPropertiesResolver.getInstance();
			String quantum = dpr.getFileProperty(DuccPropertiesResolver.ducc_rm_share_quantum);
			retVal = Long.parseLong(quantum.trim());
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
		}
		return retVal;
	}
	
	/**
	 * Create a machines facts list based 
	 * in part on the information provided by the Agents, and
	 * in part on the data comprising the ducc.nodes file.
	 */
	private void updateMachineFactsListAgent() {
		MachineFactsList factsList = new MachineFactsList();
		long quantum = getQuantum();
		ConcurrentSkipListMap<MachineInfo,NodeId> sortedMachines = getSortedMachines();
		Iterator<MachineInfo> iterator;
		iterator = sortedMachines.keySet().iterator();
		while(iterator.hasNext()) {
			MachineInfo machineInfo = iterator.next();
			MachineStatus machineStatus = machineInfo.getMachineStatus();
			String status = machineStatus.getLowerCaseName();
			String statusReason = "";
			String ip = machineInfo.getIp();
			String name = machineInfo.getName();
			String memTotal = calculateMem(quantum, machineInfo.getMemTotal());
			String memFree = calculateMem(quantum, machineInfo.getMemFree());
			String swapInuse = machineInfo.getSwapInuse();
			String swapDelta = ""+machineInfo.getSwapDelta();
			String swapFree = machineInfo.getSwapFree();
			double cpu = machineInfo.getCpu();
			boolean cGroups = machineInfo.getCgroups();
			List<String> aliens = machineInfo.getAliens();
			String heartbeat = ""+machineInfo.getElapsed();
			MachineFacts facts = new MachineFacts(status,statusReason,ip,name,memTotal,memFree,swapInuse,swapDelta,swapFree,cpu,cGroups,aliens,heartbeat);
			// when not using DB, memResrve == memTotal
			facts.memReserve = memTotal;
			factsList.add(facts);
		}
		machineFactsList = factsList;
	}
	
	/**
	 * Create a machines facts list based 
	 * in part on the entries in the RM-maintained database, and
	 * in part on the information provided by the Agents, and
	 * in part on the data comprising the ducc.nodes file.
	 */
	private void updateMachineFactsListDb() {
		String location = "updateMachineFactsListDb";
		// The returnable
		MachineFactsList mfl = new MachineFactsList();
		// Working map used to generate the returnable
		ConcurrentSkipListMap<MachineInfo,NodeId> dbSortedMachines = new ConcurrentSkipListMap<MachineInfo,NodeId>();
		// Get map from DB courtesy of RM
		DbQuery dbQuery = DbQuery.getInstance();
		Map<String, IDbMachine> dbMapMachines = dbQuery.getMapMachines();
		// Working list of known machines, by short name
		List<String> knownShortNames = new ArrayList<String>();
		// Update working map and list of short name from DB
		for(Entry<String, IDbMachine> entry : dbMapMachines.entrySet()) {
			String name = entry.getKey();
			NodeId nodeId = new NodeId(name);
			MachineInfo mi = unsortedMachines.get(nodeId);
			IDbMachine dbMachine = entry.getValue();
			if(mi != null) {
				int quantum = dbMachine.getQuantum();
				int total = quantum*dbMachine.getShareOrder();
				int free = quantum*dbMachine.getSharesLeft();
				mi.setMemTotal(""+total);
				mi.setMemFree(""+free);
				dbSortedMachines.put(mi, nodeId);
				String shortName = nodeId.getShortName();
				knownShortNames.add(shortName);
			}
		}
		// Initialize returnable with "defined" machines
		ArrayList<String> duccNodes = DuccNodes.getInstance().get();
		for(String name : duccNodes) {
			NodeId nodeId = new NodeId(name);;
			String shortName = nodeId.getShortName();
			// skip defined machine if it already appears in DB
			if(knownShortNames.contains(shortName)) {
				continue;
			}
			String status = "defined";
			String statusReason = "";
			String ip = "";
			String memTotal = "";
			String memFree = "";
			String swapInuse = "";
			String swapDelta = "";
			String swapFree = "";
			double cpu = 0;
			boolean cGroups = false;
			List<String> aliens = new ArrayList<String>();
			String heartbeat = "";
			MachineFacts facts = new MachineFacts(status,statusReason,ip,name,memTotal,memFree,swapInuse,swapDelta,swapFree,cpu,cGroups,aliens,heartbeat);
			mfl.add(facts);
		}
		// Augment returnable with data from Agents & RM (from DB)
		for(Entry<MachineInfo, NodeId> entry : dbSortedMachines.entrySet()) {
			MachineInfo machineInfo = entry.getKey();
			MachineStatus machineStatus = machineInfo.getMachineStatus();
			String status = machineStatus.getLowerCaseName();
			String statusReason = machineInfo.getMachineStatusReason();
			String ip = machineInfo.getIp();
			String name = machineInfo.getName();
			String memTotal = machineInfo.getMemTotal();
			String memFree = machineInfo.getMemFree();
			String swapInuse = machineInfo.getSwapInuse();
			String swapDelta = ""+machineInfo.getSwapDelta();
			String swapFree = machineInfo.getSwapFree();
			double cpu = machineInfo.getCpu();
			boolean cGroups = machineInfo.getCgroups();
			List<String> aliens = machineInfo.getAliens();
			String heartbeat = ""+machineInfo.getElapsed();
			MachineFacts facts = new MachineFacts(status,statusReason,ip,name,memTotal,memFree,swapInuse,swapDelta,swapFree,cpu,cGroups,aliens,heartbeat);
			facts.memReserve = machineInfo.getMemTotal();
			facts.quantum = ""+machineInfo.getQuantum();
			logger.trace(location, jobid, facts.status+" "+facts.statusReason);
			mfl.add(facts);
		}
		machineFactsList = mfl;
	}
	
	public MachineFactsList getMachineFactsList() {
		return machineFactsList;
	}
	
}
