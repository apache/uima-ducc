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

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.ProcessInfo;
import org.apache.uima.ducc.ws.types.NodeId;


public class MachineInfo implements Comparable<MachineInfo> {

	private static DuccLogger logger = DuccLoggerComponents.getWsLogger(MachineInfo.class.getName());
	private static DuccId jobid = null;
	
	public enum MachineStatus { 
		Defined, 
		Down, 
		Up;
		public String getLowerCaseName() {
			String retVal = name().toLowerCase();
			return retVal;
		}
	};

	private String fileDef;
	private String ip;
	private String name;
	private String memTotal;
	private String memReserve;
	private String memFree;
	private String swapInuse;
	private String swapFree;
	private double cpu;
	private boolean cGroupsEnabled;
	private boolean cGroupsCpuReportingEnabled;
	private List<ProcessInfo> alienPids;
	private long swapDelta;
	private long heartbeat;
	private long heartbeatMax;
	private long heartbeatMaxTOD;
	private long pubSize;
	private long pubSizeMax;
	
	private MachineStatus machineStatus = MachineStatus.Defined;
	private Boolean responsive = null;
	private Boolean online = null;
	private Boolean blacklisted = null;
	
	private Integer quantum = null;
	
	private NodeId nodeid;
	
	public MachineInfo(String fileDef, String ip, String name, String memTotal, String memReserve, String memFree, String swapInuse, String swapFree, double cpu, boolean cGroupsEnabled, boolean cGroupsCpuReportingEnabled, List<ProcessInfo> alienPids, long heartbeat, long pubSize) {
		init(MachineStatus.Defined, fileDef, ip, name, memTotal, memReserve, memFree, swapInuse, swapFree, cpu, cGroupsEnabled, cGroupsCpuReportingEnabled, alienPids, heartbeat, pubSize);
	}
	
	public MachineInfo(MachineStatus machineStatus, String fileDef, String ip, String name, String memTotal, String memReserve, String memFree, String swapInuse, String swapFree, double cpu, boolean cGroupsEnabled, boolean cGroupsCpuReportingEnabled, List<ProcessInfo> alienPids, long heartbeat, long pubSize) {
		init(machineStatus, fileDef, ip, name, memTotal, memReserve, memFree, swapInuse, swapFree, cpu, cGroupsEnabled, cGroupsCpuReportingEnabled, alienPids, heartbeat, pubSize);
	}
	
	private void init(MachineStatus machineStatus, String fileDef, String ip, String name, String memTotal, String memReserve, String memFree, String swapInuse, String swapFree, double cpu, boolean cGroupsEnabled, boolean cGroupsCpuReportingEnabled, List<ProcessInfo> alienPids, long heartbeat, long pubSize) {
		this.machineStatus = machineStatus;
		this.fileDef = fileDef;
		this.ip = ip;
		this.name = name;
		this.memTotal = memTotal;
		this.memReserve = memReserve;
		this.memFree = memFree;
		this.swapInuse = swapInuse;
		this.swapFree = swapFree;
		this.cpu = cpu;
		this.cGroupsEnabled = cGroupsEnabled;
		this.cGroupsCpuReportingEnabled = cGroupsCpuReportingEnabled;
		this.alienPids = alienPids;
		if(this.alienPids == null) {
			this.alienPids = new ArrayList<ProcessInfo>();
		}
		this.swapDelta = 0;
		this.heartbeat = heartbeat;
		this.heartbeatMax = 0;
		this.heartbeatMaxTOD = 0;
		this.pubSize = pubSize;
		this.pubSizeMax = 0;
		
		this.nodeid = new NodeId(name);
	}
	
	/*
	 * Derived status based on DB or Agent supplied data, 
	 * one of: defined, down, up
	 */
	
	public String getStatus() {
		return machineStatus.getLowerCaseName();
	}
	
	public MachineStatus getMachineStatus() {
		return this.machineStatus;
	}
	
	public void setMachineStatus(MachineStatus machineStatus) {
		this.machineStatus = machineStatus;
	}
	
	/**
	 * Hover string for status of Down/Up on Machine page
	 */
	
	public String getMachineStatusReason() {
		String retVal = "";
		StringBuffer sb = new StringBuffer();
		sb.append(getResponsive());
		sb.append(" ");
		sb.append(getOnline());
		sb.append(" ");
		sb.append(getBlacklisted());
		sb.append(" ");
		retVal = sb.toString().trim();
		return retVal;
	}
	
	/**
	 * Resource Manager determined value for "responsive"
	 */
	
	public void setResponsive(boolean value) {
		if(value) {
			setResponsive();
		}
		else {
			setNotResponsive();
		}
	}
	
	public void setResponsive() {
		responsive = new Boolean(true);
	}
	
	public void setNotResponsive() {
		responsive = new Boolean(false);
	}
	
	public String getResponsive() {
		String retVal = "";
		if(responsive != null) {
			if(responsive.booleanValue()) {
				retVal = "responsive=true";
			}
			else {
				retVal = "responsive=false";
			}
		}
		return retVal;
	}
	
	/**
	 * Resource Manager determined value for "online"
	 */
	
	public void setOnline(boolean value) {
		if(value) {
			setOnline();
		}
		else {
			setNotOnline();
		}
	}
	
	public void setOnline() {
		online = new Boolean(true);
	}
	
	public void setNotOnline() {
		online = new Boolean(false);
	}
	
	public String getOnline() {
		String retVal = "";
		if(online != null) {
			if(online.booleanValue()) {
				retVal = "online=true";
			}
			else {
				retVal = "online=false";
			}
		}
		return retVal;
	}
	
	/**
	 * Resource Manager determined value for "blacklisted"
	 */
	
	public void setBlacklisted(boolean value) {
		if(value) {
			setBlacklisted();
		}
		else {
			setNotBlacklisted();
		}
	}
	
	public void setBlacklisted() {
		blacklisted = new Boolean(true);
	}
	
	public void setNotBlacklisted() {
		blacklisted = new Boolean(false);
	}
	
	public String getBlacklisted() {
		String retVal = "";
		if(blacklisted != null) {
			if(blacklisted.booleanValue()) {
				retVal = "blacklisted=true";
			}
			else {
				retVal = "blacklisted=false";
			}
		}
		return retVal;
	}
	
	/**
	 * Resource Manager determined value for "quantum"
	 */
	
	public void setQuantum(Integer quantum) {
		this.quantum = quantum;
	}
	
	public Integer getQuantum() {
		return quantum;
	}
	
	//
	
	public String getFileDef() {
		return this.fileDef;
	}
	
	public String getIp() {
		return this.ip;
	}
	
	public String getName() {
		return this.name;
	}
	
	public void setMemTotal(String value) {
		this.memTotal = value;
	}
	
	public String getMemTotal() {
		return this.memTotal;
	}
	
	public void setMemReserve(String value) {
		this.memReserve = value;
	}
	
	public String getMemReserve() {
		return this.memReserve;
	}
	
	public void setMemFree(String value) {
		this.memFree = value;
	}
	
	public String getMemFree() {
		return this.memFree;
	}
	
	public String getSwapInuse() {
		return this.swapInuse;
	}
	
	public String getSwapFree() {
		return this.swapFree;
	}
	
	public double getCpu() {
		return cpu;
	}
	
	public boolean getCgroupsEnabled() {
		return this.cGroupsEnabled;
	}
	
	public boolean getCgroupsCpuReportingEnabled() {
		return this.cGroupsCpuReportingEnabled;
	}
	
	public List<String> getAliens() {
		ArrayList<String> list = new ArrayList<String>();
		Iterator<ProcessInfo> iterator = alienPids.iterator();
		while(iterator.hasNext()) {
			ProcessInfo processInfo = iterator.next();
			String uid = processInfo.getUid();
			String pid = processInfo.getPid();
			String alien = uid+":"+pid;
			list.add(alien);
		}
		return list;
	}
	
	public List<ProcessInfo> getAlienPids() {
		return this.alienPids;
	}
	
	public List<String> getAliensPidsOnly() {
		ArrayList<String> list = new ArrayList<String>();
		Iterator<ProcessInfo> iterator = alienPids.iterator();
		while(iterator.hasNext()) {
			ProcessInfo processInfo = iterator.next();
			list.add(processInfo.getPid());
		}
		return list;
	}
	
	public long getAlienPidsCount() {
		long retVal = 0;
		if(this.alienPids != null) {
			retVal = this.alienPids.size();
		}
		return retVal;
	}
	
	public long getSwapDelta() {
		return this.swapDelta;
	}
	
	public void setSwapDelta(long value) {
		this.swapDelta = value;
	}
	
	public long getHeartbeat() {
		return this.heartbeat;
	}
	
	public long getHeartbeatMax() {
		return this.heartbeatMax;
	}
	
	public void setHeartbeatMax(long value) {
		this.heartbeatMax = value;
	}
	
	public long getHeartbeatMaxTOD() {
		return this.heartbeatMaxTOD;
	}
	
	public void setHeartbeatMaxTOD(long value) {
		this.heartbeatMaxTOD = value;
	}
	
	public long getPubSizeMax() {
		return this.pubSizeMax;
	}
	
	public void setPubSizeMax(long value) {
		this.pubSizeMax = value;
	}
	
	public boolean isExpired(long millisLimit) {
		String location = "isExpired";
		long millisElapsed = getElapsedSeconds() * 1000;
		logger.trace(location, jobid, "millisElapsed:"+millisElapsed+" "+"millisLimit:"+millisLimit);
		return millisElapsed > millisLimit;
	}
	
	public long getElapsedSeconds() {
		long retVal = -1;
		if(heartbeat >= 0) {
			retVal = (System.currentTimeMillis()-heartbeat)/1000;
		}
		return retVal;
	}
	
	public String getElapsed() {
		String retVal = "";
		long elapsedSeconds = getElapsedSeconds();
		if(elapsedSeconds >= 0) {
			retVal = ""+elapsedSeconds;
		}
		return retVal;
	}
	
	public long getPubSize() {
		return pubSize;
	}
	
	public String getPublicationSizeLast() {
		String retVal = "";
		if(pubSize > 0) {
			retVal += pubSize;
		}
		return retVal;
	}
	
	public String getPublicationSizeMax() {
		String retVal = "";
		if(pubSizeMax > 0) {
			retVal += pubSizeMax;
		}
		return retVal;
	}
	
	public String getHeartbeatLast() {
		String retVal = getElapsed();
		return retVal;
	}	

	// @return true if the short names match
	
	@Override
	public boolean equals(Object object) {
		boolean retVal = false;
		if(object != null) {
			if(object instanceof MachineInfo) {
				MachineInfo that = (MachineInfo) object;
				retVal = (this.hashCode() == that.hashCode());
			}
		}
		return retVal;
	}
		
	// @return use short name as hashCode
	
	@Override
	public int hashCode()
	{
		return this.nodeid.getShortName().hashCode();
	}
	
	private int compareString(String s1, String s2) {
		int retVal = 0;
		if(s1 != null) {
			if(s2 != null) {
				retVal = s1.compareTo(s2);
			}
		}
		return retVal;
	}
	
	private int compareToMachineName(MachineInfo that) {
		int retVal = 0;
		if(this.nodeid != null) {
			if(that.nodeid != null) {
				retVal = compareString(this.nodeid.getShortName(), that.nodeid.getShortName());
			}
		}
		return retVal;
	}
	
	private int compareToMachineStatus(MachineInfo that) {
		int retVal = 0;
		MachineStatus v1 = this.getMachineStatus();
		MachineStatus v2 = that.getMachineStatus();
		switch(v1) {
		default:
		case Defined:
			switch(v2) {
			default:
			case Defined:
				retVal = 0;
				break;
			case Down:
				retVal = -1;
				break;
			case Up:
				retVal = -1;
				break;
			}
			break;
		case Down:
			switch(v2) {
			default:
			case Defined:
				retVal = 1;
				break;
			case Down:
				retVal = 0;
				break;
			case Up:
				retVal = -1;
				break;
			}
			break;
		case Up:
			switch(v2) {
			default:
				retVal = 1;
			case Defined:
				break;
			case Down:
				retVal = 1;
				break;
			case Up:
				retVal = 0;
				break;
			}
			break;
		}
		return retVal;
	}
	
	private int compareToMachineSwapInuse(MachineInfo that) {
		int retVal = 0;
		try {
			long v1 = Long.parseLong(this.getSwapInuse());
			long v2 = Long.parseLong(that.getSwapInuse());
			if(v1 > v2) {
				return -1;
			}
			if(v1 < v2) {
				return 1;
			}
		}
		catch(Throwable t) {
		}
		return retVal;
	}
	
	private int compareToMachineAlienPIDs(MachineInfo that) {
		int retVal = 0;
		try {
			long v1 = this.getAlienPidsCount();
			long v2 = that.getAlienPidsCount();
			if(v1 > v2) {
				return -1;
			}
			if(v1 < v2) {
				return 1;
			}
		}
		catch(Throwable t) {
		}
		return retVal;
	}
	
	/**
	 * Sort order: status, swap-inuse, alien-PIDs, machine short name
	 */
	@Override
	public int compareTo(MachineInfo that) {
		int retVal = 0;
		if(that != null) {
			if(retVal == 0) {
				retVal = compareToMachineStatus(that);
			}
			if(retVal == 0) {
				retVal = compareToMachineSwapInuse(that);
			}
			if(retVal == 0) {
				retVal = compareToMachineAlienPIDs(that);
			}
			if(retVal == 0) {
				retVal = compareToMachineName(that);
			}
		}
		return retVal;
	}

}
