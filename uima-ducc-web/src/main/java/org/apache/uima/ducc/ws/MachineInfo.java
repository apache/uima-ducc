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
import java.util.Properties;

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.ProcessInfo;
import org.apache.uima.ducc.ws.server.DuccWebProperties;


public class MachineInfo implements Comparable<MachineInfo> {

	private static DuccLogger logger = DuccLoggerComponents.getWsLogger(MachineInfo.class.getName());
	private static DuccId jobid = null;
	
	private static final int SECONDS_PER_MILLI = 1000;
	private static final int DOWN_AFTER_SECONDS = 65;
	
	private String fileDef;
	private String ip;
	private String name;
	private String memTotal;
	private String memSwap;
	private List<ProcessInfo> alienPids;
	private String sharesTotal;
	private String sharesInuse;
	private long heartbeat;
	private long heartbeatMax;
	private long heartbeatMaxTOD;
	private long pubSize;
	private long pubSizeMax;
	
	public MachineInfo(String fileDef, String ip, String name, String memTotal, String memSwap, List<ProcessInfo> alienPids, String sharesTotal, String sharesInuse, long heartbeat, long pubSize) {
		this.fileDef = fileDef;
		this.ip = ip;
		this.name = name;
		this.memTotal = memTotal;
		this.memSwap = memSwap;
		this.alienPids = alienPids;
		if(this.alienPids == null) {
			this.alienPids = new ArrayList<ProcessInfo>();
		}
		this.sharesTotal = sharesTotal;
		this.sharesInuse = sharesInuse;
		this.heartbeat = heartbeat;
		this.heartbeatMax = 0;
		this.heartbeatMaxTOD = 0;
		this.pubSize = pubSize;
		this.pubSizeMax = 0;
	}
	
	private long getAgentMillisMIA() {
		String location = "getAgentMillisMIA";
		long secondsMIA = DOWN_AFTER_SECONDS*SECONDS_PER_MILLI;
		Properties properties = DuccWebProperties.get();
		String s_tolerance = properties.getProperty("ducc.rm.node.stability");
		String s_rate = properties.getProperty("ducc.agent.node.metrics.publish.rate");
		try {
			long tolerance = Long.parseLong(s_tolerance.trim());
			long rate = Long.parseLong(s_rate.trim());
			secondsMIA = (tolerance * rate) / 1000;
		}
		catch(Throwable t) {
			logger.warn(location, jobid, t);
		}
		return secondsMIA;
	}
	
	private String calculateStatus() {
		String status = "";
		if(getElapsedSeconds() < 0) {
			status = "defined";
		}
		else if(isExpired(getAgentMillisMIA())) {
			status = "down";
		}
		else {
			status = "up";
		}
		return status;
	}
	
	public String getStatus() {
		return calculateStatus();
	}
	
	public String getFileDef() {
		return this.fileDef;
	}
	
	public String getIp() {
		return this.ip;
	}
	
	public String getName() {
		return this.name;
	}
	
	public String getMemTotal() {
		return this.memTotal;
	}
	
	public String getMemSwap() {
		return this.memSwap;
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
	
	public String getSharesTotal() {
		return this.sharesTotal;
	}
	
	public String getSharesInuse() {
		return this.sharesInuse;
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
	
	public boolean isExpired(long seconds) {
		return getElapsedSeconds() > seconds;
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
	
	
	public int compareTo(MachineInfo machine) {
		int retVal = 0;
		MachineInfo m1 = this;
		MachineInfo m2 = machine;
		String s1 = m1.name;
		String s2 = m2.name;
		if(s1 != null) {
			if(s2 != null) {
				if(s1.trim().equals(s2.trim()))
					return retVal;
			}
		}
		retVal = compareStatus(m1, m2);
		if(retVal != 0) {
			return retVal;
		}
		retVal = compareSwapInuse(m1, m2);
		if(retVal != 0) {
			return retVal;
		}
		retVal = compareAlienPids(m1, m2);
		if(retVal != 0) {
			return retVal;
		}
		retVal = compareSharesInuse(m1, m2);
		if(retVal != 0) {
			return retVal;
		}
		retVal = compareSharesTotal(m1, m2);
		if(retVal != 0) {
			return retVal;
		}
		/*
		retVal = compareHeartbeat(m1, m2);
		if(retVal != 0) {
			return retVal;
		}
		*/
		retVal = compareIp(m1, m2);
		if(retVal != 0) {
			return retVal;
		}
		retVal = compareName(m1, m2);
		return retVal;
	}
	
	private int compareStatus(MachineInfo m1, MachineInfo m2) {
		int retVal = 0;
		try {
			String v1 = m1.getStatus();
			String v2 = m2.getStatus();
			if(!v1.equals(v2)) {
				if(v1.equals("defined")) {
					return -1;
				}
				if(v2.equals("defined")) {
					return 1;
				}
				if(v1.equals("down")) {
					return -1;
				}
				if(v2.equals("down")) {
					return 1;
				}
				if(v1.equals("up")) {
					return 1;
				}
				if(v2.equals("up")) {
					return -1;
				}
			}
		}
		catch(Throwable t) {
		}
		return retVal;
	}
	
	private int compareSwapInuse(MachineInfo m1, MachineInfo m2) {
		int retVal = 0;
		try {
			long v1 = Long.parseLong(m1.getMemSwap());
			long v2 = Long.parseLong(m2.getMemSwap());
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
	
	private int compareAlienPids(MachineInfo m1, MachineInfo m2) {
		int retVal = 0;
		try {
			long v1 = m1.getAlienPidsCount();
			long v2 = m2.getAlienPidsCount();
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
	
	private int compareSharesInuse(MachineInfo m1, MachineInfo m2) {
		int retVal = 0;
		try {
			long v1 = Long.parseLong(m1.getSharesInuse());
			long v2 = Long.parseLong(m2.getSharesInuse());
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
	
	private int compareSharesTotal(MachineInfo m1, MachineInfo m2) {
		int retVal = 0;
		try {
			long v1 = Long.parseLong(m1.getSharesTotal());
			long v2 = Long.parseLong(m2.getSharesTotal());
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
	
/*	
	private int compareHeartbeat(MachineInfo m1, MachineInfo m2) {
		int retVal = 0;
		try {
			long v1 = m1.getHeartbeat();
			long v2 = m2.getHeartbeat();
			if(v1 > v2) {
				return 1;
			}
			if(v1 < v2) {
				return -1;
			}
		}
		catch(Throwable t) {
		}
		return retVal;
	}
*/
	private int compareIp(MachineInfo m1, MachineInfo m2) {
		return m1.ip.compareTo(m2.ip);
	}
	
	private int compareName(MachineInfo m1, MachineInfo m2) {
		return m1.name.compareTo(m2.name);
	}

}
