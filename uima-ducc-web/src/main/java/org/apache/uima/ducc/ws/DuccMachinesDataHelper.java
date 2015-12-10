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

import java.util.Map;

import org.apache.uima.ducc.cli.ws.json.MachineFacts;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.ws.db.IDbMachine;

public class DuccMachinesDataHelper {

	private static DuccLogger logger = DuccLoggerComponents.getWsLogger(DuccMachinesDataHelper.class.getName());
	private static DuccId jobid = null;
	
	public enum MachineStatus { up, down, unknown };
	
	private static String[] getMachineStatusWs(MachineFacts facts, Map<String, IDbMachine> dbMachineMap) {
		String location = "getMachineStatusWs";
		String[] retVal = null;
		if(facts != null) {
			if(facts.status != null) {
				if(facts.status.equals("up")) {
					retVal = new String[2];
					retVal[0] = facts.status;
					retVal[1] = "";
					logger.debug(location, jobid, retVal[0]+" "+retVal[1]);
				}
				else if(facts.status.equals("down")) {
					retVal = new String[2];
					retVal[0] = facts.status;
					retVal[1] = "heartbeat=missing";
					logger.debug(location, jobid, retVal[0]+" "+retVal[1]);
				}
			}
		}
		return retVal;
	}
	
	// up: responsive == true && online == true && blacklisted == false
	
	private static String[] getMachineStatusDb(MachineFacts facts, Map<String, IDbMachine> dbMachineMap) {
		String location = "getMachineStatusDb";
		String[] retVal = null;
		if(facts != null) {
			if(facts.status != null) {
				if(dbMachineMap != null) {
					IDbMachine dbMachine = dbMachineMap.get(facts.name);
					if(dbMachine != null) {
						Boolean responsive = dbMachine.getResponsive();
						Boolean online = dbMachine.getOnline();
						Boolean blacklisted = dbMachine.getBlacklisted();
						StringBuffer sb = new StringBuffer();
						sb.append("responsive="+responsive);
						sb.append(" ");
						sb.append("online="+online);
						sb.append(" ");
						sb.append("blacklisted="+blacklisted);
						retVal = new String[2];
						retVal[0] = MachineStatus.down.name();
						retVal[1] = sb.toString();
						if(responsive) {
							if(online) {
								if(!blacklisted) {
									retVal[0] = MachineStatus.up.name();
								}
							}
						}
						logger.debug(location, jobid, sb);
					}
				}
			}
		}
		return retVal;
	}
	
	public static String[] getMachineStatus(MachineFacts facts, Map<String, IDbMachine> dbMachineMap) {
		String[] retVal = null;
		if(retVal == null) {
			retVal = getMachineStatusDb(facts, dbMachineMap);
		}
		if(retVal == null) {
			retVal = getMachineStatusWs(facts, dbMachineMap);
		}
		if(retVal == null) {
			retVal = new String[2];
			retVal[0] = facts.status;
			retVal[1] = "";
		}
		return retVal;
	}
	
	private static String getMachineReserveSizeWs(MachineFacts facts, Map<String, IDbMachine> dbMachineMap) {
		String location = "getMachineReserveSizeWs";
		String retVal = null;
		if(facts != null) {
			if(!facts.status.equals("defined")) {
				retVal = facts.memFree;
				logger.debug(location, jobid, retVal);
			}
		}
		return retVal;
	}
	
	// reserve size: quantum * shareOrder
	
	private static String getMachineReserveSizeDb(MachineFacts facts, Map<String, IDbMachine> dbMachineMap) {
		String location = "getMachineReserveSizeDb";
		String retVal = null;
		if(dbMachineMap != null) {
			IDbMachine dbMachine = dbMachineMap.get(facts.name);
			if(dbMachine != null) {
				retVal = ""+dbMachine.getQuantum()*dbMachine.getShareOrder();
				logger.debug(location, jobid, retVal);
			}
		}
		return retVal;
	}
	
	public static String getMachineReserveSize(MachineFacts facts, Map<String, IDbMachine> dbMachineMap) {
		String retVal = null;
		if(retVal == null) {
			retVal = getMachineReserveSizeDb(facts, dbMachineMap);
		}
		if(retVal == null) {
			retVal = getMachineReserveSizeWs(facts, dbMachineMap);
		}
		if(retVal == null) {
			retVal = "0";
		}
		return retVal;
	}
	
	// quantum
	
	public static String getMachineQuantum(MachineFacts facts, Map<String, IDbMachine> dbMachineMap) {
		String location = "getMachineQuantum";
		String retVal = "";
		if(dbMachineMap != null) {
			IDbMachine dbMachine = dbMachineMap.get(facts.name);
			if(dbMachine != null) {
				retVal = ""+dbMachine.getQuantum();
				logger.debug(location, jobid, retVal);
			}
		}
		return retVal;
	}
	
}
