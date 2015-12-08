package org.apache.uima.ducc.ws.server;

import java.util.Map;

import org.apache.uima.ducc.cli.ws.json.MachineFacts;
import org.apache.uima.ducc.cli.ws.json.MachineFactsList;
import org.apache.uima.ducc.ws.db.IDbMachine;

public class Helper {
	
	// up: responsive == true && online == true && blacklisted == false
	
	public enum MachineStatus { up, down, unknown };
	
	private static String[] getMachineStatusWs(MachineFacts facts, Map<String, IDbMachine> dbMachineMap) {
		String[] retVal = null;
		if(facts != null) {
			if(facts.status != null) {
				if(facts.status.equals("up")) {
					retVal = new String[2];
					retVal[0] = facts.status;
					retVal[1] = "";
				}
				else if(facts.status.equals("down")) {
					retVal = new String[2];
					retVal[0] = facts.status;
					retVal[1] = "heartbeat=missing";
				}
			}
		}
		return retVal;
	}
	
	private static String[] getMachineStatusDb(MachineFacts facts, Map<String, IDbMachine> dbMachineMap) {
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
	
	public static void updateMachineStatus(MachineFactsList factsList, Map<String, IDbMachine> dbMachineMap) {
		if(factsList!= null) {
			if(dbMachineMap != null) {
				for(MachineFacts facts : factsList) {
					String[] machineStatus = getMachineStatus(facts, dbMachineMap);
					facts.status = machineStatus[0];
				}
			}
		}
	}
}
