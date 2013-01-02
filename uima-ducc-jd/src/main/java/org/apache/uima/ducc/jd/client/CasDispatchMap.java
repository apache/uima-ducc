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

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.uima.cas.CAS;
import org.apache.uima.ducc.common.NodeIdentity;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.transport.event.common.IDuccProcess;
import org.apache.uima.ducc.transport.event.common.IDuccWorkJob;


public class CasDispatchMap {
	
	private static DuccLogger duccOut = DuccLoggerComponents.getJdOut(CasDispatchMap.class.getName());
	//private static Messages duccMsg = JobDriverContext.getInstance().getSystemMessages();
	
	private ConcurrentHashMap<String,ThreadLocation> map = new ConcurrentHashMap<String,ThreadLocation>();
	private ConcurrentHashMap<String,CasTuple> cmap = new ConcurrentHashMap<String,CasTuple>();
	
	public void put(CasTuple casTuple, ThreadLocation threadLocation) {
		String methodName = "put";
		String casId = ""+casTuple.getCas().hashCode();
		String node = null;
		String pid = null;
		try {
			try {
				node = threadLocation.getNodeId();
				pid = threadLocation.getProcessId();
			}
			catch(Exception e) {
				duccOut.warn(methodName, null, e);
			}
			synchronized(map) {
				map.put(casId, threadLocation);
				cmap.put(casId, casTuple);
			}
			String message = "casId:"+casId+" "+"node:"+node+" "+"pid:"+pid+" "+"mapsizes: "+map.size()+" "+cmap.size();
			duccOut.debug(methodName, null, message);
		}
		catch(Exception e) {
			String message = "casId:"+casId+" "+"node:"+node+" "+"pid:"+pid+" "+"mapsizes: "+map.size()+" "+cmap.size();
			duccOut.error(methodName, null, message, e);
		}
		return;
	}
	
	public void update(String casId, String nodeIP, String pid) {
		String methodName = "update";
		ThreadLocation threadLocation;
		try {
			synchronized(map) {
				threadLocation = map.get(casId);
			}
			String message;
			try {
				threadLocation.setNodeId(nodeIP);
				threadLocation.setProcessId(pid);
				message = "casId:"+casId+" "+"node:"+nodeIP+" "+"pid:"+pid+" "+"mapsizes: "+map.size()+" "+cmap.size();
				duccOut.debug(methodName, null, message);
			}
			catch(Exception e) {
				message = "casId:"+casId+" lookup failed.";
				duccOut.error(methodName, null, message, e);
			}
		}
		catch(Exception e) {
			String message = "casId:"+casId+" "+"node:"+nodeIP+" "+"pid:"+pid+" "+"mapsizes: "+map.size()+" "+cmap.size();
			duccOut.error(methodName, null, message, e);
		}
		return;
	}
	
	public boolean containsKey(String casId) {
		String methodName = "containsKey";
		boolean retVal = false;
		try {
			synchronized(map) {
				retVal = map.containsKey(casId);
			}
			duccOut.debug(methodName, null, retVal);
		}
		catch(Exception e) {
			String nodeIP = "NA";
			String pid = "NA";
			String message = "casId:"+casId+" "+"node:"+nodeIP+" "+"pid:"+pid+" "+"mapsizes: "+map.size()+" "+cmap.size();
			duccOut.error(methodName, null, message, e);
		}
		return retVal;
	}
	
	public boolean reserveKey(CAS cas) {
		String methodName = "reserveKey";
		boolean retVal = false;
		try {
			String casId = ""+cas.hashCode();
			synchronized(map) {
				retVal = !containsKey(casId);
				if(retVal) {
					ThreadLocation threadLocation = new ThreadLocation("reserved");
					map.put(casId,threadLocation);
					CasTuple casTuple = new CasTuple(cas,-1);
					cmap.put(casId, casTuple);
					String nodeIP = "NA";
					String pid = "NA";
					String message = "casId:"+casId+" "+"node:"+nodeIP+" "+"pid:"+pid+" "+"mapsizes: "+map.size()+" "+cmap.size();
					duccOut.debug(methodName, null, message);
				}
			}
			duccOut.debug(methodName, null, retVal);
		}
		catch(Exception e) {
			String casId = "NA";
			String nodeIP = "NA";
			String pid = "NA";
			if(cas != null) {
				casId = ""+cas.hashCode();
			}
			String message = "casId:"+casId+" "+"node:"+nodeIP+" "+"pid:"+pid+" "+"mapsizes: "+map.size()+" "+cmap.size();
			duccOut.error(methodName, null, message, e);
		}
		return retVal;
	}
	
	public ThreadLocation get(String casId) {
		String methodName = "get";
		ThreadLocation threadLocation = null;
		String node = null;
		String pid = null;
		try {
			synchronized(map) {
				threadLocation = map.get(casId);
			}
			try {
				node = threadLocation.getNodeId();
				pid = threadLocation.getProcessId();
				String message = "casId:"+casId+" "+"node:"+node+" "+"pid:"+pid+" "+"mapsizes: "+map.size()+" "+cmap.size();
				duccOut.debug(methodName, null, message);
			}
			catch(Exception e) {
				String message = "casId:"+casId+" "+"node:"+node+" "+"pid:"+pid+" "+"mapsizes: "+map.size()+" "+cmap.size();
				duccOut.warn(methodName, null, message, e);
			}
		}
		catch(Exception e) {
			String message = "casId:"+casId+" "+"node:"+node+" "+"pid:"+pid+" "+"mapsizes: "+map.size()+" "+cmap.size();
			duccOut.error(methodName, null, message, e);
		}
		return threadLocation;
		
	}
	
	public void remove(String casId) {
		String methodName = "remove";
		ThreadLocation threadLocation = null;
		String node = null;
		String pid = null;
		try {
			synchronized(map) {
				threadLocation = map.get(casId);
				map.remove(casId);
				cmap.remove(casId);
			}
			try {
				node = threadLocation.getNodeId();
				pid = threadLocation.getProcessId();
				String message = "casId:"+casId+" "+"node:"+node+" "+"pid:"+pid+" "+"mapsizes: "+map.size()+" "+cmap.size();
				duccOut.debug(methodName, null, message);
			}
			catch(Exception e) {
				String message = "casId:"+casId+" "+"node:"+node+" "+"pid:"+pid+" "+"mapsizes: "+map.size()+" "+cmap.size();
				duccOut.warn(methodName, null, message, e);
			}
			
		}
		catch (Exception e) {
			String message = "casId:"+casId+" "+"node:"+node+" "+"pid:"+pid+" "+"mapsizes: "+map.size()+" "+cmap.size();
			duccOut.error(methodName, null, message, e);
		}
		return;
	}
	
	public int size() {
		synchronized(map) {
			return map.size();
		}
	}
	
	private boolean existAndCompare(String a, String b) {
		boolean retVal = false;
		if(a != null) {
			if(b != null) {
				return a.equals(b);
			}
		}
		return retVal;
	}
	
	private void interruptThread(IDuccWorkJob job, IDuccProcess duccProcess, String casId) {
		String methodName = "interruptThread";
		int seqNo = -1;
		ThreadLocation threadLocation = null;
		String shareNodeIP = null;
		String shareNodePID = null;
		try {
			// get subject process node and PID
			NodeIdentity nodeIdentity = duccProcess.getNodeIdentity();
			String processNodeIP = nodeIdentity.getIp();
			String processNodePID = duccProcess.getPID();
			// get CAS-in-question seqNo
			CasTuple casTuple = cmap.get(casId);
			seqNo = casTuple.getSeqno();
			// get CAS-in-question thread location
			threadLocation = map.get(casId);
			shareNodeIP = threadLocation.getNodeId();
			shareNodePID = threadLocation.getProcessId();
			if(shareNodePID != null) {
				shareNodePID = shareNodePID.split(":")[0];
			}
			// if CAS-in-question mapped to subject process, then interrupt 
			if(existAndCompare(shareNodeIP,processNodeIP)) {
				if(existAndCompare(shareNodePID,processNodePID)) {
					String message = "cancel"+" "+"seqNo:"+seqNo+" "+"casId:"+casId+" "+"node:"+shareNodeIP+" "+"pid:"+shareNodePID;
					duccOut.debug(methodName, job.getDuccId(), duccProcess.getDuccId(), message);
					boolean rc = threadLocation.getPendingWork().cancel(true);
					message += " "+"rc:"+rc;
					duccOut.debug(methodName, job.getDuccId(), duccProcess.getDuccId(), message);
				}
			}
		}
		catch(Exception e) {
			String message = "cancel"+" "+"seqNo:"+seqNo+" "+"casId:"+casId+" "+"node:"+shareNodeIP+" "+"pid:"+shareNodePID;
			duccOut.warn(methodName, job.getDuccId(), duccProcess.getDuccId(), message, e);
		}
	}
	
	public void interrupt(IDuccWorkJob job, IDuccProcess duccProcess) {
		String methodName = "interrupt";
		duccOut.trace(methodName, job.getDuccId(), duccProcess.getDuccId(), "enter");
		try {
			synchronized(map) {
				Iterator<String> iterator = map.keySet().iterator();
				while(iterator.hasNext()) {
					String casId = iterator.next();
					interruptThread(job, duccProcess, casId);
				}
			}
		}
		catch(Exception e) {
			String message = "";
			duccOut.error(methodName, null, message, e);
		}
		duccOut.trace(methodName, job.getDuccId(), duccProcess.getDuccId(), "exit");
	}
}
