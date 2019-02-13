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
package org.apache.uima.ducc.ws.server;

import java.text.DecimalFormat;
import java.util.Map.Entry;

import org.apache.uima.ducc.common.Node;
import org.apache.uima.ducc.common.NodeIdentity;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.common.IDuccProcess;
import org.apache.uima.ducc.transport.event.common.IDuccProcessMap;
import org.apache.uima.ducc.transport.event.common.IDuccStandardInfo;
import org.apache.uima.ducc.transport.event.common.IDuccWorkJob;
import org.apache.uima.ducc.transport.event.common.ITimeWindow;

public class DuccHandlerUtils {

	private static DuccLogger duccLogger = DuccLogger.getLogger(DuccHandlerUtils.class);
	private static DuccId jobid = null;
	
	public static String warn(String text) {
		StringBuffer sb = new StringBuffer();
		sb.append("<span class=\"health_red\""+">");
		sb.append(text);
		sb.append("</span>");
		return sb.toString();
	}
	
	public static String down() {
		StringBuffer sb = new StringBuffer();
		sb.append("<span class=\"health_red\""+">");
		sb.append("down");
		sb.append("</span>");
		return sb.toString();
	}
	
	public static String up() {
		StringBuffer sb = new StringBuffer();
		sb.append("<span class=\"health_green\""+">");
		sb.append("up");
		sb.append("</span>");
		return sb.toString();
	}
	
	public static String disabled() {
		StringBuffer sb = new StringBuffer();
		sb.append("<span class=\"health_black\""+">");
		sb.append("disabled");
		sb.append("</span>");
		return sb.toString();
	}
	
	public static String up_provisional(String text) {
		StringBuffer sb = new StringBuffer();
		sb.append("<span class=\"health_black\""+">");
		sb.append("up"+text);
		sb.append("</span>");
		return sb.toString();
	}
	
	public static String unknown() {
		StringBuffer sb = new StringBuffer();
		sb.append("<span class=\"health_black\""+">");
		sb.append("unknown");
		sb.append("</span>");
		return sb.toString();
	}
	
	// *****
	
	private static DecimalFormat formatter = new DecimalFormat("###0.0");
	
	public static String getSwapSizeDisplay(double swapBytes) {
		String retVal = formatter.format(swapBytes/GB);;
		return retVal;
	}
	
	public static double GB = Math.pow(10,9);
	public static double MB = Math.pow(10,6);
	public static double KB = Math.pow(10,3);

	public static String getSwapSizeHover(double swapBytes) {
		String retVal = null;
		if(swapBytes == 0) {
			retVal = formatter.format(swapBytes/GB)+" "+"GB";
		}
		else if(swapBytes >= GB/10) {
			retVal = formatter.format(swapBytes/GB)+" "+"GB";
		}
		else if(swapBytes >= MB/10) {
			retVal = formatter.format(swapBytes/MB)+" "+"MB";
		}
		else if(swapBytes >= KB/10) {
			retVal = formatter.format(swapBytes/KB)+" "+"KB";
		}
		else {
			retVal = formatter.format(swapBytes)+" "+"Bytes";
		}
		return retVal;
	}
	
	public static boolean safeCompare(String s1, String s2) {
		boolean retVal = false;
		if (s1 != null) {
			if (s2 != null) {
				retVal = s1.equals(s2);
			}
		}
		return retVal;
	}

	public static String getNode(IDuccProcess p) {
		String retVal = null;
		Node node = p.getNode();
		NodeIdentity ni = node.getNodeIdentity();
		String name = ni.getIp();
		retVal = name;
		return retVal;
	}

	public static String getPid(IDuccProcess p) {
		String retVal = p.getPID();
		return retVal;
	}

	public static IDuccProcess getJobProcess(IDuccWorkJob job, String node, String pid) {
		String location = "getJobProcess";
		IDuccProcess retVal = null;
		try {
			IDuccProcessMap map = job.getProcessMap();
			for (Entry<DuccId, IDuccProcess> entry : map.entrySet()) {
				IDuccProcess p = entry.getValue();
				String pnode = getNode(p);
				String ppid = getPid(p);
				duccLogger.debug(location, jobid, node, pid, pnode, ppid);
				if (safeCompare(node, pnode)) {
					if (safeCompare(pid, ppid)) {
						retVal = p;
						break;
					}
				}
			}
		} catch (Exception e) {
			// no worries
		}
		return retVal;
	}

	public static long getReferenceTime(IDuccWorkJob job, String node, String pid) {
		String location = "getReferenceTime";
		long retVal = System.currentTimeMillis();
		if (job != null) {
			if (job.isCompleted()) {
				IDuccStandardInfo stdInfo = job.getStandardInfo();
				long time = stdInfo.getDateOfCompletionMillis();
				if(time > 0) {
					duccLogger.debug(location, jobid, "job: "+time);
					retVal = time;
				}
			}
			IDuccProcess p = getJobProcess(job, node, pid);
			if (p != null) {
				ITimeWindow tw = p.getTimeWindowRun();
				if (tw != null) {
					long time = tw.getEndLong();
					if (time > 0) {
						duccLogger.debug(location, jobid, "process: "+time);
						retVal = time;
					}
				}
			}
		}
		return retVal;
	}
	
}
