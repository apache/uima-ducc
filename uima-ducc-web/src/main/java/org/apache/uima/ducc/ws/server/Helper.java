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
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimeZone;
import java.util.TreeMap;

import org.apache.uima.ducc.common.ConvertSafely;
import org.apache.uima.ducc.common.NodeIdentity;
import org.apache.uima.ducc.common.SizeBytes;
import org.apache.uima.ducc.common.SizeBytes.Type;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.SynchronizedSimpleDateFormat;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.Constants;
import org.apache.uima.ducc.transport.event.common.DuccWorkJob;
import org.apache.uima.ducc.transport.event.common.IDuccProcess;
import org.apache.uima.ducc.transport.event.common.IDuccProcessWorkItems;
import org.apache.uima.ducc.transport.event.common.IDuccWorkJob;
import org.apache.uima.ducc.transport.event.common.IDuccWorkMap;
import org.apache.uima.ducc.transport.event.common.IProcessState.ProcessState;
import org.apache.uima.ducc.transport.event.common.IResourceState.ProcessDeallocationType;
import org.apache.uima.ducc.transport.event.common.TimeWindow;
import org.apache.uima.ducc.ws.DuccData;
import org.apache.uima.ducc.ws.DuccMachinesData;
import org.apache.uima.ducc.ws.DuccMachinesDataHelper;
import org.apache.uima.ducc.ws.MachineInfo;
import org.apache.uima.ducc.ws.types.NodeId;
import org.apache.uima.ducc.ws.utils.FormatHelper;
import org.apache.uima.ducc.ws.utils.FormatHelper.Precision;
import org.apache.uima.ducc.ws.utils.LinuxSignals;
import org.apache.uima.ducc.ws.utils.LinuxSignals.Signal;
import org.apache.uima.ducc.ws.utils.alien.EffectiveUser;
import org.apache.uima.ducc.ws.utils.alien.FileInfo;
import org.apache.uima.ducc.ws.utils.alien.OsProxy;

public class Helper {
	
	private static DuccLogger duccLogger = DuccLogger.getLogger(Helper.class);
	private static DuccId jobid = null;
	
	public static  enum AllocationType { JD, MR, SPC, SPU, UIMA };
	public static enum LogType { POP, UIMA };
	
	public static String notAvailable = "N/A";
	
	public static DuccWorkJob getJob(String jobNo) {
		DuccWorkJob job = null;
		IDuccWorkMap duccWorkMap = DuccData.getInstance().get();
		if(duccWorkMap.getJobKeySet().size()> 0) {
			Iterator<DuccId> iterator = null;
			iterator = duccWorkMap.getJobKeySet().iterator();
			while(iterator.hasNext()) {
				DuccId jobId = iterator.next();
				String fid = ""+jobId.getFriendly();
				if(jobNo.equals(fid)) {
					job = (DuccWorkJob) duccWorkMap.findDuccWork(jobId);
					break;
				}
			}
		}
		return job;
	}
	
	public static DuccWorkJob getManagedReservation(String reservationNo) {
		DuccWorkJob managedReservation = null;
		IDuccWorkMap duccWorkMap = DuccData.getInstance().get();
		if(duccWorkMap.getServiceKeySet().size()> 0) {
			Iterator<DuccId> iterator = null;
			iterator = duccWorkMap.getServiceKeySet().iterator();
			while(iterator.hasNext()) {
				DuccId jobId = iterator.next();
				String fid = ""+jobId.getFriendly();
				if(reservationNo.equals(fid)) {
					managedReservation = (DuccWorkJob) duccWorkMap.findDuccWork(jobId);
					break;
				}
			}
		}
		return managedReservation;
	}
	
	public static String getDuration(DuccId jobId, String millisV2, String millisV1, Precision precision) {
		String methodName = "getDuration";
		String retVal = "";
		try {
			long d2 = Long.parseLong(millisV2);
			long d1 = Long.parseLong(millisV1);
			long diff = d2 - d1;
			if(diff < 0) {
				diff = 0;
			}
			retVal = FormatHelper.duration(diff, precision);
		}
		catch(Exception e) {
			duccLogger.trace(methodName, null, "no worries", e);
		}
		catch(Throwable t) {
			duccLogger.trace(methodName, null, "no worries", t);
		}
		return retVal;
	}
	
	public static SizeBytes getSummaryReserve() {
		String methodName = "getSummaryReserve";
		long sumReserve = 0;
		DuccMachinesData instance = DuccMachinesData.getInstance();
		Map<MachineInfo, NodeId> machines = instance.getMachines();
		if(!machines.isEmpty()) {
			for(Entry<MachineInfo, NodeId> entry : machines.entrySet()) {
				MachineInfo machineInfo = entry.getKey();
				if(DuccMachinesDataHelper.isUp(machineInfo)) {
					try {
						// Calculate total for Memory(GB):usable
						sumReserve += ConvertSafely.String2Long(machineInfo.getMemReserve());
					}
					catch(Exception e) {
						duccLogger.trace(methodName, jobid, e);
					}
				}
			}
		}
		SizeBytes retVal = new SizeBytes(Type.GBytes, sumReserve);
		return retVal;
	}
	
	public static Map<String, FileInfo> getFileInfoMap(EffectiveUser eu, String directory) {
		String location = "";
		Map<String, FileInfo> map = new TreeMap<String, FileInfo>();
		try {
			map = OsProxy.getFilesInDirectory(eu, directory);
		}
		catch(Throwable t) {
			duccLogger.error(location, jobid, t);
		}
		return map;
	}
	
	public static String getLogFileDirectory(IDuccWorkJob job) {
		String retVal = "";
		if(job != null) {
			retVal = job.getLogDirectory();
		}
		return retVal;
	}
	
	public static String getLogFileName(IDuccWorkJob job, IDuccProcess process, AllocationType type) {
		String retVal = "";
		if(process != null) {
			switch(type) {
			case UIMA:
				retVal = job.getDuccId().getFriendly()+"-"+LogType.UIMA.name()+"-"+process.getNodeIdentity().getName()+"-"+process.getPID()+".log";
				break;
			case MR:
				retVal = job.getDuccId().getFriendly()+"-"+LogType.POP.name()+"-"+process.getNodeIdentity().getName()+"-"+process.getPID()+".log";
				break;
			case SPU:
				retVal = job.getDuccId().getFriendly()+"-"+LogType.UIMA.name()+"-"+process.getNodeIdentity().getName()+"-"+process.getPID()+".log";
				break;
			case SPC:
				retVal = job.getDuccId().getFriendly()+"-"+LogType.POP.name()+"-"+process.getNodeIdentity().getName()+"-"+process.getPID()+".log";
				break;
			case JD:
				retVal = "jd.out.log";
				// <UIMA-3802>
				// {jobid}-JD-{node}-{PID}.log
				String node = process.getNodeIdentity().getName();
				String pid = process.getPID();
				retVal = job.getDuccId()+"-"+"JD"+"-"+node+"-"+pid+".log";
				// </UIMA-3802>
				break;
			}
		}
		return retVal;
	}
	
	public static String getLogFileSize(IDuccWorkJob job, IDuccProcess process, String log_file, Map<String, FileInfo> fileInfoMap) {
		String location = "getLogFileSize";
		String retVal = "0";
		try {
			String dir_name = job.getUserLogDir();
			String file_name = dir_name+log_file;
			long size = getFileSize(file_name, fileInfoMap);
			retVal = normalize(size);
		}
		catch(Exception e) {
			duccLogger.warn(location,jobid,e);
		}
		return retVal;
	}
	
	private static DecimalFormat sizeFormatter = new DecimalFormat("##0.00");
	
	public static String normalize(long fileSize) {
		String location = "normalize";
		String retVal = "0";
		try {
			double size = fileSize;
			size = size / Constants.MB;
			retVal = sizeFormatter.format(size);
		}
		catch(Exception e) {
			duccLogger.warn(location,jobid,e);
		}
		return retVal;
	}
	
	public static long getFileSize(String filename, Map<String, FileInfo> fileInfoMap) {
		long retVal = 0;
		if(filename != null) {
			if(fileInfoMap != null) {
				FileInfo fileInfo = fileInfoMap.get(filename);
				if(fileInfo != null) {
					retVal = fileInfo.length;
				}
			}
		}
		return retVal;
	}
	
	public static String getHostname(IDuccWorkJob job, IDuccProcess process) {
		StringBuffer sb = new StringBuffer();
		if(process != null) {
			NodeIdentity ni = process.getNodeIdentity();
			if(ni != null) {
				String name = ni.getName();
				if(name != null) {
					sb.append(name);
				}
			}
		}
		return sb.toString();
	}
	
	public static String getPid(IDuccWorkJob job, IDuccProcess process) {
		StringBuffer sb = new StringBuffer();
		if(process != null) {
			String pid = process.getPID();
			if(pid != null) {
				sb.append(pid);
			}
		}
		return sb.toString();
	}
	
	public static String getSchedulerState(IDuccWorkJob job, IDuccProcess process) {
		StringBuffer sb = new StringBuffer();
		if(process != null) {
			sb.append(process.getResourceState());
		}
		return sb.toString();
	}
	
	private static String getRmReason(IDuccWorkJob job) {
		StringBuffer sb = new StringBuffer();
		String rmReason = job.getRmReason();
		if(rmReason != null) {
			sb.append("<span>");
			sb.append(rmReason);
			sb.append("</span>");
		}
		return sb.toString();
	}
	
	private static String getProcessReason(IDuccProcess process) {
		StringBuffer sb = new StringBuffer();
		if(process != null) {
			switch(process.getProcessState()) {
			case Starting:
			case Started:
			case Initializing:
			case Running:
				break;
			default:
				ProcessDeallocationType deallocationType = process.getProcessDeallocationType();
				switch(deallocationType) {
				case Undefined:
					break;
				default:
					sb.append(process.getProcessDeallocationType());
					break;
				}
				break;
			}
		}
		return sb.toString();
	}
	
	public static String getSchedulerReason(IDuccWorkJob job, IDuccProcess process) {
		StringBuffer sb = new StringBuffer();
		if(job.isOperational()) {
			switch(job.getJobState()) {
			case WaitingForResources:
				sb.append(getRmReason(job));
				break;
			default:
				sb.append(getProcessReason(process));
				break;
			}
		}
		else {
			sb.append(getProcessReason(process));
		}
		return sb.toString();
	}
	
	public static String getAgentState(IDuccWorkJob job, IDuccProcess process) {
		StringBuffer sb = new StringBuffer();
		if(process != null) {
			ProcessState ps = process.getProcessState();
			switch(ps) {
			case Undefined:
				break;
			default:
				sb.append(ps);
				break;
			}
		}
		return sb.toString();
	}
	
	public static String getAgentReason(IDuccWorkJob job, IDuccProcess process) {
		String retVal = null;
		if(process != null) {
			retVal = process.getReasonForStoppingProcess();
		}
		return retVal;
	}
	
	public static String getAgentReasonExtended(IDuccWorkJob job, IDuccProcess process) {
		String retVal = null;
		if(process != null) {
			retVal = process.getExtendedReasonForStoppingProcess();
		}
		return retVal;
	}
	
	public static String getExit(IDuccWorkJob job, IDuccProcess process) {
		StringBuffer sb = new StringBuffer();
		if(process != null) {
			boolean suppressExitCode = false;
			if(!suppressExitCode) {
				switch(process.getProcessState()) {
				case LaunchFailed:
				case Stopped:
				case Failed:
				case FailedInitialization:
				case InitializationTimeout:
				case Killed:
					int code = process.getProcessExitCode();
					if(LinuxSignals.isSignal(code)) {
						Signal signal = LinuxSignals.lookup(code);
						if(signal != null) {
							sb.append(signal.name()+"("+signal.number()+")");
						}
						else {
							sb.append("UnknownSignal"+"("+LinuxSignals.getValue(code)+")");
						}
					}
					else {
						sb.append("ExitCode"+"="+code);
					}
					break;
				default:
					break;
				}
			}
		}
		return sb.toString();
	}
	
	public static String getTimeInit(IDuccWorkJob job, IDuccProcess process, AllocationType type) {
		String location = "getTimeInit";
		StringBuffer sb = new StringBuffer();
		if(process != null) {
			switch(type) {
			case MR:
				break;
			default:
				String initTime = "00";
				try {
					TimeWindow t = (TimeWindow) process.getTimeWindowInit();
					if(t != null) {
						long now = System.currentTimeMillis();
						String tS = t.getStart(""+now);
						String tE = t.getEnd(""+now);
						initTime = getDuration(jobid,tE,tS,Precision.Whole);
					}
				}
				catch(Exception e) {
					duccLogger.trace(location, jobid, "no worries", e);
				}
				catch(Throwable t) {
					duccLogger.trace(location, jobid, "no worries", t);
				}
				sb.append(initTime);
				break;
			}
		}
		return sb.toString();
	}
	
	public static boolean isTimeInitEstimated(IDuccWorkJob job, IDuccProcess process, AllocationType type) {
		String location = "isTimeInitEstimated";
		boolean retVal = false;
		if(process != null) {
			switch(type) {
			case MR:
				break;
			default:
				try {
					TimeWindow t = (TimeWindow) process.getTimeWindowInit();
					if(t != null) {
						if(t.isEstimated()) {
							retVal = true;
						}
					}
				}
				catch(Exception e) {
					duccLogger.trace(location, jobid, "no worries", e);
				}
				catch(Throwable t) {
					duccLogger.trace(location, jobid, "no worries", t);
				}
				break;
			}
		}
		return retVal;
	}
	
	public static long getTimeStart(IDuccProcess process) {
		String location = "getTimeStart";
		long retVal = -1;
		if(process != null) {
			try {
				TimeWindow t = (TimeWindow) process.getTimeWindowRun();
				if(t != null) {
					retVal = t.getStartLong();
				}
			}
			catch(Exception e) {
				duccLogger.trace(location, jobid, "no worries", e);
			}
		}
		return retVal;
	}
	
	public static String getTimeRun(IDuccWorkJob job, IDuccProcess process, AllocationType type) {
		String location = "getTimeRun";
		StringBuffer sb = new StringBuffer();
		if(process != null) {
			String runTime = "00";
			// <UIMA-3351>
			boolean useTimeRun = true;
			switch(type) {
			case SPC:
				break;
			case SPU:
				break;
			case MR:
				break;
			case JD:
				break;
			case UIMA:
				if(!process.isAssignedWork()) {
					useTimeRun = false;
				}
				break;
			default:
				break;
			}
			// </UIMA-3351>
			if(useTimeRun) {
				try {
					TimeWindow t = (TimeWindow) process.getTimeWindowRun();
					if(t != null) {
						long now = System.currentTimeMillis();
						String tS = t.getStart(""+now);
						String tE = t.getEnd(""+now);
						runTime = Helper.getDuration(jobid,tE,tS,Precision.Whole);
					}
				}
				catch(Exception e) {
					duccLogger.trace(location, jobid, "no worries", e);
				}
				catch(Throwable t) {
					duccLogger.trace(location, jobid, "no worries", t);
				}
			}
			sb.append(runTime);
		}
		return sb.toString();
	}

	public static boolean isTimeRunEstimated(IDuccWorkJob job, IDuccProcess process, AllocationType type) {
		String location = "isTimeRunEstimated";
		boolean retVal = false;
		if(process != null) {
			// <UIMA-3351>
			boolean useTimeRun = true;
			switch(type) {
			case SPC:
				break;
			case SPU:
				break;
			case MR:
				break;
			case JD:
				break;
			case UIMA:
				if(!process.isAssignedWork()) {
					useTimeRun = false;
				}
				break;
			default:
				break;
			}
			// </UIMA-3351>
			if(useTimeRun) {
				try {
					TimeWindow t = (TimeWindow) process.getTimeWindowRun();
					if(t != null) {
						if(t.isEstimated()) {
							retVal = true;
						}
					}
				}
				catch(Exception e) {
					duccLogger.trace(location, jobid, "no worries", e);
				}
				catch(Throwable t) {
					duccLogger.trace(location, jobid, "no worries", t);
				}
			}
		}
		return retVal;
	}
	
	private static String chomp(String leading, String whole) {
		String retVal = whole;
		while((retVal.length() > leading.length()) && (retVal.startsWith(leading))) {
			retVal = retVal.replaceFirst(leading, "");
		}
		/*
		if(retVal.equals("00:00")) {
			retVal = "0";
		}
		*/
		return retVal;
	}
	
	private static SynchronizedSimpleDateFormat dateFormat = new SynchronizedSimpleDateFormat("HH:mm:ss");

	public static String getTimeGC(IDuccWorkJob job, IDuccProcess process, AllocationType type) {
		StringBuffer sb = new StringBuffer();
		if(process != null) {
			switch(type) {
			case MR:
				break;
			default:
				long timeGC = 0;
				try {
					timeGC = process.getGarbageCollectionStats().getCollectionTime();
				}
				catch(Exception e) {
				}
				if(timeGC < 0) {
					sb.append("<span class=\"health_black\""+">");
					sb.append(notAvailable);
					sb.append("</span>");
				}
				else {
					dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
					String displayGC = dateFormat.format(new Date(timeGC));
					displayGC = chomp("00:", displayGC);
					sb.append(displayGC);
				}
				break;
			}
		}
		return sb.toString();
	}
	
	public static String getPgIn(IDuccWorkJob job, IDuccProcess process, AllocationType sType) {
		StringBuffer sb = new StringBuffer();
		if(process != null) {
			switch(sType) {
			default:
				long faults = 0;
				try {
					faults = process.getMajorFaults();
				}
				catch(Exception e) {
				}
				if(faults < 0) {
					sb.append("<span class=\"health_black\""+">");
					sb.append(notAvailable);
					sb.append("</span>");
				}
				else {
					double swap = process.getSwapUsageMax();
					if((swap * faults) > 0) {
						sb.append("<span class=\"health_red\""+">");
					}
					else {
						sb.append("<span class=\"health_black\""+">");
					}
					sb.append(faults);
					sb.append("</span>");
				}
				break;
			}
		}
		return sb.toString();
	}
	
	public static boolean isSwapping(IDuccWorkJob job, IDuccProcess process, AllocationType sType) {
		boolean retVal = false;
		if(process != null) {
			switch(sType) {
			default:
				if(!process.isActive()) {
					double swap = process.getSwapUsageMax();
					if(swap > 0) {
						retVal = true;
					}
				}
				else {
					double swap = process.getSwapUsage();
					if(swap > 0) {
						retVal = true;
					}
				}
				break;
			}
		}
		return retVal;
	}

	private static DecimalFormat formatter = new DecimalFormat("##0.0");
	
	public static String getSwap(IDuccWorkJob job, IDuccProcess process, AllocationType sType) {
		StringBuffer sb = new StringBuffer();
		if(process != null) {
			switch(sType) {
			default:
				if(!process.isActive()) {
					double swap = process.getSwapUsageMax();
					if(swap < 0) {
						sb.append(notAvailable);
					}
					else {
						swap = swap/Constants.GB;
						String displaySwap = formatter.format(swap);
						sb.append(displaySwap);
					}
				}
				else {
					double swap = process.getSwapUsage();
					if(swap < 0) {
						sb.append(notAvailable);
					}
					else {
						swap = swap/Constants.GB;
						String displaySwap = formatter.format(swap);
						sb.append(displaySwap);
					}
				}
				break;
			}
		}
		return sb.toString();
	}
	
	public static String getSwapMax(IDuccWorkJob job, IDuccProcess process, AllocationType sType) {
		String retVal = null;
		if(process != null) {
			switch(sType) {
			default:
				if(!process.isActive()) {
				}
				else {
					double swap = process.getSwapUsage();
					if(swap < 0) {
					}
					else {
						double swapMax = process.getSwapUsageMax();
						swapMax = swapMax/Constants.GB;
						String displaySwapMax = formatter.format(swapMax);
						retVal = displaySwapMax;
					}
				}
				break;
			}
		}
		return retVal;
	}
	
	private static String formatPctCpu(double pctCpu) {
		String retVal = "";
		if(pctCpu < 0) {
			retVal = "N/A";
		}
		else {
			retVal = formatter.format(pctCpu);
		}
		return retVal;
	}
	
	public static String getPctCpuOverall(IDuccWorkJob job, IDuccProcess process, AllocationType sType) {
		StringBuffer sb = new StringBuffer();
		if(process != null) {
			if(process.getDataVersion() < 1) {
				// Not supported
			}
			else {
				switch(sType) {
				default:
					double pctCPU_overall = process.getCpuTime();
					String fmtCPU_overall = formatPctCpu(pctCPU_overall);
					sb.append(fmtCPU_overall);
					break;
				}
			}
		}
		return sb.toString();
	}

	public static String getPctCpuCurrent(IDuccWorkJob job, IDuccProcess process, AllocationType sType) {
		StringBuffer sb = new StringBuffer();
		if(process != null) {
			if(process.getDataVersion() < 1) {
				// Not supported
			}
			else {
				switch(sType) {
				default:
					double pctCPU_current = process.getCurrentCPU();
					String fmtCPU_current = formatPctCpu(pctCPU_current);
					sb.append(fmtCPU_current);
					break;
				}
			}
		}
		return sb.toString();
	}
	
	public static String getRss(IDuccWorkJob job, IDuccProcess process, AllocationType sType) {
		StringBuffer sb = new StringBuffer();
		if(process != null) {
			switch(sType) {
			default:
				if(process.isComplete()) {
					double rss = process.getResidentMemoryMax();
					if(rss < 0) {
						sb.append(notAvailable);
					}
					else {
						rss = rss/Constants.GB;
						String displayRss = formatter.format(rss);
						sb.append(displayRss);
					}
				}
				else {
					double rss = process.getResidentMemory();
					if(rss < 0) {
						sb.append(notAvailable);
					}
					else {
						rss = rss/Constants.GB;
						String displayRss = formatter.format(rss);
						sb.append(displayRss);
					}
				}
				break;
			}
		}
		return sb.toString();
	}
	
	public static String getRssMax(IDuccWorkJob job, IDuccProcess process, AllocationType sType) {
		String retVal = null;
		if(process != null) {
			switch(sType) {
			default:
				if(process.isComplete()) {
					// 
				}
				else {
					double rss = process.getResidentMemory();
					if(rss < 0) {
						//
					}
					else {
						double rssMax = process.getResidentMemoryMax();
						rssMax = rssMax/Constants.GB;
						String displayRssMax = formatter.format(rssMax);
						retVal = displayRssMax;
					}
				}
				break;
			}
		}
		return retVal;
	}
	
	public static String getWiTimeAvg(IDuccWorkJob job, IDuccProcess process, AllocationType sType) {
		StringBuffer sb = new StringBuffer();
		if(process != null) {
			IDuccProcessWorkItems pwi = process.getProcessWorkItems();
			if(sType != null) {
				switch(sType) {
				case JD:
					if(pwi != null) {
						String value = ""+(job.getWiMillisAvg()/1000);
						sb.append(value);
					}
					break;
				default:
					if(pwi != null) {
						String value = ""+pwi.getSecsAvg();
						sb.append(value);
					}
					break;
				}
			}
		}
		return sb.toString();
	}
	
	public static String getWiTimeMax(IDuccWorkJob job, IDuccProcess process, AllocationType sType) {
		StringBuffer sb = new StringBuffer();
		if(process != null) {
			IDuccProcessWorkItems pwi = process.getProcessWorkItems();
			if(sType != null) {
				switch(sType) {
				default:
					if(pwi != null) {
						String value = ""+pwi.getSecsMax();
						sb.append(value);
					}
					break;
				}
			}
		}
		return sb.toString();
	}
	
	public static String getWiTimeMin(IDuccWorkJob job, IDuccProcess process, AllocationType sType) {
		StringBuffer sb = new StringBuffer();
		if(process != null) {
			IDuccProcessWorkItems pwi = process.getProcessWorkItems();
			if(sType != null) {
				switch(sType) {
				default:
					if(pwi != null) {
						String value = ""+pwi.getSecsMin();
						sb.append(value);
					}
					break;
				}
			}
		}
		return sb.toString();
	}
	
	public static String getWiDone(IDuccWorkJob job, IDuccProcess process, AllocationType sType) {
		StringBuffer sb = new StringBuffer();
		if(process != null) {
			IDuccProcessWorkItems pwi = process.getProcessWorkItems();
			if(sType != null) {
				switch(sType) {
				default:
					if(pwi != null) {
						String value = ""+pwi.getCountDone();
						sb.append(value);
					}
					break;
				}
			}
		}
		return sb.toString();
	}
	
	public static String getWiError(IDuccWorkJob job, IDuccProcess process, AllocationType sType) {
		StringBuffer sb = new StringBuffer();
		if(process != null) {
			IDuccProcessWorkItems pwi = process.getProcessWorkItems();
			if(sType != null) {
				switch(sType) {
				default:
					if(pwi != null) {
						String value = ""+pwi.getCountError();
						sb.append(value);
					}
					break;
				}
			}
		}
		return sb.toString();
	}
	
	public static String getWiDispatch(IDuccWorkJob job, IDuccProcess process, AllocationType sType) {
		StringBuffer sb = new StringBuffer();
		if(process != null) {
			IDuccProcessWorkItems pwi = process.getProcessWorkItems();
			if(sType != null) {
				switch(sType) {
				default:
					if(pwi != null) {
						String value = ""+pwi.getCountDispatch();
						if(job.isCompleted()) {
							value = "0";
						}
						sb.append(value);
					}
					break;
				}
			}
		}
		return sb.toString();
	}
	
	public static String getWiRetry(IDuccWorkJob job, IDuccProcess process, AllocationType sType) {
		StringBuffer sb = new StringBuffer();
		if(process != null) {
			IDuccProcessWorkItems pwi = process.getProcessWorkItems();
			if(sType != null) {
				switch(sType) {
				default:
					if(pwi != null) {
						String value = ""+pwi.getCountRetry();
						sb.append(value);
					}
					break;
				}
			}
		}
		return sb.toString();
	}
	
	public static String getWiPreempt(IDuccWorkJob job, IDuccProcess process, AllocationType sType) {
		StringBuffer sb = new StringBuffer();
		if(process != null) {
			IDuccProcessWorkItems pwi = process.getProcessWorkItems();
			if(sType != null) {
				switch(sType) {
				default:
					if(pwi != null) {
						String value = ""+pwi.getCountPreempt();
						sb.append(value);
					}
					break;
				}
			}
		}
		return sb.toString();
	}
	
	public static String getJConsoleUrl(IDuccWorkJob job, IDuccProcess process, AllocationType sType) {
		StringBuffer sb = new StringBuffer();
		if(process != null) {
			switch(process.getProcessState()) {
			case Initializing:
			case Running:
				String jmxUrl = process.getProcessJmxUrl();
				if(jmxUrl != null) {
					sb.append(jmxUrl);
				}
				break;
			default:
				break;
			}
		}
		return sb.toString();
	}
}
