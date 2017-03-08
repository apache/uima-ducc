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
package org.apache.uima.ducc.transport.json.jp;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.uima.ducc.common.utils.IOHelper;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.common.DuccWorkPopDriver;
import org.apache.uima.ducc.transport.event.common.IDuccProcess;
import org.apache.uima.ducc.transport.event.common.IDuccProcessMap;
import org.apache.uima.ducc.transport.event.common.IDuccProcessWorkItems;
import org.apache.uima.ducc.transport.event.common.IDuccWorkJob;
import org.apache.uima.ducc.transport.event.common.TimeWindow;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class JobProcessCollection {

	public static final String job_processes_data_json_gz = "job-processes-data.json.gz";
	public static final String encoding = "UTF-8";

	private String directory = null;
	private String fileName = null;

	private Gson gson = new Gson();

	public JobProcessCollection(IDuccWorkJob job) {
		String fileDirectory = job.getUserLogDir();
		initialize(fileDirectory);
	}

	public JobProcessCollection(String fileDirectory) {
		initialize(fileDirectory);
	}

	private static long getId(IDuccWorkJob job, IDuccProcess process) {
		long retVal = 0;
		try {
			retVal = Long.parseLong(process.getDuccId().toString().trim());
		}
		catch(Exception e) {
		}
		return retVal;
	}

	private static String getLogDirectory(IDuccWorkJob job, IDuccProcess process) {
		return job.getUserLogDir();
	}

	private static ArrayList<FileInfo> getLogFileList(IDuccWorkJob job, IDuccProcess process) {
		ArrayList<FileInfo> retVal = new ArrayList<FileInfo>();
		try {
			FileInfo fileInfo = new FileInfo();
			fileInfo.name = "jd.out.log";
		}
		catch(Exception e) {
		}
		return retVal;
	}

	private static String getHostName(IDuccWorkJob job, IDuccProcess process) {
		String retVal = null;
		try {
			retVal = process.getNodeIdentity().getName();
		}
		catch(Exception e) {
		}
		return retVal;
	}

	private static long getHostPid(IDuccWorkJob job, IDuccProcess process) {
		long retVal = 0;
		try {
			retVal = Long.parseLong(process.getPID());
		}
		catch(Exception e) {
		}
		return retVal;
	}

	private static String getSchedulerState(IDuccWorkJob job, IDuccProcess process) {
		String retVal = null;
		try {
			retVal = process.getResourceState().toString();
		}
		catch(Exception e) {
		}
		return retVal;
	}

	private static String getSchedulerReason(IDuccWorkJob job, IDuccProcess process) {
		String retVal = null;
		try {
			retVal = process.getProcessDeallocationType().toString();
		}
		catch(Exception e) {
		}
		return retVal;
	}

	private static String getAgentState(IDuccWorkJob job, IDuccProcess process) {
		String retVal = null;
		try {
			retVal = process.getProcessState().toString();
		}
		catch(Exception e) {
		}
		return retVal;
	}

	private static String getAgentReason(IDuccWorkJob job, IDuccProcess process) {
		String retVal = null;
		try {
			retVal = process.getReasonForStoppingProcess();
		}
		catch(Exception e) {
		}
		return retVal;
	}

	private static long getTimeInitStart(IDuccWorkJob job, IDuccProcess process) {
		long retVal = 0;
		try {
			retVal = ((TimeWindow) process.getTimeWindowInit()).getStartLong();
		}
		catch(Exception e) {
		}
		return retVal;
	}

	private static long getTimeInitEnd(IDuccWorkJob job, IDuccProcess process) {
		long retVal = 0;
		try {
			retVal = ((TimeWindow) process.getTimeWindowInit()).getEndLong();
		}
		catch(Exception e) {
		}
		return retVal;
	}

	private static long getTimeRunStart(IDuccWorkJob job, IDuccProcess process) {
		long retVal = 0;
		try {
			retVal = ((TimeWindow) process.getTimeWindowRun()).getStartLong();
		}
		catch(Exception e) {
		}
		return retVal;
	}

	private static long getTimeRunEnd(IDuccWorkJob job, IDuccProcess process) {
		long retVal = 0;
		try {
			retVal = ((TimeWindow) process.getTimeWindowRun()).getEndLong();
		}
		catch(Exception e) {
		}
		return retVal;
	}

	private static long getTimeGC(IDuccWorkJob job, IDuccProcess process) {
		long retVal = 0;
		try {
			retVal = process.getGarbageCollectionStats().getCollectionTime();
		}
		catch(Exception e) {
		}
		return retVal;
	}

	private static long getPgIn(IDuccWorkJob job, IDuccProcess process) {
		long retVal = 0;
		try {
			retVal = process.getMajorFaults();
		}
		catch(Exception e) {
		}
		return retVal;
	}

	private static long getSwap(IDuccWorkJob job, IDuccProcess process) {
		long retVal = 0;
		try {
			retVal = process.getSwapUsage();
		}
		catch(Exception e) {
		}
		return retVal;
	}

	private static long getSwapMax(IDuccWorkJob job, IDuccProcess process) {
		long retVal = 0;
		try {
			retVal = process.getSwapUsageMax();
		}
		catch(Exception e) {
		}
		return retVal;
	}

	private static long getRss(IDuccWorkJob job, IDuccProcess process) {
		long retVal = 0;
		try {
			retVal = process.getResidentMemory();
		}
		catch(Exception e) {
		}
		return retVal;
	}

	private static long getRssMax(IDuccWorkJob job, IDuccProcess process) {
		long retVal = 0;
		try {
			retVal = process.getResidentMemoryMax();
		}
		catch(Exception e) {
		}
		return retVal;
	}

	private static long getWiTimeAvg(IDuccWorkJob job, IDuccProcess process) {
		long retVal = 0;
		try {
			IDuccProcessWorkItems pwi = process.getProcessWorkItems();
			retVal = pwi.getSecsAvg();
		}
		catch(Exception e) {
		}
		return retVal;
	}

	private static long getWiTimeMax(IDuccWorkJob job, IDuccProcess process) {
		long retVal = 0;
		try {
			IDuccProcessWorkItems pwi = process.getProcessWorkItems();
			retVal = pwi.getSecsMax();
		}
		catch(Exception e) {
		}
		return retVal;
	}

	private static long getWiTimeMin(IDuccWorkJob job, IDuccProcess process) {
		long retVal = 0;
		try {
			IDuccProcessWorkItems pwi = process.getProcessWorkItems();
			retVal = pwi.getSecsMin();
		}
		catch(Exception e) {
		}
		return retVal;
	}

	private static long getWiDone(IDuccWorkJob job, IDuccProcess process) {
		long retVal = 0;
		try {
			IDuccProcessWorkItems pwi = process.getProcessWorkItems();
			retVal = pwi.getCountDone();
		}
		catch(Exception e) {
		}
		return retVal;
	}

	private static long getWiError(IDuccWorkJob job, IDuccProcess process) {
		long retVal = 0;
		try {
			IDuccProcessWorkItems pwi = process.getProcessWorkItems();
			retVal = pwi.getCountError();
		}
		catch(Exception e) {
		}
		return retVal;
	}

	private static long getWiRetry(IDuccWorkJob job, IDuccProcess process) {
		long retVal = 0;
		try {
			IDuccProcessWorkItems pwi = process.getProcessWorkItems();
			retVal = pwi.getCountRetry();
		}
		catch(Exception e) {
		}
		return retVal;
	}

	private static long getWiPreempt(IDuccWorkJob job, IDuccProcess process) {
		long retVal = 0;
		try {
			IDuccProcessWorkItems pwi = process.getProcessWorkItems();
			retVal = pwi.getCountPreempt();
		}
		catch(Exception e) {
		}
		return retVal;
	}

	private static String getJConsole(IDuccWorkJob job, IDuccProcess process) {
		String retVal = null;
		try {
			retVal = process.getProcessJmxUrl();
		}
		catch(Exception e) {
		}
		return retVal;
	}

	private void transform(ConcurrentSkipListMap<Long, JobProcessData> map, IDuccWorkJob job, IDuccProcessMap processMap) {
		if(job != null) {
			if(processMap != null) {
				for(DuccId duccId : processMap.keySet()) {
					IDuccProcess process = processMap.get(duccId);
					JobProcessData jpd = new JobProcessData();
					jpd.id = getId(job, process);
					jpd.logDirectory = getLogDirectory(job, process);
					jpd.logFileList = getLogFileList(job, process);
					jpd.hostName = getHostName(job, process);
					jpd.hostPid = getHostPid(job, process);
					jpd.schedulerState = getSchedulerState(job, process);
					jpd.schedulerReason = getSchedulerReason(job, process);
					jpd.agentState = getAgentState(job, process);
					jpd.agentReason = getAgentReason(job, process);
					jpd.timeInitStart = getTimeInitStart(job, process);
					jpd.timeInitEnd = getTimeInitEnd(job, process);
					jpd.timeRunStart = getTimeRunStart(job, process);
					jpd.timeRunEnd = getTimeRunEnd(job, process);
					jpd.timeGC = getTimeGC(job, process);
					jpd.pageIn = getPgIn(job, process);
					jpd.swap = getSwap(job, process);
					jpd.swapMax = getSwapMax(job, process);
					jpd.rss = getRss(job, process);
					jpd.rssMax = getRssMax(job, process);
					jpd.wiTimeAvg = getWiTimeAvg(job, process);
					jpd.wiTimeMax = getWiTimeMax(job, process);
					jpd.wiTimeMin = getWiTimeMin(job, process);
					jpd.wiDone = getWiDone(job, process);
					jpd.wiError = getWiError(job, process);
					jpd.wiRetry = getWiRetry(job, process);
					jpd.wiPreempt = getWiPreempt(job, process);
					jpd.jConsole = getJConsole(job, process);
					Long key = new Long(jpd.id);
					JobProcessData value = jpd;
					map.put(key, value);
				}
			}
		}
	}

	public ConcurrentSkipListMap<Long, JobProcessData> transform(IDuccWorkJob job) {
		ConcurrentSkipListMap<Long, JobProcessData> map = new ConcurrentSkipListMap<Long, JobProcessData>();
		if(job != null) {
			IDuccProcessMap jpMap = job.getProcessMap();
			transform(map, job, jpMap);
			DuccWorkPopDriver driver = job.getDriver();
			if(driver != null) {
				IDuccProcessMap jdMap = driver.getProcessMap();
				transform(map, job, jdMap);
			}
		}
		return map;
	}

	public void initialize(String fileDirectory) {
		if(fileDirectory == null) {
			throw new NullPointerException();
		}
		directory = fileDirectory;
		fileName = IOHelper.marryDir2File(directory,job_processes_data_json_gz);
	}

	public String getDirectory() {
		return directory;
	}

	public String getQualifiedFileName() {
		return fileName;
	}

	public void exportData(ConcurrentSkipListMap<Long, JobProcessData> map) throws IOException {
		if(map == null) {
			throw new NullPointerException();
		}
		BufferedWriter writer = null;
		OutputStreamWriter osr = null;
		GZIPOutputStream gos = null;
		FileOutputStream fos = null;
		File file = null;
		try {
			String json = gson.toJson(map);
			file = new File(fileName);
			fos = new FileOutputStream(file);
			gos = new GZIPOutputStream(fos);
			osr = new OutputStreamWriter(gos, encoding);
	        writer = new BufferedWriter(osr);
	        writer.write(json);
		}
		finally {
	        if(writer != null) {
	        	writer.close();
	        }
	    }
	}

	private int getSize() throws IOException {
		int size = 0;
		BufferedReader reader = null;
		InputStreamReader isr = null;
		GZIPInputStream gis = null;
		FileInputStream fis = null;
		File file = null;
		int rc = 0;
		try {
			file = new File(fileName);
			fis = new FileInputStream(file);
			gis = new GZIPInputStream(fis);
			isr = new InputStreamReader(gis, encoding);
	        reader = new BufferedReader(isr);
	        while(rc >= 0) {
				rc = reader.read();
		        size++;
			}
		}
		finally {
	        if(reader != null) {
	        	reader.close();
	        }
	    }
		return size-1;
	}

	public ConcurrentSkipListMap<Long, JobProcessData> importData() throws IOException {
		ConcurrentSkipListMap<Long, JobProcessData> map = new ConcurrentSkipListMap<Long, JobProcessData>();
		BufferedReader reader = null;
		InputStreamReader isr = null;
		GZIPInputStream gis = null;
		FileInputStream fis = null;
		File file = null;
		char[] cbuf = new char[getSize()];
		try {
			file = new File(getQualifiedFileName());
			fis = new FileInputStream(file);
			gis = new GZIPInputStream(fis);
			isr = new InputStreamReader(gis, encoding);
	        reader = new BufferedReader(isr);
	        reader.read(cbuf);
	        String json = new String(cbuf);
	        Type typeOfMap = new TypeToken<ConcurrentSkipListMap<Long,JobProcessData>>() { }.getType();
			map = gson.fromJson(json, typeOfMap);
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		finally {
	        if(reader != null) {
	        	reader.close();
	        }
	    }
		return map;
	}

}
