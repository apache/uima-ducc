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
package org.apache.uima.ducc.agent.processors;

import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.camel.Exchange;
import org.apache.uima.ducc.agent.NodeAgent;
import org.apache.uima.ducc.agent.launcher.ManagedProcess;
import org.apache.uima.ducc.agent.metrics.collectors.DuccGarbageStatsCollector;
import org.apache.uima.ducc.agent.metrics.collectors.ProcessCpuUsageCollector;
import org.apache.uima.ducc.agent.metrics.collectors.ProcessResidentMemoryCollector;
import org.apache.uima.ducc.common.agent.metrics.cpu.ProcessCpuUsage;
import org.apache.uima.ducc.common.agent.metrics.memory.ProcessResidentMemory;
import org.apache.uima.ducc.common.node.metrics.ProcessGarbageCollectionStats;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.transport.event.common.IDuccProcess;
import org.apache.uima.ducc.transport.event.common.IDuccProcess.ReasonForStoppingProcess;
import org.apache.uima.ducc.transport.event.common.IProcessState.ProcessState;


public class LinuxProcessMetricsProcessor extends BaseProcessor 
implements ProcessMetricsProcessor {
	private RandomAccessFile statmFile;
	//private RandomAccessFile nodeStatFile;
	private RandomAccessFile processStatFile;
	private final ExecutorService pool;
	private IDuccProcess process;
	private DuccGarbageStatsCollector gcStatsCollector;
	private int blockSize = 4096;  // default, OS specific
	private DuccLogger logger;
	private ManagedProcess managedProcess;
	private NodeAgent agent;
  private int fudgeFactor = 5;  // default is 5%
  
	public LinuxProcessMetricsProcessor(DuccLogger logger, IDuccProcess process, NodeAgent agent,String statmFilePath, String nodeStatFilePath, String processStatFilePath, ManagedProcess managedProcess) throws FileNotFoundException{
		this.logger = logger;
		statmFile = new RandomAccessFile(statmFilePath, "r");
		//nodeStatFile = new RandomAccessFile(nodeStatFilePath, "r");
		processStatFile = new RandomAccessFile(processStatFilePath, "r");
		this.managedProcess = managedProcess;
		this.agent = agent;
		pool = Executors.newFixedThreadPool(3);
		this.process = process;
    gcStatsCollector = new DuccGarbageStatsCollector(logger, process);
		//	read the block size from ducc.properties
		if ( System.getProperty("os.page.size") != null ) {
			try {
				blockSize = Integer.parseInt(System.getProperty("os.page.size"));
			} catch(NumberFormatException e) {
				e.printStackTrace();
			}
		}
    if ( System.getProperty("ducc.agent.share.size.fudge.factor") != null ) {
      try {
        fudgeFactor = Integer.parseInt(System.getProperty("ducc.agent.share.size.fudge.factor"));
      } catch(NumberFormatException e) {
        e.printStackTrace();
      }
    }
		
	}
	public void close() {
		try {
			if (statmFile != null) {
				statmFile.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	public void process(Exchange e) {
		if ( process.getProcessState().equals(ProcessState.Initializing) ||
			 process.getProcessState().equals(ProcessState.Running))
		try {
			ProcessResidentMemoryCollector collector = 
				new ProcessResidentMemoryCollector(statmFile, 2, 0);
			Future<ProcessResidentMemory> prm = pool.submit(collector);
			
			ProcessCpuUsageCollector processCpuUsageCollector =
					new ProcessCpuUsageCollector(logger, process.getPID(), processStatFile,42,0);
			Future<ProcessCpuUsage> processCpuUsage = pool.submit(processCpuUsageCollector);
			
			logger.trace("process", null, "----------- PID:"+process.getPID()+" Cumulative CPU Time (jiffies):"+processCpuUsage.get().getTotalJiffies()); 
			//	Publish cumulative CPU usage
			process.setCpuTime(processCpuUsage.get().getTotalJiffies());
			// if the fudgeFactor is negative, don't check if the process exceeded its 
			// memory assignment.
			
			if ( fudgeFactor > -1 && managedProcess.getProcessMemoryAssignment() > 0 ) {
			  // RSS is in terms of pages(blocks) which size is system dependent. Default 4096 bytes
        long rss = (prm.get().get()*(blockSize/1024))/1024;  // normalize RSS into MB
        logger.trace("process", null, "*** Process with PID:"+managedProcess.getPid()+ " Assigned Memory (MB): "+ managedProcess.getProcessMemoryAssignment()+" MBs. Current RSS (MB):"+rss);
        //  check if process resident memory exceeds its memory assignment calculate in the PM
        if ( rss > managedProcess.getProcessMemoryAssignment() ) {
          logger.error("process", null, "\n\n********************************************************\n\tProcess with PID:"+managedProcess.getPid()+ " Exceeded its max memory assignment (including a fudge factor) of "+ managedProcess.getProcessMemoryAssignment()+" MBs. This Process Resident Memory Size: "+rss+" MBs .Killing process ...\n********************************************************\n\n" );
         try {
           managedProcess.kill();  // mark it for death
           process.setReasonForStoppingProcess(ReasonForStoppingProcess.ExceededShareSize.toString());
           agent.stopProcess(process); 
         } catch( Exception ee) {
           logger.error("process", null,ee);           
         }
         return;
        }
			} 
	    //  Publish resident memory
	    process.setResidentMemory((prm.get().get()*blockSize));
	    ProcessGarbageCollectionStats gcStats = 
	          gcStatsCollector.collect();
	    process.setGarbageCollectionStats( gcStats );
	    logger.info("process", null, "PID:"+process.getPID()+" Total GC Collection Count :" + gcStats.getCollectionCount()+ " Total GC Collection Time :" +  gcStats.getCollectionTime());

		} catch( Exception ex) {
			ex.printStackTrace();
		}
		
	}
	public static void main(String[] args) {
		try {
//			LinuxProcessMetricsProcessor p = new LinuxProcessMetricsProcessor(null,null,args[0]);
//			p.process(null);
		} catch( Exception e) {
			e.printStackTrace();
		}
	}
}
