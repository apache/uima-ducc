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
import org.apache.uima.ducc.agent.metrics.collectors.ProcessMajorFaultCollector;
import org.apache.uima.ducc.agent.metrics.collectors.ProcessResidentMemoryCollector;
import org.apache.uima.ducc.common.agent.metrics.cpu.ProcessCpuUsage;
import org.apache.uima.ducc.common.agent.metrics.memory.ProcessResidentMemory;
import org.apache.uima.ducc.common.agent.metrics.swap.DuccProcessSwapSpaceUsage;
import org.apache.uima.ducc.common.agent.metrics.swap.ProcessMemoryPageLoadUsage;
import org.apache.uima.ducc.common.node.metrics.ProcessGarbageCollectionStats;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.Utils;
import org.apache.uima.ducc.transport.event.common.IDuccProcess;
import org.apache.uima.ducc.transport.event.common.IDuccProcess.ReasonForStoppingProcess;
import org.apache.uima.ducc.transport.event.common.IProcessState.ProcessState;

public class LinuxProcessMetricsProcessor extends BaseProcessor implements ProcessMetricsProcessor {
  private RandomAccessFile statmFile;

  // private RandomAccessFile nodeStatFile;
  private RandomAccessFile processStatFile;

  private long initTimeinSeconds = 0;
  
  private boolean initializing = true;
  
  private final ExecutorService pool;

  private IDuccProcess process;

  private DuccGarbageStatsCollector gcStatsCollector;

  private int blockSize = 4096; // default, OS specific

  private DuccLogger logger;

  private ManagedProcess managedProcess;

  private NodeAgent agent;

  private int fudgeFactor = 5; // default is 5%

  // private int logCounter=0;
  public LinuxProcessMetricsProcessor(DuccLogger logger, IDuccProcess process, NodeAgent agent,
          String statmFilePath, String nodeStatFilePath, String processStatFilePath,
          ManagedProcess managedProcess) throws FileNotFoundException {
    this.logger = logger;
    statmFile = new RandomAccessFile(statmFilePath, "r");
    // nodeStatFile = new RandomAccessFile(nodeStatFilePath, "r");
    processStatFile = new RandomAccessFile(processStatFilePath, "r");
    this.managedProcess = managedProcess;
    this.agent = agent;
    pool = Executors.newFixedThreadPool(30);
    this.process = process;
    gcStatsCollector = new DuccGarbageStatsCollector(logger, process);

    blockSize = agent.getOSPageSize();
    
    if (System.getProperty("ducc.agent.share.size.fudge.factor") != null) {
      try {
        fudgeFactor = Integer.parseInt(System.getProperty("ducc.agent.share.size.fudge.factor"));
      } catch (NumberFormatException e) {
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
    if (process.getProcessState().equals(ProcessState.Initializing)
            || process.getProcessState().equals(ProcessState.Running))
      try {

       
        ProcessResidentMemoryCollector collector = new ProcessResidentMemoryCollector(statmFile, 2,
                0);
        Future<ProcessResidentMemory> prm = pool.submit(collector);

        ProcessCpuUsageCollector processCpuUsageCollector = new ProcessCpuUsageCollector(logger,
                process.getPID(), processStatFile, 42, 0);

        Future<ProcessCpuUsage> processCpuUsage = pool.submit(processCpuUsageCollector);

        ProcessMajorFaultCollector processMajorFaultUsageCollector = new ProcessMajorFaultCollector(
                logger, process.getPID(), processStatFile, 42, 0);

        Future<ProcessMemoryPageLoadUsage> processMajorFaultUsage = pool
                .submit(processMajorFaultUsageCollector);
        String DUCC_HOME = Utils.findDuccHome();
        // executes script DUCC_HOME/admin/ducc_get_process_swap_usage.sh which sums up swap used by
        // a process
        DuccProcessSwapSpaceUsage processSwapSpaceUsage = new DuccProcessSwapSpaceUsage(
                process.getPID(), managedProcess.getOwner(), DUCC_HOME
                        + "/admin/ducc_get_process_swap_usage.sh", logger);

        // report cpu utilization while the process is running
        if ( managedProcess.getDuccProcess().getProcessState().equals(ProcessState.Running)) {
          if (agent.cpuClockRate > 0) {
            if ( initializing ) {
              initializing = false;
              // cache how much cpu was used up during initialization of the process
              initTimeinSeconds = processCpuUsage.get().getTotalJiffies() / agent.cpuClockRate;
            }
            //  normalize cpu usage to report in seconds. Also subtract how much cpu was
            //  used during initialization
            long cpu = ( processCpuUsage.get().getTotalJiffies()/agent.cpuClockRate)-initTimeinSeconds;
            logger.info("process", null, "----------- PID:" + process.getPID()
                    + " CPU Time (seconds):" + cpu);
            // Publish cumulative CPU usage
            process.setCpuTime(cpu);
          } else {
            process.setCpuTime(0);
            logger.info("process", null,
                    "Agent is unable to determine Node's clock rate. Defaulting CPU Time to 0 For Process with PID:"
                            + process.getPID());
          }
          
        } else {
          //   report 0 for CPU while the process is initializing
          process.setCpuTime(0);
        }
        long majorFaults = processMajorFaultUsage.get().getMajorFaults();
        // collects process Major faults (swap in memory)
        process.setMajorFaults(majorFaults);
        // Current Process Swap Usage in bytes
        long st = System.currentTimeMillis();
        long processSwapUsage = processSwapSpaceUsage.getSwapUsage() * 1024;
        // collects swap usage from /proc/<PID>/smaps file via a script
        // DUCC_HOME/admin/collect_process_swap_usage.sh
        process.setSwapUsage(processSwapUsage);
        // if ( (logCounter % 2 ) == 0 ) {
        logger.info("process", null, "----------- PID:" + process.getPID() + " Major Faults:"
                + majorFaults + " Process Swap Usage:" + processSwapUsage
                + " Max Swap Usage Allowed:" + managedProcess.getMaxSwapThreshold()
                + " Time to Collect Swap Usage:" + (System.currentTimeMillis() - st));
        // }
        // logCounter++;

        if (processSwapUsage > 0 && processSwapUsage > managedProcess.getMaxSwapThreshold()) {
          logger.error(
                  "process",
                  null,
                  "\n\n********************************************************\n\tProcess with PID:"
                          + managedProcess.getPid()
                          + " Exceeded its Max Swap Usage Threshold of "
                          + (managedProcess.getMaxSwapThreshold() / 1024)
                          / 1024
                          + " MBs. The Current Swap Usage is: "
                          + (processSwapUsage / 1024)
                          / 1024
                          + " MBs .Killing process ...\n********************************************************\n\n");
          try {
            managedProcess.kill(); // mark it for death
            process.setReasonForStoppingProcess(ReasonForStoppingProcess.ExceededSwapThreshold
                    .toString());
            agent.stopProcess(process);
          } catch (Exception ee) {
            logger.error("process", null, ee);
          }
          return;
        } else {
          // if the fudgeFactor is negative, don't check if the process exceeded its
          // memory assignment.

          if (fudgeFactor > -1
                  && managedProcess.getProcessMemoryAssignment().getMaxMemoryWithFudge() > 0) {
            // RSS is in terms of pages(blocks) which size is system dependent. Default 4096 bytes
            long rss = (prm.get().get() * (blockSize / 1024)) / 1024; // normalize RSS into MB
            logger.trace("process", null, "*** Process with PID:" + managedProcess.getPid()
                    + " Assigned Memory (MB): " + managedProcess.getProcessMemoryAssignment()
                    + " MBs. Current RSS (MB):" + rss);
            // check if process resident memory exceeds its memory assignment calculate in the PM
            if (rss > managedProcess.getProcessMemoryAssignment().getMaxMemoryWithFudge()) {
              logger.error(
                      "process",
                      null,
                      "\n\n********************************************************\n\tProcess with PID:"
                              + managedProcess.getPid()
                              + " Exceeded its max memory assignment (including a fudge factor) of "
                              + managedProcess.getProcessMemoryAssignment()
                              + " MBs. This Process Resident Memory Size: "
                              + rss
                              + " MBs .Killing process ...\n********************************************************\n\n");
              try {
                managedProcess.kill(); // mark it for death
                process.setReasonForStoppingProcess(ReasonForStoppingProcess.ExceededShareSize
                        .toString());
                agent.stopProcess(process);
              } catch (Exception ee) {
                logger.error("process", null, ee);
              }
              return;
            }
          }

        }
        // Publish resident memory
        process.setResidentMemory((prm.get().get() * blockSize));
        ProcessGarbageCollectionStats gcStats = gcStatsCollector.collect();
        process.setGarbageCollectionStats(gcStats);
        logger.info(
                "process",
                null,
                "PID:" + process.getPID() + " Total GC Collection Count :"
                        + gcStats.getCollectionCount() + " Total GC Collection Time :"
                        + gcStats.getCollectionTime());

      } catch (Exception ex) {
        logger.error("process", null, e);
        ex.printStackTrace();
      }

  }

}
