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
import org.apache.uima.ducc.agent.metrics.collectors.ProcessSwapUsageCollector;
import org.apache.uima.ducc.common.agent.metrics.cpu.ProcessCpuUsage;
import org.apache.uima.ducc.common.agent.metrics.memory.ProcessResidentMemory;
import org.apache.uima.ducc.common.agent.metrics.swap.ProcessMemoryPageLoadUsage;
import org.apache.uima.ducc.common.agent.metrics.swap.ProcessSwapSpaceUsage;
import org.apache.uima.ducc.common.node.metrics.ProcessGarbageCollectionStats;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.Utils;
import org.apache.uima.ducc.transport.event.common.IDuccProcess;
import org.apache.uima.ducc.transport.event.common.IDuccProcess.ReasonForStoppingProcess;
import org.apache.uima.ducc.transport.event.common.IDuccProcessType.ProcessType;
import org.apache.uima.ducc.transport.event.common.IProcessState.ProcessState;

public class LinuxProcessMetricsProcessor extends BaseProcessor implements
		ProcessMetricsProcessor {

	private long previousCPUReadingInMillis = 0;
	
	private long previousSnapshotTime = 0;

	private final ExecutorService pool;

	private IDuccProcess process;

	private DuccGarbageStatsCollector gcStatsCollector;

	private int blockSize = 4096; // default, OS specific

	private DuccLogger logger;

	private ManagedProcess managedProcess;

	private NodeAgent agent;

	private int fudgeFactor = 5; // default is 5%

	private volatile boolean closed = true;


	private long percentCPU = 0;
	
	
	public LinuxProcessMetricsProcessor(DuccLogger logger,
			IDuccProcess process, NodeAgent agent, ManagedProcess managedProcess) throws FileNotFoundException {
		this.logger = logger;
		this.managedProcess = managedProcess;
		this.agent = agent;
		pool = Executors.newCachedThreadPool();
		this.process = process;
		gcStatsCollector = new DuccGarbageStatsCollector(logger, process);

		// keep a refernce to this so that we can call close() when the process
		// terminates. We need to
		// close fds to stat and statm files
		managedProcess.setMetricsProcessor(this);

		blockSize = agent.getOSPageSize();

		if (System.getProperty("ducc.agent.share.size.fudge.factor") != null) {
			try {
				fudgeFactor = Integer.parseInt(System
						.getProperty("ducc.agent.share.size.fudge.factor"));
			} catch (NumberFormatException e) {
				e.printStackTrace();
			}
		}
		closed = false;
	}

	public void stop() {
		try {
			if (pool != null) {
				pool.shutdown();
			}
		} catch (Exception e) {
			logger.error("LinuxProcessMetricsProcessor.stop()", null, e);

		}
	}

	public void close() {
		closed = true;
		try {
			this.stop();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private boolean collectStats(ProcessState state) {
		if (process.getProcessState().equals(ProcessState.Stopped)
				|| process.getProcessState().equals(ProcessState.Killed)
				|| process.getProcessState().equals(ProcessState.Failed)
				|| process.getProcessState().equals(ProcessState.Stopping)) {
			return false; // dont collect stats
		}
		return true;
	}

	private long getSwapUsage() throws Exception {
		long swapUsage = -1;
		if (agent.useCgroups) {

			String containerId = agent.cgroupsManager
					.getContainerId(managedProcess);

			ProcessSwapUsageCollector processSwapCollector = new ProcessSwapUsageCollector(
					logger, agent.cgroupsManager, containerId);
			logger.debug("LinuxProcessMetricsProcessor.getSwapUsage", null,
					"Fetching Swap Usage PID:" + process.getPID());
			Future<ProcessSwapSpaceUsage> processFaults = pool
					.submit(processSwapCollector);
			swapUsage = processFaults.get().getSwapUsage();
			logger.debug("LinuxProcessMetricsProcessor.getSwapUsage", null,
					" Process Swap Usage:" + swapUsage);
		}
		return swapUsage;
	}
	
	private long getFaults() throws Exception {
		long faults = -1;
		if (agent.useCgroups) {
			String containerId = agent.cgroupsManager.getContainerId(managedProcess);

			ProcessMajorFaultCollector processFaultsCollector = 
					new ProcessMajorFaultCollector(logger, agent.cgroupsManager, containerId);
	        logger.debug("LinuxProcessMetricsProcessor.getFaults",null,"Fetching Page Faults PID:"+process.getPID());
	        Future<ProcessMemoryPageLoadUsage> processFaults = pool.submit(processFaultsCollector);
		    faults = processFaults.get().getMajorFaults();
			logger.debug(
					"LinuxProcessMetricsProcessor.getFaults",null," Process Faults (pgpgin):"+faults);
		}
		return faults;
	}
	private long getRss() throws Exception {
		long rss = -1;
		if (agent.useCgroups) {
			String containerId = agent.cgroupsManager.getContainerId(managedProcess);

			ProcessResidentMemoryCollector processRSSCollector = 
					new ProcessResidentMemoryCollector(logger, agent.cgroupsManager, containerId);
	        logger.debug("LinuxProcessMetricsProcessor.getRss",null,"Fetching RSS Usage for PID:"+process.getPID());
	        Future<ProcessResidentMemory> processRss = pool.submit(processRSSCollector);
		    rss = processRss.get().get();
			logger.debug(
					"LinuxProcessMetricsProcessor.getRss",null," Process RSS:"+rss);
		}
		return rss;
	}
	
	private long getCpuUsage() throws Exception {
		long cpuUsage=-1;
		if (agent.useCgroups) {
			String containerId = agent.cgroupsManager.getContainerId(managedProcess);

			Future<ProcessCpuUsage> processCpuUsage = null;
			ProcessCpuUsageCollector processCpuUsageCollector = 
					new ProcessCpuUsageCollector(logger, agent.cgroupsManager, containerId);
	        logger.debug("LinuxProcessMetricsProcessor.getCpuUsage",null,"Fetching CPU Usage for PID:"+process.getPID());
			processCpuUsage = pool
					.submit(processCpuUsageCollector);
			long cpuUsageInNanos = processCpuUsage.get().getCpuUsage();
			if ( cpuUsageInNanos >= 0 ) {
				// cpuUsage comes from cpuacct.usage and is in nanos
				cpuUsage = Math.round( cpuUsageInNanos / 1000000 );  // normalize into millis
			} 
			logger.debug(
					"LinuxProcessMetricsProcessor.getCpuUsage",null,
					"CPU USAGE:"+cpuUsageInNanos+ " CLOCK RATE:"+agent.cpuClockRate+" Total CPU USAGE:"+cpuUsage);
		}
		return cpuUsage;
	}
	
	private long getCpuTime( long totalCpuUsageInMillis) throws Exception {
		long cp = -1;
		if (managedProcess.getDuccProcess().getProcessState()
				.equals(ProcessState.Running) ||
				managedProcess.getDuccProcess().getProcessState()
				.equals(ProcessState.Initializing)	
				) {
			if (agent.useCgroups && totalCpuUsageInMillis != -1) {
				
				long timeRunning = 1;
				if ( process.getTimeWindowInit() != null ) {
					timeRunning = process.getTimeWindowInit().getElapsedMillis();
				}
				if ( process.getTimeWindowRun() != null ) {
					timeRunning += process.getTimeWindowRun().getElapsedMillis();
				}
				// normalize time in running state into seconds
				percentCPU = Math.round(100*( (totalCpuUsageInMillis*1.0)/ (timeRunning*1.0)));
				cp = percentCPU;
			}
		} else {
			cp  = percentCPU;
		}
		return cp;
	}

	private long getCurrentCpu(long totalCpuUsageInMillis ) {
		long currentCpu=-1;
		// publish current CPU usage by computing a delta from the last time
		// CPU data was fetched.
		if ( totalCpuUsageInMillis > 0 ) {
			double millisCPU = ( totalCpuUsageInMillis - previousCPUReadingInMillis )*1.0;
			double millisRun = ( System.currentTimeMillis() - previousSnapshotTime )*1.0;
			currentCpu = Math.round(100*(millisCPU/millisRun) ) ;
			previousCPUReadingInMillis = totalCpuUsageInMillis;
			previousSnapshotTime = System.currentTimeMillis();
		} else {
			if (agent.useCgroups && totalCpuUsageInMillis != -1 ) {
				currentCpu = 0;
			}
		}
		return currentCpu;
	}
	

	private void killProcsIfExceedingMemoryThreshold() throws Exception {
		if ( !agent.useCgroups ) {
			return;
		}
		if (process.getSwapUsage() > 0
				&& process.getSwapUsage() > managedProcess
						.getMaxSwapThreshold()) {
		} else {
			String containerId = agent.cgroupsManager
					.getContainerId(managedProcess);

			String[] cgroupPids = agent.cgroupsManager
			        .getPidsInCgroup(containerId);
            logger.debug("LinuxProcessMetricsProcessor.process",null,"Container ID:"+containerId+" cgroup pids "+cgroupPids.length);

			// Use Memory Guard only if cgroups are disabled and fudge
			// factor > -1

			if ( fudgeFactor > -1
				 && managedProcess.getProcessMemoryAssignment()
							.getMaxMemoryWithFudge() > 0) {

				long rss = (process.getResidentMemory() / 1024) / 1024; // normalize RSS into MB

				logger.trace(
						"process",
						null,
						"*** Process with PID:"
								+ managedProcess.getPid()
								+ " Assigned Memory (MB): "
								+ managedProcess
										.getProcessMemoryAssignment()
								+ " MBs. Current RSS (MB):" + rss);
				// check if process resident memory exceeds its memory
				// assignment calculate in the PM
				if (rss > managedProcess.getProcessMemoryAssignment()
						.getMaxMemoryWithFudge()) {
					logger.error(
							"process",
							null,
							"\n\n********************************************************\n\tProcess with PID:"
									+ managedProcess.getPid()
									+ " Exceeded its max memory assignment (including a fudge factor) of "
									+ managedProcess
											.getProcessMemoryAssignment()
											.getMaxMemoryWithFudge()
									+ " MBs. This Process Resident Memory Size: "
									+ rss
									+ " MBs .Killing process ...\n********************************************************\n\n");
					try {
						managedProcess.kill(); // mark it for death
						process.setReasonForStoppingProcess(ReasonForStoppingProcess.ExceededShareSize
								.toString());
						agent.stopProcess(process);

						if (agent.useCgroups) {
							for (String pid : cgroupPids) {
								// skip the main process that was just
								// killed above. Only kill
								// its child processes.
								if (pid.equals(managedProcess
										.getDuccProcess().getPID())) {
									continue;
								}
								killChildProcess(pid, "-15");
							}
						}
					} catch (Exception ee) {
						if (!collectStats(process.getProcessState())) {
							return;
						}
						logger.error("process", null, ee);
					}
					return;
				}
			}

		}

	}

	private ProcessGarbageCollectionStats getGCStats() throws Exception {
//		if (!process.getProcessType().equals(ProcessType.Pop)) {
		if ( process.getProcessJmxUrl() != null
				&& process.getProcessJmxUrl().trim().length() > 0 ) {
			logger.debug("LinuxProcessMetricsProcessor.getGCStats",	null, "Collecting GC Stats");
			ProcessGarbageCollectionStats gcStats = gcStatsCollector
					.collect();
		   return gcStats;
		}
		return new ProcessGarbageCollectionStats();
	}
	public boolean processIsActive() {
		return process.getProcessState().equals(ProcessState.Starting)
               ||
               process.getProcessState().equals(ProcessState.Started)
               ||
			   process.getProcessState().equals(ProcessState.Initializing)
			   || 
			   process.getProcessState().equals(ProcessState.Running);
	}
	public void process(Exchange e) {
		// if process is stopping or already dead dont collect metrics. The
		// Camel route has just been stopped.
		if (closed || !processIsActive()) {
 		    logger.info("LinuxProcessMetricsProcessor.process",	null,"Process with PID:"+process.getPID() +" not in Running or Initializing state. Returning");	
 		    return;
		}
		try {
			
			process.setSwapUsage(getSwapUsage());
			process.setMajorFaults(getFaults());

			long rssInBytes = getRss();
			process.setResidentMemory(rssInBytes);

			long totalCpuUsageInMillis = getCpuUsage();

			// set CPU time in terms of %
			process.setCpuTime(getCpuTime(totalCpuUsageInMillis));

			process.setCurrentCPU(getCurrentCpu(totalCpuUsageInMillis));

			ProcessGarbageCollectionStats gcStats = getGCStats();
			process.setGarbageCollectionStats(gcStats);
			logger.info(
					"process",
					null,
					"----------- PID:" + process.getPID() + " RSS:" 
							+ ((rssInBytes > -1) ? (rssInBytes / (1024 * 1024))+ " MB" : "-1")
							+ " Total CPU Time (%):" + process.getCpuTime()
							+ " Delta CPU Time (%):" + process.getCurrentCPU()
							+ " Major Faults:" + process.getMajorFaults()
							+ " Process Swap Usage:" + process.getSwapUsage()
							+ " Max Swap Usage Allowed:"
							+ managedProcess.getMaxSwapThreshold()
							+ " Total GC Collection Count :"
							+ gcStats.getCollectionCount()
							+ " Total GC Collection Time :"
							+ gcStats.getCollectionTime());

			killProcsIfExceedingMemoryThreshold();

		} catch (Exception exc) {
			if (!collectStats(process.getProcessState())) {
				return;
			}
			logger.error("LinuxProcessMetricsProcessor.process", null, exc);
		}
	}

	private void killChildProcess(final String pid, final String signal) {
		// spawn a thread that will do kill -15, wait for 1 minute and kill the
		// process
		// hard if it is still alive
		(new Thread() {
			public void run() {
				String c_launcher_path = Utils
						.resolvePlaceholderIfExists(
								System.getProperty("ducc.agent.launcher.ducc_spawn_path"),
								System.getProperties());
				try {
					String[] killCmd = null;
					String useSpawn = System
							.getProperty("ducc.agent.launcher.use.ducc_spawn");
					if (useSpawn != null
							&& useSpawn.toLowerCase().equals("true")) {
						killCmd = new String[] {
								c_launcher_path,
								"-u",
								((ManagedProcess) managedProcess).getOwner(),
								"--",
								"/bin/kill",
								signal,
								((ManagedProcess) managedProcess)
										.getDuccProcess().getPID() };
					} else {
						killCmd = new String[] {
								"/bin/kill",
								"-15",
								((ManagedProcess) managedProcess)
										.getDuccProcess().getPID() };
					}
					ProcessBuilder pb = new ProcessBuilder(killCmd);
					Process p = pb.start();
					p.wait(1000 * 60); // wait for 1 minute and whack the
										// process if still alive
					p.destroy();
				} catch (Exception e) {
					logger.error("killChildProcess",
							managedProcess.getWorkDuccId(), e);
				}
			}
		}).start();

	}

}
