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
import org.apache.uima.ducc.transport.event.common.IDuccProcessType.ProcessType;
import org.apache.uima.ducc.transport.event.common.IProcessState.ProcessState;

public class LinuxProcessMetricsProcessor extends BaseProcessor implements
		ProcessMetricsProcessor {
	private RandomAccessFile statmFile;

	// private RandomAccessFile nodeStatFile;
	private RandomAccessFile processStatFile;

	//private long totalCpuInitUsage = 0;
	
	private long previousCPUReadingInMillis = 0;
	
	private long previousSnapshotTime = 0;

	//private boolean initializing = true;

	private final ExecutorService pool;

	private IDuccProcess process;

	private DuccGarbageStatsCollector gcStatsCollector;

	private int blockSize = 4096; // default, OS specific

	private DuccLogger logger;

	private ManagedProcess managedProcess;

	private NodeAgent agent;

	private int fudgeFactor = 5; // default is 5%

	private volatile boolean closed = true;

	//private long clockAtStartOfRun = 0;

	private long percentCPU = 0;

	public LinuxProcessMetricsProcessor(DuccLogger logger,
			IDuccProcess process, NodeAgent agent, String statmFilePath,
			String nodeStatFilePath, String processStatFilePath,
			ManagedProcess managedProcess) throws FileNotFoundException {
		this.logger = logger;
		statmFile = new RandomAccessFile(statmFilePath, "r");
		// nodeStatFile = new RandomAccessFile(nodeStatFilePath, "r");
		processStatFile = new RandomAccessFile(processStatFilePath, "r");
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
			if (statmFile != null && statmFile.getFD().valid()) {
				statmFile.close();
			}
			if (processStatFile != null && processStatFile.getFD().valid()) {
				processStatFile.close();
			}
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

	public void process(Exchange e) {
		if (closed) { // files closed
			return;
		}


		// if process is stopping or already dead dont collect metrics. The
		// Camel
		// route has just been stopped.
		if (!collectStats(process.getProcessState())) {
			return;
		}
		if (process.getProcessState().equals(ProcessState.Initializing)
				|| process.getProcessState().equals(ProcessState.Running))
			try {

				// executes script
				// DUCC_HOME/admin/ducc_get_process_swap_usage.sh which sums up
				// swap used by
				// a process
				long totalSwapUsage = 0;
				long totalFaults = 0;
				long totalCpuUsageInMillis = 0;
				long totalRss = 0;
				//int currentCpuUsage = 0;
				Future<ProcessMemoryPageLoadUsage> processMajorFaultUsage = null;
				Future<ProcessCpuUsage> processCpuUsage = null;
				String[] cgroupPids = new String[0];
				try {
					String swapUsageScript = System
							.getProperty("ducc.agent.swap.usage.script");

					if (agent.useCgroups) {
						String containerId = agent.cgroupsManager
								.getContainerId(managedProcess);
						cgroupPids = agent.cgroupsManager
								.getPidsInCgroup(containerId);
						for (String pid : cgroupPids) {
							// the swap usage script is defined in
							// ducc.properties
							if (swapUsageScript != null) {
								DuccProcessSwapSpaceUsage processSwapSpaceUsage = new DuccProcessSwapSpaceUsage(
										pid, managedProcess.getOwner(),
										swapUsageScript, logger);
								totalSwapUsage += processSwapSpaceUsage
										.getSwapUsage();
							}

							ProcessMajorFaultCollector processMajorFaultUsageCollector = new ProcessMajorFaultCollector(
									logger, pid);
							// if process is stopping or already dead dont
							// collect metrics. The Camel
							// route has just been stopped.
							if (!collectStats(process.getProcessState())) {
								return;
							}

							processMajorFaultUsage = pool
									.submit(processMajorFaultUsageCollector);
							totalFaults += processMajorFaultUsage.get()
									.getMajorFaults();
							try {
								if (!collectStats(process.getProcessState())) {
									return;
								}
								
							} catch( Exception ee) {
								logger.warn(
										"LinuxProcessMetricsProcessor.process",
										null,ee);
							} 
							RandomAccessFile rStatmFile = null;
							try {
								rStatmFile = new RandomAccessFile("/proc/"
										+ pid + "/statm", "r");
							} catch (FileNotFoundException fnfe) {
								logger.info(
										"LinuxProcessMetricsProcessor.process",
										null,
										"Statm File:"
												+ "/proc/"
												+ pid
												+ "/statm *Not Found*. Process must have already exited");
								return;
							}
							ProcessResidentMemoryCollector collector = new ProcessResidentMemoryCollector(
									rStatmFile, 2, 0);
							// if process is stopping or already dead dont
							// collect metrics. The Camel
							// route has just been stopped.
							if (!collectStats(process.getProcessState())) {
								return;
							}

							Future<ProcessResidentMemory> prm = pool
									.submit(collector);

							totalRss += prm.get().get();

							rStatmFile.close();
						}

						ProcessCpuUsageCollector processCpuUsageCollector = 
								new ProcessCpuUsageCollector(logger, agent.cgroupsManager, containerId);
						processCpuUsage = pool
								.submit(processCpuUsageCollector);
						long cpuUsageInNanos = processCpuUsage.get().getCpuUsage();
						if ( cpuUsageInNanos >= 0 ) {
							// cpuUsage comes from cpuacct.usage and is in nanos
							totalCpuUsageInMillis = Math.round( cpuUsageInNanos / 1000000 );  // normalize into millis
						} else {
							totalCpuUsageInMillis = -1;
						}
						logger.info(
								"LinuxProcessMetricsProcessor.process",null,
								"CPU USAGE:"+cpuUsageInNanos+ " CLOCK RATE:"+agent.cpuClockRate+" Total CPU USAGE:"+totalCpuUsageInMillis);
					} else {
						if (swapUsageScript != null) {
							DuccProcessSwapSpaceUsage processSwapSpaceUsage = new DuccProcessSwapSpaceUsage(
									process.getPID(),
									managedProcess.getOwner(), swapUsageScript,        
									logger);
							totalSwapUsage = processSwapSpaceUsage
									.getSwapUsage();
						}

						ProcessMajorFaultCollector processMajorFaultUsageCollector = new ProcessMajorFaultCollector(
								logger, process.getPID());

						// if process is stopping or already dead dont collect
						// metrics. The Camel
						// route has just been stopped.
						if (!collectStats(process.getProcessState())) {
						    return;
						}
						processMajorFaultUsage = pool
								.submit(processMajorFaultUsageCollector);
						totalFaults = processMajorFaultUsage.get()
								.getMajorFaults();
						// Cgroups are not available so percent CPU is not available
						totalCpuUsageInMillis = -1;   // -1 stands for N/A
						//currentCpuUsage = -1; // -1 stands for N/A

						ProcessResidentMemoryCollector collector = new ProcessResidentMemoryCollector(
								statmFile, 2, 0);
						// if process is stopping or already dead dont collect
						// metrics. The Camel
						// route has just been stopped.
						if (!collectStats(process.getProcessState())) {
							return;
						}

						Future<ProcessResidentMemory> prm = pool
								.submit(collector);
						totalRss = prm.get().get();
					}

				} catch (Exception exc) {
					if (!collectStats(process.getProcessState())) {
						return;
					}
					logger.error("LinuxProcessMetricsProcessor.process", null,
							exc);
				}

				// report cpu utilization while the process is running
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
						process.setCpuTime( percentCPU );
					} else {
						process.setCpuTime(-1);   // -1 stands for N/A
						percentCPU = -1;
					}
				} else {
					// if process is not dead, report the last known percentCPU
					process.setCpuTime(percentCPU);
				}
				// publish current CPU usage by computing a delta from the last time
				// CPU data was fetched.
				if ( totalCpuUsageInMillis > 0 ) {
					double millisCPU = ( totalCpuUsageInMillis - previousCPUReadingInMillis )*1.0;
					double millisRun = ( System.currentTimeMillis() - previousSnapshotTime )*1.0;
					process.setCurrentCPU(Math.round(100*(millisCPU/millisRun) ) );
					previousCPUReadingInMillis = totalCpuUsageInMillis;
					previousSnapshotTime = System.currentTimeMillis();
					
				} else {
					if (agent.useCgroups && totalCpuUsageInMillis != -1 ) {
						process.setCurrentCPU(0);
					} else {
						process.setCurrentCPU(-1);  // -1 stands for N/A
					}
				}
				logger.info(
					"process",
					null,
					"----------- PID:" + process.getPID()
					+ " Total CPU Time (%):" + process.getCpuTime()
					+ " Delta CPU Time (%):" +process.getCurrentCPU() );
				// collects process Major faults (swap in memory)
				process.setMajorFaults(totalFaults);
				// Current Process Swap Usage in bytes
				long st = System.currentTimeMillis();
				long processSwapUsage = totalSwapUsage * 1024;
				// collects swap usage from /proc/<PID>/smaps file via a script
				// DUCC_HOME/admin/collect_process_swap_usage.sh
				process.setSwapUsage(processSwapUsage);
				logger.info(
						"process",
						null,
						"----------- PID:" + process.getPID()
								+ " Major Faults:" + totalFaults
								+ " Process Swap Usage:" + processSwapUsage
								+ " Max Swap Usage Allowed:"
								+ managedProcess.getMaxSwapThreshold()
								+ " Time to Collect Swap Usage:"
								+ (System.currentTimeMillis() - st));
				if (processSwapUsage > 0
						&& processSwapUsage > managedProcess
								.getMaxSwapThreshold()) {
				} else {
					// Use Memory Guard only if cgroups are disabled and fudge
					// factor > -1

					if (!agent.useCgroups
							&& fudgeFactor > -1
							&& managedProcess.getProcessMemoryAssignment()
									.getMaxMemoryWithFudge() > 0) {
						// RSS is in terms of pages(blocks) which size is system
						// dependent. Default 4096 bytes
						long rss = (totalRss * (blockSize / 1024)) / 1024; // normalize
																			// RSS
																			// into
																			// MB
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
				// Publish resident memory
				process.setResidentMemory((totalRss * blockSize));
				// dont collect GC metrics for POPs. May not be java or may not
				// be a jmx enabled java process
				if (!process.getProcessType().equals(ProcessType.Pop)) {
					ProcessGarbageCollectionStats gcStats = gcStatsCollector
							.collect();
					process.setGarbageCollectionStats(gcStats);
					logger.info(
							"process",
							null,
							"PID:" + process.getPID()
									+ " Total GC Collection Count :"
									+ gcStats.getCollectionCount()
									+ " Total GC Collection Time :"
									+ gcStats.getCollectionTime());
				}

			} catch (Exception ex) {
				// if the child process is not running dont log the exception.
				if (!collectStats(process.getProcessState())) {
					return;
				}
				logger.error("process", null, ex);
				ex.printStackTrace();
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
