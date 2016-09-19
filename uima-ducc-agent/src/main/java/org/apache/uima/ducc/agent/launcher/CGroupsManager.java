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
package org.apache.uima.ducc.agent.launcher;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.uima.ducc.agent.NodeAgent;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.Utils;
import org.apache.uima.ducc.transport.event.common.IDuccProcessType.ProcessType;

/**
 * Manages cgroup container on a node
 * 
 * Supported operations: - cgcreate - creates cgroup container - cgset - sets
 * max memory limit for an existing container
 * 
 * This code supports both new and old cgconfig. The old configuration adds cgroups
 * to <cgroup location>/ducc where the new adds it to:
 * <cgroup location/cpu/ducc  and <cgroup location>/memory/ducc. 
 * On startup the agent detects which cgconfig is active and adjusts accordingly. 
 *  
 */
public class CGroupsManager {
	private DuccLogger agentLogger = null;
	private static final String SYSTEM = "ducc";
	// the following three properties are only used for the new cgconfig
	private static final String CGDuccMemoryPath = "/memory/"+SYSTEM+"/";
	private static final String CGDuccCpuPath = "/cpu/"+SYSTEM+"/";
	private static final String CGProcsFile = "/cgroup.procs";
	private static final String CGDuccCpuAcctPath = "/cpu/"+SYSTEM+"/";
	
	// legacy means that the cgonfig points to <cgroup location>/ducc
	private boolean legacyCgConfig = false;
	
	enum CGroupCommand {
   	 CGSET("cgset"),
   	 CGCREATE("cgcreate");

   	 String cmd;
   	 CGroupCommand(String cmd  ) {
   		 this.cmd = cmd;
   	 }
   	 public String cmd() {
   		 return cmd;
   	 }
    };
    // manages list of 'active' cgroup containers by container id
	private Set<String> containerIds = new LinkedHashSet<String>();
	// stores cgroup base location
	private String cgroupBaseDir = "";
	// stores cgroup utils location like cgcreate, cgset, etc
	private String cgroupUtilsDir=null;
	// stores comma separated list of subsystems like cpu,memory
	private String cgroupSubsystems = ""; // comma separated list of subsystems
	private long retryMax = 4;
	private long delayFactor = 2000;  // 2 secs in millis
    private long maxTimeToWaitForProcessToStop;
    private static  String fetchCgroupsBaseDir(String mounts) {
  	  String cbaseDir=null;
  	  BufferedReader br = null;
  	  try {
  		  FileInputStream fis = new FileInputStream(mounts);
  			//Construct BufferedReader from InputStreamReader
  		  br = new BufferedReader(new InputStreamReader(fis));
  		 
  		String line = null;
  		while ((line = br.readLine()) != null) {
  			System.out.println(line);
  			if ( line.trim().startsWith("cgroup") ) { 
  				String[] cgroupsInfo = line.split(" ");
  				if ( cgroupsInfo[1].indexOf("/memory") > -1 ) {
  					// return the mount point minus the memory part
  					cbaseDir = cgroupsInfo[1].substring(0, cgroupsInfo[1].indexOf("/memory") );
  				} else if ( cgroupsInfo[1].indexOf("cpu") > -1){
  					// return the mount point minus the memory part
  					cbaseDir = cgroupsInfo[1].substring(0, cgroupsInfo[1].indexOf("/cpu") );
  				} else {
  					cbaseDir = cgroupsInfo[1].trim();
  				}
  			    break;
  			}
  		}
  		 
  	  } catch( Exception e) {
  		  e.printStackTrace();
  	  } finally {
  		  if ( br != null ) {
  			  try {
  				  br.close();
  			  } catch( Exception ex ) {}
  		  }
  	  }
  	  return cbaseDir;
    }
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {

			String cgBaseDir = fetchCgroupsBaseDir("/proc/mounts");
			
			CGroupsManager cgMgr = new CGroupsManager("/usr/bin",cgBaseDir, "memory",
					null, 10000);
      	  cgMgr.validator(cgBaseDir, "test2", System.getProperty("user.name"),false)
          .cgcreate();

			System.out.println("Cgroups Installed:"
					+ cgMgr.cgroupExists("/cgroup/ducc"));
			Set<String> containers = cgMgr.collectExistingContainers();
			for (String containerId : containers) {
				System.out.println("Existing CGroup Container ID:"
						+ containerId);
			}
			cgMgr.createContainer(args[0], args[2], cgMgr.getUserGroupName(args[2]),true);
			cgMgr.setContainerMaxMemoryLimit(args[0], args[2], true,
					Long.parseLong(args[1]));
			synchronized (cgMgr) {
				cgMgr.wait(60000);
			}
			cgMgr.destroyContainer(args[0], args[2], NodeAgent.SIGKILL);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	public CGroupsManager(String cgroupUtilsDir, String cgroupBaseDir, String cgroupSubsystems,
			DuccLogger agentLogger, long maxTimeToWaitForProcessToStop) {
		this.cgroupUtilsDir = cgroupUtilsDir;
		this.cgroupBaseDir = cgroupBaseDir;
		this.cgroupSubsystems = cgroupSubsystems;
		this.agentLogger = agentLogger;
		this.maxTimeToWaitForProcessToStop = maxTimeToWaitForProcessToStop;
		// determine what cgroup base location should be. For legacy cgconfig
		// it will be <cgroup folder>/ducc
		try {
			// check if the new (standard) cgconfig is active. It should have 
			// the following format <cgroup location>/memory
			File f = new File(cgroupBaseDir+"/memory");
			if ( !f.exists()) {
				// legacy cgconfig is active
				this.cgroupBaseDir += "/"+SYSTEM+"/";
				legacyCgConfig = true;
			} else {
				// new (standard) cgconfig is active
			}
		} catch(Exception e) {
			e.printStackTrace();
			// if there is an error here, the new cgconfig is assumed and subject
			// to additional testing on agent startup.
		}
	}

	/**
	 * Return location where cgroups utils like cgcreate can be found
	 * 
	 * @return - absolute path to cgroup utils
	 */
	public String getCGroupsUtilsDir( ){
		return cgroupUtilsDir;
	}
	/**
	 * Return cgroup base dir for legacy cgconfig or base dir/subsystem 
	 * for the new cgconfig. The old looks like <cgroup folder>/ducc where
	 * the new looks like <cgroup folder>/memory
	 */
	private String getCGroupLocation(String subsystem) {
		if ( legacyCgConfig ) {
			return cgroupBaseDir;
		}
		return cgroupBaseDir + subsystem;
	}
	
	public void configure(NodeAgent agent ) {
		if ( agent != null ) {
			if ( agent.configurationFactory.maxRetryCount != null ) {
				retryMax = Integer.valueOf(agent.configurationFactory.maxRetryCount);
			}
			if ( agent.configurationFactory.retryDelayFactor != null ) {
				delayFactor = Integer.valueOf(agent.configurationFactory.retryDelayFactor);
			}
		}
	}
	public Validator validator( String cgroupsBaseDir,String containerId, String userName, boolean useDuccling) throws Exception {
		return new Validator(this, getCGroupLocation("memory"), containerId, userName, getUserGroupName(userName),useDuccling);
	}
	public String[] getPidsInCgroup(String cgroupName) throws Exception {
		
//		File f = new File(cgroupBaseDir + CGDuccMemoryPath + cgroupName + CGProcsFile);
		File f = new File(getCGroupLocation(CGDuccMemoryPath)+ cgroupName + CGProcsFile);
		//	collect all pids
		return readPids(f);
	}
	private String[] readPids(File f) throws Exception {
		List<String> pids = new ArrayList<String>();
		BufferedReader br = new BufferedReader(new FileReader(f));
		String line;
		while ((line = br.readLine()) != null) {
			pids.add(line.trim());
		}
		br.close();
		return pids.toArray(new String[pids.size()]);
	}
	/**
	 * Finds all stale CGroups and cleans them up. The code only 
	 * cleans up cgroups folders with names that follow
	 * ducc's cgroup naming convention: <id>.<id>.<id>.
	 * First, each cgroup is checked for still running processes in the
	 * cgroup by looking at /<cgroup base dir>/<id>/cgroup.proc file which
	 * includes PIDs of processes associated with the cgroups. If 
	 * processes are found, each one is killed via -9 and the cgroup
	 * is removed.
	 * 
	 * @throws Exception
	 */
	public void cleanupOnStartup() throws Exception {

		Set<NodeProcessInfo> processes = getProcessesOnNode();
		// Match any folder under /cgroup/ducc that has syntax
		// <number>.<number>.<number>
		// This syntax is assigned by ducc to each cgroup
		Pattern p = Pattern.compile("((\\d+)\\.(\\d+)\\.(\\d+))");

		File cgroupsFolder = new File(getCGroupLocation(CGDuccMemoryPath));
		String[] files = cgroupsFolder.list();
		if ( files == null || files.length == 0 ) {
			return;
		}

		for (String cgroupFolder : files) {
			Matcher m = p.matcher(cgroupFolder);
			//	only look at ducc's cgroups
			if (m.find()) {
				try {
					// open proc file which may include PIDs if processes are 
					// still running
					File f = new File(getCGroupLocation(CGDuccMemoryPath) + cgroupFolder+ CGProcsFile);
					//	collect all pids
					String[] pids = readPids(f);

					if ( pids != null && pids.length > 0 ) {
						agentLogger.info("cleanupOnStartup", null,"Agent found "+pids.length+" cgroup proceses still active after Agent restart. Proceeding to remove stale processes");
					}

					int zombieCount=0;
					// kill each runnig process via -9
					if (pids != null && pids.length > 0) {
						for (String pid : pids) {
							// Got cgroup processes still running. Kill them
							for (NodeProcessInfo proc : processes) {
								// Dont kill zombie process as it is already dead. Just increment how many of them we have
								if ( proc.isZombie() ) {
									zombieCount++;
								} else	if (proc.getPid().equals(pid)) {
									// kill process hard via -9
									kill( proc.getUserid(), proc.getPid(), NodeAgent.SIGKILL);
								}
							}
						}
						long logCount = 0;
						// it may take some time for the cgroups to udate accounting. Just cycle until
						// the procs file becomes empty under a given cgroup
						while( true ) {
							pids = readPids(f);
							// if the cgroup contains no pids or there are only zombie processes dont wait 
							// for cgroup accounting. These processes will never terminate. The idea
							// is not to enter into an infinite loop due to zombies
							if ( pids == null || pids.length == 0 || (zombieCount == pids.length)) {
								break;
							} else {
								try {
									synchronized(this) {
										// log every ~30 minutes (10000 * 200), where 200 is a wait time in ms between tries
										if ( logCount  % 10000 == 0) {
											agentLogger.info("cleanupOnStartup", null,
													"--- CGroup:" + cgroupFolder+ " procs file still showing processes running. Wait until CGroups updates acccounting");
										}
										logCount++;
										wait(200);
										
									}
								} catch( InterruptedException ee) {
									break;
								}
							}
						}
					}
					// Don't remove CGroups if there are zombie processes there. Otherwise, attempt
					// to remove the CGroup may hang a thread.
					if ( zombieCount == 0 )  {  // no zombies in the container
	 					destroyContainer(cgroupFolder, SYSTEM, NodeAgent.SIGTERM);
						agentLogger.info("cleanupOnStartup", null,
								"--- Agent Removed Empty CGroup:" + cgroupFolder);
					} else {
						agentLogger.info("cleanupOnStartup", null,"CGroup "+cgroupFolder+" Contains Zombie Processing. Not Removing the Container");
					}
				} catch (Exception e) {
					agentLogger.error("cleanupOnStartup", null, e);
				}
			}
		}
	}

	public boolean isPidInCGroup(String pid) throws Exception {
	  String[] pids = getAllCGroupPids();
	  for( String p : pids ) {
	    if ( p.equals(pid)) {
	      return true;
	    }    
	  }
	  return false;
	}
	
	/**
	 * Returns an array of PIDs managed by cgroups.
	 * 
	 * @return - String array of PIDs
	 * @throws Exception
	 */
  public String[] getAllCGroupPids() throws Exception {

	   List<String> cgroupPids = new ArrayList<String>();
	   
	    // Match any folder under <cgroup base dir> that has syntax
	    // <number>.<number>.<number>
	    // This syntax is assigned by ducc to each cgroup
	    Pattern p = Pattern.compile("((\\d+)\\.(\\d+)\\.(\\d+))");

	    File cgroupsFolder = new File(getCGroupLocation(CGDuccMemoryPath));
	    String[] files = cgroupsFolder.list();
	    if ( files == null || files.length == 0 ) {
	    	return new String[0];  // empty better than NULL
	    }
	    for (String cgroupFolder : files) {
	      Matcher m = p.matcher(cgroupFolder);
	      //  only look at ducc's cgroups
	      if (m.find()) {
	        try {
	          // open proc file which may include PIDs if processes are 
	          // still running
				File f = new File(getCGroupLocation(CGDuccMemoryPath) + cgroupFolder+ CGProcsFile);

	        	//  collect all pids
	          String[] pids = readPids(f);
	          for( String pid : pids ) {
		          cgroupPids.add(pid);
	          }
	        } catch (Exception e) {
	          agentLogger.error("getAllCGroupPids", null, e);
	          throw e;
	        }
	      }
	    }
	    String[] pids = new String[cgroupPids.size()];
	    return cgroupPids.toArray(pids);
	  }

	public void kill(final String user, final String pid, final int signal) {
		final String methodName = "kill";
		String c_launcher_path="";
		try {
			
			boolean useDuccling = false;
			String useSpawn = System.getProperty("ducc.agent.launcher.use.ducc_spawn");
			if (useSpawn != null && useSpawn.toLowerCase().equals("true")) {
				useDuccling = true;
				c_launcher_path = Utils.resolvePlaceholderIfExists(
					System.getProperty("ducc.agent.launcher.ducc_spawn_path"),
					     System.getProperties());
			}

			String cmdLine;
			String arg;
			if (Utils.isWindows()) {
				cmdLine = "taskkill";
				arg = "/PID";
			} else {
				cmdLine = "/bin/kill";
				arg = "-"+signal;
			}
			
			String[] command;
			if (useDuccling) {
			   command = new String[] { c_launcher_path, "-u", user,
			            "--", cmdLine, arg, pid };
			} else {
			   command = new String[] { cmdLine, arg, pid };
			}
		
			launchCommand(command);
			
			StringBuffer sb = new StringBuffer();
			for (String part : command) {
				sb.append(part).append(" ");
			}
			if (agentLogger == null) {
				System.out.println("--------- Killed Process:" + pid
						+ " Owned by:" + user + " Command:" + sb.toString());

			} else {
				agentLogger.info(methodName, null,
						"--------- Killed CGroup Process:" + pid + " Owned by:" + user
								+ " Command:" + sb.toString());
			}
		} catch (Exception e) {
			agentLogger.error(methodName, null,e );
		}
	}
    public String getContainerId(ManagedProcess managedProcess) {
    	String containerId;
		if ( managedProcess.getDuccProcess().getProcessType().equals(ProcessType.Service)) {
			containerId = String.valueOf(managedProcess.getDuccProcess().getCGroup().getId());
		} else {
			containerId = managedProcess.getWorkDuccId().getFriendly()+"."+managedProcess.getDuccProcess().getCGroup().getId();
		}
		return containerId;
    }

	/**
	 * Creates cgroup container with a given id and owner.
	 * 
	 * @param containerId
	 *            - new cgroup container id
	 * @param userId
	 *            - owner of the cgroup container
	 * @param useDuccSpawn
	 *            - use duccling to run 'cgcreate' command
	 * 
	 * @return - true on success, false otherwise
	 * 
	 * @throws Exception
	 */
	public  boolean createContainer(String containerId, String userName, String groupName,
			boolean useDuccSpawn) throws Exception {
		String message = "";
		agentLogger.info("createContainer", null, "Creating CGroup Container:" + containerId);
		String[] command = new String[] { cgroupUtilsDir+"/cgcreate", "-t",
				userName+":"+groupName, "-a", userName+":"+groupName, "-g",
							cgroupSubsystems + ":"+SYSTEM+"/" + containerId };
		int retCode = launchCommand(command);
		// first fetch the location of cgroups on this system. If cgroups is configured
		// with newer cgconfig add 'memory' to the base dir
		if ( cgroupExists(getCGroupLocation(CGDuccMemoryPath) + containerId)) {
			// Starting with libcgroup v.0.38, the cgcreate fails
			// with exit code = 96 even though the cgroup gets
			// created! The following code treats such return code
			// as success. In case there is an error, subsequent
			// cgset or cgexec will fail.
			if (retCode == 0 || retCode == 96) {
				containerIds.add(containerId);
				agentLogger.info("createContainer", null, ">>>>"
						+ "SUCCESS - Created CGroup Container:" + containerId+". The cgcreate return code:"+retCode);
				return true;
			} 
		} else {
			message = ">>> CGroup Container:"+containerId+ " not found in "+getCGroupLocation(CGDuccMemoryPath)+containerId;
		}
		agentLogger.error("createContainer", null, message);
		System.out.println(message);
		return false;
	}

	public long getCpuUsage(String containerId ) throws Exception {
		long usage = 0;
//		String file = getCGroupLocation("cpuacct")+containerId+System.getProperty("file.separator")+"cpuacct.stat";
		String file = getCGroupLocation("cpuacct")+containerId+System.getProperty("file.separator")+"cpuacct.usage";
		agentLogger.trace("getCpuUsage", null, "CPUACCT.USAGE file:"+file);
		File f = new File(file);
		if ( f.exists() ) {
			InputStreamReader isr = new InputStreamReader(new FileInputStream(f));
			BufferedReader br = new BufferedReader(isr);
			String line;
			try {
//				String cpu;
				while ((line = br.readLine()) != null) {
					agentLogger.trace("getCpuUsage", null, "CPUACCT.USAGE Line:"+line);
/*
					// The line read from cpuacct.stat has: NAME VALUE syntax. 
					// Need just the VALUE part
					if ( line.trim().length() > 0 ) {
						cpu = (line.trim().split(" "))[1];  // get the CPU in user mode
						// convert to long and accumulate. Need cpu both in user and system mode
						usage += Long.parseLong(cpu);
					}
					*/
					usage = Long.parseLong(line.trim());
					break;
				}
			} catch ( Exception e) {
				agentLogger.error("getCpuUsage", null, e);
			}
			finally {
				if (isr != null) {
					isr.close();
				}
				agentLogger.trace("getCpuUsage", null, "Done Reading cpuacct.stat file:"+file);

			}
		}
		return usage;
	}
	/**
	 * Sets the max memory use for an existing cgroup container.
	 * 
	 * @param containerId
	 *            - existing container id for which limit will be set
	 * @param userId
	 *            - container owner
	 * @param useDuccSpawn
	 *            - run 'cgset' command as a user
	 * @param containerMaxSize
	 *            - max memory limit
	 * 
	 * @return - true on success, false otherwise
	 * 
	 * @throws Exception
	 */
	
	public boolean setContainerMaxMemoryLimit(String containerId,
			String userId, boolean useDuccSpawn, long containerMaxSize)
			throws Exception {
		try {
			String[] command = new String[] { cgroupUtilsDir+"/cgset", "-r",
					"memory.limit_in_bytes=" + containerMaxSize,
	        		SYSTEM+"/" + containerId };
			int retCode = launchCommand(command);
			if (retCode == 0) {
				agentLogger.info("setContainerMaxMemoryLimit", null, ">>>>"
						+ "SUCCESS - Created CGroup Limit on Container:"
						+ containerId);
				return true;
			} else {
				agentLogger.info("setContainerMaxMemoryLimit", null, ">>>>"
						+ "FAILURE - Unable To Create CGroup Container:"
						+ containerId);
				return false;
			}
		} catch (Exception e) {
			agentLogger.error("setContainerMaxMemoryLimit", null, ">>>>"
					+ "FAILURE - Unable To Set Limit On CGroup Container:"
					+ containerId, e);
			return false;
		}
	}
	/**
	 * Sets the cpu shares use for an existing cgroup container.
	 * 
	 * @param containerId
	 *            - existing container id for which limit will be set
	 * @param userId
	 *            - container owner
	 * @param useDuccSpawn
	 *            - run 'cgset' command as a user
	 * @param containerCpuShares
	 *            - cpu shares
	 * 
	 * @return - true on success, false otherwise
	 * 
	 * @throws Exception
	 */
	
	public boolean setContainerCpuShares(String containerId,
			String userId, boolean useDuccSpawn, long containerCpuShares)
			throws Exception {
		try {
			String[] command = new String[] { cgroupUtilsDir+"/cgset", "-r",
					"cpu.shares=" + containerCpuShares,
        			SYSTEM+"/" + containerId };
			int retCode = launchCommand(command);
			if (retCode == 0) {
				agentLogger.info("setContainerCpuShares", null, ">>>>"
						+ "SUCCESS - Created CGroup with CPU Shares="+containerCpuShares+" on Container:"
						+ containerId);
				return true;
			} else {
				agentLogger.info("setContainerCpuShares", null, ">>>>"
						+ "FAILURE - Unable To Set CPU shares on CGroup Container:"
						+ containerId);
				return false;
			}
		} catch (Exception e) {
			agentLogger.error("setContainerCpuShares", null, ">>>>"
					+ "FAILURE - Unable To Set CPU shares On CGroup Container:"
					+ containerId, e);
			return false;
		}
	}
	
	/**
	 * Sets the memory swappiness for an existing cgroup container.
	 * 
	 * @param containerId
	 *            - existing container id for which limit will be set
	 * @param userId
	 *            - container owner
	 * @param useDuccSpawn
	 *            - run 'cgset' command as a user
	 * @param swappiness
	 *            - swappiness
	 * 
	 * @return - true on success, false otherwise
	 * 
	 * @throws Exception
	 */
	
	public boolean setContainerSwappiness(String containerId,
			String userId, boolean useDuccSpawn, long swappiness)
			throws Exception {
		try {
			String[] command = new String[] { cgroupUtilsDir+"/cgset", "-r",
					"memory.swappiness=" + swappiness,
					SYSTEM + "/" + containerId };
			int retCode = launchCommand(command);
			if (retCode == 0) {
				agentLogger.info("setContainerSwappiness", null, ">>>>"
						+ "SUCCESS - Updated CGroup with Memory Swappiness="+swappiness+" on Container:"
						+ containerId);
				return true;
			} else {
				agentLogger.info("setContainerSwappiness", null, ">>>>"
						+ "FAILURE - Unable To Set Swappiness on CGroup Container:"
						+ containerId);
				return false;
			}
		} catch (Exception e) {
			agentLogger.error("setContainerSwappiness", null, ">>>>"
					+ "FAILURE - Unable To Set Swappiness On CGroup Container:"
					+ containerId, e);
			return false;
		}
	}
	private int killChildProcesses(String containerId, String userId, int signal) throws Exception {
		int childCount=0;
		String[] pids = getPidsInCgroup(containerId);
		if ( pids != null ) {
			if ( pids.length > 0 ) {
				childCount = pids.length;
				agentLogger.info("killChildProcesses", null,"Found "+pids.length+" child processes still in container:"+containerId+" - killing all"); 
			}
			for( String pid : pids ) {
				try {
				   kill(userId, pid, signal);
				} catch(Exception ee) {
					agentLogger.warn("killChildProcesses", null, "Unable to kill child process with PID:"+pid+" from cgroup:"+containerId+"\n"+ee);
				}
			}
		}
		return childCount;
	}
	/**
	 * Removes cgroup container with a given id. Cgroups are implemented as a
	 * virtual file system. All is needed here is just rmdir.
	 * 
	 * @param containerId
	 *            - cgroup to remove
	 * @return - true on success, false otherwise
	 * 
	 * @throws Exception
	 */
	public boolean destroyContainer(String containerId, String userId, int signal) throws Exception {
		try {
			if (cgroupExists(getCGroupLocation(CGDuccMemoryPath) + containerId)) {
				if ( signal == NodeAgent.SIGTERM ) {
					agentLogger.info("destroyContainer", null, "Destroying Container "+containerId+" Using signal:"+signal +" to kill child processes if any still exist in cgroups container");

					// before removing cgroup container, make sure to kill 
					// all processes that still may be there. User process
					// may have created child processes that may still be running.
					// First use kill -15, than wait and any process still standing
					// will be killed hard via kill -9
					int childProcessCount = 
							killChildProcesses(containerId, userId, NodeAgent.SIGTERM);
					if ( childProcessCount > 0 ) {
						agentLogger.info("destroyContainer", null, "Killed "+childProcessCount+"Child Processes with kill -15");
						try {
							this.wait(maxTimeToWaitForProcessToStop);
						} catch( InterruptedException ie) {
						}
					}
				}
				// Any process remaining in a cgroup will be killed hard
				killChildProcesses(containerId, userId, NodeAgent.SIGKILL);
				String[] command = new String[] { cgroupUtilsDir+"/cgdelete",cgroupSubsystems + ":"+SYSTEM+"/" + containerId };
				int retCode = launchCommand(command);
				if ( cgroupExists(getCGroupLocation(CGDuccMemoryPath) + containerId)) {
					agentLogger.info("destroyContainer", null, "Failed to remove Container "+containerId+" Using cgdelete command. Exit code:"+retCode);
					return false;
				} else {
					containerIds.remove(containerId);
					return true;
				}
//				if (retCode == 0) {
//					containerIds.remove(containerId);
//					return true;
//				} else {
//					return false;
//				}
			}
			return true; // nothing to do, cgroup does not exist
		} catch (Exception e) {
			agentLogger.info("destroyContainer", null, e);
			return false;
		}
	}
    public String getUserGroupName(String userName) throws Exception {
    	String groupName="";
    	InputStreamReader isr = null;
    	BufferedReader reader = null;
    	try {
    		String cmd[] = {"/usr/bin/id","-g","-n",userName};;//System.getProperty("user.name")};
		    StringBuffer sb = new StringBuffer();
		    for (String s : cmd) {
			   sb.append(s).append(" ");
			}
			agentLogger.info("getuserGroupName", null, "Launching Process - Commandline:"+sb.toString());

    		ProcessBuilder processLauncher = new ProcessBuilder();
			processLauncher.command(cmd);
			processLauncher.redirectErrorStream(true);
			java.lang.Process process = processLauncher.start();
			isr = new InputStreamReader(process.getInputStream());
			reader = new BufferedReader(isr);
			String line;
			agentLogger.info("getUserGroupName", null, "Consuming Process Streams");
			while ((line = reader.readLine()) != null) {
				agentLogger.info("getUserGroupName", null, ">>>>" + line);
				System.out.println(line);
				groupName = line.trim();
			}
			agentLogger.info("getUserGroupName", null, "Waiting for Process to Exit");
			int retCode = process.waitFor();
			agentLogger.info("getUserGroupName", null, "Pocess Exit Code="+retCode);
    	    		
    	} catch( Exception e) {
			agentLogger.error("getUserGroupName", null, e);
    	
    	} finally {
    		if ( reader != null ) {
    			reader.close();
    		}
    	}
    	return groupName;
    }
	private int launchCommand(String[] command/*,	String userId*/) throws Exception {
		
		int retryCount=0;
		Object sleepMonitor = new Object();
		if ( command == null) {
			return -1;
		}
		synchronized(CGroupsManager.class) {
			long delay = delayFactor;//
			while( retryCount <= retryMax ) {
				String message = "";
				InputStreamReader in = null;
				BufferedReader reader = null;
				 StringBuffer sb = new StringBuffer();
				if ( command != null ) {
				    for (int i = 0; i < command.length; i++) {
				    	sb.append(command[i]).append(" ");
				    }
				}
				
				try {
					agentLogger.info("launchCommand", null, "Launching Process - Commandline:"+sb.toString());
					ProcessBuilder processLauncher = new ProcessBuilder();
					
					processLauncher.command(command);
					processLauncher.redirectErrorStream(true);
					java.lang.Process process = processLauncher.start();

					in = new InputStreamReader(
							process.getInputStream());
					reader = new BufferedReader(in);
					String line;
					agentLogger.info("launchCommand", null, "Consuming Process Streams");
					while ((line = reader.readLine()) != null) {
						// per team discussin 6/23/ dont need to log "Operation not permitted"
						// which is logged by cgcreate erroneously. The cgroup is actually created
						// but cgcreate still dumps this msg to stdout. If we log this, a user
						// may get confused. If thercannot remove groupe is a legitimate problem a subsequent test
						// for existence of cgroup will catch a missing cgroup and report it as 
						// error.
						if ( line.indexOf("Operation not permitted") > -1 ) {
							continue;  // dont log if the above string is in the stdout stream
						} else if ( line.indexOf("cannot remove group") > -1 ) {
							continue;   // could be false positive. Validation will catch if unable to remove
						}
						agentLogger.info("launchCommand", null, ">>>>" + line);
						System.out.println(line);
					}
					agentLogger.info("launchCommand", null, "Waiting for Process to Exit");
					int retCode = process.waitFor();
					
					// Starting with libcgroup v.0.38, the cgcreate fails
					// with exit code = 96 even though the cgroup gets
					// created! The following code treats such return code
					// as success. In case there is an error, subsequent
					// cgset or cgexec will fail.
					if (retCode == 0 || retCode == 96) {
						System.out.println("--------- Returning Code:"+retCode+" Command:"+sb.toString());

						return retCode;
					} else {
						message = ">>>>"
								+ "FAILURE - return code:"+retCode+" Unable To exec command:"+sb.toString()
								+ " Retrying in "+delay+" millis - retry#"+(retryCount+1);
					}

				} catch (Exception e) {
					e.printStackTrace();
					message =  ">>>>"
							+ "FAILURE - Unable To exec command:"+sb.toString()
						    +" Retrying in "+delay+" millis - retry#"+(retryCount+1);
				} finally {
					if ( reader != null ) {
						try {
							reader.close();
						} catch( Exception exx) {}
					}
				}
				if ( retryMax == 0 ) {
					agentLogger.error("launchCommand", null, ">>>>"
							+ "Not configured to retry command:"+sb.toString());
					break; 
				}
				agentLogger.error("launchCommand", null, message);
				System.out.println(message);
			    try {
		           synchronized(sleepMonitor) {
				      sleepMonitor.wait(delay);
	  	           } 
		        } catch( InterruptedException ie) {}

				retryCount++;
				delay += delayFactor;
			}  // while
			
			
		}
		return -1; // failure
	}

	/**
	 * Return a Set of existing cgroup Ids found in the filesystem identified by
	 * 'cgroupBaseDir'.
	 * 
	 * @return - set of cgroup ids
	 * 
	 * @throws Exception
	 */
	public Set<String> collectExistingContainers() throws Exception {
//		File duccCGroupBaseDir = new File(cgroupBaseDir);
		File duccCGroupBaseDir = new File(getCGroupLocation(CGDuccMemoryPath));
		if (duccCGroupBaseDir.exists()) {
			File[] existingCGroups = duccCGroupBaseDir.listFiles();
			if ( existingCGroups != null ) {
				for (File cgroup : existingCGroups) {
					if (cgroup.isDirectory()) {
						containerIds.add(cgroup.getName());
					}
				}
			}
		}
		return containerIds;
	}

	public String getDuccCGroupBaseDir() {
		return cgroupBaseDir;
	}

	public String getSubsystems() {
		return cgroupSubsystems;
	}

	public boolean cgroupExists(String cgroup) throws Exception {
		File duccCGroupBaseDir = new File(cgroup);
		return duccCGroupBaseDir.exists();
	}

	public Set<NodeProcessInfo> getProcessesOnNode() throws Exception {
		String location = "getProcessesOnNode";
		Set<NodeProcessInfo> processList = new HashSet<NodeProcessInfo>();
		InputStream stream = null;
		BufferedReader reader = null;
		try {

			ProcessBuilder pb = new ProcessBuilder("ps", "-Ao",
					"user:12,pid,ppid,args,stat", "--no-heading");
			pb.redirectErrorStream(true);
			java.lang.Process proc = pb.start();
			// spawn ps command and scrape the output
			stream = proc.getInputStream();
			reader = new BufferedReader(new InputStreamReader(
					stream));
			String line;
			String regex = "\\s+";

			// read the next line from ps output
			while ((line = reader.readLine()) != null) {

				String tokens[] = line.split(regex);
				String user = tokens[0];
				String pid = tokens[1];
				String ppid = tokens[2];
                String stat = tokens[4];
                
				if (tokens.length > 0) {

					processList.add(new NodeProcessInfo(pid, ppid, user, stat));
				}
			}
		} catch (Exception e) {
			if (agentLogger == null) {
				e.printStackTrace();
			} else {
				agentLogger.error(location, null, e);
			}
		} finally {
			if ( reader != null ) {
				reader.close();
			}
		}
		return processList;

	}

	public class NodeProcessInfo {
		private String pid;
		private String ppid;
		private String userid;
        private String stat;
        
		NodeProcessInfo(String pid, String ppid, String uid, String stat) {
			this.pid = pid;
			this.ppid = ppid;
			this.userid = uid;
			this.stat = stat;
		}

		public boolean isZombie() {
			return (stat == "Z") ? true : false;
		}
		public String getPid() {
			return pid;
		}

		public String getPpid() {
			return ppid;
		}

		public String getUserid() {
			return userid;
		}

		public void setUserid(String userid) {
			this.userid = userid;
		}

	}
	public class CGroupsException extends RuntimeException {
		private static final long serialVersionUID = 1L;
        private String command;
        private String msg;
        
        public CGroupsException() {
        }
        public CGroupsException(Exception e) {
        	super(e);
        }
        public CGroupsException addCommand(String command) {
        	this.command = command;
        	return this;
        }
        public CGroupsException addMessage(String msg) {
        	this.msg = msg;
        	return this;
        }
        public String getCommand() {
        	return command;
        }
        public String getMessage() {
        	return msg;
        }
		
	}
 	public class Validator {
		private CGroupsManager cgmgr=null;
		String containerId;
		String userName;
		String userGroupName;
		boolean useDuccling;
		String cgroupsBaseDir;
	    
		
		
		Validator(CGroupsManager instance, String cgroupsBaseDir,String containerId, String uid, String usergroup, boolean useDuccling) {
			cgmgr = instance;
			this.containerId = containerId;
			this.userName= uid;
			this.useDuccling = useDuccling;
			this.userGroupName = usergroup;
			this.cgroupsBaseDir = cgroupsBaseDir;
		}
		public Validator cgcreate() throws CGroupsException {
			String msg1 = "------- CGroups cgcreate failed to create a cgroup - disabling cgroups";
			String msg2 = "------- CGroups cgcreate failed to validate a cgroup - disabling cgroups";
			String msg3 = "------- CGroups cgcreate failed - disabling cgroups";
			try {
				
				if ( !cgmgr.createContainer(containerId, userName, userGroupName, useDuccling) ) {
					throw new CGroupsException().addCommand(CGroupCommand.CGCREATE.cmd())
							                     .addMessage(msg1);
				}
//				if (!cgmgr.cgroupExists(cgroupsBaseDir + "/memory/ducc/" + containerId)) {
				if (!cgmgr.cgroupExists(getCGroupLocation(CGDuccMemoryPath)+ containerId)) {
					throw new CGroupsException().addCommand(CGroupCommand.CGCREATE.cmd())
	                .addMessage(msg2);
				}
			} catch( Exception e) {
				throw new CGroupsException(e).addCommand(CGroupCommand.CGCREATE.cmd())
                .addMessage(msg3);
			}
			return this;
		}
		public Validator cgset(  long cpuShares) throws CGroupsException {
			String msg1 = "------- Check cgconfig.conf CPU control. The cgset failed to set cpu.shares";
			String msg2 = "------- Check cgconfig.conf CPU control. The cgset failed to find cpu.shares file";
			String msg3 = "------- Check cgconfig.conf CPU control. The cgset failed to write to cpu.shares file. Expected 100 shares found ";
			
		    BufferedReader reader = null;
			String shares = "";
			try {
				if (!cgmgr.setContainerCpuShares(containerId, userName, useDuccling, cpuShares) ) {
					throw new CGroupsException().addCommand(CGroupCommand.CGSET.cmd())
	                .addMessage(msg1);
				}
	  		    // now try to read created file 
//	  		    File f = new File(cgroupsBaseDir + "/cpu/ducc/" + "test/cpu.shares");
	  		    File f = new File(getCGroupLocation(CGDuccCpuPath)+ containerId+"/cpu.shares");
 			    reader = new BufferedReader(new FileReader(f));
				// read 1st line. It should be equal to cpuShares
 			    if ( reader != null  ) {
 			    	shares = reader.readLine();
 			    	if ( shares != null ) {
 			    		shares = shares.trim();
 			    	}
 			    }
    			System.out.println("----- Cgroup cgset verifier - cpu.shares read from file:"+shares);
    			if ( !String.valueOf(cpuShares).equals(shares)) {
    					throw new CGroupsException().addCommand(CGroupCommand.CGSET.cmd())
    	                .addMessage(msg3+shares);
    			} 
			} catch( FileNotFoundException e ) {
				//e.printStackTrace();
				throw new CGroupsException(e).addCommand(CGroupCommand.CGSET.cmd())
                    .addMessage(msg2);
			} catch(Exception e) {
				//e.printStackTrace();
					throw new CGroupsException(e).addCommand(CGroupCommand.CGSET.cmd())
	                .addMessage(msg3+shares);
				  
			} finally {
				  if ( reader != null ) {
					  try {
						  reader.close();
					  } catch( Exception ee) {}
				  }
			}
			return this;
		}
	}
}
