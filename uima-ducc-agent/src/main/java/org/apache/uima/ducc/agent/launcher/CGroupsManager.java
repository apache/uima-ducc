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

import org.apache.uima.ducc.agent.launcher.ManagedProcess;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.Utils;
import org.apache.uima.ducc.transport.event.common.IDuccProcessType.ProcessType;

/**
 * Manages cgroup container on a node
 * 
 * Supported operations: - cgcreate - creates cgroup container - cgset - sets
 * max memory limit for an existing container
 * 
 * 
 */
public class CGroupsManager {
	private DuccLogger agentLogger = null;

	private Set<String> containerIds = new LinkedHashSet<String>();
	private String cgroupBaseDir = "";
	private String cgroupUtilsDir=null;
	private String cgroupSubsystems = ""; // comma separated list of subsystems
											// eg. memory,cpu

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {

			CGroupsManager cgMgr = new CGroupsManager("/usr/bin","/cgroup/ducc", "memory",
					null);
			System.out.println("Cgroups Installed:"
					+ cgMgr.cgroupExists("/cgroup/ducc"));
			Set<String> containers = cgMgr.collectExistingContainers();
			for (String containerId : containers) {
				System.out.println("Existing CGroup Container ID:"
						+ containerId);
			}
			cgMgr.createContainer(args[0], args[2], true);
			cgMgr.setContainerMaxMemoryLimit(args[0], args[2], true,
					Long.parseLong(args[1]));
			synchronized (cgMgr) {
				cgMgr.wait(60000);
			}
			cgMgr.destroyContainer(args[0], args[2]);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	public String getCGroupsUtilsDir( ){
		return cgroupUtilsDir;
	}
	public CGroupsManager(String cgroupUtilsDir, String cgroupBaseDir, String cgroupSubsystems,
			DuccLogger agentLogger) {
		this.cgroupUtilsDir = cgroupUtilsDir;
		this.cgroupBaseDir = cgroupBaseDir;
		this.cgroupSubsystems = cgroupSubsystems;
		this.agentLogger = agentLogger;
	}
	public String[] getPidsInCgroup(String cgroupName) throws Exception {
		File f = new File(cgroupBaseDir + "/" + cgroupName + "/cgroup.procs");
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
	 * Finds all stale CGroups in /cgroup/ducc folder and cleans them
	 * up. The code only cleans up cgroups folders with names that follow
	 * ducc's cgroup naming convention: <id>.<id>.<id>.
	 * First, each cgroup is checked for still running processes in the
	 * cgroup by looking at /cgroup/ducc/<id>/cgroup.proc file which
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

		File cgroupsFolder = new File(cgroupBaseDir);
		String[] files = cgroupsFolder.list();
		
		for (String cgroupFolder : files) {
			Matcher m = p.matcher(cgroupFolder);
			//	only look at ducc's cgroups
			if (m.find()) {
				try {
					// open proc file which may include PIDs if processes are 
					// still running
					File f = new File(cgroupBaseDir + "/" + cgroupFolder
							+ "/cgroup.procs");
					//	collect all pids
					String[] pids = readPids(f);
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
									kill( proc.getUserid(), proc.getPid());
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
	 					destroyContainer(cgroupFolder, "ducc");
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
	   
	    // Match any folder under /cgroup/ducc that has syntax
	    // <number>.<number>.<number>
	    // This syntax is assigned by ducc to each cgroup
	    Pattern p = Pattern.compile("((\\d+)\\.(\\d+)\\.(\\d+))");

	    File cgroupsFolder = new File(cgroupBaseDir);
	    String[] files = cgroupsFolder.list();
	    
	    for (String cgroupFolder : files) {
	      Matcher m = p.matcher(cgroupFolder);
	      //  only look at ducc's cgroups
	      if (m.find()) {
	        try {
	          // open proc file which may include PIDs if processes are 
	          // still running
	          File f = new File(cgroupBaseDir + "/" + cgroupFolder
	              + "/cgroup.procs");
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

	public void kill(final String user, final String pid) {
		final String methodName = "kill";
		InputStream is = null;
		BufferedReader reader = null;
		try {
			String c_launcher_path = Utils.resolvePlaceholderIfExists(
					System.getProperty("ducc.agent.launcher.ducc_spawn_path"),
					System.getProperties());
			String cmdLine;
			String arg;
			boolean useDuccling = false;
			if (Utils.isWindows()) {
				cmdLine = "taskkill";
				arg = "/PID";
			} else {
				String useSpawn = System
						.getProperty("ducc.agent.launcher.use.ducc_spawn");
				if (useSpawn != null && useSpawn.toLowerCase().equals("true")) {
					useDuccling = true;
				}
				cmdLine = "/bin/kill";
				arg = "-9";
			}
			String[] duccling_nolog;
			if (useDuccling) {
				duccling_nolog = new String[] { c_launcher_path, "-u", user,
						"--", cmdLine, arg, pid };
			} else {
				duccling_nolog = new String[] { cmdLine, arg, pid };
			}

			// if (kill != null && Boolean.parseBoolean(kill) == true) {
			ProcessBuilder pb = new ProcessBuilder(duccling_nolog);
			pb.redirectErrorStream(true);
			java.lang.Process killedProcess = pb.start();
			is = killedProcess.getInputStream();
			reader = new BufferedReader(
					new InputStreamReader(is));
			// String line = null;
			// read the next line from kill command
			while (reader.readLine() != null) {
				// dont care about the output, just drain the buffers
			}
			is.close();
			StringBuffer sb = new StringBuffer();
			for (String part : duccling_nolog) {
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
		} finally {
			if ( reader != null ) {
				try {
					reader.close();
				} catch( Exception e) {}
			}
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
	public boolean createContainer(String containerId, String userId,
			boolean useDuccSpawn) throws Exception {

		try {
			agentLogger.info("createContainer", null, "Creating CGroup Container:" + containerId);
			
			String[] command = new String[] { cgroupUtilsDir+"/cgcreate", "-t",
					"ducc", "-a", "ducc", "-g",
					cgroupSubsystems + ":ducc/" + containerId };
			int retCode = launchCommand(command, useDuccSpawn, "ducc",
					containerId);
			// Starting with libcgroup v.0.38, the cgcreate fails
			// with exit code = 96 even though the cgroup gets
			// created! The following code treats such return code
			// as success. In case there is an error, subsequent
			// cgset or cgexec will fail.
			if (retCode == 0 || retCode == 96) {
				containerIds.add(containerId);
				agentLogger.info("createContainer", null, ">>>>"
						+ "SUCCESS - Created CGroup Container:" + containerId);

				return true;
			} else {
				agentLogger.info("createContainer", null, ">>>>"
						+ "FAILURE - Unable To Create CGroup Container:"
						+ containerId);

				return false;
			}
		} catch (Exception e) {
			agentLogger.error("createContainer", null, ">>>>"
					+ "FAILURE - Unable To Create CGroup Container:"
					+ containerId, e);

			return false;
		}
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
			///usr/bin
			String[] command = new String[] { cgroupUtilsDir+"/cgset", "-r",
					"memory.limit_in_bytes=" + containerMaxSize,
					"ducc/" + containerId };
			int retCode = launchCommand(command, useDuccSpawn, "ducc",
					containerId);
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
			///usr/bin
			String[] command = new String[] { cgroupUtilsDir+"/cgset", "-r",
					"cpu.shares=" + containerCpuShares,
					"ducc/" + containerId };
			int retCode = launchCommand(command, useDuccSpawn, "ducc",
					containerId);
			if (retCode == 0) {
				agentLogger.info("setContainerCpuShares", null, ">>>>"
						+ "SUCCESS - Created CGroup with CPU Shares="+containerCpuShares+" on Container:"
						+ containerId);
				return true;
			} else {
				agentLogger.info("setContainerCpuShares", null, ">>>>"
						+ "FAILURE - Unable To Create CGroup Container:"
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
	 * Removes cgroup container with a given id. Cgroups are implemented as a
	 * virtual file system. All is needed here is just rmdir.
	 * 
	 * @param containerId
	 *            - cgroup to remove
	 * @return - true on success, false otherwise
	 * 
	 * @throws Exception
	 */
	public boolean destroyContainer(String containerId, String userId) throws Exception {
		try {
			if (cgroupExists(cgroupBaseDir + "/" + containerId)) {
				// before removing cgroup container, make sure to kill 
				// all processes that still may be there. User process
				// may have created child processes that may still be running.
				String[] pids = getPidsInCgroup(containerId);
				if ( pids != null ) {
					if ( pids.length > 0 ) {
						agentLogger.info("destroyContainer", null,"Found "+pids.length+" child processes still in container:"+containerId+" - killing all"); 
					}
					for( String pid : pids ) {
						try {
						   kill(userId, pid);
						} catch(Exception ee) {
							agentLogger.warn("destroyContainer", null, "Unable to kill child process with PID:"+pid+" from cgroup:"+containerId+"\n"+ee);
						}
					}
				}
				String[] command = new String[] { "/bin/rmdir",
						cgroupBaseDir + "/" + containerId };
				int retCode = launchCommand(command, false, "ducc", containerId);
				if (retCode == 0) {
					containerIds.remove(containerId);
					return true;
				} else {
					return false;
				}
			}
			return true; // nothing to do, cgroup does not exist
		} catch (Exception e) {
			return false;
		}
	}

	private int launchCommand(String[] command, boolean useDuccSpawn,
			String userId, String containerId) throws Exception {
		String[] commandLine = null;
		InputStreamReader in = null;
		BufferedReader reader = null;
		try {
			//
			// Use ducc_ling (c code) as a launcher for the actual process. The
			// ducc_ling
			// allows the process to run as a specified user in order to write
			// out logs in
			// user's space as oppose to ducc space.
			String c_launcher_path = Utils.resolvePlaceholderIfExists(
					System.getProperty("ducc.agent.launcher.ducc_spawn_path"),
					System.getProperties());
			StringBuffer sb = new StringBuffer();
			
			if (useDuccSpawn && c_launcher_path != null) {
				commandLine = new String[4 + command.length];
				commandLine[0] = c_launcher_path;
				sb.append(c_launcher_path).append(" ");
				commandLine[1] = "-u";
				sb.append("-u ");
				commandLine[2] = userId;
				sb.append(userId);
				commandLine[3] = "--";
				sb.append(" -- ");
				int j = 0;
				for (int i = 4; i < commandLine.length; i++) {
					sb.append(command[j]).append(" ");
					commandLine[i] = command[j++];
				}
			} else {
				commandLine = command;
				if ( command != null ) {
   				    for (int i = 0; i < command.length; i++) {
					    sb.append(command[i]).append(" ");
				    }
				}
			}
			agentLogger.info("launchCommand", null, "Launching Process - Commandline:"+sb.toString());
			
			ProcessBuilder processLauncher = new ProcessBuilder();
			processLauncher.command(commandLine);
			processLauncher.redirectErrorStream();

			java.lang.Process process = processLauncher.start();

			in = new InputStreamReader(
					process.getInputStream());
			reader = new BufferedReader(in);
			String line;
			agentLogger.info("launchCommand", null, "Consuming Process Streams");
			while ((line = reader.readLine()) != null) {
				agentLogger.info("launchCommand", null, ">>>>" + line);
			}
			agentLogger.info("launchCommand", null, "Waiting for Process to Exit");

			int retCode = process.waitFor();
			return retCode;

		} catch (Exception e) {
			StringBuffer sb = new StringBuffer();
			if (commandLine != null) {
				for (String cmdPart : commandLine) {
					sb.append(cmdPart).append(" ");
				}
			}
			if (agentLogger != null) {
				agentLogger.error("launchCommand", null,
						"Unable to Launch Command:" + sb.toString(), e);
			} else {
				System.out
						.println("CGroupsManager.launchCommand()- Unable to Launch Command:"
								+ sb.toString());
				e.printStackTrace();
			}

		} finally {
			if ( reader != null ) {
				try {
					reader.close();
				} catch( Exception exx) {}
			}
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
		File duccCGroupBaseDir = new File(cgroupBaseDir);
		if (duccCGroupBaseDir.exists()) {
			File[] existingCGroups = duccCGroupBaseDir.listFiles();
			for (File cgroup : existingCGroups) {
				if (cgroup.isDirectory()) {
					containerIds.add(cgroup.getName());
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
}
