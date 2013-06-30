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

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.Utils;

import scala.actors.threadpool.Arrays;

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
	private String cgroupSubsystems = ""; // comma separated list of subsystems
											// eg. memory,cpu

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {

			CGroupsManager cgMgr = new CGroupsManager("/cgroup/ducc", "memory",
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
			cgMgr.destroyContainer(args[0]);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public CGroupsManager(String cgroupBaseDir, String cgroupSubsystems,
			DuccLogger agentLogger) {
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
					// kill each runnig process via -9
					if (pids != null && pids.length > 0) {
						for (String pid : pids) {
							// Got cgroup processes still running. Kill them
							for (NodeProcessInfo proc : processes) {
								if (proc.getPid().equals(pid)) {
									
									kill( proc.getUserid(), proc.getPid());
								}
							}
						}
						// it may take some time for the cgroups to udate accounting. Just cycle until
						// the procs file becomes empty under a given cgroup
						while( true ) {
							pids = readPids(f);
							if ( pids == null || pids.length == 0) {
								break;
							} else {
								try {
									synchronized(this) {
										agentLogger.info("cleanupOnStartup", null,
												"--- CGroup:" + cgroupFolder+ " procs file still showing processes running. Wait until CGroups updates acccounting");
										wait(200);
										
									}
								} catch( InterruptedException ee) {}
							}
						}
					}
					
					destroyContainer(cgroupFolder);
					agentLogger.info("cleanupOnStartup", null,
							"--- Agent Removed Empty CGroup:" + cgroupFolder);
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
	 @SuppressWarnings("unchecked")
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
	          cgroupPids.addAll(Arrays.asList(pids));
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
			InputStream is = killedProcess.getInputStream();
			BufferedReader reader = new BufferedReader(
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
		}
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
			String[] command = new String[] { "/usr/bin/cgcreate", "-t",
					"ducc", "-a", "ducc", "-g",
					cgroupSubsystems + ":ducc/" + containerId };
			int retCode = launchCommand(command, useDuccSpawn, "ducc",
					containerId);
			if (retCode == 0) {
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
			String[] command = new String[] { "/usr/bin/cgset", "-r",
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
	 * Removes cgroup container with a given id. Cgroups are implemented as a
	 * virtual file system. All is needed here is just rmdir.
	 * 
	 * @param containerId
	 *            - cgroup to remove
	 * @return - true on success, false otherwise
	 * 
	 * @throws Exception
	 */
	public boolean destroyContainer(String containerId) throws Exception {
		try {
			if (cgroupExists(cgroupBaseDir + "/" + containerId)) {
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

			if (useDuccSpawn && c_launcher_path != null) {
				commandLine = new String[4 + command.length];
				commandLine[0] = c_launcher_path;
				commandLine[1] = "-u";
				commandLine[2] = userId;
				commandLine[3] = "--";

				int j = 0;
				for (int i = 4; i < commandLine.length; i++) {
					commandLine[i] = command[j++];
				}
			} else {
				commandLine = command;
			}
			ProcessBuilder processLauncher = new ProcessBuilder();
			processLauncher.command(commandLine);
			processLauncher.redirectErrorStream();

			java.lang.Process process = processLauncher.start();

			InputStreamReader in = new InputStreamReader(
					process.getInputStream());
			BufferedReader reader = new BufferedReader(in);
			String line;
			while ((line = reader.readLine()) != null) {
				agentLogger.info("launchCommand", null, ">>>>" + line);
			}
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
		try {

			ProcessBuilder pb = new ProcessBuilder("ps", "-Ao",
					"user:12,pid,ppid,args", "--no-heading");
			pb.redirectErrorStream(true);
			java.lang.Process proc = pb.start();
			// spawn ps command and scrape the output
			InputStream stream = proc.getInputStream();
			BufferedReader reader = new BufferedReader(new InputStreamReader(
					stream));
			String line;
			String regex = "\\s+";

			// read the next line from ps output
			while ((line = reader.readLine()) != null) {

				String tokens[] = line.split(regex);
				String user = tokens[0];
				String pid = tokens[1];
				String ppid = tokens[2];

				if (tokens.length > 0) {

					processList.add(new NodeProcessInfo(pid, ppid, user));
				}
			}
		} catch (Exception e) {
			if (agentLogger == null) {
				e.printStackTrace();
			} else {
				agentLogger.error(location, null, e);
			}
		}
		return processList;

	}

	public class NodeProcessInfo {
		private String pid;
		private String ppid;
		private String userid;

		NodeProcessInfo(String pid, String ppid, String uid) {
			this.pid = pid;
			this.ppid = ppid;
			userid = uid;
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
