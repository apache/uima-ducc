package org.apache.uima.ducc.agent.launcher;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.Utils;

/**
 * Manages cgroup container on a node
 * 
 * Supported operations:
 *   - cgcreate - creates cgroup container
 *   - cgset - sets max memory limit for an existing container
 *   
 *
 */
public class CGroupsManager {
	private DuccLogger agentLogger = null;
	
	private Set<String> containerIds = new LinkedHashSet<String>();
	private String cgroupBaseDir = "";
	private String cgroupSubsystems = "";  // comma separated list of subsystems eg. memory,cpu
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			
			CGroupsManager cgMgr = new CGroupsManager("/cgroup/ducc", "memory", null);
			System.out.println("Cgroups Installed:"+cgMgr.cgroupExists("/cgroup/ducc"));
			Set<String> containers = cgMgr.collectExistingContainers();
			for ( String containerId : containers ) {
				System.out.println("Existing CGroup Container ID:"+containerId);
			}
			cgMgr.createContainer(args[0], args[2], true);
			cgMgr.setContainerMaxMemoryLimit(args[0], args[2], true, Long.parseLong(args[1]));
		    synchronized( cgMgr ) {
		    	cgMgr.wait(60000);
		    }
		    cgMgr.destroyContainer(args[0]);
		    
		} catch( Exception e) {
			e.printStackTrace();
		}
	}
	
	public CGroupsManager(String cgroupBaseDir, String cgroupSubsystems, DuccLogger agentLogger ) {
		this.cgroupBaseDir = cgroupBaseDir;
		this.cgroupSubsystems = cgroupSubsystems;
		this.agentLogger = agentLogger;
	}
    /**
     * Creates cgroup container with a given id and owner.
     * 
     * @param containerId - new cgroup container id
     * @param userId - owner of the cgroup container
     * @param useDuccSpawn - use duccling to run 'cgcreate' command
     * 
     * @return - true on success, false otherwise
     * 
     * @throws Exception
     */
	public boolean createContainer(String containerId, String userId, boolean useDuccSpawn ) throws Exception {
		
		try {
			String [] command = new String[] {"/usr/bin/cgcreate","-g", cgroupSubsystems+":ducc/"+containerId};
			int retCode = launchCommand(command, useDuccSpawn, userId, containerId);
			if ( retCode == 0 ) {
				containerIds.add(containerId);
				return true;
			} else {
				return false;
			}
		} catch ( Exception e ) {
			return false;
		}
	}
	/**
	 * Sets the max memory use for an existing cgroup container. 
	 * 
	 * @param containerId - existing container id for which limit will be set
	 * @param userId - container owner
	 * @param useDuccSpawn - run 'cgset' command as a user
	 * @param containerMaxSize - max memory limit 
	 * 
	 * @return - true on success, false otherwise
	 * 
	 * @throws Exception
	 */
	public boolean setContainerMaxMemoryLimit( String containerId, String userId, boolean useDuccSpawn, long containerMaxSize) throws Exception {
		try {
			String [] command = new String[] {"/usr/bin/cgset","-r", "memory.limit_in_bytes="+containerMaxSize, "ducc/"+containerId};
			int retCode = launchCommand(command, useDuccSpawn, userId, containerId);
			return retCode == 0 ? true : false;
		} catch ( Exception e ) {
			return false;
		}
	}
		
	/**
	 * Removes cgroup container with a given id. Cgroups are implemented as
	 * a virtual file system. All is needed here is just rmdir. 
	 * 
	 * @param containerId - cgroup to remove
	 * @return - true on success, false otherwise
	 * 
	 * @throws Exception
	 */
	public boolean destroyContainer(String containerId) throws Exception {
		try {
			if ( cgroupExists(cgroupBaseDir+"/"+containerId)) {
				String [] command = new String[] {"/bin/rmdir", cgroupBaseDir+"/"+containerId};
				int retCode = launchCommand(command, false, "ducc", containerId);
				if ( retCode == 0 ) {
					containerIds.remove(containerId);
					return true;
				} else {
					return false;
				}
			}
			return true; // nothing to do, cgroup does not exist
		} catch ( Exception e ) {
			return false;
		}
	}
	
	private int launchCommand(String[] command, boolean useDuccSpawn, String userId, String containerId) throws Exception {
		String[] commandLine = null;
		try {
			//							
			//	Use ducc_ling (c code) as a launcher for the actual process. The ducc_ling
			//  allows the process to run as a specified user in order to write out logs in
			//  user's space as oppose to ducc space.
			String c_launcher_path = 
					Utils.resolvePlaceholderIfExists(
							System.getProperty("ducc.agent.launcher.ducc_spawn_path"),System.getProperties());

			

			if ( useDuccSpawn && c_launcher_path != null ) {
				commandLine = new String[4+command.length];
				commandLine[0] = c_launcher_path;
				commandLine[1] = "-u";
				commandLine[2] = userId;
				commandLine[3] = "--";
				
				int j=0;
				for(int i=4; i < commandLine.length;i++) {
					commandLine[i] = command[j++];
				}
			} else {
				commandLine = command;
			}
			ProcessBuilder processLauncher = new ProcessBuilder();
			processLauncher.command(commandLine);
			processLauncher.redirectErrorStream();
			
			java.lang.Process process = processLauncher.start();
			
			InputStreamReader in = new InputStreamReader(process.getInputStream());
			BufferedReader reader = new BufferedReader(in);
			String line;
			while ((line = reader.readLine()) != null) {
				System.out.println(">>>>"+line);
			}
			int retCode = process.waitFor();
			return retCode;
			
		} catch( Exception e) {
			StringBuffer sb = new StringBuffer();
			if ( commandLine != null ) {
               for ( String cmdPart : commandLine ) {
		          sb.append(cmdPart).append(" ");	  
		       }
			}
           if ( agentLogger != null ) {
        	   agentLogger.error("launchCommand", null, "Unable to Launch Command:"+sb.toString(),e);
           } else {
        	   System.out.println("CGroupsManager.launchCommand()- Unable to Launch Command:"+sb.toString());
   			   e.printStackTrace();
           }

		} 
		return -1;  // failure
	}
	/**
	 * Return a Set of existing cgroup Ids found in the filesystem identified
	 * by 'cgroupBaseDir'.
	 * 
	 * @return - set of cgroup ids
	 * 
	 * @throws Exception
	 */
	public Set<String> collectExistingContainers() throws Exception {
		File duccCGroupBaseDir = new File(cgroupBaseDir);
		if ( duccCGroupBaseDir.exists()) {
			File[] existingCGroups = duccCGroupBaseDir.listFiles();
			for (File cgroup : existingCGroups ) {
				if ( cgroup.isDirectory() ) {
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
}
