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
package org.apache.uima.ducc.agent.metrics.collectors;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.uima.ducc.agent.Agent;
import org.apache.uima.ducc.common.node.metrics.NodeUsersInfo;
import org.apache.uima.ducc.common.node.metrics.NodeUsersInfo.NodeProcess;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.Utils;
import org.apache.uima.ducc.common.utils.id.IDuccId;

/**
 * Spawns "ps -ef --no-heading" cmd and scrapes the output to collect user processes. 
 * 
 * Detects and filters out Ducc daemon processes and AMQ broker.
 * 
 * Detects rogue processes which are processes that are not associated with any job 
 * and not belonging to a user node reservation. 
 * 
 */
public class NodeUsersCollector implements CallableNodeUsersCollector {
  
  DuccLogger logger;
  Agent agent;
  
  
  int uidMax = 500;   // default 
  static String ducc_user = System.getProperty("user.name");
  
  public NodeUsersCollector(Agent agent, DuccLogger logger) {
    this.agent = agent;
    this.logger = logger;
    // fetch max uid to be considered as a system user. Any process owned by
    // user id < uidMax is not rogue and will be left running.
    uidMax = Utils.getMaxSystemUserId();
  }
  /**
   * Returns true if a given userId belongs to an exclusion list defined in ducc.properties.
   * This list contains user IDs the Agent should exclude while determining rogue processes.
   * System owned (root, nfs, etc) processes should not be reported as rogue. 
   * 
   * @param userId
   * @return  boolean - true if a given user should be excluded. False otherwise
   */
  public boolean excludeUser(String userId ) {
    String userFilter =
            System.getProperty("ducc.agent.rogue.process.user.exclusion.filter");
    if ( userFilter != null) {
      //  exclusion list contains comma separated user ids
      String[] excludedUsers = userFilter.split(",");
      for ( String excludedUser : excludedUsers ) {
        if ( excludedUser.equals(userId)) {
          return true;
        }
      }
      
    }
    return false;
  }
  /**
   * Returns true if a given process belongs to an exclusion list defined in ducc.properties.
   * This list contains process names the Agent should exclude while determining rogue processes.
   * 
   * @param process
   * @return boolean - true if a given process should be excluded. False otherwise
   */
  public boolean excludeProcess(String process ) {
    String processFilter = 
            System.getProperty("ducc.agent.rogue.process.exclusion.filter");
    if ( processFilter != null ) {
      //  exclusion list contains comma separated user ids
      String[] excludedProcesses = processFilter.split(",");
      for ( String excludedProcess : excludedProcesses ) {
        if ( excludedProcess.equals(process)) {
          return true;
        }
      }
    } 
    return false;
  }

  private void aggregate( Set<NodeUsersCollector.ProcessInfo> processList, NodeUsersCollector.ProcessInfo cpi ) {
    boolean added = false;
    
    for( NodeUsersCollector.ProcessInfo pi: processList ) {
      // PIDs below are ints so safe to use ==
      if ( pi.getPid() == cpi.getPPid() ) { // is the current process a child of another Process? 
          pi.getChildren().add(cpi); // add current process as a child
         added = true;
         if ( pi.isRogue() ) { // if parent is rogue, a child is rogue as well
           cpi.setRogue(true);
           break;
         }
      } else if ( pi.getChildren().size() > 0 ) {

        for(NodeUsersCollector.ProcessInfo childpi : pi.getChildren() ) {
          // is the current process a child of another Process?
          if ( childpi.getPid() == cpi.getPPid() ) {  
            added = true;  // dont add the child here as it will cause ConcurrentModificationException
                           // just mark it for inclusion in a child list below
            if ( childpi.isRogue() ) { // if parent is rogue, a child is rogue as well
              cpi.setRogue(true);
            }
            break;  // stop iterating over children
          }
        }
      } 
      if ( added ) {
        pi.getChildren().add(cpi); // add current process as a child
        if ( logger == null ) {
          //System.out.println("********* Adding Child Process With PID:"+cpi.getPid()+ " As Child of Process:"+cpi.getPPid());
        } else {
          logger.info("aggregate", null, "********* Adding Child Process With PID:"+cpi.getPid()+ " As Child of Process:"+cpi.getPPid());
        }
        break;
     }
    }
    // not added to the list in the code above
    if ( !added ) {
      processList.add(cpi);
      if ( logger == null ) {
        //System.out.println("********* Adding Process With PID:"+cpi.getPid()+ " NO PARENT");
      } else {
        logger.info("aggregate", null, "********* Adding Process With PID:"+cpi.getPid()+ " NO PARENT");
      }
    }
  }
  private boolean duccDaemon(String[] tokens) {
    String location = "duccDaemon";
    for( String token : tokens ) {
      if ( token.startsWith("-Dducc.deploy.components")) {
        int pos = token.indexOf("=");
        if ( pos > -1 ) {
          String component = token.substring(pos+1);
          // if a process is uima-as service need to check if this is a rogue process
          if ( component.trim().startsWith("uima-as")) {
            break;  // will check if rogue process below
          }
        }
        if ( logger == null ) {
         // System.out.println(
               //   "********** Process with PID:"+tokens[1]+ " Is a Ducc Daemon:"+token+". Skipping....");
        } else {
          logger.trace(location, null, "********** Process with PID:"+tokens[1]+ " Is a Ducc Daemon:"+token+". Skipping....");
        }
        return true;
      } else if (token.startsWith("-Dactivemq.base")) {
        if ( logger == null ) {
         // System.out.println(
           //       "********** Process with PID:"+tokens[1]+ " Is an ActiveMQ Broker:"+token+". Skipping....");
        } else {
          logger.trace(location, null, "********** Process with PID:"+tokens[1]+ " Is an ActiveMQ Broker:"+token+". Skipping....");
        }
        return true;
      }
    }
    return false;
  }
  /**
   * 
   * @param pid
   * @param list
   * @return
   */
  private boolean processAncestorIsOwnedByDucc(String ppid, Set<RunningProcess> list) {
	  for( RunningProcess pi : list ) {
		  if ( pi.getPid().equals(ppid) ) {
			  if (  pi.getOwner().equalsIgnoreCase(ducc_user) ) {
				  return true;
			  } else {
				  return processAncestorIsOwnedByDucc(pi.getPpid(), list);
			  }
		  } 
	  }
	  return false;
  }
  public TreeMap<String,NodeUsersInfo> call() throws Exception {
    String location = "call";
    TreeMap<String,NodeUsersInfo> map = new TreeMap<String,NodeUsersInfo>();

    List<String> currentPids = new ArrayList<String>();
    InputStream stream = null;
    BufferedReader reader = null;
    try {

      ProcessBuilder pb;
      if ( Utils.isMac() ) {
        pb = new ProcessBuilder("ps","-Ao","user=,pid=,ppid=,uid=,args=");
      } else {
        pb = new ProcessBuilder("ps","-Ao","user:12,pid,ppid,uid,args", "--no-heading");
      }
      pb.redirectErrorStream(true);
      Process proc = pb.start();
      //  spawn ps command and scrape the output
      stream = proc.getInputStream();
      reader = new BufferedReader(new InputStreamReader(stream));
      String line;
      String regex = "\\s+";
      if ( agent != null ) {
         // copy all known reservations reported by the OR
         agent.copyAllUserReservations(map);
      }
      if ( logger == null ) {
       // System.out.println(
        //        "********** User Process Map Size After copyAllUserReservations:"+map.size());
      } else {
        logger.debug(location, null, "********** User Process Map Size After copyAllUserReservations:"+map.size());
      }
      if ( agent != null ) {
          // copy all known rogue processes detected previously
          agent.getRogueProcessReaper().copyAllUserRogueProcesses(map); 
      }
      if ( logger == null ) {
        //System.out.println(
         //       "********** User Process Map Size After copyAllUserRougeProcesses:"+map.size());
      } else {
        logger.debug(location, null, "********** User Process Map Size After copyAllUserRougeProcesses:"+map.size());
      }
      // Add all running processes to this list. Will use this list to determine if a process has a parent
      // which is a rogue process.
      Set<NodeUsersCollector.ProcessInfo> processList = 
              new HashSet<NodeUsersCollector.ProcessInfo>();
      
      Set<RunningProcess> tempProcessList = 
              new HashSet<RunningProcess>();
  
      // To detect rogues there are two scans through process list:
      // #1 - fills tempProcessList which will be used to check each
      //     process parent if its own by ducc.
      // #2 - the actual rogue process detection loop
      
      List<String> procList = new ArrayList<String>();
      // read the next line from ps output
      while ((line = reader.readLine()) != null) {
    	  // save line for subsequent processing in the for..loop below
    	  procList.add(line);
          String tokens[] = line.split(regex);
          if ( tokens.length > 0 ) {
          	RunningProcess p = 
                      new RunningProcess(tokens[1],tokens[2],tokens[0]);
            // add process to a list which is used to look up each process parent
          	tempProcessList.add(p);
          }
      }
      // the above loop filled tempProcessList, so now detect rogue processes.
      for( String procInfo : procList) {
        String tokens[] = procInfo.split(regex);
        String user = tokens[0];
        String pid = tokens[1];
        String ppid = tokens[2];
        String uid = tokens[3];
        String cmd = tokens[4];
        
        if ( tokens.length > 0 ) {
        	try {
        		// by convention processes owned by uid < gidMax are system processes thus not rogue
        		if ( Integer.valueOf(uid) < uidMax ) {
        			continue;    
        		}
        	} catch( NumberFormatException nfe) {
        		
        	}
        	//	walk up the tree of ancestor processes to check if any is owned by ducc. If so, this
            //  process is not rogue.
            if ( processAncestorIsOwnedByDucc(pid, tempProcessList)) {
            	continue;  // skip as this is not a rogue process
            }
            // any process owned by user who started the agent process is not rogue
            if ( ducc_user.equalsIgnoreCase(user)) {
            	continue;
            }
        	// Detect and skip all ducc daemons except uima-as service
//          if ( duccDaemon(tokens)) {
//            continue;
//          }
          if ( logger == null ) {
            //System.out.print(line);
          } else {
            logger.trace(location, null, line);
          }
          //  Check if current process is owned by a user that should be excluded
          //  from rogue process detection. A list of excluded users is in ducc.properties
          //  Dont include root, nfs, and other system owned processes. Also exclude
          //  processes that are defined in the process exclusion list in ducc.properties 
          if ( excludeUser(user) || excludeProcess(cmd) || Utils.getPID().equals(pid))  {
            continue;   // skip this process         
          }
          if ( agent != null ) {
            // check if this process is in any of the cgroups. If so, this process is not rogue
            //if ( ((NodeAgent)agent).useCgroups && ((NodeAgent)agent).cgroupsManager.isPidInCGroup(pid) ) {
              //continue; // not rogue, this process is in a cgroup
            //}
            NodeUsersInfo nui = null; 
            //  Check if user record is already in the map. May have been done above in
            //  copyAllUserReservations().
            if ( map.containsKey(user)) {
              nui = map.get(user);
            } else {
              nui = new NodeUsersInfo(user);
              map.put(user, nui);
            }
            if ( logger == null ) {
          //    System.out.println(
           //           "User:"+user+" Reservations:"+nui.getReservations().size()+" Rogue Processes:"+nui.getRogueProcesses().size());
            } else {
              logger.info(location, null, "User:"+user+" Reservations:"+nui.getReservations().size()+" Rogue Processes:"+nui.getRogueProcesses().size());
            }
            // add a process to a list of processes currently running on the node. The list will be used
            // to remove stale rogue processes at the end of this method
           // currentPids.add(tokens[1]);
            currentPids.add(pid);
            if ( logger == null ) {
            } else {
              logger.trace(location, null,"Current Promuscess (Before Calling aggregate() - PID:"+pid+" PPID:"+ppid+" Process List Size:"+processList.size());
            }
            NodeUsersCollector.ProcessInfo pi = 
                    new NodeUsersCollector.ProcessInfo(Integer.parseInt(pid),Integer.parseInt(ppid));
            // add the process to the list of processes. If this process has a parent, it will be added as a child. Compose
            // hierarchy of processes so that we can use it later to determine if any given process has a parent that is rogue
            aggregate(processList, pi);
            
            // fetch user reservations
            List<IDuccId> userReservations = nui.getReservations();
            //  if user has reservations on the node, any process found is not a rogue process
            if ( userReservations.size() > 0 ) {
              boolean found = false;
              //  check if this process has previously been marked as rogue
              for( NodeProcess rogue : nui.getRogueProcesses() ) {
                if ( rogue.getPid().equals(pid)) {
                  found = true;
                  break;
                }
              }
              
              if ( !found && !agent.isManagedProcess(processList, pi)) {
                // code keeps count of java and non-java processes separately, so pass the type of process (java or not) 
                // to allow distinct accounting 
                nui.addPid(pid, ppid, cmd.endsWith("java"));
              }
              continue;  // all we know that the user has a reservation and there is a process running. If there
                         // are reservations, we cant determine which user process is a rogue process
            }
           
            //  detect if this is a rogue process and add it to the rogue process list. First check if the current process
            //  has a parent and if so, check if the parent is rogue. Second, if parent is not rogue (or no parent)
            //  check if the process is in agent's inventory. If its not, we have a rogue process.
            if ( agent.isRogueProcess(user, processList, pi) ) {
              if ( nui.getRogueProcesses().size() == 0 || !inRogueList(nui.getRogueProcesses(),pid) ) {
                pi.setRogue(true);
//                agent.getRogueProcessReaper().submitRogueProcessForKill(user, pid, ppid, cmd.endsWith("java"));
              }
              agent.getRogueProcessReaper().submitRogueProcessForKill(user, pid, ppid, cmd.endsWith("java"));
            }
          }
        }
      }
    } 
    catch (Exception e) {
      if ( logger == null ) {
        e.printStackTrace();
      } else {
        logger.error(location, null, e);
      }
    } finally {
    	if ( reader != null ) {
    		try {
    			reader.close();
    		} catch( Exception exx){}
    	}
    }
    StringBuffer sb = new StringBuffer();
    // if no processes found, clear rogue process list and list of processes associated with a reserve
    if ( currentPids.isEmpty()) {
      for( Map.Entry<String,NodeUsersInfo> entry : map.entrySet()) {
        entry.getValue().getReserveProcesses().clear();
        entry.getValue().getRogueProcesses().clear();
      }
    }

    for( Map.Entry<String,NodeUsersInfo> entry : map.entrySet()) {
      sb.append(entry.getValue().toString()).append("\n");
    }
    if ( logger == null ) {
      System.out.println(sb.toString());
      System.out.println(
            "***************************************************************************************");
    } else {
    	if ( sb.length() > 0 ) {
    	      logger.info(location, null, sb.toString());
    	      logger.info(location, null, "******************************************************************************");
    	}
    }
    // remove any rogue processes that are not in the list of current processes just collected
    agent.getRogueProcessReaper().removeDeadRogueProcesses(currentPids);
    return map;
  }
  private boolean inRogueList(List<NodeProcess> rogueProcesses, String pid) {
    for( NodeProcess rogue : rogueProcesses ) {
      if ( rogue.getPid().equals(pid)) {
        return true;
      }
    }
    return false;
  }
  public class ProcessInfo {
    private int pid;
    private int ppid;
    boolean rogue;
    Set<NodeUsersCollector.ProcessInfo> childProcesses = 
            new HashSet<NodeUsersCollector.ProcessInfo>();
    ProcessInfo(int pid, int ppid) {
      this.pid = pid;
      this.ppid = ppid;
    }

    public int getPid() {
      return pid;
    }

    public int getPPid() {
      return ppid;
    }

    public boolean isRogue() {
      return rogue;
    }

    public void setRogue(boolean rogue) {
      this.rogue = rogue;
    }

    public Set<NodeUsersCollector.ProcessInfo> getChildren() {
      return childProcesses;
    }

  }
  public class RunningProcess {
	  String pid;
	  String ppid;
	  public RunningProcess(String pid, String ppid, String owner) {
		  this.pid = pid;
		  this.ppid = ppid;
		  this.owner = owner;
	  }
	  public String getPid() {
		return pid;
	}
	public String getPpid() {
		return ppid;
	}
	public String getOwner() {
		return owner;
	}
	String owner;
	  
  }
  /*
  private void dump(Set<NodeUsersCollector.ProcessInfo> processList) {
     for( NodeUsersCollector.ProcessInfo pi: processList ) {
       if ( logger == null ) {
         System.out.println("Process PID:"+pi.getPid()+" PPID:"+pi.getPPid());
       } else {
         logger.trace("dump",null,"Process PID:"+pi.getPid()+" PPID:"+pi.getPPid());
       }
       if ( pi.getChildren().size() > 0 ) {
          if ( logger == null ) {
            System.out.println("\t=>");
          } else {
            logger.trace("dump",null,"\t=>");
          }
          for(NodeUsersCollector.ProcessInfo childpi : pi.getChildren() ) {
            if ( logger == null ) {
              System.out.println("PID:"+childpi.getPid()+" PPID:"+childpi.getPPid()+" | ");
            } else {
              logger.trace("dump",null,"PID:"+childpi.getPid()+" PPID:"+childpi.getPPid()+" | ");
            }
          }
          if ( logger == null ) {
            System.out.println("\n");

          } else {
            logger.trace("dump",null,"\n");
            
          }
        } 
     }
  }
  */
  public static void main(String[] args) {
/*
    try {
      Set<NodeUsersCollector.ProcessInfo> processList = new HashSet<NodeUsersCollector.ProcessInfo>();
      
      NodeUsersCollector.ProcessInfo pi1 = new NodeUsersCollector.ProcessInfo(102,100);
      NodeUsersCollector.ProcessInfo pi2 = new NodeUsersCollector.ProcessInfo(103,110);
      NodeUsersCollector.ProcessInfo pi11 = new NodeUsersCollector.ProcessInfo(104,102);
      NodeUsersCollector.ProcessInfo pi12 = new NodeUsersCollector.ProcessInfo(105,102);
      NodeUsersCollector.ProcessInfo pi3 = new NodeUsersCollector.ProcessInfo(106,111);
      
      NodeUsersCollector collector = new NodeUsersCollector(null);
      collector.aggregate(processList, pi1);
//      collector.dump(processList);
      collector.aggregate(processList, pi2);
      collector.aggregate(processList, pi11);
      collector.aggregate(processList, pi12);
      collector.aggregate(processList, pi3);
      collector.dump(processList);
      
    } catch( Exception e) {
      e.printStackTrace();
    }
    */
  }
}
