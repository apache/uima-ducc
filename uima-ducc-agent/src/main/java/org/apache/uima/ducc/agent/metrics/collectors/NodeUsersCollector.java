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
import org.apache.uima.ducc.common.utils.id.DuccId;
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
  DuccId jobid = null;
  Agent agent;
  
  public NodeUsersCollector(Agent agent, DuccLogger logger) {
    this.agent = agent;
    this.logger = logger;
  }
  /**
   * Returns true if a given userId belongs to an exclusion list defined in ducc.properties.
   * This list contains user IDs the Agent should exclude while determining rogue processes.
   * System owned (root, nfs, etc) processes should not be reported as rogue. 
   * 
   * @param userId
   * @return
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
   * @return
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
          logger.trace(location, jobid, "********** Process with PID:"+tokens[1]+ " Is a Ducc Daemon:"+token+". Skipping....");
        }
        return true;
      } else if (token.startsWith("-Dactivemq.base")) {
        if ( logger == null ) {
         // System.out.println(
           //       "********** Process with PID:"+tokens[1]+ " Is an ActiveMQ Broker:"+token+". Skipping....");
        } else {
          logger.trace(location, jobid, "********** Process with PID:"+tokens[1]+ " Is an ActiveMQ Broker:"+token+". Skipping....");
        }
        return true;
      }
    }
    return false;
  }
  public TreeMap<String,NodeUsersInfo> call() throws Exception {
    String location = "call";
    TreeMap<String,NodeUsersInfo> map = new TreeMap<String,NodeUsersInfo>();

    List<String> currentPids = new ArrayList<String>();
    try {
      
      ProcessBuilder pb = new ProcessBuilder("ps","-Ao","user:12,pid,ppid,args", "--no-heading");
      pb.redirectErrorStream(true);
      Process proc = pb.start();
      //  spawn ps command and scrape the output
      InputStream stream = proc.getInputStream();
      BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
      String line;
      String regex = "\\s+";
      // copy all known reservations reported by the OR
      agent.copyAllUserReservations(map);
      if ( logger == null ) {
       // System.out.println(
        //        "********** User Process Map Size After copyAllUserReservations:"+map.size());
      } else {
        logger.info(location, jobid, "********** User Process Map Size After copyAllUserReservations:"+map.size());
      }
      // copy all known rogue processes detected previously
      agent.getRogueProcessReaper().copyAllUserRogueProcesses(map); 
      if ( logger == null ) {
        //System.out.println(
         //       "********** User Process Map Size After copyAllUserRougeProcesses:"+map.size());
      } else {
        logger.info(location, jobid, "********** User Process Map Size After copyAllUserRougeProcesses:"+map.size());
      }
      // Add all running processes to this list. Will use this list to determine if a process has a parent
      // which is a rogue process.
      Set<NodeUsersCollector.ProcessInfo> processList = 
              new HashSet<NodeUsersCollector.ProcessInfo>();
      
      // read the next line from ps output
      while ((line = reader.readLine()) != null) {

        String tokens[] = line.split(regex);
        String user = tokens[0];
        String pid = tokens[1];
        String ppid = tokens[2];
        String cmd = tokens[3];
        
        if ( tokens.length > 0 ) {
          // Detect and skip all ducc daemons except uima-as service
          if ( duccDaemon(tokens)) {
            continue;
          }
          if ( logger == null ) {
            //System.out.print(line);
          } else {
            logger.trace(location, jobid, line);
          }
          //  Check if current process is owned by a user that should be excluded
          //  from rogue process detection. A list of excluded users is in ducc.properties
          //  Dont include root, nfs, and other system owned processes. Also exclude
          //  processes that are defined in the process exclusion list in ducc.properties 
          if ( excludeUser(user) || excludeProcess(cmd) || Utils.getPID().equals(pid))  {
            continue;   // skip this process         
          }
          if ( agent != null ) {
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
              logger.info(location, jobid, "User:"+user+" Reservations:"+nui.getReservations().size()+" Rogue Processes:"+nui.getRogueProcesses().size());
            }
            // add a process to a list of processes currently running on the node. The list will be used
            // to remove stale rogue processes at the end of this method
            currentPids.add(tokens[1]);
            currentPids.add(pid);
            if ( logger == null ) {
            } else {
              logger.trace(location, jobid,"Current Process (Before Calling aggregate() - PID:"+pid+" PPID:"+ppid+" Process List Size:"+processList.size());
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
                nui.addPid(pid,cmd.endsWith("java"));
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
                agent.getRogueProcessReaper().submitRogueProcessForKill(user, pid,cmd.endsWith("java"));
              }
            }
          }
        }
      }
    } 
    catch (Exception e) {
      if ( logger == null ) {
        e.printStackTrace();
      } else {
        logger.error(location, jobid, e);
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
      logger.info(location, jobid, sb.toString());
      logger.info(location, jobid, "******************************************************************************");
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
