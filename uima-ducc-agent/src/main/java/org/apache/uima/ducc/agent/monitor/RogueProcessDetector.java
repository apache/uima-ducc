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
package org.apache.uima.ducc.agent.monitor;

import org.apache.activemq.camel.component.ActiveMQComponent;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.uima.ducc.agent.Agent;
import org.apache.uima.ducc.agent.RogueProcessReaper;
import org.apache.uima.ducc.agent.metrics.collectors.NodeUsersCollector;
import org.apache.uima.ducc.agent.metrics.collectors.NodeUsersCollector.ProcessInfo;
import org.apache.uima.ducc.common.NodeIdentity;
import org.apache.uima.ducc.common.component.AbstractDuccComponent;
import org.apache.uima.ducc.common.node.metrics.NodeUsersInfo;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.cmdline.ICommandLine;
import org.apache.uima.ducc.transport.event.NodeInventoryUpdateDuccEvent;
import org.apache.uima.ducc.transport.event.common.DuccUserReservation;
import org.apache.uima.ducc.transport.event.common.IDuccProcess;
import org.apache.uima.ducc.transport.event.common.IDuccStandardInfo;
import org.apache.uima.ducc.transport.event.common.ProcessMemoryAssignment;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


public class RogueProcessDetector extends AbstractDuccComponent implements Processor{
  public static final String DUCC_PROPERTY_FILE="ducc.deploy.configuration";
  public static final String AGENT_NODE_INVENTORY_ENDPOINT="ducc.agent.node.inventory.endpoint";
  //  locks agent inventory 
  public static Object lock = new Object();
  //  processes managed by the agent
  List<ManagedProcess> agentProcesses = new ArrayList<ManagedProcess>();
  //  processes not managed by the agent, added to the inventory for testing via -p option
  List<ManagedProcess> testProcesses = new ArrayList<ManagedProcess>();
  
  private static ActiveMQComponent duccAMQComponent = null;

  List<DuccUserReservation> reservations = new ArrayList<DuccUserReservation>();
  //  Executor which runs detection of rogue processes at fixed intervals
  ScheduledThreadPoolExecutor executor = 
          new ScheduledThreadPoolExecutor(5);
  
  String nodeToMonitor = "";
  
  public RogueProcessDetector(List<DuccUserReservation> reservations) {
    super("rpd", new DefaultCamelContext());
    this.reservations = reservations;
  }
  /**
   * Route builder to receive node agent inventory updates
   * @param delegate - processor where the node inventory is collected and filtered
   * @param endpoint - agent inventory topic
   * @return RouteBuilder
   */
  public RouteBuilder routeBuilderForEndpoint(final Processor delegate, final String endpoint) {
    return new RouteBuilder() {
      public void configure() {
        onException(Exception.class).handled(true).process(new ErrorProcessor());
        from(endpoint)
        .process(delegate);
      }
    };
  }
  public void start( long delay, String nodeName, String[] pids) {
    try {
      Agent agent = new TestAgent(agentProcesses);
      for (String pid : pids) {
        testProcesses.add( new ManagedProcess(pid));
      }
      agentProcesses.addAll(testProcesses);
      nodeToMonitor = nodeName;
      
      loadProperties(DUCC_PROPERTY_FILE);

      String brokerUrl = System.getProperty("ducc.broker.url");
      
      duccAMQComponent = ActiveMQComponent.activeMQComponent(brokerUrl);
      //  by default, Camel includes ActiveMQComponent connected to localhost. We require
      //  an external broker. So replace the default with one connected to the desired broker.
      if ( super.getContext().getComponent("activemq") != null) {
        super.getContext().removeComponent("activemq");
      }
      super.getContext().addComponent("activemq",duccAMQComponent);

      String agentNodeInventoryEndpoint = 
              System.getProperty(AGENT_NODE_INVENTORY_ENDPOINT);
      System.out.println("Starting Simulator: broker="+brokerUrl+" node="+nodeName+" delay="+delay+" agent inventory="+agentNodeInventoryEndpoint);
      super.getContext().addRoutes(this.routeBuilderForEndpoint(this, agentNodeInventoryEndpoint));
      super.getContext().start();
      //  Schedule rogue process detection at a given interval
      final NodeUsersCollector nodeUsersCollector = new NodeUsersCollector(agent, null);
      executor.scheduleAtFixedRate(new Runnable() {
        public void run() {
            try {
              //TreeMap<String,NodeUsersInfo> userInfo = 
              nodeUsersCollector.call();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
      }, 0, delay, TimeUnit.SECONDS);
    } catch( Exception e) {
      e.printStackTrace();
    }
    
  }
  /**
   * Dummy Agent
   *
   */
  class TestAgent implements Agent {
    public DuccLogger logger = DuccLogger.getLogger(this.getClass(), COMPONENT_NAME);

    List<ManagedProcess> deployedProcesses;
    private RogueProcessReaper rogueProcessReaper = 
            new RogueProcessReaper(null, 5, 10);
    
    public TestAgent(List<ManagedProcess> deployedProcesses) {
      this.deployedProcesses = deployedProcesses;
    }
    public void startProcess(IDuccProcess process, ICommandLine commandLine,
            IDuccStandardInfo info, DuccId workDuccId, ProcessMemoryAssignment shareMemorySize) {
    }

    public void stopProcess(IDuccProcess process) {
    }

    public NodeIdentity getIdentity() {
      return null;
    }

    public HashMap<DuccId, IDuccProcess> getInventoryCopy() {
      return null;
    }

    public HashMap<DuccId, IDuccProcess> getInventoryRef() {
      return null;
    }
    public boolean isManagedProcess(Set<NodeUsersCollector.ProcessInfo> processList, NodeUsersCollector.ProcessInfo cpi) {
      try {
        
        synchronized(lock) {
          for (ManagedProcess deployedProcess : deployedProcesses) {
            if ( deployedProcess != null ) {
              // Check if process has been deployed but has not yet reported its PID. 
              // This is normal. It takes a bit of time until the JP reports
              // its PID to the Agent. If there is at least one process in Agent
              // deploy list with no PID we assume it is the one. 
              if ( deployedProcess.getPid() == null ) {
                return true; 
              }
              String dppid = deployedProcess.getPid();
              if (dppid != null && dppid.equals(String.valueOf(cpi.getPid()))) {
                return true;
              }
            }
          }
          for( NodeUsersCollector.ProcessInfo pi: processList ) {
            if ( pi.getPid() == cpi.getPPid() && pi.getChildren().size() > 0 ) { // is the current process a child of another java Process? 
               return isManagedProcess(pi.getChildren(), pi);
            }
          }
        }
      } catch ( Exception e) {
        e.printStackTrace();
      } 
      return false;
    }

    public boolean isRogueProcess(String uid, Set<ProcessInfo> processList, ProcessInfo cpi)
            throws Exception {
        // Agent adds a process to its inventory before launching it. So it is 
        // possible that the inventory contains a process with no PID. If there
        // is such process in the inventory we cannot determine that a given 
        // pid is rogue yet. Eventually, the launched process reports its PID
        boolean foundDeployedProcessWithNoPID = false;
        try {
          synchronized(lock) {
            for (ManagedProcess deployedProcess : deployedProcesses) {
              // Check if process has been deployed but has not yet reported its PID. 
              // This is normal. It takes a bit of time until the JP reports
              // its PID to the Agent. If there is at least one process in Agent
              // deploy list with no PID we assume it is the one. 
              if ( deployedProcess.getPid() == null ) {
                foundDeployedProcessWithNoPID = true; 
                break;
              } else {
                String dppid = deployedProcess.getPid();
                if (dppid != null && dppid.equals(String.valueOf(cpi.getPid()))) {
                  System.out.println("++++++++++++++++ PID:"+cpi.getPid() +" Is *NOT* Rogue");
                  return false;
                }
              }
            }
          }
          
        } catch( Exception e ) {
          e.printStackTrace();
        }
        // not found
        if ( foundDeployedProcessWithNoPID ) {
          return false;
        } else {
          return isParentProcessRogue(processList, cpi);
        }
    }
    private boolean isParentProcessRogue(Set<NodeUsersCollector.ProcessInfo> processList, NodeUsersCollector.ProcessInfo cpi) {
      //boolean found = false;
      for( NodeUsersCollector.ProcessInfo pi: processList ) {
        if ( pi.getPid() == cpi.getPPid() ) { // is the current process a child of another java Process? 
          //found = true; 
          if ( pi.isRogue() ) { // if parent is rogue, a child is rogue as well
            System.out.println("++++++++++++++++ parent PID:"+pi.getPid() +" Is Rogue");
            return true;
          }
          System.out.println("++++++++++++++++ parent PID:"+pi.getPid() +" Is NOT Rogue");
          return false;
        } else {
          if ( pi.getChildren().size() > 0 ) {
            return isParentProcessRogue(pi.getChildren(), cpi);
          }
        } 
      }
      System.out.println("++++++++++++++++ PID:"+cpi.getPid() +" Is Rogue");

      return true;

    }
    public void copyAllUserReservations(TreeMap<String, NodeUsersInfo> map) {
      try {
        if ( reservations != null ) {
//          System.out.println("+++++++++++ Copying User Reservations - List Size:"+reservations.size());
          for( DuccUserReservation r : reservations ) {
            if ( "System".equals(r.getUserId())) {
              continue;
            }
            NodeUsersInfo nui = null;
            if ( map.containsKey(r.getUserId())) {
              nui = map.get(r.getUserId());
            } else {
              nui = new NodeUsersInfo(r.getUserId());
              map.put(r.getUserId(), nui);
            }
            nui.addReservation(r.getReserveID());
          }
        } else {
//          System.out.println(" ***********  No Reservations");
        }
      } catch( Exception e) {
        e.printStackTrace();
      } 
    }

    public RogueProcessReaper getRogueProcessReaper() {
      return rogueProcessReaper;
    }
	public int getOSPageSize() {
		// TODO Auto-generated method stub
		return 0;
	}
    
  }
  
  public static class ManagedProcess {
    String pid;

    public ManagedProcess(String pid) {
      this.pid = pid;
    }

    public String getPid() {
      return pid;
    }

    public void setPid(String pid) {
      this.pid = pid;
    }

  }
  
  
  public static void main(String[] args) {
    
    CommandLineParser parser = new BasicParser( );
    Options options = new Options( );
    options.addOption("h", "help", false, "Print this usage information");
    options.addOption( new Option("d", true,"") );
    options.addOption( new Option("r", true,"") );
    options.addOption( new Option("n", true,"") );
    options.addOption( new Option("p", true,"") );
    // Parse the program arguments
    CommandLine commandLine = null;    
    try {
      commandLine = parser.parse( options, args );
    } catch( Exception e) {
      e.printStackTrace();
      System.exit(1);
    }
    
    long delay = 10;
    String[] pids = new String[0];
    String nodeName = null;

    
    List<DuccUserReservation> reservations = new ArrayList<DuccUserReservation>();
    if (commandLine.hasOption("r")) {
      try {
        String[] users = ((String)commandLine.getOptionValue("r")).split(",");
        int dummyReservationId = 11111;
        for( String userId : users ) {
          DuccUserReservation reservation = 
                  new DuccUserReservation(userId, new DuccId(dummyReservationId++), null);
          reservations.add(reservation);
        }
      } catch( Exception e) {
        e.printStackTrace();
        System.exit(1);
      }
    }

    if (commandLine.hasOption("p")) {
      try {
        pids = ((String)commandLine.getOptionValue("p")).split(",");
      } catch( Exception e) {
        e.printStackTrace();
        System.exit(1);
      }
    }

    if (commandLine.hasOption("d")) {
      try {
        delay = Long.parseLong((String)commandLine.getOptionValue("d"));
      } catch( Exception e) {
        e.printStackTrace();
        System.exit(1);
      }
    }

    if (commandLine.hasOption("n")) {
      try {
        nodeName = (String)commandLine.getOptionValue("n");
      } catch( Exception e) {
        e.printStackTrace();
        System.exit(1);
      }
    } else {
      try {
        nodeName = InetAddress.getLocalHost().getHostName();
      } catch( Exception e) {
        e.printStackTrace();
        System.exit(1);

      }
    }

    if (commandLine.hasOption("h")) {
      System.out.println("Usage: java -cp <..> -p <pid1,pid2,..> -n <node name> -d <delay> -r <user1,user2,...>");
      System.out.println("Options:");
      System.out.println("-p: comma separated list of pids as if the processes where managed by an agent");
      System.out.println("-n: node name ");
      System.out.println("-d: delay at which to collect processes");
      System.out.println("-r: comma separated list of user ids owning a reservation on a node");
      System.exit(1);
    }
    RogueProcessDetector detector = 
            new RogueProcessDetector(reservations);

    
    detector.start( delay, nodeName, pids);

  }
  public void process(Exchange arg0) throws Exception {
    if ( arg0.getIn().getBody() instanceof NodeInventoryUpdateDuccEvent ) {
      NodeInventoryUpdateDuccEvent event =
              (NodeInventoryUpdateDuccEvent)arg0.getIn().getBody();
      HashMap<DuccId, IDuccProcess> processes = event.getProcesses();
      try {
        boolean clear = true;
        synchronized(lock) {
          for( Map.Entry<DuccId, IDuccProcess> process : processes.entrySet()) {
            if ( process.getValue().getNodeIdentity().getName().equals(nodeToMonitor) ) {
              if ( clear ) {
                clear = false;
                agentProcesses.clear(); // remove old inventory and refresh from new agent inventory
                agentProcesses.addAll(testProcesses); // copy test processes
              }
              System.out.println("---------- Got Node Process - PID="+process.getValue().getPID());
              agentProcesses.add(new ManagedProcess(process.getValue().getPID()));
            }
          }
        }
      } catch( Exception e ) {
        e.printStackTrace();
      }
    }
  }
  class DebugProcessor implements Processor {

    public void process(Exchange arg0) throws Exception {
      
    }
    
  }
  class ErrorProcessor implements Processor {

    public void process(Exchange exchange) throws Exception {
      Throwable caused = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Throwable.class);
      caused.printStackTrace();
    }
    
  }
  
  
}
