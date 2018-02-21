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

package org.apache.uima.ducc.ws.server.nodeviz;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.uima.ducc.common.Node;
import org.apache.uima.ducc.common.NodeConfiguration;
import org.apache.uima.ducc.common.NodeIdentity;
import org.apache.uima.ducc.common.SizeBytes;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccPropertiesResolver;
import org.apache.uima.ducc.common.utils.SystemPropertyResolver;
import org.apache.uima.ducc.common.utils.Version;
import org.apache.uima.ducc.transport.event.OrchestratorStateDuccEvent;
import org.apache.uima.ducc.transport.event.common.IDuccProcess;
import org.apache.uima.ducc.transport.event.common.IDuccProcessMap;
import org.apache.uima.ducc.transport.event.common.IDuccReservation;
import org.apache.uima.ducc.transport.event.common.IDuccReservationMap;
import org.apache.uima.ducc.transport.event.common.IDuccSchedulingInfo;
import org.apache.uima.ducc.transport.event.common.IDuccStandardInfo;
import org.apache.uima.ducc.transport.event.common.IDuccTypes.DuccType;
import org.apache.uima.ducc.transport.event.common.IDuccWork;
import org.apache.uima.ducc.transport.event.common.IDuccWorkExecutable;
import org.apache.uima.ducc.transport.event.common.IDuccWorkMap;
import org.apache.uima.ducc.transport.event.common.IDuccWorkReservation;
import org.apache.uima.ducc.transport.event.common.IDuccWorkService;
import org.apache.uima.ducc.transport.event.common.IDuccWorkService.ServiceDeploymentType;
import org.apache.uima.ducc.transport.event.common.IProcessState.ProcessState;
import org.apache.uima.ducc.ws.DuccMachinesData;
import org.apache.uima.ducc.ws.IListenerOrchestrator;
import org.apache.uima.ducc.ws.MachineInfo;
import org.apache.uima.ducc.ws.server.DuccListeners;
import org.apache.uima.ducc.ws.types.NodeId;

public class NodeViz
    implements IListenerOrchestrator
{
    private static DuccLogger logger = DuccLogger.getLogger(NodeViz.class);

    private DuccMachinesData machineData;              // handle to static machine information

    //private long lastUpdate = 0;                       // time of last orchestrator state
    private String visualization;                      // cached visualization
    private long update_interval = 60000;              // Only gen a new viz every 'this long'

    static int default_quantum = 4;                    // for hosts with no jobs so we don't know or care that much
    private String version = "1.1.0";
    static String wshost = "";
    static String wsport = "42133";
    static boolean strip_domain = true;

	public NodeViz()
	{
        String methodName = "NodeViz";

        update_interval = SystemPropertyResolver.getLongProperty("ducc.viz.update.interval", update_interval);                                
        default_quantum = SystemPropertyResolver.getIntProperty("ducc.rm.share.quantum", default_quantum);
        wshost          = SystemPropertyResolver.getStringProperty("ducc.ws.node", System.getProperty("ducc.head"));
        wsport          = SystemPropertyResolver.getStringProperty("ducc.ws.port", wsport);
        strip_domain    = SystemPropertyResolver.getBooleanProperty("ducc.ws.visualization.strip.domain", true);
        
        logger.info(methodName, null, "------------------------------------------------------------------------------------");
        logger.info(methodName, null, "Node Visualization starting:");
        logger.info(methodName, null, "    DUCC home               : ", System.getProperty("DUCC_HOME"));
        logger.info(methodName, null, "    ActiveMQ URL            : ", System.getProperty("ducc.broker.url"));
        logger.info(methodName, null, "Default Quantum             : ", default_quantum);
        logger.info(methodName, null, "Viz update Interval         : ", update_interval);
        logger.info(methodName, null, "Web Server Host             : ", wshost);
        logger.info(methodName, null, "Web Server Port             : ", wsport);
        logger.info(methodName, null, "Strip Domains               : ", strip_domain);
        logger.info(methodName, null, "");
        logger.info(methodName, null, "    JVM                     : ", System.getProperty("java.vendor") +
                                                                   " "+ System.getProperty("java.version"));
        logger.info(methodName, null, "    JAVA_HOME               : ", System.getProperty("java.home"));
        logger.info(methodName, null, "    JVM Path                : ", System.getProperty("ducc.jvm"));
        logger.info(methodName, null, "    JMX URL                 : ", System.getProperty("ducc.jmx.url"));
        logger.info(methodName, null, "");
        logger.info(methodName, null, "    OS Architecture         : ", System.getProperty("os.arch"));
        logger.info(methodName, null, "");
        logger.info(methodName, null, "    DUCC Version            : ", Version.version());
        logger.info(methodName, null, "    Vizualization Version   : ", version);
        logger.info(methodName, null, "------------------------------------------------------------------------------------");

        DuccListeners.getInstance().register(this);
        machineData = DuccMachinesData.getInstance();

        visualization = "<html><p>Waiting for node updates ...</p></html>";
	}
	
	public String getVisualization()
    {
        String methodName = "getVisualization";
        logger.debug(methodName, null, "Request for visualization");
        return visualization;
    }

	public void generateVisualization(OrchestratorStateDuccEvent ev)
	{        
        String methodName = "generateVisualization";
        Map<String, VisualizedHost> hosts = new HashMap<String, VisualizedHost>();
        DuccMachinesData machinesData = DuccMachinesData.getInstance();
        IDuccWorkMap jobmap = ev.getWorkMap();

        int job_gb = 0;
        int service_gb = 0;
        int pop_gb = 0;
        int reservation_gb = 0;

        // Must find host configuration so we can work out the quantum used to schedule each job
        String class_definitions = SystemPropertyResolver
            .getStringProperty(DuccPropertiesResolver
                               .ducc_rm_class_definitions, "scheduler.classes");
        String user_registry = SystemPropertyResolver
            .getStringProperty(DuccPropertiesResolver
                               .ducc_rm_user_registry, "ducc.users");
        class_definitions = System.getProperty("DUCC_HOME") + "/resources/" + class_definitions;
        NodeConfiguration nc = new NodeConfiguration(class_definitions, null, user_registry, logger);        // UIMA-4142 make the config global
        try {
			nc.readConfiguration();
		} catch (Exception e ) {
            logger.error(methodName, null, "Cannot read node configuration.  Some information may not be quite right.");
		}

        // first step, generate the viz from the OR map which seems to have everything we need
        // next stop,  walk the machines list and generate empty node for any machine in that list 
        //             that had no work on it
        // lext step, walk the machines data and overlay a graphic for any node that is 'down' or 'defined'
        //              move 'down' hosts with jobs on them to the front
        //              move all onther 'not up' hosts to the end
        // finally, walk the list and make them render

        for ( Object o : jobmap.values() ) {
        	IDuccWork w = (IDuccWork) o;
            DuccType type = w.getDuccType();
            String service_endpoint = null;
            String service_id = null;          // UIMA-4209
            // If it looks service-y and os of deployment type 'other' it's a Pop.
            if ( type == DuccType.Service ) {
                IDuccWorkService dws = (IDuccWorkService) w;
                if ( dws.getServiceDeploymentType() == ServiceDeploymentType.other) {
                    type = DuccType.Pop;
                } else {
                    service_endpoint = dws.getServiceEndpoint();
                    service_id = dws.getServiceId();  // UIMA-4209
                }
            }
            
            if ( ! w.isSchedulable() ) {
                logger.debug(methodName, w.getDuccId(), "Ignoring unschedulable work:", w.getDuccType(), ":", w.getStateObject());
                continue;
            }
                        
            IDuccStandardInfo si      = w.getStandardInfo();
            IDuccSchedulingInfo sti   = w.getSchedulingInfo();

            String              user    = si.getUser();
            String              duccid  = service_id == null ? Long.toString(w.getDuccId().getFriendly()) : service_id;     // UIMA-4209
            int                 jobmem  = (int) (sti.getMemorySizeAllocatedInBytes()/SizeBytes.GB);

            String              sclass = sti.getSchedulingClass();
            int                 quantum = default_quantum;
            try {
                quantum = nc.getQuantumForClass(sclass);
            } catch ( Exception e ) {
                // this most likely caused by a reconfigure so that a job's class no longer exists.  nothing to do about it
                // but punt and try not to crash.
                logger.warn(methodName, null, "Cannot find scheduling class or quantum for " + sclass + ". Using default quantum of " + default_quantum);
            }
            int                qshares = jobmem / quantum;
            if ( jobmem % quantum != 0 ) qshares++;

            switch ( type ) {
                case Job:
                case Pop:
                case Service:
                {
                    IDuccWorkExecutable de = (IDuccWorkExecutable) w;
                    IDuccProcessMap pm = de.getProcessMap();
                    logger.debug(methodName, w.getDuccId(), "Receive:", type, w.getStateObject(), "processes[", pm.size() + "]");
                    
                    for ( IDuccProcess proc : pm.values() ) {
                        String pid = proc.getPID();
                        ProcessState state = proc.getProcessState();
                        Node n = proc.getNode();

                        logger.debug(methodName, w.getDuccId(), (n == null ? "N/A" : n.getNodeIdentity().getName()), "Process[", pid, "] state [", state, "] is complete[", proc.isComplete(), "]");
                        if ( proc.isComplete() ) {
                            continue;
                        }

                        switch ( type ) {
                            case Job:
                                job_gb += jobmem;
                                break;
                            case Pop:                                
                                pop_gb += jobmem;
                                break;
                            case Service:                                
                                service_gb += jobmem;
                                break;
                            default:
                            	break;
                        }
                        NodeIdentity ni = n.getNodeIdentity();
                    	NodeId nodeId = new NodeId(ni.getName());
                        MachineInfo mi = machinesData.getMachineInfoForNodeid(nodeId);
                        if ( mi != null ) {
                            String key = strip(n.getNodeIdentity().getName());
                            VisualizedHost vh = hosts.get(key);
                            if ( vh == null ) {
                                // System.out.println("Set host from OR with key:" + key + ":");
                                vh = new VisualizedHost(mi, quantum);
                                hosts.put(key, vh);
                            }
                            vh.addWork(type, user, duccid, jobmem, qshares, service_endpoint);
                        }
                        else {
                        	String message = nodeId.getShortName()+" not found?";
                        	logger.debug(methodName, w.getDuccId(), message);
                        }
                    }
                }
                break;
                                    
                case Reservation: 
                {
                    IDuccWorkReservation de = (IDuccWorkReservation) w;
                    IDuccReservationMap  rm = de.getReservationMap();
                    
                    logger.debug(methodName, w.getDuccId(), "Receive:", type, w.getStateObject(), "processes[", rm.size(), "] Completed:", w.isCompleted());
                    reservation_gb += jobmem;
                    
                    for ( IDuccReservation r: rm.values()) {
                        Node n = r.getNode();                        
                        if ( n == null ) {
                            logger.debug(methodName, w.getDuccId(),  "Node [N/A] mem[N/A");
                        } else if ( n.getNodeIdentity() == null ) {
                            logger.debug(methodName, w.getDuccId(),  "NodeIdentity [N/A] mem[N/A");
                        } else {
                        	NodeIdentity ni = n.getNodeIdentity();
                        	NodeId nodeId = new NodeId(ni.getName());
                            MachineInfo mi = machinesData.getMachineInfoForNodeid(nodeId);
                            if ( mi != null ) {
                                String key = strip(n.getNodeIdentity().getName());
                                VisualizedHost vh = hosts.get(key);
                                if ( vh == null ) {
                                    // System.out.println("Set host from OR with key:" + key + ":");
                                    vh = new VisualizedHost(mi, quantum);
                                    hosts.put(key, vh);
                                }
                                vh.addWork(type, user, duccid, jobmem, qshares, null);
                            }
                            else {
                            	String message = nodeId.getShortName()+" not found?";
                            	logger.debug(methodName, w.getDuccId(), message);
                            }
                        }
                    }
                }
                break;
                
                default:
                    logger.warn(methodName, w.getDuccId(), "Received work of type ?", w.getDuccType());
                    break;
            }
        }

        logger.debug(methodName, null, "Generateing visualizaiton");
        Map<MachineInfo,NodeId> m = machineData.getMachines();

        for (Entry<MachineInfo, NodeId> entry : m.entrySet()) {
        	// 
            // This is for hosts that have no work on them so they didn't come in the work map
            //
        	MachineInfo mi = entry.getKey();
            // NOTE: the map changes all the time so the value may be gone.  This situation
            //       will be fixed one day but for now just forget the node, it will show up 
            //       next time we get here.            
            if ( mi == null ) continue;
            String s = mi.getName();
            if ( !mi.getStatus().equals("up") ) continue; // filter non-up nodes
            String key = strip(s);             // our key, possibly with domain stripped
            if ( ! hosts.containsKey(key) ) {
                VisualizedHost vh = new VisualizedHost(mi, nc.getQuantumForNode(s));
                hosts.put(key, vh);
            }
        }

        int total_gb = 0;
        Markup markup = new Markup();
        VisualizedHost[] sorted = hosts.values().toArray(new VisualizedHost[hosts.size()]);
        Arrays.sort(sorted, new HostSorter());
        for ( VisualizedHost vh : sorted ) {
            vh.toSvg(markup);
            total_gb += vh.countRam();
        }
        String page = markup.close();

        int unoccupied_gb = total_gb - (job_gb + pop_gb + service_gb + reservation_gb);

		visualization = 
            "<html>" + 
            "<div id=\"viz-header\" style=\"text-align:center\">" +

            "Sort By " +
            "<i onclick=\"ducc_viz_node_sorter('size')\" id=\"ducc-viz-sort-size\" style=\"color:red\">Size </i>" +
            "<i onclick=\"ducc_viz_node_sorter('name')\" id=\"ducc-viz-sort-name\"\">Name</i>" +
            "</br>" +
            "<b>Memory Available:</b> " + total_gb +
            "GB, <b>In use: </b>" + 
            "<b><i>Jobs:</i></b> " + (job_gb) +
            "GB, <b><i>Services:</i></b> " + (service_gb) +
            "GB, <b><i>Managed Reservations:</i></b> " + (pop_gb) +
            "GB, <b><i>Reservations:</i></b> " + (reservation_gb) +
            "GB, <b><i>Unoccupied:</i></b> " + (unoccupied_gb) +
            "GB</div>" +
            "<br>" +
            //"<div id=\"nodelist\" style=\"background-color:e5e5e5\">" +
            "<div id=\"nodelist\" style=\"background-color:eeeeee;padding:3\">" +
            page +
            "</div>" +
            "<script>" +
            "ducc_viz_onreload();" +
            "</script>" +
            "</html>";
        // logger.info(methodName, null, "Size of node visualization:", visualization.length());
        hosts = null;
	}

    /**
     * Possibly strip domain name from hostname
     */
    static String strip(String n)
    {
        if ( strip_domain ) {
            int ndx = n.indexOf(".");
            if ( ndx >= 0) {
            	n = n.substring(0, ndx);
            }
        }
        return n;
    }

	public void update(OrchestratorStateDuccEvent ev)
    {
        String methodName = "update";
        logger.debug(methodName, null, "Received Orchestrator Event");
        //long currentUpdate = System.currentTimeMillis();

        // if ( currentUpdate - lastUpdate > update_interval ) {
        if ( true ) {          // for debug, never skip
            generateVisualization(ev);
            //lastUpdate = currentUpdate;
        } 
        //else {
            //logger.debug(methodName, null, "Skipping visualization");
        //}
    }

    /**
     * The contract:
     * - when equals returns true,  compare must return 0
     * - when equals returns false, compare must return 1 or -1
     */
    private static class HostSorter
        implements Comparator<VisualizedHost>
    {
        public int compare(VisualizedHost h1, VisualizedHost h2)
        {
            if ( h1 == h2 ) return 0;
            if ( h1.equals(h2) ) return 0;       
            if ( h2.mem_reserve == h1.mem_reserve ) return h1.name.compareTo(h2.name);
            return h2.mem_reserve - h1.mem_reserve;
        }
    }

}
