package org.apache.uima.ducc.ws.server.nodeviz;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.uima.ducc.common.Node;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.SystemPropertyResolver;
import org.apache.uima.ducc.common.utils.Version;
import org.apache.uima.ducc.transport.event.OrchestratorStateDuccEvent;
import org.apache.uima.ducc.transport.event.common.DuccWorkMap;
import org.apache.uima.ducc.transport.event.common.IDuccProcess;
import org.apache.uima.ducc.transport.event.common.IDuccProcessMap;
import org.apache.uima.ducc.transport.event.common.IDuccReservation;
import org.apache.uima.ducc.transport.event.common.IDuccReservationMap;
import org.apache.uima.ducc.transport.event.common.IDuccSchedulingInfo;
import org.apache.uima.ducc.transport.event.common.IDuccStandardInfo;
import org.apache.uima.ducc.transport.event.common.IDuccTypes.DuccType;
import org.apache.uima.ducc.transport.event.common.IDuccWork;
import org.apache.uima.ducc.transport.event.common.IDuccWorkExecutable;
import org.apache.uima.ducc.transport.event.common.IDuccWorkReservation;
import org.apache.uima.ducc.transport.event.common.IDuccWorkService;
import org.apache.uima.ducc.transport.event.common.IDuccWorkService.ServiceDeploymentType;
import org.apache.uima.ducc.transport.event.common.IProcessState.ProcessState;
import org.apache.uima.ducc.ws.DuccMachinesData;
import org.apache.uima.ducc.ws.IListenerOrchestrator;
import org.apache.uima.ducc.ws.MachineInfo;
import org.apache.uima.ducc.ws.server.DuccListeners;

public class NodeViz
    implements IListenerOrchestrator
{
    private static DuccLogger logger = DuccLoggerComponents.getWsLogger(NodeViz.class.getName());

    private DuccMachinesData machineData;              // handle to static machine information

    //private long lastUpdate = 0;                       // time of last orchestrator state
    private String visualization;                      // cached visualization
    private long update_interval = 60000;              // Only gen a new viz every 'this long'

    private String version = "1.1.0";
    static int quantum = 4;
    static String wshost = "";
    static String wsport = "42133";
    static boolean strip_domain = true;

	public NodeViz()
	{
        String methodName = "NodeViz";

        update_interval = SystemPropertyResolver.getLongProperty("ducc.viz.update.interval", update_interval);                                
        quantum         = SystemPropertyResolver.getIntProperty("ducc.rm.share.quantum", quantum);

        wshost          = SystemPropertyResolver.getStringProperty("ducc.ws.node", System.getProperty("ducc.head"));
        wsport          = SystemPropertyResolver.getStringProperty("ducc.ws.port", wsport);
        strip_domain   = SystemPropertyResolver.getBooleanProperty("ducc.ws.visualization.strip.domain", true);
        
        logger.info(methodName, null, "------------------------------------------------------------------------------------");
        logger.info(methodName, null, "Node Visualization starting:");
        logger.info(methodName, null, "    DUCC home               : ", System.getProperty("DUCC_HOME"));
        logger.info(methodName, null, "    ActiveMQ URL            : ", System.getProperty("ducc.broker.url"));
        logger.info(methodName, null, "Using Share Quantum         : ", quantum);
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

        DuccWorkMap jobmap = ev.getWorkMap();

        int job_shares = 0;
        int service_shares = 0;
        int pop_shares = 0;
        int reservation_shares = 0;

        // first step, generate the viz from the OR map which seems to have everything we need
        // next stop,  walk the machines list and generate empty node for any machine in that list 
        //             that had no work on it
        // lext step, walk the machines data and overlay a graphic for any node that is 'down' or 'defined'
        //              move 'down' hosts with jobs on them to the front
        //              move all onther 'not up' hosts to the end
        // finally, walk the list and make them render

        for ( IDuccWork w : jobmap.values() ) {
            DuccType type = w.getDuccType();
            String service_endpoint = null;
            // If it looks service-y and os of deployment type 'other' it's a Pop.
            if ( type == DuccType.Service ) {
                IDuccWorkService dws = (IDuccWorkService) w;
                if ( dws.getServiceDeploymentType() == ServiceDeploymentType.other) {
                    type = DuccType.Pop;
                } else {
                    service_endpoint = dws.getServiceEndpoint();
                }
            }
            
            if ( ! w.isSchedulable() ) {
                logger.debug(methodName, w.getDuccId(), "Ignoring unschedulable work:", w.getDuccType(), ":", w.getStateObject());
                continue;
            }
                        
            IDuccStandardInfo si      = w.getStandardInfo();
            IDuccSchedulingInfo sti   = w.getSchedulingInfo();

            String            user    = si.getUser();
            String            duccid  = Long.toString(w.getDuccId().getFriendly());
            int               jobmem  = Integer.parseInt(sti.getShareMemorySize());
            int               qshares = jobmem / quantum;
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
                                job_shares += qshares;
                                break;
                            case Pop:
                                pop_shares += qshares;
                                break;
                            case Service:
                                service_shares += qshares;
                                break;
                        }

                        if ( n != null ) {
                            String key = n.getNodeIdentity().getName();
                            VisualizedHost vh = hosts.get(key);
                            if ( vh == null ) {
                                vh = new VisualizedHost(n, quantum);
                                hosts.put(key, vh);
                            }

                            vh.addWork(type, user, duccid, jobmem, qshares, service_endpoint);
                        }
                    }
                }
                break;
                                    
                case Reservation: 
                {
                    IDuccWorkReservation de = (IDuccWorkReservation) w;
                    IDuccReservationMap  rm = de.getReservationMap();
                    
                    logger.debug(methodName, w.getDuccId(), "Receive:", type, w.getStateObject(), "processes[", rm.size(), "] Completed:", w.isCompleted());
                    reservation_shares += qshares;
                    
                    for ( IDuccReservation r: rm.values()) {
                        Node n = r.getNode();                        
                        if ( n == null ) {
                            logger.debug(methodName, w.getDuccId(),  "Node [N/A] mem[N/A");
                        } else {
                            String key = n.getNodeIdentity().getName();
                            VisualizedHost vh = hosts.get(key);
                            if ( vh == null ) {
                                vh = new VisualizedHost(n, quantum);
                                hosts.put(key, vh);
                            }
                            vh.addWork(type, user, duccid, jobmem, qshares, null);
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
        ConcurrentSkipListMap<String,MachineInfo> m = machineData.getMachines();


        for (String s : m.keySet()) {
            if ( ! hosts.containsKey(s) ) {
                VisualizedHost vh = new VisualizedHost(m.get(s), quantum);
                hosts.put(s, vh);
            }
        }

        int total_shares = 0;
        int total_ram = 0;
        Markup markup = new Markup();
        VisualizedHost[] sorted = hosts.values().toArray(new VisualizedHost[hosts.size()]);
        Arrays.sort(sorted, new HostSorter());
        for ( VisualizedHost vh : sorted ) {
            vh.toSvg(markup);
            total_shares += vh.countShares();
            total_ram += vh.countRam();
        }
        String page = markup.close();

        int unoccupied_shares = total_shares - (job_shares + pop_shares + service_shares + reservation_shares);

		visualization = 
            "<html>" + 
            "<div id=\"viz-header\" style=\"text-align:center\">" +
            "Sort By " +
            "<i onclick=\"ducc_viz_node_sorter('size')\" id=\"ducc-viz-sort-size\" style=\"color:red\">Size </i>" +
            "<i onclick=\"ducc_viz_node_sorter('name')\" id=\"ducc-viz-sort-name\"\">Name</i>" +
            "</br>" +
            "<b>Shares of size " + quantum + "GB: </b>" + total_shares + 
            ", <b>Jobs: </b>" + job_shares +
            ", <b>Services: </b>" + service_shares +
            ", <b>Managed Reservations: </b>" + pop_shares +
            ", <b>Reservations: </b>" + reservation_shares +
            ", <b>Unoccupied: </b>" + unoccupied_shares +
            "<br><i><small>" +
            "<b>RAM Total:</b> " + total_ram +
            "GB, <b>For shares:</b> " + (total_shares * quantum) +
            "GB, <b>Jobs:</b> " + (job_shares * quantum) +
            "GB, <b>Services:</b> " + (service_shares * quantum) +
            "GB, <b>Managed Reservations:</b> " + (pop_shares * quantum) +
            "GB, <b>Reservations:</b> " + (reservation_shares * quantum) +
            "GB, <b>Unoccupied:</b> " + (unoccupied_shares * quantum) +
            "</small></i>" +
            "</div>" +
            "<br>" +
            //"<div id=\"nodelist\" style=\"background-color:e5e5e5\">" +
            "<div id=\"nodelist\" style=\"background-color:eeeeee\">" +
            page +
            "</div>" +
            "<script>" +
            "ducc_viz_onreload();" +
            "</script>" +
            "</html>";
        // logger.info(methodName, null, "Size of node visualization:", visualization.length());
        hosts = null;
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
            if ( h2.mem == h1.mem ) return h1.name.compareTo(h2.name);
            return h2.mem - h1.mem;
        }
    }

}
