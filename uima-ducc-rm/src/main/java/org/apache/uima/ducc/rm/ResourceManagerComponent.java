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
package org.apache.uima.ducc.rm;

import java.util.Timer;
import java.util.TimerTask;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.uima.ducc.common.NodeIdentity;
import org.apache.uima.ducc.common.admin.event.DuccAdminEvent;
import org.apache.uima.ducc.common.admin.event.RmAdminQLoad;
import org.apache.uima.ducc.common.admin.event.RmAdminQOccupancy;
import org.apache.uima.ducc.common.admin.event.RmAdminReconfigure;
import org.apache.uima.ducc.common.admin.event.RmAdminReply;
import org.apache.uima.ducc.common.admin.event.RmAdminVaryOff;
import org.apache.uima.ducc.common.admin.event.RmAdminVaryOn;
import org.apache.uima.ducc.common.admin.event.RmAdminVaryReply;
import org.apache.uima.ducc.common.boot.DuccDaemonRuntimeProperties;
import org.apache.uima.ducc.common.boot.DuccDaemonRuntimeProperties.DaemonName;
import org.apache.uima.ducc.common.component.AbstractDuccComponent;
import org.apache.uima.ducc.common.head.IDuccHead;
import org.apache.uima.ducc.common.head.IDuccHead.DuccHeadTransition;
import org.apache.uima.ducc.common.main.DuccService;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.IDuccLoggerComponents.Daemon;
import org.apache.uima.ducc.common.utils.SystemPropertyResolver;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.rm.scheduler.ISchedulerMain;
import org.apache.uima.ducc.rm.scheduler.JobManagerUpdate;
import org.apache.uima.ducc.rm.scheduler.SchedConstants;
import org.apache.uima.ducc.rm.scheduler.Scheduler;
import org.apache.uima.ducc.transport.dispatcher.DuccEventDispatcher;
import org.apache.uima.ducc.transport.event.DaemonDuccEvent;
import org.apache.uima.ducc.transport.event.DuccEvent.EventType;
import org.apache.uima.ducc.transport.event.RmStateDuccEvent;
import org.apache.uima.ducc.transport.event.common.DuccWorkMap;
import org.apache.uima.ducc.transport.event.common.IDuccWorkMap;


public class ResourceManagerComponent 
    extends AbstractDuccComponent
    implements ResourceManager,
               SchedConstants,
               Runnable
{
    private static DuccLogger logger = DuccLogger.getLogger(ResourceManagerComponent.class, COMPONENT_NAME);
    private static DuccId jobid = null;
    
    int nodeStability;                // number of heartbeats from agent metrics we are allowed to miss before purging node
    int initStability;                // number of heartbeats from agent metrics we must wait for during init befor starting
    int nodeMetricsUpdateRate;
    int orPublishingRate;
    int minRmPublishingRate;
    boolean schedulerReady = false;

    ISchedulerMain scheduler;
    JobManagerConverter converter;

    // These guys are used to manage my own epoch
    int schedulingRatio = 1;
    // int schedulingEpoch = 60000;

    long lastSchedule = 0;
    DuccEventDispatcher eventDispatcher;
    String stateEndpoint;
    String stateChangeEndpoint;

    NodeStability stabilityManager = null;
    
    private IDuccHead dh = null;

    public ResourceManagerComponent(CamelContext context) {
        super("ResourceManager", context);
        this.scheduler = new Scheduler(this);          // UIMA-4142 pass 'this' in so we can reconfig (reread ducc.properties)
    }
    
    public ISchedulerMain getScheduler()
    {
        return this.scheduler;
    }

    public boolean isSchedulerReady()
    {
        return schedulerReady;
    }

    public void setNodeStability(NodeStability ns)
    {
        this.stabilityManager = ns;
    }
    public DuccLogger getLogger() {
        return logger; 
    }


    /**
     * Creates Camel Router for Ducc RM admin events.
     * 
     * @param endpoint
     *          - ducc admin endpoint
     * @param delegate
     *          - who to call when admin event arrives
     * @throws Exception
     */
    private void startRmAdminChannel(final String endpoint, final AbstractDuccComponent delegate)
        throws Exception 
    {
        getContext().addRoutes(new RouteBuilder() {
                public void configure() {
                    System.out.println("Configuring RM Admin Channel on Endpoint:" + endpoint);
                    onException(Exception.class).handled(true).process(new ErrorProcessor());
                    
                    from(endpoint).routeId("RMAdminRoute").unmarshal().xstream()
                         .process(new RmAdminEventProcessor(delegate));
                }
            });

	getContext().startRoute("RMAdminRoute");
        if (logger != null) {
            logger.info("startRMAdminChannel", null, "Admin Channel Activated on endpoint:" + endpoint);
        }
    }

    
    class RmAdminEventProcessor implements Processor 
    {
        final AbstractDuccComponent delegate;

        public RmAdminEventProcessor(final AbstractDuccComponent delegate) 
        {
            this.delegate = delegate;
        }

        public void process(final Exchange exchange) 
            throws Exception 
        {            
            String methodName = "RmAdminEventProcessor.process";
            Object body = exchange.getIn().getBody();
            logger.info(methodName, null, "Received Admin Message of Type:",  body.getClass().getName());

            RmAdminReply reply = null;
            if ( body instanceof DuccAdminEvent ) {
                DuccAdminEvent dae = (DuccAdminEvent) body;
                if (body instanceof RmAdminVaryOff) {
                    if ( ! validateAdministrator(dae) ) {
                        reply = new RmAdminVaryReply();
                        reply.setRc(false);
                        reply.setMessage("Not authorized");
                    } else {
                        RmAdminVaryOff vo = (RmAdminVaryOff) body;
                        reply = scheduler.varyoff(vo.getNodes());
                    }
                } else
                if (body instanceof RmAdminVaryOn) {
                    if ( ! validateAdministrator(dae) ) {
                        reply = new RmAdminVaryReply();
                        reply.setRc(false);
                        reply.setMessage("Not authorized");
                    } else {
                        RmAdminVaryOn vo = (RmAdminVaryOn) body;            	 
                        reply = scheduler.varyon(vo.getNodes());
                    }
                } else
                if (body instanceof RmAdminReconfigure) {    // UIMA-4142
                    if ( ! validateAdministrator(dae) ) {
                        reply = new RmAdminReply();
                        reply.setRc(false);
                        reply.setMessage("Not authorized");
                    } else {
                        reply = scheduler.reconfigure();
                    }
                } else
                if (body instanceof RmAdminQLoad) {
                    // not priveleged
                    reply = scheduler.queryLoad();
                } else
                if (body instanceof RmAdminQOccupancy) {
                    // not priveleged
                    reply = scheduler.queryOccupancy();
                } else {
                    logger.info(methodName, null, "Invalid admin command:", body.getClass().getName());
                    reply = new RmAdminReply();
                    reply.setMessage("Unrecognized RM admin request.");
                }
            } else {
                logger.info(methodName, null, "Invalid RM event:", body.getClass().getName());
                reply = new RmAdminReply();
                reply.setMessage("Unrecognized RM event.");
            }
            exchange.getIn().setBody(reply);
        }
    }
	
    /**
     * Tell Orchestrator about state change for recording into system-events.log
     */
    private void stateChange(EventType eventType) {
    	String methodName = "stateChange";
        try {
    		Daemon daemon = Daemon.ResourceManager;
    		NodeIdentity nodeIdentity = new NodeIdentity();
        	DaemonDuccEvent ev = new DaemonDuccEvent(daemon, eventType, nodeIdentity);
            eventDispatcher.dispatch(stateChangeEndpoint, ev, "");
            logger.info(methodName, null, stateChangeEndpoint, eventType.name(), nodeIdentity.getCanonicalName());
        }
    	catch(Exception e) {
    		logger.error(methodName, null, e);
    	}
    }
    
    public void start(DuccService service, String[] args)
        throws Exception
    {
    	String methodName = "start";
        converter = new JobManagerConverter(scheduler, stabilityManager);

        super.start(service, args);
        DuccDaemonRuntimeProperties.getInstance().boot(DaemonName.ResourceManager, super.getProcessJmxUrl());

        initStability         = SystemPropertyResolver.getIntProperty("ducc.rm.init.stability", DEFAULT_INIT_STABILITY_COUNT);
        nodeStability         = SystemPropertyResolver.getIntProperty("ducc.rm.node.stability", DEFAULT_STABILITY_COUNT);
        nodeMetricsUpdateRate = SystemPropertyResolver.getIntProperty("ducc.agent.node.metrics.publish.rate", DEFAULT_NODE_METRICS_RATE);
        schedulingRatio       = SystemPropertyResolver.getIntProperty("ducc.rm.state.publish.ratio", DEFAULT_SCHEDULING_RATIO);
        orPublishingRate      = SystemPropertyResolver.getIntProperty("ducc.orchestrator.state.publish.rate", DEFAULT_OR_PUBLISH_RATE);
        minRmPublishingRate   = orPublishingRate - DEFAULT_RM_PUBLISHING_SLOP;
        if ( minRmPublishingRate <=0 ) minRmPublishingRate = DEFAULT_RM_PUBLISHING_SLOP;        // somewhat arbitrary, but what else?

        String adminEndpoint         = System.getProperty("ducc.rm.admin.endpoint");
        if ( adminEndpoint == null ) {
            logger.warn(methodName, null, "No admin endpoint configured.  Not starting admin channel.");
        } else {
            startRmAdminChannel(adminEndpoint, this);
        }

        scheduler.init();
        
        startStabilityTimer();

        // Start the main processing loop
        Thread rmThread = new Thread(this);
        rmThread.setDaemon(true);
        rmThread.start();

        schedulerReady = true;
        stateChange(EventType.BOOT);
    }

    public RmStateDuccEvent getState() throws Exception 
    {
        String methodName = "getState";        
        JobManagerUpdate jobManagerUpdate = null;

        try {
            logger.info(methodName, null, "-------------------- Entering scheduling loop --------------------");
            jobManagerUpdate = scheduler.schedule();                        
            logger.info(methodName, null, "-------------------- Scheduling loop returns  --------------------");
        } catch (Exception e1) {
            logger.error(methodName, null, "Error running scheduler:", e1);
        }
        
        try {
        	if ( jobManagerUpdate != null ) { 
        		return converter.createState(jobManagerUpdate);
        	}
        } catch ( Exception e ) {
            logger.error(methodName, null, "Error converting state for Orchestrator", e);
        }
        return null;
    }

    public void stop()
    	throws Exception
    {
    	String methodName = "stop";
        logger.info(methodName, null, "Stopping RM database connection");
        stateChange(EventType.SHUTDOWN);
        scheduler.stop();
        super.stop();
    }

    public void setTransportConfiguration(DuccEventDispatcher eventDispatcher, String stateEndpoint, String stateChangeEndpoint)
    {
        this.eventDispatcher = eventDispatcher;
        this.stateEndpoint = stateEndpoint;
        this.stateChangeEndpoint = stateChangeEndpoint;
    }

    public void run()
    {
        while ( true ) {
            runScheduler();
        }
    }

    long epoch_counter = 0;
    public void runScheduler()
    //public void runScheduler()
    {
        String methodName = "runScheduler";
        JobManagerUpdate jobManagerUpdate;

        while ( true ) {

            synchronized(this) {
                try {
                    //Thread.sleep(schedulingEpoch);                               // and linger a while
                    wait();
                } catch (InterruptedException e) {
                    logger.info(methodName, null, "Scheduling wait interrupted, executing out-of-band epoch.");
                }
            
                try {
                    // logger.info(methodName, null, "Publishing RM state to", stateEndpoint);
                    logger.info(methodName, null, "--------", epoch_counter, "------- Entering scheduling loop --------------------");
                    
                    jobManagerUpdate = scheduler.schedule();          
                    if ( jobManagerUpdate != null ) {             // returns null while waiting for node stability
                        RmStateDuccEvent ev = converter.createState(jobManagerUpdate);
                        eventDispatcher.dispatch(stateEndpoint, ev, "");  // tell the world what is scheduled (note empty string)
                }
                    
                    logger.info(methodName, null, "--------", epoch_counter, "------- Scheduling loop returns  --------------------");
                } catch (Throwable e1) {
                    logger.fatal(methodName, null, e1);
                }
            }
            
        }
    }
    
//     public void nodeArrives(Node n)
//     {
//         String methodName = "nodeArrives";        
//         try {
//             if ( ! schedulerReady ) {
//                 logger.warn(methodName, null, "Ignoring node update, scheduler is still booting.");
//                 return;
//             }

//             scheduler.nodeArrives(n);
//         } catch ( Exception e ) {
//             logger.error(methodName, null, "Exception processing Agent event for node", n, ":\n", e);
//         }
//     }

    int stabilityCount = 0;
    Timer stabilityTimer = new Timer();
    protected void startStabilityTimer() 
    {
    	String methodName = "startStabilityTimer";
    	logger.info(methodName, null, "Starting stability timer[", nodeMetricsUpdateRate, "] init stability[", initStability, "]");
        stabilityTimer.schedule(new StabilityTask(), nodeMetricsUpdateRate);
    }

    private class StabilityTask
        extends TimerTask
    {
        public void run()
        {
            if ( ++stabilityCount < initStability ) {
                logger.info("NodeStability", null, "NodeStability wait:  Countdown", stabilityCount, ":", initStability);
                stabilityTimer.schedule(new StabilityTask(), nodeMetricsUpdateRate);
            } else {
                stabilityTimer = null;              // done with it, discard it
                scheduler.start();
                logger.info("NodeStability", null, "Initial node stability reached: scheduler started.");
            }
        }
    }

    private IDuccHead getDuccHead() {
    	if(dh == null) {
    		dh = DuccHead.getInstance();
    		if(dh.is_ducc_head_backup()) {
    			stateChange(EventType.INIT_AS_BACKUP);
    		}
    		else {
    			stateChange(EventType.INIT_AS_MASTER);
    		}
    	}
    	return dh;
    }
    
    private IDuccWorkMap adjustMap(IDuccWorkMap map) {
		String location = "adjustMap";
		IDuccWorkMap retVal = map;
		DuccId jobid = null;
		DuccHeadTransition transition = getDuccHead().transition();
		switch(transition) {
		case master_to_master:
			logger.debug(location, jobid, "ducc head == master");
			break;
		case master_to_backup:
			logger.warn(location, jobid, "ducc head -> backup");
			stateChange(EventType.SWITCH_TO_BACKUP);
			retVal = new DuccWorkMap();
			break;
		case backup_to_master:
			logger.warn(location, jobid, "ducc head -> master");
			stateChange(EventType.SWITCH_TO_MASTER);
			break;
		case backup_to_backup:
			logger.debug(location, jobid, "ducc head == backup");
			retVal = new DuccWorkMap();
			break;
		default:
			logger.debug(location, jobid, "ducc head == unspecified");
			break;
		}
		return retVal;
    }
    
    private void handleNewMaster() {
    	String methodName = "handleNewMaster";
    	try {
    		DuccHeadTransition dh_transition = getDuccHead().transition();
    		logger.info(methodName, jobid, dh_transition);
    		switch(dh_transition) {
    		case master_to_backup:
    			stateChange(EventType.SWITCH_TO_BACKUP);
    			logger.warn(methodName, jobid, "ducc head -> backup");
    			break;
    		case backup_to_master:
    			stateChange(EventType.SWITCH_TO_MASTER);
    			logger.warn(methodName, jobid, "ducc head -> master");
    			scheduler.reconfigure();
    			break;
    		case master_to_master:
    			logger.debug(methodName, jobid, "ducc head == master");
    			break;
    		case backup_to_backup:
    			logger.debug(methodName, jobid, "ducc head == backup");
    			break;
    		default:
    			logger.debug(methodName, jobid, "ducc head == unspecified");
    			break;
    		}
    	}
    	catch(Exception e) {
    		logger.error(methodName, jobid, e);
    	}
    }
    
    public void onOrchestratorStateUpdate(IDuccWorkMap map)
    {
        String methodName = "onJobManagerStateUpdate";

        try {
            logger.info(methodName, null, "-------> OR state arrives");
            synchronized(this) {
                // If the OR publications come too fast just ignore them.
                // We try to set the minSchedulingRate to be something reasonably less than
                // the OR rate in order to be as responsive as possible.
                long now = System.currentTimeMillis();
                if ( now - lastSchedule >= minRmPublishingRate ) {
                	handleNewMaster();
                	IDuccWorkMap adjustedMap = adjustMap(map);
                    converter.eventArrives(adjustedMap);
                    if ( ((++epoch_counter) % schedulingRatio) == 0 ) {
                        notify();
                    }
                    lastSchedule = now;
                } else {
                    logger.warn(methodName, null, "-------> OR publication ignored, arrived too soon (less than", minRmPublishingRate, "delay). Delay was", (now-lastSchedule));
                }
            }
        } catch ( Throwable e ) {
            logger.error(methodName, null, "Excepton processing Orchestrator event:", e);
        }
    }

}
