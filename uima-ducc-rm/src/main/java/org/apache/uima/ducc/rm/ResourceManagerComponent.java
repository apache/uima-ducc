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
import org.apache.uima.ducc.common.boot.DuccDaemonRuntimeProperties;
import org.apache.uima.ducc.common.boot.DuccDaemonRuntimeProperties.DaemonName;
import org.apache.uima.ducc.common.component.AbstractDuccComponent;
import org.apache.uima.ducc.common.main.DuccService;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.SystemPropertyResolver;
import org.apache.uima.ducc.rm.scheduler.ISchedulerMain;
import org.apache.uima.ducc.rm.scheduler.JobManagerUpdate;
import org.apache.uima.ducc.rm.scheduler.SchedConstants;
import org.apache.uima.ducc.rm.scheduler.Scheduler;
import org.apache.uima.ducc.transport.dispatcher.DuccEventDispatcher;
import org.apache.uima.ducc.transport.event.RmStateDuccEvent;
import org.apache.uima.ducc.transport.event.common.DuccWorkMap;


public class ResourceManagerComponent 
    extends AbstractDuccComponent
    implements ResourceManager,
               SchedConstants,
               Runnable
{
    private static DuccLogger logger = DuccLogger.getLogger(ResourceManagerComponent.class, COMPONENT_NAME);

    int nodeStability;                // number of heartbeats from agent metrics we are allowed to miss before purging node
    int initStability;                // number of heartbeats from agent metrics we must wait for during init befor starting
    int nodeMetricsUpdateRate;
    boolean schedulerReady = false;

    ISchedulerMain scheduler;
    JobManagerConverter converter;

    // These guys are used to manage my own epoch
    int schedulingRatio = 6;
    int schedulingEpoch = 60000;
    DuccEventDispatcher eventDispatcher;
    String stateEndpoint;

    NodeStability stabilityManager = null;

    public ResourceManagerComponent(CamelContext context) {
        super("ResourceManager", context);
        this.scheduler = new Scheduler();
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

    public void start(DuccService service, String[] args)
        throws Exception
    {
        super.start(service, args);
        DuccDaemonRuntimeProperties.getInstance().boot(DaemonName.ResourceManager,getProcessJmxUrl());

        converter = new JobManagerConverter(scheduler, stabilityManager);

        initStability         = SystemPropertyResolver.getIntProperty("ducc.rm.init.stability", DEFAULT_INIT_STABILITY_COUNT);
        nodeStability         = SystemPropertyResolver.getIntProperty("ducc.rm.node.stability", DEFAULT_STABILITY_COUNT);
        nodeMetricsUpdateRate = SystemPropertyResolver.getIntProperty("ducc.agent.node.metrics.publish.rate", DEFAULT_NODE_METRICS_RATE);
        schedulingRatio       = SystemPropertyResolver.getIntProperty("ducc.rm.state.publish.ratio", DEFAULT_SCHEDULING_RATIO);
        schedulingEpoch       = SystemPropertyResolver.getIntProperty("ducc.rm.state.publish.rate", DEFAULT_SCHEDULING_RATE);
        scheduler.init();

        startStabilityTimer();

        // Start the main processing loop
        Thread rmThread = new Thread(this);
        rmThread.setDaemon(true);
        rmThread.start();

        schedulerReady = true;
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

    public void setTransportConfiguration(DuccEventDispatcher eventDispatcher, String endpoint)
    {
        this.eventDispatcher = eventDispatcher;
        this.stateEndpoint = endpoint;
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

            try {
                Thread.sleep(schedulingEpoch);                               // and linger a while
                //wait();
            } catch (InterruptedException e) {
            	logger.info(methodName, null, "Scheduling wait interrupted, executing out-of-band epoch.");
            }
            
            try {
                // logger.info(methodName, null, "Publishing RM state to", stateEndpoint);
                logger.info(methodName, null, "--------", ++epoch_counter, "------- Entering scheduling loop --------------------");

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

    public void onOrchestratorStateUpdate(DuccWorkMap map)
    {
        String methodName = "onJobManagerStateUpdate";
        try {
            logger.info(methodName, null, "-------> OR state arrives");
            converter.eventArrives(map);
            //if ( ((epoch_counter++) % schedulingRatio) == 0 ) {
            //    notify();
            //}
        } catch ( Throwable e ) {
            logger.error(methodName, null, "Excepton processing Orchestrator event:", e);
        }
    }


}
