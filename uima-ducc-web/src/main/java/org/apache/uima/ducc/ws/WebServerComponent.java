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
package org.apache.uima.ducc.ws;

import java.io.File;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.camel.CamelContext;
import org.apache.uima.ducc.common.IDuccEnv;
import org.apache.uima.ducc.common.NodeIdentity;
import org.apache.uima.ducc.common.boot.DuccDaemonRuntimeProperties;
import org.apache.uima.ducc.common.boot.DuccDaemonRuntimeProperties.DaemonName;
import org.apache.uima.ducc.common.component.AbstractDuccComponent;
import org.apache.uima.ducc.common.config.CommonConfiguration;
import org.apache.uima.ducc.common.internationalization.Messages;
import org.apache.uima.ducc.common.main.DuccService;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.IDuccLoggerComponents.Daemon;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.dispatcher.DuccEventDispatcher;
import org.apache.uima.ducc.transport.event.DaemonDuccEvent;
import org.apache.uima.ducc.transport.event.DuccEvent.EventType;
import org.apache.uima.ducc.transport.event.DuccJobsStateEvent;
import org.apache.uima.ducc.transport.event.NodeMetricsUpdateDuccEvent;
import org.apache.uima.ducc.transport.event.OrchestratorStateDuccEvent;
import org.apache.uima.ducc.transport.event.PmStateDuccEvent;
import org.apache.uima.ducc.transport.event.RmStateDuccEvent;
import org.apache.uima.ducc.transport.event.SmHeartbeatDuccEvent;
import org.apache.uima.ducc.transport.event.SmStateDuccEvent;
import org.apache.uima.ducc.transport.event.common.IDuccWorkMap;
import org.apache.uima.ducc.ws.registry.ServicesRegistry;
import org.apache.uima.ducc.ws.self.message.WebServerStateDuccEvent;
import org.apache.uima.ducc.ws.server.DuccListeners;
import org.apache.uima.ducc.ws.server.DuccWebServer;
import org.apache.uima.ducc.ws.utils.DatedNodeMetricsUpdateDuccEvent;


public class WebServerComponent extends AbstractDuccComponent 
implements IWebServer {
	
	private static DuccLogger duccLogger = DuccLogger.getLogger(WebServerComponent.class);
	
	private static DuccId jobid = null;
	
	private DuccWebServer duccWebServer = null;
	private Messages duccMsg= Messages.getInstance();
	
	private static AtomicInteger jobCount = new AtomicInteger(0);
	private static AtomicInteger serviceCount = new AtomicInteger(0);
	private static AtomicInteger reservationCount = new AtomicInteger(0);
	
	private static AtomicLong updateLast = new AtomicLong(System.currentTimeMillis());
	public static long updateIntervalSecondsInitial = 5;
	public static long updateIntervalSecondsNormal = 60;
	public static AtomicLong updateIntervalCount = new AtomicLong(0);
	public static long updateIntervalLimit = 12;
	
	private DuccEventDispatcher eventDispatcher;
	private String stateChangeEndpoint;

	public WebServerComponent(CamelContext context, CommonConfiguration common) {
		super("WebServer",context);
		String methodName = "WebServerComponent";
		duccLogger.info(methodName, jobid, "##### boot #####");
		
		String cp = System.getProperty("java.class.path");
		String[] cpArray = cp.split(":");
		int lc = 0;
		for(String line : cpArray) {
			duccLogger.trace(methodName, jobid, "cp."+lc+" "+line);
			lc++;
		}
		
		String[] propertyNames = { "ducc.broker.url" };
		for(String property : propertyNames) {
			duccLogger.info(methodName, jobid, property+"="+System.getProperty(property));
		}
		duccLogger.info(methodName, jobid, System.getProperty("ducc.broker.url"));
		duccWebServer = new DuccWebServer(common);
		init();
	}
	
    /**
     * Tell Orchestrator about state change for recording into system-events.log
     */
    private void stateChange(EventType eventType) {
    	String methodName = "stateChange";
        try {
    		Daemon daemon = Daemon.WebServer;
    		NodeIdentity nodeIdentity = new NodeIdentity();
        	DaemonDuccEvent ev = new DaemonDuccEvent(daemon, eventType, nodeIdentity);
            eventDispatcher.dispatch(stateChangeEndpoint, ev, "");
            duccLogger.info(methodName, null, stateChangeEndpoint, eventType.name(), nodeIdentity.getName());
        }
    	catch(Exception e) {
    		duccLogger.error(methodName, null, e);
    	}
    }

	public void start(DuccService service, String[] args) throws Exception {
		super.start(service, args);
		DuccDaemonRuntimeProperties.getInstance().boot(DaemonName.Webserver,getProcessJmxUrl());
        stateChange(EventType.BOOT);
	}
	
	public void stop() throws Exception {
		stateChange(EventType.SHUTDOWN);
		super.stop();
	}
	
	public void setDuccEventDispatcher(DuccEventDispatcher eventDispatcher) {
		this.eventDispatcher = eventDispatcher;
	}
	
	public void setstateChangeEndpoint(String stateChangeEndpoint) {
		this.stateChangeEndpoint = stateChangeEndpoint;
	}
	
	public DuccLogger getLogger() {
	    return duccLogger;
	  }
	public void webServerStart() {
		String methodName = "webServerStart";
		try {
			duccWebServer.start();
			duccLogger.info(methodName, jobid, "webserver started");
		} catch (Exception e) {
			duccLogger.error(methodName, jobid, e);
		}
	}
	
	public void webServerStop() {
		String methodName = "webServerStop";
		try {
			duccWebServer.stop();
			duccLogger.info(methodName, jobid, "webserver stopped");
		} catch (Exception e) {
			duccLogger.error(methodName, jobid, e);
		}
	}
	
	private void init() {
		String methodName = "init";
		duccLogger.trace(methodName, jobid, duccMsg.fetch("enter"));
		File file = new File(IDuccEnv.DUCC_LOGS_WEBSERVER_DIR);
		file.mkdirs();
		webServerStart();
		duccLogger.trace(methodName, jobid, duccMsg.fetch("exit"));
	}
	
	
	public void update(OrchestratorStateDuccEvent duccEvent) {
		String methodName = "update";
		duccLogger.trace(methodName, jobid, duccMsg.fetch("enter"));
		duccLogger.debug(methodName, jobid, duccMsg.fetchLabel("received")+"OrchestratorStateDuccEvent");
		DuccDaemonsData.getInstance().put(duccEvent);
		IDuccWorkMap wm = duccEvent.getWorkMap();
		boolean change = false;
		int count;
		count = wm.getJobCount();
		if(count != jobCount.get()) {
			jobCount.set(count);
			change = true;
		}
		count = wm.getReservationCount();
		if(count != reservationCount.get()) {
			reservationCount.set(count);
			change = true;
		}
		count = wm.getServiceCount();
		if(count !=serviceCount.get()) {
			serviceCount.set(count);
			change = true;
		}
		if(change) {
			duccLogger.info(methodName, jobid, duccMsg.fetchLabel("jobs")+jobCount.get()+" "+duccMsg.fetchLabel("reservations")+reservationCount.get()+" "+duccMsg.fetchLabel("services")+serviceCount.get());
		}
		DuccData.getInstance().put(wm);
		DuccPlugins.getInstance().update(wm);
		DuccListeners.getInstance().update(duccEvent);
		Map<String,Long> map = Distiller.deriveMachineMemoryInUse(duccEvent);
		report(map);
		duccLogger.trace(methodName, jobid, duccMsg.fetch("exit"));
	}

	private void report(Map<String,Long> map) {
		String location = "report";
		for(Entry<String, Long> entry : map.entrySet()) {
			duccLogger.trace(location, jobid, entry.getKey()+"="+entry.getValue());
		}
	}
	
	/**
	 * Sort machines if interval has elapsed (60 seconds) 
	 * 
	 * Note: Use an initial short interval (e.g. every 5 seconds)
	 * when the Web Server first boots in order to populate quickly.
	 * After N (e.g. 12) quick recalculations, revert to the normal
	 * interval (e.g. every 60 seconds).
	 */
	private void sortMachines() {
		String methodName = "sortMachines";
		long last = updateLast.get();
		long updateIntervalMilliSeconds = updateIntervalSecondsNormal * 1000;
		if(updateIntervalCount.get() < updateIntervalLimit) {
			updateIntervalMilliSeconds = updateIntervalSecondsInitial * 1000;
		}
		long deadline = last + updateIntervalMilliSeconds;
		long now = System.currentTimeMillis();
		if(now > deadline) {
			boolean success = updateLast.compareAndSet(last, now);
			if(success) {
				DuccMachinesData.getInstance().updateSortedMachines();
				updateIntervalCount.incrementAndGet();
				duccLogger.trace(methodName, jobid, "count: "+updateIntervalCount.get());
			}
			else {
				duccLogger.trace(methodName, jobid, "missed: "+"last="+last+" "+"now="+now);
			}
		}
		else {
			duccLogger.trace(methodName, jobid, "togo: "+(deadline - now)/1000);
		}
	}
	
	public void update(NodeMetricsUpdateDuccEvent duccEvent) {
		String methodName = "update";
		duccLogger.trace(methodName, jobid, duccMsg.fetch("enter"));
		duccLogger.trace(methodName, jobid, duccMsg.fetchLabel("received")+"NodeMetricsUpdateDuccEvent");
		DuccMachinesData dmd = DuccMachinesData.getInstance();
		DatedNodeMetricsUpdateDuccEvent datedEvent = new DatedNodeMetricsUpdateDuccEvent(duccEvent);
		dmd.put(datedEvent);
		sortMachines();
		duccLogger.trace(methodName, jobid, duccMsg.fetch("exit"));
	}

	
	public void update(RmStateDuccEvent duccEvent) {
		String methodName = "update";
		duccLogger.trace(methodName, jobid, duccMsg.fetch("enter"));
		duccLogger.info(methodName, jobid, duccMsg.fetchLabel("received")+"RmStateDuccEvent");
		DuccDaemonsData.getInstance().put(duccEvent);
		duccLogger.trace(methodName, jobid, duccMsg.fetch("exit"));
	}
	
	
	public void update(SmStateDuccEvent duccEvent) {
		String methodName = "update";
		duccLogger.trace(methodName, jobid, duccMsg.fetch("enter"));
		duccLogger.info(methodName, jobid, duccMsg.fetchLabel("received")+"SmStateDuccEvent");
		DuccDaemonsData.getInstance().put(duccEvent);
		ServicesRegistry.getInstance().update();
		duccLogger.trace(methodName, jobid, duccMsg.fetch("exit"));
	}
	
	
	public void update(DuccJobsStateEvent duccEvent) {
		String methodName = "update";
		duccLogger.trace(methodName, jobid, duccMsg.fetch("enter"));
		duccLogger.debug(methodName, jobid, duccMsg.fetchLabel("received")+"DuccJobsStateEvent");
		DuccDaemonsData.getInstance().put(duccEvent);
		duccLogger.trace(methodName, jobid, duccMsg.fetch("exit"));
	}
	
	
	public void update(PmStateDuccEvent duccEvent) {
		String methodName = "update";
		duccLogger.trace(methodName, jobid, duccMsg.fetch("enter"));
		duccLogger.info(methodName, jobid, duccMsg.fetchLabel("received")+"PmStateDuccEvent");
		DuccDaemonsData.getInstance().put(duccEvent);
		duccLogger.trace(methodName, jobid, duccMsg.fetch("exit"));
	}
	
	/**
	 * process the received self-publication (broker is alive!)
	 */
	public void update(WebServerStateDuccEvent duccEvent) {
		String methodName = "update";
		duccLogger.trace(methodName, jobid, duccMsg.fetch("enter"));
		duccLogger.debug(methodName, jobid, duccMsg.fetchLabel("received")+"WebServerStateDuccEvent (broker is alive)");
		DuccDaemonsData.getInstance().put(duccEvent);
		duccLogger.trace(methodName, jobid, duccMsg.fetch("exit"));
	}

	@Override
	public void update(SmHeartbeatDuccEvent duccEvent) {
		String methodName = "update";
		duccLogger.trace(methodName, jobid, duccMsg.fetch("enter"));
		duccLogger.debug(methodName, jobid, duccMsg.fetchLabel("received")+"SmHeartbeatDuccEvent (SM is alive)");
		DuccDaemonsData.getInstance().put(duccEvent);
		duccLogger.trace(methodName, jobid, duccMsg.fetch("exit"));
	}
}
