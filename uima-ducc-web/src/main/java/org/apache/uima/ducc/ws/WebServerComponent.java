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

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.camel.CamelContext;
import org.apache.uima.ducc.common.boot.DuccDaemonRuntimeProperties;
import org.apache.uima.ducc.common.boot.DuccDaemonRuntimeProperties.DaemonName;
import org.apache.uima.ducc.common.component.AbstractDuccComponent;
import org.apache.uima.ducc.common.config.CommonConfiguration;
import org.apache.uima.ducc.common.internationalization.Messages;
import org.apache.uima.ducc.common.main.DuccService;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.NodeMetricsUpdateDuccEvent;
import org.apache.uima.ducc.transport.event.OrchestratorStateDuccEvent;
import org.apache.uima.ducc.transport.event.PmStateDuccEvent;
import org.apache.uima.ducc.transport.event.RmStateDuccEvent;
import org.apache.uima.ducc.transport.event.SmStateDuccEvent;
import org.apache.uima.ducc.transport.event.common.DuccWorkMap;
import org.apache.uima.ducc.ws.registry.ServicesRegistry;
import org.apache.uima.ducc.ws.server.DuccListeners;
import org.apache.uima.ducc.ws.server.DuccWebServer;
import org.apache.uima.ducc.ws.utils.DatedNodeMetricsUpdateDuccEvent;


public class WebServerComponent extends AbstractDuccComponent 
implements IWebServer {
	
	private static DuccLogger duccLogger = DuccLoggerComponents.getWsLogger(WebServerComponent.class.getName());
	
	private static DuccId jobid = null;
	
	private DuccWebServer duccWebServer = null;
	private Messages duccMsg= Messages.getInstance();
	
	private static AtomicInteger jobCount = new AtomicInteger(0);
	private static AtomicInteger serviceCount = new AtomicInteger(0);
	private static AtomicInteger reservationCount = new AtomicInteger(0);
	
	public WebServerComponent(CamelContext context, CommonConfiguration common) {
		super("WebServer",context);
		String methodName = "WebServerComponent";
		duccLogger.info(methodName, jobid, "##### boot #####");
		String[] propertyNames = { "ducc.broker.url" };
		for(String property : propertyNames) {
			duccLogger.info(methodName, jobid, property+"="+System.getProperty(property));
		}
		duccLogger.info(methodName, jobid, System.getProperty("ducc.broker.url"));
		duccWebServer = new DuccWebServer(common);
		init();
	}
	
	public void start(DuccService service, String[] args) throws Exception {
		super.start(service, args);
		 DuccDaemonRuntimeProperties.getInstance().boot(DaemonName.Webserver,getProcessJmxUrl());
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
		webServerStart();
		duccLogger.trace(methodName, jobid, duccMsg.fetch("exit"));
	}
	
	
	public void update(OrchestratorStateDuccEvent duccEvent) {
		String methodName = "update";
		duccLogger.trace(methodName, jobid, duccMsg.fetch("enter"));
		duccLogger.debug(methodName, jobid, duccMsg.fetchLabel("received")+"OrchestratorStateDuccEvent");
		DuccDaemonsData.getInstance().put(duccEvent);
		DuccWorkMap wm = duccEvent.getWorkMap();
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
		DuccListeners.getInstance().update(duccEvent);
		CacheManager.getInstance().update(wm);
		duccLogger.trace(methodName, jobid, duccMsg.fetch("exit"));
	}

	
	public void update(NodeMetricsUpdateDuccEvent duccEvent) {
		String methodName = "update";
		duccLogger.trace(methodName, jobid, duccMsg.fetch("enter"));
		duccLogger.trace(methodName, jobid, duccMsg.fetchLabel("received")+"NodeMetricsUpdateDuccEvent");
		DuccMachinesData.getInstance().put(new DatedNodeMetricsUpdateDuccEvent(duccEvent));
		duccLogger.trace(methodName, jobid, duccMsg.fetch("exit"));
	}

	
	public void update(RmStateDuccEvent duccEvent) {
		String methodName = "update";
		duccLogger.trace(methodName, jobid, duccMsg.fetch("enter"));
		duccLogger.debug(methodName, jobid, duccMsg.fetchLabel("received")+"RmStateDuccEvent");
		DuccDaemonsData.getInstance().put(duccEvent);
		duccLogger.trace(methodName, jobid, duccMsg.fetch("exit"));
	}
	
	
	public void update(SmStateDuccEvent duccEvent) {
		String methodName = "update";
		duccLogger.trace(methodName, jobid, duccMsg.fetch("enter"));
		duccLogger.debug(methodName, jobid, duccMsg.fetchLabel("received")+"SmStateDuccEvent");
		DuccDaemonsData.getInstance().put(duccEvent);
		ServicesRegistry.getInstance().update();
		duccLogger.trace(methodName, jobid, duccMsg.fetch("exit"));
	}
	
	
	public void update(PmStateDuccEvent duccEvent) {
		String methodName = "update";
		duccLogger.trace(methodName, jobid, duccMsg.fetch("enter"));
		duccLogger.debug(methodName, jobid, duccMsg.fetchLabel("received")+"PmStateDuccEvent");
		DuccDaemonsData.getInstance().put(duccEvent);
		duccLogger.trace(methodName, jobid, duccMsg.fetch("exit"));
	}
}
