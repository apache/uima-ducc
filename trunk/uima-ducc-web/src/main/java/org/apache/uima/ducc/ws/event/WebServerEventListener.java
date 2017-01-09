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
package org.apache.uima.ducc.ws.event;

import org.apache.camel.Body;
import org.apache.camel.Header;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.dispatcher.DuccEventDispatcher;
import org.apache.uima.ducc.transport.event.DuccJobsStateEvent;
import org.apache.uima.ducc.transport.event.NodeMetricsUpdateDuccEvent;
import org.apache.uima.ducc.transport.event.OrchestratorStateDuccEvent;
import org.apache.uima.ducc.transport.event.PmStateDuccEvent;
import org.apache.uima.ducc.transport.event.RmStateDuccEvent;
import org.apache.uima.ducc.transport.event.SmStateDuccEvent;
import org.apache.uima.ducc.transport.event.delegate.DuccEventDelegateListener;
import org.apache.uima.ducc.ws.IWebServer;
import org.apache.uima.ducc.ws.self.message.WebServerStateDuccEvent;

public class WebServerEventListener implements DuccEventDelegateListener {
	
	private DuccLogger duccLogger = DuccLoggerComponents.getWsLogger(WebServerEventListener.class.getName());
	private DuccId jobid = null;
	
	private IWebServer webServer;
	
	public WebServerEventListener(IWebServer webServer) {
		this.webServer = webServer;
	}
	
	public void setDuccEventDispatcher( DuccEventDispatcher eventDispatcher ) {
	}
	
	public void setEndpoint( String endpoint ) {
	}
	
	public void onOrchestratorStateDuccEvent(@Body OrchestratorStateDuccEvent duccEvent, @Header("pubSize")Long pubSize) {
		String location = "onOrchestratorStateDuccEvent";
		try {
			duccEvent.setEventSize(pubSize);
			webServer.update(duccEvent);
		}
		catch(Throwable t) {
			duccLogger.error(location, jobid, t);
		}
	}
	
	public void onNodeMetricsUpdateDuccEvent(@Body NodeMetricsUpdateDuccEvent duccEvent, @Header("pubSize")Long pubSize) throws Exception {
		String location = "onNodeMetricsUpdateDuccEvent";
		try {
			duccEvent.setEventSize(pubSize);
			webServer.update(duccEvent);
		}
		catch(Throwable t) {
			duccLogger.error(location, jobid, t);
		}
	}
	
	public void onRmStateDuccEvent(@Body RmStateDuccEvent duccEvent, @Header("pubSize")Long pubSize) {
		String location = "onRmStateDuccEvent";
		try {
			duccEvent.setEventSize(pubSize);
			webServer.update(duccEvent);
		}
		catch(Throwable t) {
			duccLogger.error(location, jobid, t);
		}
	}
	
	public void onSmStateDuccEvent(@Body SmStateDuccEvent duccEvent, @Header("pubSize")Long pubSize) {
		String location = "onSmStateDuccEvent";
		try {
			duccEvent.setEventSize(pubSize);
			webServer.update(duccEvent);
		}
		catch(Throwable t) {
			duccLogger.error(location, jobid, t);
		}
	}
	
	/**
	 * This is what PM publishes to Agents to be acted upon
	 */
	public void onDuccJobsStateEvent(@Body DuccJobsStateEvent duccEvent, @Header("pubSize")Long pubSize) throws Exception {
		String location = "onDuccJobsStateEvent";
		try {
			duccEvent.setEventSize(pubSize);
			webServer.update(duccEvent);
		}
		catch(Throwable t) {
			duccLogger.error(location, jobid, t);
		}
	}
	
	/**
	 * This is what PM publishes to WS as aliveness indicator
	 */
	public void onPmStateDuccEvent(@Body PmStateDuccEvent duccEvent, @Header("pubSize")Long pubSize) {
		String location = "onPmStateDuccEvent";
		try {
			duccEvent.setEventSize(pubSize);
			webServer.update(duccEvent);
		}
		catch(Throwable t) {
			duccLogger.error(location, jobid, t);
		}
	}
	
	/**
	 * Receipt of self-publication indicates that broker is alive!
	 */
	public void onWsStateDuccEvent(@Body WebServerStateDuccEvent duccEvent, @Header("pubSize")Long pubSize) {
		String location = "onWsStateDuccEvent";
		try {
			duccEvent.setEventSize(pubSize);
			webServer.update(duccEvent);
		}
		catch(Throwable t) {
			duccLogger.error(location, jobid, t);
		}
	}
}
