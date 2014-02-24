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
package org.apache.uima.ducc.agent.deploy.uima;

import org.apache.camel.Body;
import org.apache.uima.ducc.agent.deploy.ManagedService;
import org.apache.uima.ducc.transport.dispatcher.DuccEventDispatcher;
import org.apache.uima.ducc.transport.event.ProcessStopDuccEvent;
import org.apache.uima.ducc.transport.event.delegate.DuccEventDelegateListener;


public class ProcessEventListener implements DuccEventDelegateListener{

	//private DuccEventDispatcher eventDispatcher;
	private ManagedService service;
	
	public ProcessEventListener(ManagedService service) {
		this.service = service;
	}
	public void setDuccEventDispatcher(DuccEventDispatcher eventDispatcher) {
		//this.eventDispatcher = eventDispatcher;
	}
	public void onProcessStop(@Body ProcessStopDuccEvent event) {
		service.stopService();
	}

}
