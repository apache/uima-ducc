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
package org.apache.uima.ducc.transport.configurator.jp;

import java.util.List;

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.Utils;
import org.apache.uima.ducc.container.jp.iface.IJobProcessManagerCallbackListener;
import org.apache.uima.ducc.transport.agent.IUimaPipelineAEComponent;
import org.apache.uima.ducc.transport.agent.ProcessStateUpdate;
import org.apache.uima.ducc.transport.configuration.jp.iface.IAgentSession;
import org.apache.uima.ducc.transport.dispatcher.DuccEventDispatcher;
import org.apache.uima.ducc.transport.event.ProcessStateUpdateDuccEvent;
import org.apache.uima.ducc.transport.event.common.IProcessState.ProcessState;


/**
 *	Responsible for delegating state changes to a remote Agent. 
 *
 */
public class AgentSession 
implements IAgentSession, IJobProcessManagerCallbackListener {
	DuccLogger logger = DuccLogger.getLogger(this.getClass(), "UIMA AS Service");

	//	Dispatcher is responsible for sending state update event to jms endpoint
	private DuccEventDispatcher dispatcher;
	//	Caches process PID
	private String pid=null;
	//	Unique ID assigned to the process. This is different from OS PID
	private String duccProcessId;
	
	private ProcessState state;
	
	private String endpoint;
	
	private Object stateLock = new Object();
	
	/**
	 * JMS based adapter C'tor
	 * 
	 * @param dispatcher - initialized instance of {@link DuccEventDispatcher}
	 * @param duccProcessId - unique ID assigned by Ducc infrastructure 
	 */
	public AgentSession(DuccEventDispatcher dispatcher, String duccProcessId, String endpoint) {
		this.dispatcher = dispatcher;
		this.duccProcessId = duccProcessId;
		this.endpoint = endpoint;
	}
	public void notify(ProcessState state) {
		notify(state, null);
	}
	public void notify(ProcessState state, String message) {
	  synchronized( stateLock ) {
	    this.state = state;
	    if ( pid == null ) {
	      // Get the PID once and cache for future reference
	      pid = Utils.getPID();
	    }
	    ProcessStateUpdate processUpdate = null;
	    if ( message == null ) {
	      processUpdate = new ProcessStateUpdate(state, pid, duccProcessId,null);
	    } else {
	      processUpdate = new ProcessStateUpdate(state, pid, duccProcessId,message, null);
	    }
	    //System.out.println("................. >>> ProcessStateUpdate==NULL?"+(processUpdate==null)+" JmxUrl="+processJmxUrl);
	    if (endpoint != null ) {
	      processUpdate.setSocketEndpoint(endpoint);
	    }
	    this.notify(processUpdate);
	  }
	}
	/**
	 * Called on UIMA AS status change. Sends a {@link ProcessStateUpdateDuccEvent} message
	 * via configured dispatcher to a configured endpoint.
	 * 
	 */
	public void notify(ProcessStateUpdate state) {
		try {
			ProcessStateUpdateDuccEvent duccEvent = 
				new ProcessStateUpdateDuccEvent(state);
      logger.info("notifyAgentWithStatus",null," >>>>>>> UIMA AS Service Deployed - PID:"+pid);

      if (endpoint != null ) {
        state.setSocketEndpoint(endpoint);
      }
			//	send the process update to the remote
			dispatcher.dispatch(duccEvent, System.getenv("IP"));
			String jmx = state.getProcessJmxUrl() == null ? "N/A" : state.getProcessJmxUrl();
			logger.info("notifyAgentWithStatus",null,"... UIMA AS Service Deployed - PID:"+pid+". Service State: "+state+". JMX Url:"+jmx+" Dispatched State Update Event to Agent with IP:"+System.getenv("IP"));
		} catch( Exception e) {
			e.printStackTrace();
		}
	}
	public void notify(List<IUimaPipelineAEComponent> pipeline) {
	   synchronized( stateLock ) {
	     //  Only send update if the AE is initializing
	     if ( state.equals(ProcessState.Initializing)) {
	       try {
	         ProcessStateUpdate processUpdate = 
	           new ProcessStateUpdate(state, pid, duccProcessId, null, pipeline);
	         notify(processUpdate);
	       } catch( Exception e) {
	         e.printStackTrace();
	       }
	     }
	   }
	}
	public void stop() throws Exception {
		dispatcher.stop();
	}
}
