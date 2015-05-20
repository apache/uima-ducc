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
package org.apache.uima.ducc.rm.event;

import org.apache.camel.Body;
import org.apache.uima.ducc.common.ANodeStability;
import org.apache.uima.ducc.rm.ResourceManager;
import org.apache.uima.ducc.rm.scheduler.SchedConstants;
import org.apache.uima.ducc.transport.dispatcher.DuccEventDispatcher;
import org.apache.uima.ducc.transport.event.NodeInventoryUpdateDuccEvent;
import org.apache.uima.ducc.transport.event.NodeMetricsUpdateDuccEvent;
import org.apache.uima.ducc.transport.event.OrchestratorStateDuccEvent;
import org.apache.uima.ducc.transport.event.delegate.DuccEventDelegateListener;


public class ResourceManagerEventListener 
    implements DuccEventDelegateListener,
               SchedConstants
{
    //private static DuccLogger logger = DuccLogger.getLogger(ResourceManagerEventListener.class, COMPONENT_NAME);

	private String targetEndpoint;
    private ResourceManager rm;
    private DuccEventDispatcher eventDispatcher;
    private ANodeStability nodeStability;

    public DuccEventDispatcher getEventDispatcher() {
		return eventDispatcher;
	}

	public void setEventDispatcher(DuccEventDispatcher eventDispatcher) {
		this.eventDispatcher = eventDispatcher;
	}

    public void setNodeStability(ANodeStability ns)
    {
        this.nodeStability = ns;
    }

	public String getTargetEndpoint() {
		return targetEndpoint;
	}

	public void setTargetEndpoint(String targetEndpoint) {
		this.targetEndpoint = targetEndpoint;
	}
        
    public ResourceManagerEventListener(ResourceManager rm) 
    {
        this.rm = rm;
    }

    public void setDuccEventDispatcher( DuccEventDispatcher eventDispatcher ) 
    {
        this.eventDispatcher = eventDispatcher;
    }

    public void setEndpoint( String endpoint ) 
    {
        this.targetEndpoint = endpoint;
    }

    /**
     * Receives {@code NodeMetricsUpdateDuccEvent} events from transport. 
     * 
     * @param duccEvent
     * @throws Exception
     */
    public void onNodeMetricsEvent(@Body NodeMetricsUpdateDuccEvent duccEvent) throws Exception 
    {
        //rm.nodeArrives(duccEvent.getNode());
        nodeStability.nodeArrives(duccEvent.getNode());
    }

    public void onNodeInventoryUpdateEvent(@Body NodeInventoryUpdateDuccEvent duccEvent) throws Exception {
    }

    /**
     * Receives {@code OrchestratorDuccEvent} events from transport.
     * 
     * @param duccEvent
     * @throws Exception
     */
    public void onOrchestratorStateUpdateEvent(@Body OrchestratorStateDuccEvent duccEvent) throws Exception 
    {
    	//String methodName = "onOrchestratorStateUpdateEvent";
        //logger.info(methodName, null, "Event arrives");
        rm.onOrchestratorStateUpdate(duccEvent.getWorkMap());
    }

}
