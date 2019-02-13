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
package org.apache.uima.ducc.sm.event;

import org.apache.camel.Body;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.sm.IServiceManager;
import org.apache.uima.ducc.transport.dispatcher.DuccEventDispatcher;
import org.apache.uima.ducc.transport.event.OrchestratorStateDuccEvent;
import org.apache.uima.ducc.transport.event.ServiceDisableEvent;
import org.apache.uima.ducc.transport.event.ServiceEnableEvent;
import org.apache.uima.ducc.transport.event.ServiceIgnoreEvent;
import org.apache.uima.ducc.transport.event.ServiceModifyEvent;
import org.apache.uima.ducc.transport.event.ServiceObserveEvent;
import org.apache.uima.ducc.transport.event.ServiceQueryEvent;
import org.apache.uima.ducc.transport.event.ServiceRegisterEvent;
import org.apache.uima.ducc.transport.event.ServiceReplyEvent;
import org.apache.uima.ducc.transport.event.ServiceStartEvent;
import org.apache.uima.ducc.transport.event.ServiceStopEvent;
import org.apache.uima.ducc.transport.event.ServiceUnregisterEvent;
import org.apache.uima.ducc.transport.event.delegate.DuccEventDelegateListener;


/**
 * 
 *
 */
public class ServiceManagerEventListener 
    implements DuccEventDelegateListener
{
/**
	 * 
	 */
	//	private DuccEventDispatcher eventDispatcher;
//	private String targetEndpoint;
	private IServiceManager serviceManager;
	

	private static DuccLogger logger = DuccLogger.getLogger(ServiceManagerEventListener.class.getName(), "SM");	

	public ServiceManagerEventListener(IServiceManager serviceManager) 
    {
		this.serviceManager = serviceManager;
	}
    
    // TODO not used
	public void setDuccEventDispatcher( DuccEventDispatcher eventDispatcher ) 
    {
//		this.eventDispatcher = eventDispatcher;
	}

    // TODO not used
	public void setEndpoint( String endpoint ) 
    {
//		this.targetEndpoint = endpoint;
	}

    private ServiceReplyEvent failureEvent(String message)
    {
        ServiceReplyEvent ret = new ServiceReplyEvent();       // UIMA-4336 construct the response beanily
        ret.setReturnCode(false);
        ret.setMessage(message);
        ret.setEndpoint("no.endpoint");
        ret.setId(-1);
        return ret;
    }

    private ServiceReplyEvent failureEvent()
    {
        return failureEvent("Internal error, check SM logs.");
    }

    // Incoming API requests
	public void onServiceRegisterEvent(@Body ServiceRegisterEvent duccEvent) 
        throws Exception 
    {
		String methodName = "onServiceRegisterEvent";
        try {
            serviceManager.register(duccEvent);
        } catch ( IllegalStateException e) {
            duccEvent.setReply(failureEvent(e.getMessage()));
            logger.error(methodName, null, e);
        } catch ( Throwable t ) {
            duccEvent.setReply(failureEvent());
            logger.error(methodName, null, t);
        }
	}

    // Incoming API requests
	public void onServiceUnregisterEvent(@Body ServiceUnregisterEvent duccEvent) 
        throws Exception 
    {
		String methodName = "onServiceUnregisterEvent";
        try {
            serviceManager.unregister(duccEvent);
        } catch ( Throwable t ) {
            duccEvent.setReply(failureEvent());
            logger.error(methodName, null, t);
        }
	}

    // Incoming API requests
	public void onServiceStartEvent(@Body ServiceStartEvent duccEvent) throws Exception 
    {
		String methodName = "onServiceStartEvent";
        try {
            logger.info(methodName, null, "-------------- Start ----------", duccEvent.toString());
            serviceManager.start(duccEvent);
        } catch ( Throwable t ) {
            duccEvent.setReply(failureEvent());
            logger.error(methodName, null, t);
        }
	}

    // Incoming API requests
	public void onServiceStopEvent(@Body ServiceStopEvent duccEvent) throws Exception 
    {
		String methodName = "onServiceStopEvent";
        try {
            serviceManager.stop(duccEvent);
        } catch ( Throwable t ) {
            duccEvent.setReply(failureEvent());
            logger.error(methodName, null, t);
        }
	}

    // Incoming API requests
	public void onServiceEnableEvent(@Body ServiceEnableEvent duccEvent) throws Exception 
    {
		String methodName = "onServiceEnableEvent";
        try {
            serviceManager.enable(duccEvent);
        } catch ( Throwable t ) {
            duccEvent.setReply(failureEvent());
            logger.error(methodName, null, t);
        }
	}

    // Incoming API requests
	public void onServiceDisableEvent(@Body ServiceDisableEvent duccEvent) throws Exception 
    {
		String methodName = "onServiceDisableEvent";
        try {
            serviceManager.disable(duccEvent);
        } catch ( Throwable t ) {
            duccEvent.setReply(failureEvent());
            logger.error(methodName, null, t);
        }
	}

    // Incoming API requests
	public void onServiceIgnoreEvent(@Body ServiceIgnoreEvent duccEvent) throws Exception 
    {
		String methodName = "onServiceIgnoreEvent";
        try {
            serviceManager.ignore(duccEvent);
        } catch ( Throwable t ) {
            duccEvent.setReply(failureEvent());
            logger.error(methodName, null, t);
        }
	}

    // Incoming API requests
	public void onServiceObserveEvent(@Body ServiceObserveEvent duccEvent) throws Exception 
    {
		String methodName = "onServiceObserveEvent";
        try {
            serviceManager.observe(duccEvent);
        } catch ( Throwable t ) {
            duccEvent.setReply(failureEvent());
            logger.error(methodName, null, t);
        }
	}

    // Incoming API requests
	public void onServiceModifyEvent(@Body ServiceModifyEvent duccEvent) throws Exception 
    {
		String methodName = "onServiceModifyEvent";
        try {
            logger.info(methodName, null, "-------------- Modify ----------", duccEvent.toString());
            serviceManager.modify(duccEvent);
        } catch ( Throwable t ) {
            duccEvent.setReply(failureEvent());
            logger.error(methodName, null, t);
        }
	}

    // Incoming API requests
	public void onServiceQueryEvent(@Body ServiceQueryEvent duccEvent) 
        throws Exception 
    {
		String methodName = "onServiceQueryEvent";
        try {
            serviceManager.query(duccEvent);
        } catch ( Throwable t ) {
            duccEvent.setReply(failureEvent());
            logger.error(methodName, null, t);
        }
	}

    // OR state
	public void onOrchestratorStateDuccEvent(@Body OrchestratorStateDuccEvent duccEvent) 
        throws Exception 
    {
		String methodName = "onOrchestratorStateDuccEvent";
		System.out.println("......... Service Manager Received OrchestratorStateDuccEvent.");
		// serviceManager.evaluateServiceRequirements(duccEvent.getWorkMap());
        try {
            serviceManager.orchestratorStateArrives(duccEvent.getWorkMap());
        } catch ( Throwable t ) {
            logger.error(methodName, null, t);
        }

		//serviceManager.evaluateServiceRequirements(duccEvent);
//		DuccEvent de = new StartServiceDuccEvent();
//		eventDispatcher.dispatch(targetEndpoint, duccEvent);
	}

}
