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
package org.apache.uima.ducc.sm;

import org.apache.uima.ducc.cli.IUiOptions.UiOption;
import org.apache.uima.ducc.transport.event.AServiceRequest;
import org.apache.uima.ducc.transport.event.ServiceModifyEvent;
import org.apache.uima.ducc.transport.event.ServiceStartEvent;
import org.apache.uima.ducc.transport.event.ServiceStopEvent;
import org.apache.uima.ducc.transport.event.ServiceUnregisterEvent;
import org.apache.uima.ducc.transport.event.sm.IService.Trinary;


/**
 * This class runs API commands in a thread, allowing the API to return quickly while the
 * work proceeds in the background.
 *
 * It's just a threaded front-end to the API implementations in ServiceHandler.
 */
class ApiHandler
    implements SmConstants,
               Runnable
{
	/**
	 * 
	 */
	UiOption cmd;
    ServiceHandler serviceHandler;

    long friendly;
    String endpoint;
    int instances;
    Trinary autostart;

    AServiceRequest event;

    ApiHandler(ServiceUnregisterEvent event, ServiceHandler serviceHandler)
    {
        this.cmd = UiOption.Unregister;
        this.event = event;
        this.serviceHandler = serviceHandler;
    }

    ApiHandler(ServiceStartEvent event, ServiceHandler serviceHandler)
    {
        this.cmd = UiOption.Start;
        this.event = event;
        this.serviceHandler = serviceHandler;
    }

    ApiHandler(ServiceStopEvent event, ServiceHandler serviceHandler)
    {
        this.cmd = UiOption.Stop;
        this.event = event;
        this.serviceHandler = serviceHandler;
    }

    ApiHandler(ServiceModifyEvent event, ServiceHandler serviceHandler)
    {
        this.cmd = UiOption.Modify;
        this.event = event;
        this.serviceHandler = serviceHandler;
    }

    public void run()
    {
        switch ( cmd ) {
           case Start: 
               serviceHandler.doStart((ServiceStartEvent) event);
               break;

           case Stop:
               serviceHandler.doStop((ServiceStopEvent) event);
               break;

           case Unregister:
               serviceHandler.doUnregister((ServiceUnregisterEvent) event);
               break;

           case Modify:
               serviceHandler.doModify((ServiceModifyEvent) event);
               break;
        }
    }
}
