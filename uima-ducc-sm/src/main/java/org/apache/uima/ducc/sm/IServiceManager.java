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

import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.AServiceRequest;
import org.apache.uima.ducc.transport.event.ServiceModifyEvent;
import org.apache.uima.ducc.transport.event.ServiceQueryEvent;
import org.apache.uima.ducc.transport.event.ServiceRegisterEvent;
import org.apache.uima.ducc.transport.event.ServiceStartEvent;
import org.apache.uima.ducc.transport.event.ServiceStopEvent;
import org.apache.uima.ducc.transport.event.ServiceUnregisterEvent;
import org.apache.uima.ducc.transport.event.common.DuccWorkMap;
import org.apache.uima.ducc.transport.event.sm.ServiceMap;

/**
 * 
 */
public interface IServiceManager 
{
    // Receive the new map and kick the thread to process it
	public void orchestratorStateArrives(DuccWorkMap workMap);

    // Deal with the incoming orchestrator map
	public void processIncoming(DuccWorkMap workMap);

    public void register(ServiceRegisterEvent ev);

    public void unregister(ServiceUnregisterEvent ev);

    public void start(ServiceStartEvent ev);

    public void stop(ServiceStopEvent ev);

    public void query(ServiceQueryEvent ev);

    public void modify(ServiceModifyEvent ev);

	//public SmStateDuccEvent getState();

    public void publish(ServiceMap map);

    public DuccId newId() throws Exception;

    public boolean isAdministrator(AServiceRequest user);
}
