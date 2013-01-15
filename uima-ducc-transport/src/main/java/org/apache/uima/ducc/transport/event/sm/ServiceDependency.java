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
package org.apache.uima.ducc.transport.event.sm;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.uima.ducc.transport.event.sm.IService.ServiceState;


@SuppressWarnings("serial")
public class ServiceDependency implements Serializable {
	
	private ServiceState state = ServiceState.Undefined;  // this is the cumulative service state for all the job's services
    private Map<String, String> messages = null;          // if anything needs more info we put strings into here
                                                          // one per service, keyed on service endpoint

	public ServiceDependency() 
	{
        this.messages = new HashMap<String, String>();
	}
	
    /*
	 * Services State
	 */
	public void setState(ServiceState state) 
    {
		this.state = state;
	}
	
	public ServiceState getState() 
    {
		return this.state;
	}

    /**
     * Set service state.  This is called at the end of resolving dependencies.
     */
    public void setIndividualState(String endpoint, ServiceState state)
    {
        messages.put(endpoint, state.decode());
    }

    /**
     * Set an error message about the individual services.  
     */
    public void addMessage(String endpoint, String message)
    {
        messages.put(endpoint, message);
    }

    /**
     * This job/service is completing.  Clear potenitally confusing messages that might be left over.
     */
    public void clearMessages()
    {
        messages.clear();
    }

    public Map<String, String> getMessages()
    {
        return messages;
    }

}
