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
    private Map<String, ServiceState> individualState = null;

	public ServiceDependency() 
	{
        this.messages    = new HashMap<String, String>();
        this.individualState = new HashMap<String, ServiceState>();
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
        individualState.put(endpoint, state);
    }

    /**
     * Build up message string for service.
     */
    public void addMessage(String endpoint, String message)
    {
        if ( message == null || message.equals("") ) return;

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
        HashMap<String, String> ret = new HashMap<String, String>();
        for ( String ep : individualState.keySet() ) {
            String msg = messages.get(ep);
            String dec = individualState.get(ep).decode();
            if ( msg == null ) {
                ret.put(ep, dec);
            } else {
                msg = dec + "; " + msg;
                ret.put(ep, msg);
            }
        }
            
        return ret;
    }

}

