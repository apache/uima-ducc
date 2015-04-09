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
package org.apache.uima.ducc.transport.event;

import java.util.ArrayList;
import java.util.List;

import org.apache.uima.ducc.transport.event.sm.IServiceDescription;
import org.apache.uima.ducc.transport.event.sm.IServiceReply;


@SuppressWarnings("serial")
public class ServiceReplyEvent 
    extends AbstractDuccEvent
    implements IServiceReply
{
    /**
	 * 
	 */
	//private static final long serialVersionUID = -7049634721774766694L;
	private boolean return_code;
    private String message = "N/A";
    private String endpoint = "N/A";
    private long id = -1l;

	//List<IServiceDescription> services = new ArrayList<IServiceDescription>();

    public ServiceReplyEvent()
    {
        super(EventType.SERVICE_REPLY);
    }

	// public ServiceReplyEvent(boolean rc, String message, String endpoint, long id)
    // {
	// 	super(EventType.SERVICE_REPLY);
    //     this.return_code = rc;
    //     this.message = message;
    //     this.endpoint = endpoint;
    //     this.id = id;
	// }
    
	public boolean getReturnCode() {
		return return_code;
	}

	public void setReturnCode(boolean return_code) {
		this.return_code = return_code;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public String getEndpoint() {
		return endpoint;
	}

	public void setEndpoint(String endpoint) {
		this.endpoint = endpoint;
	}

	public long getId() 
    {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

    public List<IServiceDescription> getServiceDescriptions()
    {
        return null;
    }

    //public void addService(IServiceDescription s)
    //{
    //    this.services.add(s);
    //}

	@Override
	public String toString() {
		return "ServiceReplyEvent [return_code=" + (return_code ? "OK" : "NOTOK") + ", message="
				+ message + ", endpoint=" + endpoint + ", id=" + id + "]";
	}
	
  }
