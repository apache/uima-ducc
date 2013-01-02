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

import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.sm.IService;


@SuppressWarnings("serial")
public class ServiceReplyEvent 
    extends AbstractDuccEvent
    implements IService
{
    private ServiceCode return_code;
    private String message = "N/A";
    private String endpoint;
    private DuccId id;
    
	public ServiceReplyEvent(ServiceCode rc, String message, String endpoint, DuccId id)
    {
		super(EventType.SERVICE_REPLY);
        this.return_code = rc;
        this.message = message;
        this.endpoint = endpoint;
        this.id = id;
	}

	public ServiceCode getReturnCode() {
		return return_code;
	}

	public void setReturn_code(ServiceCode return_code) {
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

	public DuccId getId() {
		return id;
	}

	public void setId(DuccId id) {
		this.id = id;
	}

	@Override
	public String toString() {
		return "ServiceReplyEvent [return_code=" + return_code + ", message="
				+ message + ", endpoint=" + endpoint + ", id=" + id + "]";
	}
	
  }
