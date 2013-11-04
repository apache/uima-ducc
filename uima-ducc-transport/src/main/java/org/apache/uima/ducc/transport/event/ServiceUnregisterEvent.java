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

@SuppressWarnings("serial")
public class ServiceUnregisterEvent 
    extends AServiceRequest
{

    private long friendly;
    private String epname;

	public ServiceUnregisterEvent(String user, long friendly, String epname, byte[] auth_block)
    {
		super(EventType.SERVICE_UNREGISTER, user, auth_block);
        this.friendly = friendly;
        this.epname = epname;
	}

	public long getFriendly() 
    {
		return friendly;
	}

	public void setFriendly(long f)
	{
		this.friendly = f;
	}
	
    public String getEndpoint()
    {
        return epname;
    }

    public void setEndpoint(String ep)
    {
    	this.epname = ep;
    }
    
	@Override
	public String toString() {
		return "ServiceUnregisterEvent [friendly=" + friendly + ", user="
				+ user + "]";
	}

}
