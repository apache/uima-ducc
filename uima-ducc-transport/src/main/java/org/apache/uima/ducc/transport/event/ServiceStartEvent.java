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
public class ServiceStartEvent 
    extends ServiceRequestEvent
{
    private int friendly;
    private String epname;
    private int instances;

	public ServiceStartEvent(String user, int friendly, String epname, byte[] auth_block, int cli_version)
    {
		super(EventType.SERVICE_START, user, auth_block, cli_version);
        this.friendly = friendly;
        this.epname = epname;
        this.instances = -1;         // default, don't change number of instances
	}

	public int getFriendly() {
		return friendly;
	}

    public String getEndpoint()
    {
        return epname;
    }

	public int getInstances() {
		return instances;
	}

    public void setInstances(int instances)
    {
        this.instances = instances;
    }

	public String toString() {
		return "ServiceStartEvent [friendly=" + friendly + ", user=" + user + ", instances=" + instances + "]";
	}
	
}
