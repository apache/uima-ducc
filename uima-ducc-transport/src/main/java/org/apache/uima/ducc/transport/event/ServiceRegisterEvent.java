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

import org.apache.uima.ducc.common.utils.DuccProperties;
import org.apache.uima.ducc.transport.event.sm.IService.Trinary;

@SuppressWarnings("serial")
public class ServiceRegisterEvent 
    extends AServiceRequest
{
    
    private int ninstances;
    private Trinary autostart;
    private String endpoint;
    private DuccProperties descriptor;

	public ServiceRegisterEvent(String user, int ninstances, Trinary autostart, String endpoint, DuccProperties descriptor, byte[] auth_block)
    {
		super(EventType.SERVICE_REGISTER, user, auth_block);
        this.ninstances = ninstances;
        this.autostart = autostart;
        this.endpoint = endpoint;
        this.descriptor = descriptor;
	}

	public int getNinstances() {
		return ninstances;
	}

    public Trinary getAutostart() {
        return autostart;
    }

	public String getEndpoint() {
		return endpoint;
	}


	public DuccProperties getDescriptor() {
		return descriptor;
	}


	@Override
	public String toString() {
		return "ServiceRegisterEvent [ninstances=" + ninstances + ", autostart=" + autostart + ", endpoint="
				+ endpoint + ", user=" + user + "]";
	}

}
