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


@SuppressWarnings("serial")
public class ServiceQueryReplyEvent 
    extends ServiceReplyEvent
{
    // same as a ServiceReply Event plus it has a collection of info about services.

    List<IServiceDescription> services = new ArrayList<IServiceDescription>();
    public ServiceQueryReplyEvent()
    {
        super();           // UIMA-4336 Enforce beany construction
    }

    public void addService(IServiceDescription s)
    {
        this.services.add(s);
    }

    public List<IServiceDescription> getServiceDescriptions()
    {
        return services;
    }

    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        if ( services.size() == 0 ) {
            if ( this.getReturnCode() == false ) {
                return super.getMessage();
            } else {
                return "No Services";
            }
        } else {
            for ( IServiceDescription sd : services ) {
                sb.append(sd.toString());
                sb.append("\n");
            }
        }
        return sb.toString();
    }
}
