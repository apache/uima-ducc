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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.uima.ducc.common.utils.id.DuccId;


@SuppressWarnings("serial")

// Note this is now a ConcurrentHahMap - most operations do not need to be synchronized.
// Any method added here that loops should synchronize itself though, to insure
// consistency over the operation.
public class ServiceMap 
    extends ConcurrentHashMap<DuccId,ServiceDependency> 
    implements Serializable 
{    
	public void addService(DuccId duccId, ServiceDependency services)
    {
        super.put(duccId,services);
    }
    
    
	public void removeService(DuccId duccId) 
    {
        super.remove(duccId);
	}

    public synchronized void removeAll(Set<DuccId> ids)
    {
        for ( Object o : ids ) {
            super.remove(o);
        }
    }

    public synchronized String toPrint()
    {
        StringBuffer sb = new StringBuffer("Service Map\n");
        if ( size() == 0 ) {
            sb.append("[empty]\n");
        } else {
            for ( DuccId id : keySet() ) {
                ServiceDependency svc = get(id);
                sb.append("Job ");
                sb.append(id.toString());
                sb.append(" Service state ");
                sb.append(svc.getState().toString());
                
                Map<String, String> msgs = svc.getMessages();
                if (msgs != null ) {
                	for ( String s : msgs.keySet() ) {
                		sb.append(" [");
                		sb.append(s);
                		sb.append(" : ");
                		sb.append(msgs.get(s));
                		sb.append("] ");
                	}
                }
                sb.append("\n");
            }
        }
        return sb.toString();
    }
}
