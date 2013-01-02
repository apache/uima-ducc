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

/**
 * OBSOLETE
 * This represents the thing the user uses to specify service dependencies.
 */
class ServiceSpecifier 
    implements SmConstants
{
    String key;
    String broker;
    String endpoint;
	
    public ServiceSpecifier(String key)
    {
        this.key = key;
        if ( key.startsWith(ServiceType.UimaAs.decode()) ) {
            int ndx = key.indexOf(":");
            key = key.substring(ndx);
            ndx = key.indexOf(":");
            endpoint = key.substring(0, ndx);
            broker = key.substring(ndx);
        }
    }

    public String getBroker() 
    {
        return broker;
    }

    public String getEndpoint() 
    {
        return endpoint;
    }
    
    public String key()
    {
        return key;
    }
    
}

