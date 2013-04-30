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
package org.apache.uima.ducc.common;

public abstract class AServicePing
{
    /**
     * Called by the ping driver, to pass in useful things the pinger may want.
     * @param endpoint This is the name of the service endpoint, as passed in
     *                 at service registration.
     */
    public abstract void init(String endpoint)  throws Exception;

    /**
     * Stop is called by the ping wrapper when it is being killed.  Implementors may optionally
     * override this method with conenction shutdown code.
     */
    public abstract void stop();

    /**
     * Returns the object with application-derived health and statistics.
     */
    public abstract ServiceStatistics getStatistics();
    
    /**
     * Clears lingering service state.  In the case of UIMA-AS services, this will delete the
     * queue, if nobody is using it any more.  Other types of services do service-specific things.y
     */
    public void clearQueues()
    	throws Throwable
    {
        // default, do nothing
    }
}
