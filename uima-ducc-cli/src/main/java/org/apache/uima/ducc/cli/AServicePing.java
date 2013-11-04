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
package org.apache.uima.ducc.cli;

import org.apache.uima.ducc.common.IServiceStatistics;

/**
 * Abstraction for service pinger.
 */

public abstract class AServicePing
{

    int implementors = 0;
    int references = 0;
    int runErrors = 0;

    /**
     * Called by the ping driver, to pass in useful things the pinger may want.
     * @param arguments This is passed in from the service specification's
     *                  service_ping_arguments string.
     *
     * @param endpoint This is the name of the service endpoint, as passed in
     *                 at service registration.
     */
    public abstract void init(String arguments, String endpoint)  throws Exception;

    /**
     * Stop is called by the ping wrapper when it is being killed.  Implementors may optionally
     * override this method with conenction shutdown code.
     */
    public abstract void stop();

    /**
     * Returns the object with application-derived health and statistics.
     * @return This object contains the informaton the service manager and web server require
     *     for correct management and display of the service.
     */
    public abstract IServiceStatistics getStatistics();
    
    public int countAdditions()
    {
        return 0;
    }

    public int countDeletions()
    {
        return 0;
    }

    public int countImplementors()
    {
    	return implementors;
    }

    public int countReferences()
    {
    	return references;
    }

    public void setImplementors(int imp)
    {
        this.implementors = imp;
    }

    public void setReferences(int ref)
    {
        this.references = ref;
    }

    public void setRunErrors(int e)
    {
        this.runErrors = e;
    }

    public boolean isExcessiveErrors()
    {
        return false;
    }

}
