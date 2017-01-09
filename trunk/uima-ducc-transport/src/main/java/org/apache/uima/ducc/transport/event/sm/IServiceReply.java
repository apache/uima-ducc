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

import java.util.List;

/**
 * This interface defines the reply structure returned by the various invocations of
 * the DuccServiceApi methods.
 */
public interface IServiceReply
{
    /**
     * This method indicates whether the associated services verb was successful.
     * @return true if the action was successful; false otherwise.
     */
    public boolean getReturnCode();

    /**
     * If there was an informational or error message associated with the action, this
     * method is used to return it.
     *
     * @return Return the message as a String.  If the action was successful as indicated
     *         by getReturnCode() this message may be null.
     */
    public String getMessage();

    /**
     * This returns the service endpoint that was operated upon.  API uses may use this to
     * verify the correct endpoint for their action. For some actions, if getReturnCode()
     * is null this method may return null.
     *
     * @return A string, containing the service endpoint, or NULL. 
     */
    public String getEndpoint();

    /**
     * Return the uniqud numeric ID assigned by the service manager for this service.  If
     * the action is "register" and the service manager is unable to register the service,
     * the return code will be returned false, a reason will be returned in getMessage(), and
     * the id will be returned as -1.
     *
     * @return A number, unique to this service.
     */
    public long getId();

    /**
     * This returns null for non-query events.  For query events, this returns a list
     * of the services known to the service manager, as filtered by the query parameters.
     *
     * @return A list of services known to the service manager for query events, null otherwise.
     */
    public List<IServiceDescription> getServiceDescriptions();
}
