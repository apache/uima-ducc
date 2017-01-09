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
package org.apache.uima.ducc.common.admin.event;

/**
 * Construct and send this event to vary off a set of hosts.
 */
public class RmAdminVaryOff
    extends DuccAdminEvent 
{
	private static final long serialVersionUID = 1L;
    private String[] nodes;
    
    /**
     * This build a request to vary off a set of hosts.
     *
     * @param nodes This is a string array containing the names of the hosts to vary off.
     * @param user  This is the name of the user making the request, as required by the superclass.
     * @param auth  This is the authentication block for the request, as required by the superclass.
     */
    public RmAdminVaryOff(String[] nodes, String user, byte[] auth)
    {
        super(user, auth);
        this.nodes = nodes;
    }

    public String[] getNodes() { return nodes;}
}
