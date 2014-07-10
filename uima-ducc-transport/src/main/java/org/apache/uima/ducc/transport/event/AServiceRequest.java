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

/**
 * Common service constants.
 */
@SuppressWarnings("serial")
public abstract class AServiceRequest
    extends AbstractDuccEvent
    //implements IService
{

    String user;
    byte[] auth_block;
    boolean as_administrator;

    ServiceReplyEvent reply;
    private int cli_version;
    
    public AServiceRequest(EventType eventType, String user, byte[] auth_block, int cli_version)
    {
        super(eventType);
        this.user = user;
        this.as_administrator = false;
        this.auth_block = auth_block;
        this.cli_version = cli_version;
    }

    public ServiceReplyEvent getReply()
    {
        return reply;
    }

    public void setAdministrative(boolean val) 
    {
        this.as_administrator = val;
    }

    public void setReply(ServiceReplyEvent reply)
    {
        this.reply = reply;
    }

    public String getUser()
    {
        return user;
    }

    public boolean asAdministrator()
    {
        return as_administrator;
    }

    public byte[] getAuth()
    {
        return auth_block;
    }

    public int getCliVersion()
    {
        return cli_version;
    }
    
    @Override
    public String toString() {
        return "AServiceRequest [reply=" + reply + "]";
    }
};
