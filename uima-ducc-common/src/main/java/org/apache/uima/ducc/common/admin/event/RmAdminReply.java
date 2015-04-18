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

import java.io.Serializable;

/**
 * This is a generic response packet to the RM Administrative interface.
 */
public class RmAdminReply
	implements Serializable
{
	private static final long serialVersionUID = 1;

    private boolean ready   = true;     // if false, RM is not initialized
    private boolean rc      = true;     // if true, the action worked, otherwise not
    private String  message = "";       // String response from RM for console messages.
    
    public RmAdminReply()
    {
    }

    /* RM only, other use produces incorrect results. */
    public void    notReady()                     { this.ready = false; }

    /**
     * @return True if RM is able to schedule and be queried, false otherwise. If the RM is not yet
     * ready to schedule, e.g. immediately after boot or reconfigure, this method will return false.
     */
    public boolean isReady()                      { return ready; }

    /*
     * RM use only
     */
    public void setRc(boolean rc)                 { this.rc = rc; }

    /**
     * Returns success or failure status.
     * @return true if the action worked, false otherwise.
     */
    public boolean getRc()
    {
        return rc;
    }

    /*
     * RM use only.
     */
    public void setMessage(String m)
    {
        this.message = m;
    }

    /**
     * @return a string with the RM's response message.
     */
    public String getMessage()
    {
    	return message;
    }
}
