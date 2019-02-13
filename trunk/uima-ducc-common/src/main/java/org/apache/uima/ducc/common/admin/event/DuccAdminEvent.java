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
 * This class provides the abstraction for requests to DUCC's administrative interface.
 */
public abstract class DuccAdminEvent implements Serializable 
{
	private static final long serialVersionUID = 1L;

    private String user;
    private byte[] auth_block;

    @SuppressWarnings("unused")
	private DuccAdminEvent()  // prevent use of this, only the form with user and auth_block is allowed
    {
    }

    /**
     * Create a request to send to DUCC.
     *
     * @param user This is the id of the user making the request.
     *
     * @param auth_block This is the authentication block as returned by {@link org.apache.uima.ducc.common.crypto.Crypto Crypto}.  The receiver
     *        will validate the messsage and if the <code>auth_block</code> does not match what receiver expects,
     *        the request is denied.  Nota that this is NOT intended to provide foolproof security.  It's only
     *        intention is to provde basic authentication of the user (is the user who he claims to be?).
     */
    protected DuccAdminEvent(String user, byte[] auth_block)
    {
        this.user = user;
        this.auth_block = auth_block;
    }

    /**
     * 
     * @return -  the id of the user making the request. 
     */
    public String getUser()
    {
        return user;
    }

    /**
     * @return the authentication block for the request.
     */
    public byte[] getAuthBlock()
    {
        return auth_block;
    }

	public String toString() 
    {
		return this.getClass().getName();
	}
}
