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

public abstract class DuccAdminEvent implements Serializable 
{
	private static final long serialVersionUID = 1753129558912646806L;

    String user;
    byte[] auth_block;

    @SuppressWarnings("unused")
	private DuccAdminEvent()  // prevent use of this, only the form with user and auth_block is allowed
    {
    }

    protected DuccAdminEvent(String user, byte[] auth_block)
    {
        this.user = user;
        this.auth_block = auth_block;
    }

    public String getUser()
    {
        return user;
    }

    public byte[] getAuthBlock()
    {
        return auth_block;
    }

	public String toString() 
    {
		return this.getClass().getName();
	}
}
