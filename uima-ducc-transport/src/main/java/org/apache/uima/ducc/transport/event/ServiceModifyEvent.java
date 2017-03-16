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

import org.apache.uima.ducc.common.utils.DuccProperties;

@SuppressWarnings("serial")
public class ServiceModifyEvent 
    extends ServiceRequestEvent
{
    private long friendly;      // the "friendly" part of a DuccId
    private String epname;
    // private int instances;      // 0 ==> don't modify instances
    // private Trinary autostart;
    // private boolean activate;
    // private String pingClass;

    private DuccProperties props;

	public ServiceModifyEvent(String user, long friendly, String epname, DuccProperties props, byte[] auth_block, int cli_version)
    {
		super(EventType.SERVICE_STOP, user, auth_block, cli_version);
        this.friendly = friendly;
        this.epname = epname;
        this.props = props;
        // this.instances = -1;   // default, instances aren't changed
        // this.autostart = Trinary.Unset;
        // this.activate = false;
        // this.pingClass = null;
	}

    public DuccProperties getProperties()
    {
        return props;
    }


    // public void setInstances(int instances) {
    //     this.instances = instances;
    // }

    // public int getInstances() {
    //     return instances;
    // }

    // public void setAutostart(Trinary autostart) {
    //     this.autostart = autostart;
    // }

    // public Trinary getAutostart() {
    //     return autostart;
    // }

    // public void setPingClass(String cl)
    // {
    //     this.pingClass = cl;
    // }

    // public String getPingClass()
    // {
    //     return this.pingClass;
    // }

    // public void setActivate(boolean activate)
    // {
    //     this.activate = activate;
    // }

    // public boolean getActivate()
    // {
    //     return activate;
    // }

	public long getFriendly() {
		return friendly;
	}

	public void setFriendly(long friendly) {
		this.friendly = friendly;
	}

    public String getEndpoint()
    {
        return epname;
    }

	@Override
	public String toString() {
		return "ServiceModifyEvent [friendly=" + friendly + ", user=" + user + "]";
	}
	

}
