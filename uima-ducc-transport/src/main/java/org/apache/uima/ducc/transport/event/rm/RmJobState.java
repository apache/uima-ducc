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
package org.apache.uima.ducc.transport.event.rm;

import java.util.Map;

import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.common.IDuccTypes.DuccType;


public class RmJobState implements IRmJobState {

	private static final long serialVersionUID = 1L;
	private DuccId duccId;                              // this job's DuccId as assigned by OR
    private DuccType ducc_type;                         // for messages :(

    // for all maps:
    //     key:  DuccId of a share, assigned by RM
    //   value:  A share as allocated by RM
    private Map<DuccId, IResource> resources;           // currently allocated
    private Map<DuccId, IResource> pendingRemovals;     // pending removals,  which have not yet been confirmed by OR
    private Map<DuccId, IResource> pendingAdditions;    // pending additions, which have not yet been confirmed by OR

    boolean refused = false;                            // is the job refussed by scheduler?
    String  reason = "<none>";                          // if so, here's why

    // disallow this constructor
    @SuppressWarnings("unused")
	private RmJobState()
    {
    }

    public RmJobState(DuccId duccId, 
                      Map<DuccId, IResource> resources, 
                      Map<DuccId, IResource> removals, 
                      Map<DuccId, IResource> additions)
    {
        this.duccId = duccId;
        this.resources = resources;
        this.pendingRemovals = removals;
        this.pendingAdditions = additions;
    }

    public RmJobState(DuccId duccId, String refusalReason)
    {
        this.duccId = duccId;
        this.refused = true;
        this.reason = refusalReason;
    }

    
    public DuccId getId() 
    {
        return duccId;
    }


    
    public Map<DuccId, IResource> getResources() 
    {
        return resources;
    }

    
    public Map<DuccId, IResource> getPendingRemovals() 
    {
        return pendingRemovals;
    }

    
    public Map<DuccId, IResource> getPendingAdditions() 
    {
        return pendingAdditions;
    }

    public boolean isRefused()
    {
        return refused;
    }

    public String getReason()
    {
        return reason;
    }

    public DuccType getDuccType()
    {
        return ducc_type;
    }

    public void setDuccType(DuccType dt)
    {
        this.ducc_type = dt;
    }
}
